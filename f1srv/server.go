// Package f1srv is a clean-room RESP server built straight on engine/f1raw, the
// lock-free hash-over-log point store. It is the from-first-principles string path:
// because the store is lock-free and safe for any number of concurrent readers and
// writers, a connection goroutine calls it directly. There is no keyspace mutex, no
// per-shard worker, no write-behind queue, and no value cache between the socket and
// the store. A command is parsed inline out of the read buffer, dispatched, and its
// reply appended to a write buffer that flushes once per batch, so a pipeline of N
// commands costs one read and one write.
//
// The surface is the string point path the in-memory benchmark exercises: PING, SET,
// GET, DEL, EXISTS, INCR/INCRBY/DECR/DECRBY, MSET, MGET, plus the admin verbs a bench
// harness needs to set up and tear down (FLUSHALL/FLUSHDB, DBSIZE, EXPIRE/TTL stubs,
// and benign replies for CONFIG/CLIENT/SELECT/COMMAND/INFO). Larger-than-memory and
// the on-disk .aki file are deliberately out of scope here; this package is the
// in-memory wire path that the 2x claim is measured on first.
package f1srv

import (
	"net"
	"sync"

	"github.com/tamnd/aki/engine/f1raw"
)

// Config sizes the store and tunes the listener.
type Config struct {
	Addr         string // listen address, host:port
	IndexBuckets int    // f1raw primary index buckets (rounded up to a power of two)
	ArenaBytes   int    // f1raw arena size in bytes
	ReadBufSize  int    // initial per-connection read buffer
	IncrStripes  int    // INCR-family RMW lock stripes (rounded up to a power of two)
}

// DefaultConfig returns a config sized for a multi-million-key in-memory benchmark.
func DefaultConfig(addr string) Config {
	return Config{
		Addr:         addr,
		IndexBuckets: 1 << 22,  // ~4M buckets, ~29M key slots before overflow
		ArenaBytes:   2 << 30,  // 2 GiB arena
		ReadBufSize:  64 << 10, // 64 KiB
		IncrStripes:  1 << 10,  // 1024 stripes
	}
}

// Server is a running f1srv instance.
type Server struct {
	cfg   Config
	store *f1raw.Store
	ln    net.Listener

	// incrMu serializes the read-modify-write of one key's INCR family so two
	// counters on the same key sum rather than clobber. It does not touch the
	// lock-free GET/SET path. Striped by key hash to keep distinct keys parallel.
	incrMu   []sync.Mutex
	incrMask uint32

	wg sync.WaitGroup
}

// New builds a server and its store. It does not listen yet; call ListenAndServe.
func New(cfg Config) *Server {
	if cfg.IndexBuckets <= 0 {
		cfg.IndexBuckets = 1 << 20
	}
	if cfg.ArenaBytes <= 0 {
		cfg.ArenaBytes = 256 << 20
	}
	if cfg.ReadBufSize <= 0 {
		cfg.ReadBufSize = 64 << 10
	}
	stripes := 1
	for stripes < cfg.IncrStripes {
		stripes <<= 1
	}
	if stripes < 1 {
		stripes = 1
	}
	return &Server{
		cfg:      cfg,
		store:    f1raw.New(cfg.IndexBuckets, cfg.ArenaBytes),
		incrMu:   make([]sync.Mutex, stripes),
		incrMask: uint32(stripes - 1),
	}
}

// Addr reports the address the server is listening on, valid after ListenAndServe
// has bound the socket.
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.cfg.Addr
	}
	return s.ln.Addr().String()
}

// Listen binds the socket without serving, so a caller (or a test) can learn the
// bound address before accepting. ListenAndServe calls it when needed.
func (s *Server) Listen() error {
	if s.ln != nil {
		return nil
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

// ListenAndServe binds (if not already bound) and accepts connections until the
// listener is closed. Each connection runs on its own goroutine.
func (s *Server) ListenAndServe() error {
	if err := s.Listen(); err != nil {
		return err
	}
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

// Close stops accepting and waits for in-flight connections to drain.
func (s *Server) Close() error {
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	s.wg.Wait()
	return err
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c := &connState{
		srv:  s,
		conn: conn,
		rbuf: make([]byte, 0, s.cfg.ReadBufSize),
		out:  make([]byte, 0, s.cfg.ReadBufSize),
	}
	c.loop()
}
