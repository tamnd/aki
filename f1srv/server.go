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
// and benign replies for CONFIG/CLIENT/SELECT/COMMAND/INFO).
//
// The larger-than-memory string regime is engaged by setting Config.ColdPath: the
// store then keeps its lock-free index and record keys resident and writes any value
// past the separation threshold to an append-only cold log on disk (f1raw milestone
// M1, WiscKey key-value separation). With ColdPath empty the server is the pure
// in-memory path unchanged. The durable single-file .aki format and recovery are a
// later milestone; the cold log here is fresh-start only.
package f1srv

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f1raw"
)

// Config sizes the store and tunes the listener.
type Config struct {
	Addr         string // listen address, host:port
	IndexBuckets int    // f1raw primary index buckets (rounded up to a power of two)
	ArenaBytes   int    // f1raw arena size in bytes
	ReadBufSize  int    // initial per-connection read buffer
	IncrStripes  int    // INCR-family RMW lock stripes (rounded up to a power of two)
	NetMode      string // "go" (goroutine-per-conn, default) or "reactor" (Linux epoll)

	// ColdPath, when non-empty, engages the larger-than-memory string tier: the store
	// opens an append-only cold value log at this path and writes any value longer than
	// SepThreshold there, keeping only the index and record keys resident. Empty means a
	// pure in-memory store. The log is truncated on open (fresh start; recovery is a
	// later milestone).
	ColdPath string
	// SepThreshold is the inline-versus-separated value cutoff in bytes; a non-positive
	// value uses the engine default. It is ignored when ColdPath is empty.
	SepThreshold int
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
	cfg     Config
	store   *f1raw.Store
	ln      net.Listener
	initErr error // deferred cold-log open error, surfaced by Listen

	// incrMu serializes the read-modify-write of one key's INCR family so two
	// counters on the same key sum rather than clobber. It does not touch the
	// lock-free GET/SET path. Striped by key hash to keep distinct keys parallel.
	incrMu   []sync.Mutex
	incrMask uint32

	// block is the per-key blocked-client registry the blocking list commands park in
	// (list-model spec 2064/f1_rewrite_ltm/08 section 9). It lives beside the storage and
	// holds no element bytes; a blocked client is one queue entry per key it waits on.
	block blockReg

	// volatile counts the keys that currently carry a TTL (an expire sibling row). It is
	// the hot-path gate for lazy expiry: when it is zero no key can be expired, so the
	// read path skips the expiry probe entirely after one atomic load, which keeps the
	// TTL-free benchmark workload paying nothing for the machinery. Every setExpiry that
	// creates a fresh expire row bumps it and every clear that removes one drops it, both
	// under the key's stripe lock so the count stays exact.
	volatile atomic.Int64

	// watch is the optimistic-locking table behind WATCH/EXEC. watchVer holds a monotonic
	// version per currently-watched key: WATCH snapshots it, a write to that key bumps it,
	// and EXEC compares. watching is the hot-path gate, the count of live (connection, key)
	// watches across all clients; when it is zero no key is watched, so the write path skips
	// the version bump entirely after one atomic load, the same gate pattern volatile uses
	// for TTLs. watchMu guards the map and the refcounts inside it.
	watchMu   sync.Mutex
	watchVer  map[string]*watchEntry
	watching  atomic.Int64

	// Pub/sub registry. psChan/psPat/psShard map a channel name, a pattern, or a shard channel
	// to the set of connections subscribed to it; PUBLISH walks psChan plus every matching entry
	// of psPat, SPUBLISH walks psShard alone (shard channels are a separate namespace regular
	// PUBLISH never reaches). psMu guards all three maps and the subscriber sets inside them. An
	// entry lives only while it has at least one subscriber, so the table stays bounded to the
	// channels under active subscription. There is no hot-path gate here because pub/sub touches
	// no keyspace and the GET/SET path never consults these maps.
	psMu    sync.Mutex
	psChan  map[string]map[*connState]struct{}
	psPat   map[string]map[*connState]struct{}
	psShard map[string]map[*connState]struct{}

	wg sync.WaitGroup
}

// watchEntry is one watched key's version and how many connections currently watch it. The
// entry lives only while refs > 0, so the table stays bounded to the keys under active
// WATCH rather than every key ever written.
type watchEntry struct {
	ver  uint64
	refs int
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
	srv := &Server{
		cfg:      cfg,
		incrMu:   make([]sync.Mutex, stripes),
		incrMask: uint32(stripes - 1),
	}
	srv.block.waiters = make(map[string][]*listWaiter)
	srv.watchVer = make(map[string]*watchEntry)
	srv.psChan = make(map[string]map[*connState]struct{})
	srv.psPat = make(map[string]map[*connState]struct{})
	srv.psShard = make(map[string]map[*connState]struct{})
	// A cold path engages the larger-than-memory tier; opening the log can fail on a
	// disk error, so defer that error to Listen and keep New's signature simple for the
	// many in-memory callers and tests that never set ColdPath.
	if cfg.ColdPath != "" {
		store, err := f1raw.NewWithCold(cfg.IndexBuckets, cfg.ArenaBytes, cfg.ColdPath, cfg.SepThreshold)
		if err != nil {
			srv.initErr = err
		} else {
			srv.store = store
		}
	} else {
		srv.store = f1raw.New(cfg.IndexBuckets, cfg.ArenaBytes)
	}
	if srv.store != nil {
		// Teach the engine which record kinds are top-level keys so it can keep an O(1)
		// live-key counter for DBSIZE, the same policy KEYS/SCAN/RANDOMKEY hand ScanKeys.
		// Set before the server accepts traffic, on the still-empty store.
		srv.store.SetTopKindFunc(isTopKind)
	}
	return srv
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
	if s.initErr != nil {
		return s.initErr
	}
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
// listener is closed. With NetMode "reactor" on Linux it hands the listener to the
// epoll event-loop driver; otherwise, and everywhere reactor is unavailable, each
// connection runs on its own goroutine.
func (s *Server) ListenAndServe() error {
	if err := s.Listen(); err != nil {
		return err
	}
	if handled, err := serveWithReactor(s); handled {
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

// Close stops accepting, waits for in-flight connections to drain, and closes the
// store (which shuts the cold value log when the LTM tier is engaged).
func (s *Server) Close() error {
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	s.wg.Wait()
	if s.store != nil {
		if cerr := s.store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c := &connState{
		srv:       s,
		conn:      conn,
		rbuf:      make([]byte, 0, s.cfg.ReadBufSize),
		out:       make([]byte, 0, s.cfg.ReadBufSize),
		blockable: true, // a per-connection goroutine may park on a blocking command
	}
	// On the goroutine driver a message frame is written straight to the socket, under writeMu
	// so a publisher on another goroutine and this connection's own flush cannot interleave.
	c.deliver = c.writeToConn
	c.loop()
	// A client can disconnect mid-transaction, while holding watches, or while subscribed;
	// release all three so the watch table's refcounts (and the global watching gate) and the
	// pub/sub registry do not leak past the connection.
	c.discardTx()
	c.unsubscribeAll()
}
