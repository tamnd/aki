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
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
	// It is an RWMutex so a whole-collection read (HGETALL/SMEMBERS and friends)
	// can take the shared lock and let many readers of one hot key run on many
	// cores at once, which a single-threaded server cannot; writers still take
	// the exclusive lock, so a reader never sees a field move mid-walk.
	incrMu   []sync.RWMutex
	incrMask uint32

	// listWin is the resident hot-list window registry (spec 2064/f1_rewrite_ltm/impl/26). Each
	// shard is a map from a list key to its listWindow, indexed by the same stripe hash as incrMu,
	// so a key's window shard and its stripe lock line up. A window is admitted on a list's first
	// push, lets subsequent pushes append lock-free off the stripe lock through a reserved/committed
	// bound split, and is retired the moment any non-push command lands on the key. listWinLive is
	// the count of resident windows across all shards, the hot-path gate: when it is zero no list
	// has a window, so every read and every non-push list command skips the registry after one
	// atomic load and the all-cold workload pays nothing for the machinery.
	listWin     []listWinShard
	listWinLive atomic.Int64

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

	// hfe counts hash fields across the keyspace that currently carry a field TTL (a
	// kindHashFieldTTL sibling row). It is the hot-path gate for hash-field lazy expiry, the
	// same pattern volatile uses for key TTLs: when it is zero no hash field can be expired, so
	// every hash read skips the field-expiry probe after one atomic load and the TTL-free
	// benchmark workload pays nothing for the machinery. Each set of a field's first TTL bumps
	// it and each clear or reap of one drops it, both under the hash's stripe lock. A non-zero
	// hfe only means some hash somewhere has a field TTL; a whole-hash read still consults the
	// per-hash kindHashTTLMeta hint before it will scan, so a TTL-free hash stays O(1).
	hfe atomic.Int64

	// watch is the optimistic-locking table behind WATCH/EXEC. watchVer holds a monotonic
	// version per currently-watched key: WATCH snapshots it, a write to that key bumps it,
	// and EXEC compares. watching is the hot-path gate, the count of live (connection, key)
	// watches across all clients; when it is zero no key is watched, so the write path skips
	// the version bump entirely after one atomic load, the same gate pattern volatile uses
	// for TTLs. watchMu guards the map and the refcounts inside it.
	watchMu  sync.Mutex
	watchVer map[string]*watchEntry
	watching atomic.Int64

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

	// nextConnID hands out the per-connection identifier CLIENT ID reports. Redis numbers clients
	// from 1, so the first Add returns 1. Both drivers (goroutine and reactor) draw from it, so the
	// ids stay unique across the whole server regardless of which accept path took the connection.
	// Its current value is also the total number of connections ever accepted, which INFO reports
	// as total_connections_received.
	nextConnID atomic.Int64

	// clients is the count of connections currently open, bumped up when a connection's state is
	// created and back down when it tears down, on both the goroutine and the reactor driver. It
	// costs one atomic add per accept and one per close, nothing per command, so the GET/SET hot
	// path never touches it. INFO reports it as connected_clients.
	clients atomic.Int64

	// startTime is the wall-clock instant New built the server, the origin INFO's uptime fields
	// count from. runID is a 40-hex random token generated once at construction, the identity a
	// client uses to tell one server run from another (INFO run_id, matching Redis's width). Both
	// are set once and never mutated, so they need no synchronization.
	startTime time.Time
	runID     string

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
		cfg:       cfg,
		incrMu:    make([]sync.RWMutex, stripes),
		incrMask:  uint32(stripes - 1),
		startTime: time.Now(),
		runID:     newRunID(),
	}
	srv.listWin = make([]listWinShard, stripes)
	for i := range srv.listWin {
		srv.listWin[i].m = make(map[string]*listWindow)
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

// newRunID returns a 40-character hex token identifying this server run, the width Redis uses
// for its run_id. It draws 20 random bytes; if the system source fails (it does not in practice),
// it falls back to a fixed marker so New never fails on account of an identity string.
func newRunID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
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
		id:        s.nextConnID.Add(1),
		rbuf:      make([]byte, 0, s.cfg.ReadBufSize),
		out:       make([]byte, 0, s.cfg.ReadBufSize),
		blockable: true, // a per-connection goroutine may park on a blocking command
	}
	s.clients.Add(1)
	defer s.clients.Add(-1)
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
