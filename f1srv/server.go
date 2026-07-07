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
	"errors"
	"log"
	"net"
	"runtime"
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
	NetMode      string // "auto" (reactor on Linux, goroutine elsewhere; the default), "go" (goroutine-per-conn), or "reactor" (Linux epoll)
	ExecModel    string // "shared" (default: any loop runs any key under its stripe lock) or "affinity" (route each key to its owning shard worker, spec 2064/17). Inert until the routing slices land.

	// ColdPath, when non-empty, engages the larger-than-memory string tier: the store
	// opens an append-only cold value log at this path and writes any value longer than
	// SepThreshold there, keeping only the index and record keys resident. Empty means a
	// pure in-memory store. The log is truncated on open (fresh start; recovery is a
	// later milestone).
	ColdPath string
	// SepThreshold is the inline-versus-separated value cutoff in bytes; a non-positive
	// value uses the engine default. It is ignored when ColdPath is empty.
	SepThreshold int

	// ArenaSegmented switches the store's arena to the reclaimable segmented layout of the
	// collection cold-record tiering plan (spec 2064/21), milestone M0. Default false keeps
	// the grow-only bump arena, so the resident point path is unchanged. When true the arena
	// is divided into ArenaSegmentBytes segments that can be freed and reused, with overflow
	// buckets in their own never-reclaimed region; it composes with a cold value log. The
	// milestones that make it close the collection-LTM gap (the tier-tagged index, the cold
	// record region, and the migrator) land on top of this in later slices, so on its own the
	// segmented arena changes only how bytes are reclaimed, not what spills to disk.
	ArenaSegmented bool
	// ArenaSegmentBytes is the segment size when ArenaSegmented is set; a non-positive value
	// uses the engine default (8 MiB). It is floored at the largest record so no record spans
	// a segment. ArenaOverflowBytes sizes the never-reclaimed overflow-bucket region; a
	// non-positive value reserves an eighth of the arena. Both are ignored unless
	// ArenaSegmented is set.
	ArenaSegmentBytes  int
	ArenaOverflowBytes int

	// ColdRecordsPath, when non-empty, opens the cold record region (spec 2064/21 M1): the
	// tier the background migrator sinks whole string records into under arena fill pressure.
	// It is distinct from ColdPath, which separates large values alone; a store can run either,
	// both, or neither. Empty leaves the record region off, so nothing migrates and the resident
	// path is unchanged. Like ColdPath the file is truncated on open (durable reopen is a later
	// milestone), and opening it can fail on a disk error, which New defers to Listen.
	ColdRecordsPath string
	// Migrator starts the background migrator that drives records cold once the resident arena
	// crosses its high-water mark (spec 2064/21 M3), the switch that makes a bounded arena serve a
	// string dataset larger than itself. It requires ArenaSegmented and a ColdRecordsPath: the
	// migrator drains whole segments into the record region, so both must be set or New records a
	// configuration error surfaced by Listen. Default false leaves the arena grow-only with no
	// migrator goroutine, so a store that never sets it pays nothing for the machinery.
	Migrator bool

	// SetPartitionMax caps the partitions one hot set can engage under the adaptive intra-key
	// partitioning of spec 2064/f1_rewrite_ltm/19. The default of 1 leaves the feature off: every
	// set stays unpartitioned and the set commands run their existing single-lock bodies with no
	// added cost. A value above 1 (rounded up to a power of two in New) turns the engage-and-grow
	// trigger on and bounds how far one set can spread, typically the machine's data-core count.
	// SetPartitionThreshold is the cardinality a set must reach before it engages at all, and
	// SetPartitionTarget is the members-per-partition the grow aims for; a non-positive threshold
	// or target takes the built-in default. All three are settled empirically in slice 6c.
	SetPartitionMax       int
	SetPartitionThreshold int
	SetPartitionTarget    int
}

// Built-in defaults for the adaptive set-partitioning knobs, used when the feature is on
// (SetPartitionMax > 1) but the threshold or target is left unset. Slice 6c's microbenchmark sweep
// settled both from the clean (allocation-free) grid on the 14-data-core GamingPC: the exclusive-lock
// write split pays off from P=4 upward with no uncontended tax, while the non-destructive weighted
// draw pays a tax that climbs with P. So engagement is gated to genuinely large hot sets (threshold
// 262144, the cardinality band where a set stops fitting comfortably in cache and write contention is
// the plausible bottleneck) and the target is sized (262144 members per partition) so an engaged set
// lands on P=4 and only climbs to P=8/P=16 past ~2M/~4M members, keeping the common case at the sweep's
// balance point rather than the draw-taxing high-P end.
const (
	defaultSetPartitionThreshold = 1 << 18 // 262144: a set engages once it crosses this cardinality
	defaultSetPartitionTarget    = 1 << 18 // 262144: the members-per-partition a grow aims for
)

// DefaultConfig returns a config sized for a multi-million-key in-memory benchmark.
func DefaultConfig(addr string) Config {
	return Config{
		Addr:         addr,
		IndexBuckets: 1 << 22,  // ~4M buckets, ~29M key slots before overflow
		ArenaBytes:   2 << 30,  // 2 GiB arena
		ReadBufSize:  64 << 10, // 64 KiB
		IncrStripes:  1 << 10,  // 1024 stripes
		NetMode:      "auto",   // reactor on Linux, goroutine driver elsewhere
		ExecModel:    "shared", // stripe-locked shared store; "affinity" is opt-in until proven (spec 2064/17)
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

	// forceP forces the partition count P every set command routes through, the slice-3 test and
	// microbench hook for intra-key set partitioning (spec 2064/f1_rewrite_ltm/19). It is 0 in
	// production, so partitionsFor returns P=1 after one atomic load and the set point commands run
	// their existing unpartitioned bodies untouched. A test or a contention microbenchmark stores a
	// power of two here to exercise and measure the routed partition path before the adaptive engage
	// transition (slice 6) exists to grow a hot set to P>1 on its own. It is read on the hot path and
	// written only in tests, so it is an atomic rather than a plain field.
	forceP atomic.Int64

	// setPartP is the per-key partition registry: the source partitionsFor reads to decide the P a
	// set's commands route through once the whole-server forceP hook is retired (spec
	// 2064/f1_rewrite_ltm/19 slice 6). It is an atomically-published immutable map keyed by the set's
	// key bytes, nil until the first set engages partitioning, so a keyspace with no engaged set pays
	// one atomic load and a nil check on the set hot path and nothing more. The adaptive engage-and-
	// grow transition installs a key's P here under setPartMu when it re-homes the set to P>1, and the
	// drop path removes it so a deleted-and-recreated key restarts at P=1 (section 3.1). partitionsFor
	// reads it lock-free; only the rare engage/grow/drop takes setPartMu to swap the map by
	// copy-on-write, the same discipline the store's descriptor and vector maps use.
	setPartP  atomic.Pointer[map[string]int]
	setPartMu sync.Mutex

	// setPartMax, setPartThreshold, and setPartTarget are the resolved adaptive-partitioning knobs
	// (Config.SetPartition*), read-only after New. setPartMax is the power-of-two cap on how far one
	// set may spread and, at its default of 1, the master off switch: when it is 1 the engage trigger
	// (maybeEngageSet) returns after one comparison, no set is ever partitioned, and every set command
	// keeps its unpartitioned fast path. Above 1 the trigger grows a set toward
	// min(setPartMax, roundUpPow2(card/setPartTarget)) once its cardinality reaches setPartThreshold.
	setPartMax       int
	setPartThreshold int
	setPartTarget    int

	// execModel is the resolved command-execution model (spec 2064/17), parsed once from
	// cfg.ExecModel in New. execShards is the shard count the affinity model routes over,
	// GOMAXPROCS so one shard maps to one worker core. Both are read-only after New. The
	// affinity routing path is not wired yet; these carry the resolved choice so INFO and
	// the routing slices that follow read one field instead of re-parsing the flag string.
	execModel  execModel
	execShards int

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
	// Resolve the execution model once. An unrecognized value falls back to the shared
	// default rather than refusing to start; the warning is logged so a typo is visible.
	em, ok := parseExecModel(cfg.ExecModel)
	if !ok {
		log.Printf("f1srv: unknown --exec-model %q, using %q", cfg.ExecModel, em)
	}
	shards := runtime.GOMAXPROCS(0)
	if shards < 1 {
		shards = 1
	}
	// Resolve the adaptive set-partitioning knobs once. The cap rounds up to a power of two because
	// P is always a power of two; a cap of 1 (the default) leaves the feature off. The threshold and
	// target fall back to the built-in defaults when unset, so turning the feature on needs only the
	// cap.
	partMax := 1
	if cfg.SetPartitionMax > 1 {
		partMax = int(nextPow2(int64(cfg.SetPartitionMax)))
	}
	partThreshold := cfg.SetPartitionThreshold
	if partThreshold <= 0 {
		partThreshold = defaultSetPartitionThreshold
	}
	partTarget := cfg.SetPartitionTarget
	if partTarget <= 0 {
		partTarget = defaultSetPartitionTarget
	}
	srv := &Server{
		cfg:              cfg,
		incrMu:           make([]sync.RWMutex, stripes),
		incrMask:         uint32(stripes - 1),
		execModel:        em,
		execShards:       shards,
		setPartMax:       partMax,
		setPartThreshold: partThreshold,
		setPartTarget:    partTarget,
		startTime:        time.Now(),
		runID:            newRunID(),
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
	if srv.store != nil && cfg.ArenaSegmented {
		// Switch the arena to the reclaimable segmented layout before serving, on the
		// still-empty store, so it composes with a cold log or a pure in-memory store
		// (spec 2064/21 M0). Default off leaves the grow-only bump path in place.
		srv.store.EnableSegments(cfg.ArenaSegmentBytes, cfg.ArenaOverflowBytes)
	}
	if srv.store != nil && srv.initErr == nil && cfg.ColdRecordsPath != "" {
		// Open the cold record region the migrator sinks whole frames into (spec 2064/21 M1),
		// on the still-empty store. Like the cold value log, opening the file can fail on a disk
		// error, deferred to Listen; a failure leaves srv.store set but the region unopened, so
		// the migrator wiring below sees no region and reports the same configuration error.
		if err := srv.store.EnableColdRecords(cfg.ColdRecordsPath); err != nil {
			srv.initErr = err
		}
	}
	if srv.store != nil {
		// Teach the engine which record kinds are top-level keys so it can keep an O(1)
		// live-key counter for DBSIZE, the same policy KEYS/SCAN/RANDOMKEY hand ScanKeys.
		// Set before the server accepts traffic, on the still-empty store.
		srv.store.SetTopKindFunc(isTopKind)
		// Widen the background migrator past its string floor to the collection element kinds the
		// server has proven tier-safe (spec 2064/21 D22 Option B), so those elements can sink cold
		// under fill pressure while their header row stays resident. The hash field and both zset
		// element kinds and the list element kind qualify today; isMigratableKind documents why the
		// rest still wait. This
		// is inert unless cfg.Migrator started the migrator below, and like SetTopKindFunc it is set
		// once here on the still-empty store, before the server accepts traffic.
		srv.store.SetMigratableKindFunc(isMigratableKind)
		// Take the O(log n) ordered-index splice off the element-delete reply path: a delete
		// queues the removal and a background folder splices it under the index lock later
		// (spec 2064/16 slice 2). Enable once here, before the server accepts traffic.
		srv.store.EnableDeferredRemoval()
	}
	if cfg.Migrator {
		// Start the background migrator that sinks records cold under fill pressure (spec 2064/21
		// M3). It needs the segmented arena and an open cold record region, both set up above, so
		// a bad combination is a configuration error surfaced by Listen rather than a panic from
		// EnableMigrator. The store is guarded because a deferred cold-log open error above can
		// leave it nil.
		switch {
		case srv.initErr != nil:
			// An earlier cold-region open already failed; keep that error.
		case srv.store == nil || !cfg.ArenaSegmented || cfg.ColdRecordsPath == "":
			srv.initErr = errors.New("f1srv: Migrator requires ArenaSegmented and ColdRecordsPath")
		default:
			srv.store.EnableMigrator()
		}
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
// listener is closed. With NetMode "auto" (the default) or "reactor" on Linux it hands
// the listener to the epoll event-loop driver; otherwise, and everywhere the reactor is
// unavailable or its setup fails, each connection runs on its own goroutine.
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
	connID := s.nextConnID.Add(1)
	c := &connState{
		srv:       s,
		conn:      conn,
		id:        connID,
		rngState:  seedConnRNG(connID),
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
