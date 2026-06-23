// Package keyspace is aki's logical key dictionary (spec 2064 doc 05). It maps
// N independent logical databases onto one .aki file. Each database is split
// into NumShards independent B-trees, each owned by a dedicated write-worker
// goroutine, so concurrent writes on different hash-slot ranges proceed without
// contention. Keys route to shards by HashSlot(key) & (NumShards - 1). Reads
// and cross-shard writes use the per-shard RWMutex on dbShard for isolation.
package keyspace

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/pager"
)

// NumShards is the number of independent B-tree shards per logical database.
// It must be a power of 2; keys route to shards by HashSlot(key) & shardMask.
// Changing this value requires a format migration (FormatVersion bump).
const NumShards = 8

// shardMask is the bitmask applied to a hash slot to get the shard index.
const shardMask = NumShards - 1

// recordSize is the on-disk size of one per-DB catalog record (doc 05 §2.2).
// Format v2: NumShards × 4-byte roots + keyCount(8) + expireCount(8) +
// avgTTL(4) + numShards(4) + padding(8) = NumShards*4 + 32 bytes.
const recordSize = NumShards*4 + 32

// catalogDataStart is the byte offset of the first catalog record on the catalog
// page, just past the common 16-byte page header.
const catalogDataStart = format.CommonHeaderSize

// ErrDBRange is returned when a database index is outside [0, DBCount).
var ErrDBRange = errors.New("aki/keyspace: database index out of range")

// nowMillis returns the current wall clock in Unix epoch milliseconds. It is a
// variable so tests can pin time and exercise TTL expiry deterministically.
var nowMillis = func() int64 { return time.Now().UnixMilli() }

// NowMillis returns the keyspace clock in Unix epoch milliseconds. The command
// layer uses it to turn a relative TTL like EX seconds into the absolute
// millisecond deadline that Set stores, so both layers read the same clock.
func NowMillis() int64 { return nowMillis() }

// ExpiredKey names a key that lazy expiry removed, tagged with the database it
// lived in. The command layer drains these after a keyspace access to fire the
// "expired" notification, which the keyspace layer cannot fire on its own.
type ExpiredKey struct {
	DB  int
	Key []byte
}

// entryOverhead is the fixed per-key cost folded into the live-data estimate on
// top of the key name and the body. It stands in for the value header and the
// B-tree cell bookkeeping so used memory tracks roughly with what a key occupies.
const entryOverhead = 64

// Keyspace owns every logical database in one .aki file.
type Keyspace struct {
	pgr     *pager.Pager
	dbs     []*DB
	catRoot uint32 // catalog page number, NullPage until first persisted
	sysRoot uint32 // system table B-tree root, NullPage until first SystemPut
	sysTree *btree.Tree
	version atomic.Uint64 // monotonic write version; use NextVersion to advance

	// expiredLog collects keys deleted by lazy expiry since the last drain. The
	// command layer empties it with TakeExpired after each access to fire the
	// "expired" keyspace event. expiredMu guards it so concurrent reads can log
	// expired keys without holding the engine write lock.
	expiredMu  sync.Mutex
	expiredLog []ExpiredKey

	// dataBytes is the running estimate of live key and value bytes, the figure
	// INFO reports as used_memory and the maxmemory eviction loop compares against
	// the limit. Set and Delete keep it current via atomic Add so concurrent shard
	// writers can update it without holding the global write lock.
	dataBytes atomic.Int64

	// lfuLogFactor and lfuDecayTime back the lfu-log-factor and lfu-decay-time
	// config knobs. Open seeds them with the Redis defaults; the command layer
	// overrides them through SetLFUParams. lfuDecayTime in minutes, 0 disables
	// decay.
	lfuLogFactor int
	lfuDecayTime int
}

// SetLFUParams sets the LFU counter tuning the eviction sampler uses, from the
// lfu-log-factor and lfu-decay-time config knobs. A log factor below zero clamps
// to zero, which makes the counter climb on every access. A decay time of zero or
// below disables decay, so a counter never falls on its own.
func (k *Keyspace) SetLFUParams(logFactor, decayTime int) {
	if logFactor < 0 {
		logFactor = 0
	}
	if decayTime < 0 {
		decayTime = 0
	}
	k.lfuLogFactor = logFactor
	k.lfuDecayTime = decayTime
}

// UsedMemory returns the live-data estimate in bytes, the value compared against
// maxmemory. It is the sum of key name, body and per-key overhead across every
// live key, which shrinks as keys are deleted or evicted.
func (ks *Keyspace) UsedMemory() int64 { return ks.dataBytes.Load() }

// EvictionCandidate is a key the eviction loop may remove, carrying the fields the
// policies sort on: the expiry for volatile-ttl, the last-access time for the lru
// policies, and the decayed frequency for the lfu policies.
type EvictionCandidate struct {
	DB     int
	Key    []byte
	TTLms  int64
	HasTTL bool
	Atime  uint32 // unix seconds of last access; smaller is older, evicted first by LRU
	Freq   uint8  // decayed LFU counter; smaller is colder, evicted first by LFU
}

// SampleForEviction reservoir-samples up to n eviction candidates across every
// database. When volatileOnly is set it considers only keys that carry a TTL,
// which is what the volatile-* policies evict from.
func (ks *Keyspace) SampleForEviction(n int, volatileOnly bool) []EvictionCandidate {
	if n <= 0 {
		n = 1
	}
	out := make([]EvictionCandidate, 0, n)
	seen := 0
	for _, db := range ks.dbs {
		if volatileOnly && db.totalExpireCount() == 0 {
			continue
		}
		_ = db.forEachLive(func(ck []byte, h ValueHeader) error {
			if volatileOnly && !h.HasTTL() {
				return nil
			}
			raw := copyRaw(ck)
			atime, freq := db.accessMetrics(raw)
			cand := EvictionCandidate{
				DB:     db.index,
				Key:    raw,
				TTLms:  h.TTLms,
				HasTTL: h.HasTTL(),
				Atime:  atime,
				Freq:   freq,
			}
			if len(out) < n {
				out = append(out, cand)
			} else if j := rand.IntN(seen + 1); j < n {
				out[j] = cand
			}
			seen++
			return nil
		})
	}
	return out
}

// TakeExpired returns the keys lazily expired since the last call and clears the
// log. Guarded by expiredMu so concurrent reads on different goroutines can
// append without data-racing the slice.
func (ks *Keyspace) TakeExpired() []ExpiredKey {
	ks.expiredMu.Lock()
	defer ks.expiredMu.Unlock()
	if len(ks.expiredLog) == 0 {
		return nil
	}
	out := ks.expiredLog
	ks.expiredLog = nil
	return out
}

// dbShard is one of the NumShards independent B-trees that back a logical DB.
// Writes within one shard serialise on mu.Lock; reads on the same shard take
// mu.RLock so they are excluded from concurrent writes. Shards on different
// key ranges run fully in parallel.
type dbShard struct {
	mu       sync.RWMutex
	rootPage uint32 // btree root page, NullPage when this shard has no keys
	tree     *btree.Tree
	// keyCount and expireCount are updated atomically so Len() and
	// totalExpireCount() can read them without holding the shard mutex.
	keyCount    atomic.Uint64
	expireCount atomic.Uint64

	// wbMu guards wbPending for this shard. Keeping write-behind state per
	// shard means concurrent writers on different key ranges do not compete
	// on a single lock, reducing contention 1/NumShards.
	wbMu      sync.RWMutex
	wbPending map[string]wbPendingEntry

	// pendingUncertain counts write-behind keys that entered wbPending but
	// have not yet been applied to the B-tree. A key is counted once when it
	// first enters wbPending (not yet present there) and removed once the shard
	// worker's removeWBPending call actually deletes it (version matched). This
	// lets Len() include acknowledged-but-not-yet-committed keys so DBSIZE is
	// accurate even while async B-tree writes are in flight.
	//
	// The count can transiently overcount by 1 for keys that were already in
	// the B-tree and are being updated: from the moment the B-tree write
	// completes (keyCount unchanged) until removeWBPending decrements the
	// counter, both keyCount and pendingUncertain count the same key. The
	// window is bounded by the shard worker's batch latency (~10–100 µs).
	pendingUncertain atomic.Int64
}

// ShardOf returns the shard index for key, which is the low bits of its hash
// slot. Callers that want to route a write or a lock-free read to a single
// shard use this instead of recomputing the slot themselves.
func ShardOf(key []byte) int { return int(HashSlot(key)) & shardMask }

// DB is one logical database: NumShards B-trees plus shared metadata.
type DB struct {
	ks    *Keyspace
	index int

	shards [NumShards]dbShard
	avgTTL uint32

	// access holds the in-memory LRU and LFU bookkeeping per key, keyed by the raw
	// key name. It is built up as keys are read and written and is not persisted,
	// matching how Redis treats approximate eviction state across a restart.
	// accessMu guards access independently of the engine lock so concurrent reads
	// can update eviction bookkeeping without serializing on the write lock.
	accessMu sync.Mutex
	access   map[string]*keyAccess

	// hc is the hot-value cache for this database. A Get on a cached key returns
	// the body and header without walking the B-tree. Set and Delete invalidate
	// the entry so the next read always sees fresh data. Using atomic.Pointer so
	// hot-GET callers can load the cache without the engine read lock, and SwapDB
	// can exchange the cache pointer while readers are active.
	hc atomic.Pointer[dbCache]
}

// wbPendingEntry is one entry in the write-behind pending table. It carries
// the same fields PrepareWriteBehind stored in the hot cache so the read path
// can serve the value without touching the B-tree.
type wbPendingEntry struct {
	body []byte
	hdr  ValueHeader
}

// Open binds a Keyspace to a pager and loads the catalog. The number of
// databases comes from the file header; a fresh file with no catalog page yields
// empty databases that materialise their B-trees on first write. Files written
// by format version 1 (single-tree catalog) are rejected; recreate the file.
func Open(pgr *pager.Pager) (*Keyspace, error) {
	hdr := pgr.Header()
	if hdr.FormatVersion != 0 && hdr.FormatVersion < format.FormatVersion {
		return nil, fmt.Errorf("aki/keyspace: file format v%d is too old (need v%d); recreate the file",
			hdr.FormatVersion, format.FormatVersion)
	}
	dbCount := int(hdr.DBCount)
	if dbCount <= 0 {
		dbCount = int(format.DefaultDBCount)
	}
	ks := &Keyspace{
		pgr:          pgr,
		dbs:          make([]*DB, dbCount),
		catRoot:      pgr.Meta().CatalogRoot,
		sysRoot:      normalizeRoot(pgr.Meta().SystemRoot),
		lfuLogFactor: lfuLogFactor,
		lfuDecayTime: lfuDecayTime,
	}
	for i := range ks.dbs {
		db := &DB{ks: ks, index: i}
		for s := range NumShards {
			db.shards[s].rootPage = format.NullPage
		}
		db.hc.Store(newDBCache())
		ks.dbs[i] = db
	}
	if ks.catRoot != format.NullPage {
		if err := ks.loadCatalog(); err != nil {
			return nil, err
		}
	}
	return ks, nil
}

// NextVersion atomically increments the keyspace write counter and returns the
// new version. The write-behind path calls this under the engine read lock to
// assign a stable version before the B-tree write is queued, so WATCH and the
// hot-value cache always see a consistent, monotonically increasing value.
func (ks *Keyspace) NextVersion() uint64 { return ks.version.Add(1) }

// PagerStats returns the underlying pager's counters for the file-growth INFO
// fields. It is a passthrough so the command layer does not reach into the pager.
func (k *Keyspace) PagerStats() pager.Stats { return k.pgr.Stats() }

// PagerName returns the file path the underlying pager was opened with, empty
// for an in-memory backing.
func (k *Keyspace) PagerName() string { return k.pgr.Name() }

// DBCount returns the number of logical databases.
func (ks *Keyspace) DBCount() int { return len(ks.dbs) }

// DB returns the database at index, or an error if the index is out of range.
func (ks *Keyspace) DB(index int) (*DB, error) {
	if index < 0 || index >= len(ks.dbs) {
		return nil, ErrDBRange
	}
	return ks.dbs[index], nil
}

// loadCatalog reads the catalog page and fills each DB's shard roots and counters.
func (ks *Keyspace) loadCatalog() error {
	pg, err := ks.pgr.Get(ks.catRoot)
	if err != nil {
		return err
	}
	defer ks.pgr.Unpin(pg, false)
	for i, db := range ks.dbs {
		off := catalogDataStart + i*recordSize
		if off+recordSize > len(pg.Data) {
			break
		}
		rec := pg.Data[off:]
		// Bytes 0..NumShards*4-1: shard root pages (uint32 each)
		for s := range NumShards {
			db.shards[s].rootPage = encoding.U32(rec[s*4:])
		}
		base := NumShards * 4
		keyCount := encoding.U64(rec[base:])
		expireCount := encoding.U64(rec[base+8:])
		db.avgTTL = encoding.U32(rec[base+16:])
		// Distribute the persisted totals evenly across shards (they are
		// re-counted in-memory as keys are accessed, so the split only needs
		// to be roughly right to keep the expireCount == 0 fast-path working).
		perShard := keyCount / uint64(NumShards)
		remainder := keyCount % uint64(NumShards)
		expPerShard := expireCount / uint64(NumShards)
		expRemainder := expireCount % uint64(NumShards)
		for s := range NumShards {
			db.shards[s].keyCount.Store(perShard)
			db.shards[s].expireCount.Store(expPerShard)
		}
		db.shards[0].keyCount.Add(remainder)
		db.shards[0].expireCount.Add(expRemainder)
	}
	return nil
}

// Index returns the database's index.
func (db *DB) Index() int { return db.index }

// Len returns the total number of live keys across all shards, the value DBSIZE reports.
func (db *DB) Len() uint64 {
	var total uint64
	for s := range NumShards {
		total += db.shards[s].keyCount.Load()
		if p := db.shards[s].pendingUncertain.Load(); p > 0 {
			total += uint64(p)
		}
	}
	return total
}

// totalExpireCount returns the sum of expireCount across all shards.
func (db *DB) totalExpireCount() uint64 {
	var total uint64
	for s := range NumShards {
		total += db.shards[s].expireCount.Load()
	}
	return total
}

// loadShardTree returns the B-tree for shard s, opening it from the stored root.
// Returns nil when the shard has never been written. Caller holds shard mu.
func (db *DB) loadShardTree(s int) *btree.Tree {
	sh := &db.shards[s]
	if sh.tree != nil {
		return sh.tree
	}
	if sh.rootPage == format.NullPage {
		return nil
	}
	sh.tree = btree.Open(db.ks.pgr, sh.rootPage)
	return sh.tree
}

// ensureShardTree returns the B-tree for shard s, creating one if the shard is
// empty. Caller holds shard mu.Lock().
func (db *DB) ensureShardTree(s int) (*btree.Tree, error) {
	if t := db.loadShardTree(s); t != nil {
		return t, nil
	}
	t, err := btree.Create(db.ks.pgr)
	if err != nil {
		return nil, err
	}
	db.shards[s].tree = t
	db.shards[s].rootPage = t.Root()
	return t, nil
}

// Set writes key with the given body, type, encoding and TTL. A ttlMs of -1
// means no expiry; a positive ttlMs is an absolute Unix epoch in milliseconds.
// A key whose absolute TTL is already in the past is not written and any
// existing key under that name is removed, matching Redis's write-time expiry.
func (db *DB) Set(key, body []byte, typ, enc uint8, ttlMs int64) error {
	return db.set(key, body, typ, enc, ttlMs, 0)
}

// SetWithVersion is like Set but uses the pre-assigned version number instead
// of advancing the global write counter. The write-behind path calls this from
// the write worker to apply the B-tree write after PrepareWriteBehind has
// already made the value visible in the hot-value cache and wbPending table.
func (db *DB) SetWithVersion(key, body []byte, typ, enc uint8, ttlMs int64, preVersion uint64) error {
	return db.set(key, body, typ, enc, ttlMs, preVersion)
}

// set is the shared implementation of Set and SetWithVersion. When preVersion
// is zero it atomically increments the global write counter; otherwise it uses
// preVersion directly (the counter was already advanced by NextVersion).
// The caller must NOT hold any shard lock; set acquires the shard write lock
// internally and releases it before returning.
func (db *DB) set(key, body []byte, typ, enc uint8, ttlMs int64, preVersion uint64) error {
	if ttlMs >= 0 && ttlMs <= nowMillis() {
		_, err := db.Delete(key)
		return err
	}

	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	t, err := db.ensureShardTree(s)
	if err != nil {
		return err
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	var version uint64
	if preVersion > 0 {
		version = preVersion
	} else {
		version = db.ks.version.Add(1)
	}
	h := ValueHeader{
		Type:     typ,
		Encoding: enc,
		TTLms:    -1,
		Version:  version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	}

	// A body up to maxInlineBody rides in the leaf cell; a larger one goes to an
	// overflow chain and the cell carries only the header with BodyRef set.
	var cell []byte
	if len(body) <= maxInlineBody {
		h.Flags |= FlagInlineBody
		cell = h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
		cell = append(cell, body...)
	} else {
		head, werr := db.ks.writeOverflow(body)
		if werr != nil {
			return werr
		}
		h.BodyRef = uint64(head)
		cell = h.AppendTo(make([]byte, 0, HeaderSize))
	}

	// Upsert writes the new cell and returns the previous cell in a single
	// traversal, replacing the separate lookup + Put pair.
	prevCell, err := t.Upsert(ck, cell)
	if err != nil {
		return err
	}
	db.shards[s].rootPage = t.Root()

	var prev ValueHeader
	existed := prevCell != nil
	if existed {
		prev, _, _ = parseHeader(prevCell)
	}
	isNew := !existed
	hadTTL := existed && prev.HasTTL()

	// The previous value's overflow pages are now unreferenced.
	if existed && prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
		if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
			return err
		}
	}

	if isNew {
		db.shards[s].keyCount.Add(1)
	} else {
		db.ks.dataBytes.Add(-(int64(len(key)) + int64(prev.BodyLen) + entryOverhead))
	}
	db.ks.dataBytes.Add(int64(len(key)) + int64(len(body)) + entryOverhead)
	if h.HasTTL() && !hadTTL {
		db.shards[s].expireCount.Add(1)
	} else if !h.HasTTL() && hadTTL {
		db.shards[s].expireCount.Add(^uint64(0))
	}
	db.recordAccess(key, isNew)

	// For inline values, populate the hot-value cache with the new body so the
	// next Get returns it from cache without a B-tree walk. For overflow values,
	// invalidate so the next Get re-reads through the page chain.
	sk := string(key)
	if h.Flags&FlagInlineBody != 0 {
		db.hc.Load().cput(sk, cell[HeaderSize:], h)
	} else {
		db.hc.Load().cinvalidate(key)
	}
	if preVersion > 0 {
		db.removeWBPending(sk, preVersion)
	}
	return nil
}

// MaxInlineBody is the largest value body stored in the B-tree leaf cell. The
// write-behind fast path only handles bodies at or below this size; larger
// values require overflow page management that cannot be pre-staged.
const MaxInlineBody = maxInlineBody

// PrepareWriteBehind records a pending inline write in both the hot-value cache
// and the write-behind pending table. It is called synchronously by the
// write-behind path (under the engine read lock) before the async B-tree write
// is queued. After this returns, any Get for key sees body and hdr immediately,
// even before the write worker applies the B-tree write.
//
// The caller must have used ks.NextVersion to assign hdr.Version so the version
// counter advances before the entry is visible to readers.
func (db *DB) PrepareWriteBehind(key, body []byte, hdr ValueHeader) {
	sk := string(key)
	s := ShardOf(key)
	db.hc.Load().cput(sk, body, hdr)
	db.shards[s].wbMu.Lock()
	if db.shards[s].wbPending == nil {
		db.shards[s].wbPending = make(map[string]wbPendingEntry, 16)
	}
	_, alreadyPending := db.shards[s].wbPending[sk]
	db.shards[s].wbPending[sk] = wbPendingEntry{body: body, hdr: hdr}
	if !alreadyPending {
		// Key newly entered wbPending. We don't yet know if it is a new key
		// (not in the B-tree) or an update. Count it provisionally so Len()
		// includes it. removeWBPending decrements when the B-tree write lands.
		db.shards[s].pendingUncertain.Add(1)
	}
	db.shards[s].wbMu.Unlock()
}

// removeWBPending removes the write-behind pending entry for key if its version
// matches. Mismatched version means a newer write was already staged, so the
// older entry should not be removed.
func (db *DB) removeWBPending(key string, version uint64) {
	s := ShardOf([]byte(key))
	db.shards[s].wbMu.Lock()
	if e, ok := db.shards[s].wbPending[key]; ok && e.hdr.Version == version {
		delete(db.shards[s].wbPending, key)
		db.shards[s].pendingUncertain.Add(-1)
	}
	db.shards[s].wbMu.Unlock()
}

// getWBPending returns the pending write-behind value for key, if any. The read
// path calls this on a hot-cache miss to avoid serving a stale B-tree value
// when the write worker has not yet applied the write.
func (db *DB) getWBPending(key string) ([]byte, ValueHeader, bool) {
	s := ShardOf([]byte(key))
	db.shards[s].wbMu.RLock()
	e, ok := db.shards[s].wbPending[key]
	db.shards[s].wbMu.RUnlock()
	if !ok {
		return nil, ValueHeader{}, false
	}
	return e.body, e.hdr, true
}

// HotGet is a lock-free best-effort read that only consults the hot-value cache.
// It returns (body, hdr, true) on a cache hit and (nil, _, false) on a miss.
// The caller must fall back to Get on a miss; HotGet never touches the B-tree.
//
// HotGet may be called without the engine read lock because the hot-cache shards
// each carry their own mutex, and the cache pointer itself is stored as an
// atomic.Pointer so SwapDB's exchange is race-safe. The trade-off: a HotGet
// during a concurrent FlushDB or SwapDB may observe either the pre- or
// post-operation state depending on the shard lock interleaving. Both outcomes
// are valid under Redis's linearizability model — the operation simply resolves
// before or after the flush/swap.
func (db *DB) HotGet(key []byte) (body []byte, hdr ValueHeader, found bool) {
	// cget takes []byte and uses string(key) directly for the map lookup, which
	// the Go compiler optimizes to a temporary — zero allocations. cget also
	// updates the entry's atime atomically so the eviction sampler sees a fresh
	// timestamp without us taking any lock here.
	b, h, ok := db.hc.Load().cget(key)
	if !ok {
		return nil, ValueHeader{}, false
	}
	if db.expired(h) {
		// Entry expired after we cached it; invalidate so the next active
		// expiry cycle handles the B-tree deletion. Return not-found rather than
		// clearing it here — we do not have the write lock.
		db.hc.Load().cinvalidate(key)
		return nil, ValueHeader{}, false
	}
	// cget already updated the hot-cache entry's atime atomically. recordAccess
	// is still called to keep the LFU frequency counter current so OBJECT FREQ
	// and LFU eviction remain accurate for hot-cache hits.
	db.recordAccess(key, false)
	return b, h, true
}

// Get returns the body and header for key and records an LRU and LFU access. It
// is the read path data commands use. found is false when the key is absent or
// has expired; an expired key is deleted as a side effect (lazy expiry).
func (db *DB) Get(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	return db.get(key, true)
}

// Peek is Get without recording an access, the read path introspection commands
// use so OBJECT, EXISTS and friends do not reset a key's idle time or bump its
// frequency.
func (db *DB) Peek(key []byte) (body []byte, hdr ValueHeader, found bool, err error) {
	return db.get(key, false)
}

// get is the shared read path. When touch is set it records an access for the
// eviction bookkeeping. An expired key is returned as not-found immediately;
// its B-tree deletion is deferred to the next active expiry cycle so this
// function is safe to call concurrently with writes on other shards.
func (db *DB) get(key []byte, touch bool) (body []byte, hdr ValueHeader, found bool, err error) {
	// Hot-cache check comes before the string conversion: cget and cinvalidate
	// take []byte and do the map op with string(key) as a compiler-elided
	// temporary, so a hot hit returns without any heap allocation.
	if touch {
		if b, h, ok := db.hc.Load().cget(key); ok {
			if db.expired(h) {
				db.hc.Load().cinvalidate(key)
				return nil, ValueHeader{}, false, nil
			}
			db.recordAccess(key, false)
			return b, h, true, nil
		}
	}
	// String conversion deferred to here: on a hot-cache hit the allocation
	// never happens. The wbPending map and B-tree paths need the string key.
	sk := string(key)
	// Check the write-behind pending table before falling through to the B-tree.
	if b, h, ok := db.getWBPending(sk); ok {
		if db.expired(h) {
			return nil, ValueHeader{}, false, nil
		}
		if touch {
			db.recordAccess(key, false)
			db.hc.Load().cput(sk, b, h)
		}
		return b, h, true, nil
	}

	// B-tree read: take the shard read lock so concurrent shard writes are
	// excluded from the same page range.
	s := ShardOf(key)
	db.shards[s].mu.RLock()
	t := db.loadShardTree(s)
	if t == nil {
		db.shards[s].mu.RUnlock()
		return nil, ValueHeader{}, false, nil
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	h, cell, ok, readErr := db.read(t, ck)
	ckPool.Put(ckp)
	db.shards[s].mu.RUnlock()

	if readErr != nil || !ok {
		return nil, ValueHeader{}, false, readErr
	}
	if db.expired(h) {
		return nil, ValueHeader{}, false, nil
	}
	if touch {
		db.recordAccess(key, false)
	}
	var out []byte
	if h.Flags&FlagInlineBody == 0 {
		out, err = db.ks.readOverflow(uint32(h.BodyRef), int(h.BodyLen))
		if err != nil {
			return nil, ValueHeader{}, false, err
		}
	} else {
		out = cell
	}
	if touch {
		db.hc.Load().cput(sk, out, h)
	}
	return out, h, true, nil
}

// Exists reports whether key is present and unexpired without recording an
// access. An expired key is deleted.
func (db *DB) Exists(key []byte) (bool, error) {
	_, _, found, err := db.Peek(key)
	return found, err
}

// Delete removes key. It returns whether a key was present.
// The caller must NOT hold any shard lock; Delete acquires the shard write lock
// internally and releases it before returning.
func (db *DB) Delete(key []byte) (bool, error) {
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	t := db.loadShardTree(s)
	if t == nil {
		return false, nil
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	prev, existed, err := db.lookup(t, ck)
	if err != nil || !existed {
		return false, err
	}
	ok, err := t.Delete(ck)
	if err != nil {
		return false, err
	}
	if ok {
		db.shards[s].rootPage = t.Root()
		db.shards[s].keyCount.Add(^uint64(0))
		db.ks.dataBytes.Add(-(int64(len(key)) + int64(prev.BodyLen) + entryOverhead))
		db.dropAccess(key)
		db.hc.Load().cinvalidate(key)
		if prev.HasTTL() {
			db.shards[s].expireCount.Add(^uint64(0))
		}
		if prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
			if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
				return ok, err
			}
		}
	}
	return ok, nil
}

// ActiveExpireCycle walks every database for volatile keys whose TTL has passed,
// deletes them, and records each in the expired log so the command layer can fire
// the "expired" event. It returns the number of keys removed. A database with no
// volatile keys is skipped on the cheap expireCount guard.
func (ks *Keyspace) ActiveExpireCycle() (int, error) {
	now := nowMillis()
	total := 0
	for _, db := range ks.dbs {
		if db.totalExpireCount() == 0 {
			continue
		}
		keys, err := db.expiredVolatileKeys(now)
		if err != nil {
			return total, err
		}
		for _, k := range keys {
			ok, err := db.Delete(k)
			if err != nil {
				return total, err
			}
			if ok {
				ks.expiredMu.Lock()
				ks.expiredLog = append(ks.expiredLog, ExpiredKey{DB: db.index, Key: k})
				ks.expiredMu.Unlock()
				total++
			}
		}
	}
	return total, nil
}

// expiredVolatileKeys returns the raw names of every key in the DB whose absolute
// TTL is at or before now. It scans each shard under a read lock, collecting
// names into a flat slice rather than deleting during the walk.
func (db *DB) expiredVolatileKeys(now int64) ([][]byte, error) {
	var out [][]byte
	for s := range NumShards {
		if db.shards[s].expireCount.Load() == 0 {
			continue
		}
		db.shards[s].mu.RLock()
		t := db.loadShardTree(s)
		if t == nil {
			db.shards[s].mu.RUnlock()
			continue
		}
		c := t.Cursor()
		if err := c.First(); err != nil {
			db.shards[s].mu.RUnlock()
			return nil, err
		}
		for c.Valid() {
			h, _, ok := parseHeader(c.Value())
			if ok && h.HasTTL() && h.TTLms <= now {
				out = append(out, copyRaw(c.Key()))
			}
			if err := c.Next(); err != nil {
				db.shards[s].mu.RUnlock()
				return nil, err
			}
		}
		db.shards[s].mu.RUnlock()
	}
	return out, nil
}

// read fetches the raw cell for a composite key and splits off its header.
func (db *DB) read(t *btree.Tree, ck []byte) (ValueHeader, []byte, bool, error) {
	cell, ok, err := t.Get(ck)
	if err != nil || !ok {
		return ValueHeader{}, nil, false, err
	}
	h, n, valid := parseHeader(cell)
	if !valid {
		return ValueHeader{}, nil, false, nil
	}
	return h, cell[n:], true, nil
}

// lookup returns just the header for a composite key.
func (db *DB) lookup(t *btree.Tree, ck []byte) (ValueHeader, bool, error) {
	h, _, ok, err := db.read(t, ck)
	return h, ok, err
}

// expired reports whether a header's TTL has passed.
func (db *DB) expired(h ValueHeader) bool {
	return h.HasTTL() && h.TTLms <= nowMillis()
}

// Commit persists the catalog and every DB's shard roots, then commits the
// pager. The catalog page is allocated on first commit that has data to record.
// Each shard's read lock is held briefly while we snapshot its root and
// counters, so shard writers always see a consistent view at commit time.
func (ks *Keyspace) Commit() error {
	if ks.catRoot == format.NullPage {
		pg, err := ks.pgr.Allocate()
		if err != nil {
			return err
		}
		ks.catRoot = pg.No
		ks.pgr.Unpin(pg, false)
	}
	pg, err := ks.pgr.Get(ks.catRoot)
	if err != nil {
		return err
	}
	for i := range pg.Data {
		pg.Data[i] = 0
	}
	hdr := format.PageHeader{Type: format.PageTypeCatalog, FreeStart: catalogDataStart, FreeEnd: uint16(len(pg.Data))}
	if err := pg.PutHeader(hdr); err != nil {
		ks.pgr.Unpin(pg, false)
		return err
	}
	for i, db := range ks.dbs {
		off := catalogDataStart + i*recordSize
		if off+recordSize > len(pg.Data) {
			break
		}
		rec := pg.Data[off:]
		var keyCount, expireCount uint64
		for s := range NumShards {
			db.shards[s].mu.RLock()
			encoding.PutU32(rec[s*4:], db.shards[s].rootPage)
			keyCount += db.shards[s].keyCount.Load()
			expireCount += db.shards[s].expireCount.Load()
			db.shards[s].mu.RUnlock()
		}
		base := NumShards * 4
		encoding.PutU64(rec[base:], keyCount)
		encoding.PutU64(rec[base+8:], expireCount)
		encoding.PutU32(rec[base+16:], db.avgTTL)
		encoding.PutU32(rec[base+20:], uint32(NumShards))
	}
	ks.pgr.Unpin(pg, true)

	if err := ks.pgr.Commit(pager.CommitInfo{
		CatalogRoot:    ks.catRoot,
		SetCatalogRoot: true,
		SystemRoot:     ks.sysRoot,
		SetSystemRoot:  true,
	}); err != nil {
		return err
	}
	ks.assertConsistent()
	return nil
}
