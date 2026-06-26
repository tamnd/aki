package keyspace

import (
	"sync"
	"sync/atomic"
)

// dbCache is a sharded value cache that stores decoded values above the B-tree.
// A db.get() hit returns the body and header without walking the B-tree or
// decoding a cell. Invalidation on every write keeps consistency simple: any
// write to a key removes its cache entry so the next read falls through to the
// B-tree.
//
// The cache is per-DB, so the key is just the raw Redis key string. It is sharded
// into dbCacheShards independent stripes, each with its own mutex, so a hit on one
// shard never blocks a hit on another.
//
// The cache is bounded by bytes, not by entry count (perf/03 section 13.3). Each
// entry costs len(key) + len(body) + entryOverhead, every shard tracks its used
// bytes, and a shard evicts its LRU entries until it is back under its byte budget.
// A byte budget is the right bound for a value cache because entries vary in size:
// a fixed entry count is far too small for a working set of many small keys (the
// case the GET benchmark exercises) and needlessly large for a handful of big ones.
// The budget is value-cache-fraction of the page budget, sized from buffer-pool-size
// at open and divided evenly across the shards.
//
// This is still not a full W-TinyLFU cache. That upgrade (frequency sketch,
// admission filter, scan-aware eviction) is spec perf/03 and can be layered on
// later without changing the callsites; the byte budget and the eviction hook are
// already where that policy will attach.

const (
	dbCacheShards = 64

	// defaultValueCacheBytes is the cache byte budget used when Open is given no
	// explicit budget: tests and the offline check path that build a keyspace
	// without the server's buffer-pool sizing. It is 0.10 of the 128 MiB default
	// buffer pool, the same tenth perf/03 section 13.2 gives the value cache, so a
	// keyspace opened without configuration behaves like a default server.
	defaultValueCacheBytes = 128 << 20 / 10 // ~12.8 MiB

	// minShardBytes floors each shard's budget so a tiny configured cache still
	// holds a few entries per shard rather than thrashing on every put.
	minShardBytes = 4 << 10
)

// cachedValue holds the cached body, header, and last-access time for one key.
// atime is updated atomically on each HotGet hit without holding any lock, so
// the eviction sampler always sees a recent timestamp for keys that are actively
// served from the hot cache rather than via the B-tree path.
//
// cachedValue is always accessed through a pointer (cacheEntry.val) so the
// atomic.Uint32 field is never value-copied.
type cachedValue struct {
	body  []byte
	hdr   ValueHeader
	atime atomic.Uint32 // Unix seconds of last access; updated on each HotGet hit
}

type cacheEntry struct {
	key string
	// val is an atomic pointer so cput can swap in a new cachedValue for an
	// existing key without allocating a new cacheEntry. Concurrent cget calls
	// that captured the cacheEntry pointer before the swap read the old value
	// safely — the atomic.Pointer.Store makes the new value visible without a
	// lock, and the old cachedValue stays live until GC collects it.
	val atomic.Pointer[cachedValue]
}

// entryBytes is the accounted cost of caching key with a body of bodyLen bytes:
// the key, the body, and a fixed overhead for the entry struct, the map slot, and
// the LRU link. It mirrors the entryOverhead the keyspace folds into its live-data
// estimate (perf/03 section 13.3) so the cache and the dataBytes accounting agree.
func entryBytes(keyLen, bodyLen int) int64 {
	return int64(keyLen) + int64(bodyLen) + entryOverhead
}

type cacheShard struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	// lru tracks insertion order so the oldest entry is evicted first when the
	// shard is over its byte budget. head is the index of the oldest live key; the
	// slice is compacted when head grows past half its length so the backing array
	// does not leak under a steady stream of evictions.
	lru  []string
	head int
	// used is the shard's accounted byte cost. It is read on the lock-free cput
	// fast path and written under mu on the slow path, so it is atomic; the slow
	// path holds mu and still uses atomic stores so the fast path never sees a torn
	// value. budget is the shard's byte cap, set once at construction.
	used   atomic.Int64
	budget int64
}

type dbCache struct {
	shards [dbCacheShards]cacheShard
}

// newDBCache builds a value cache whose total byte budget is capacityBytes,
// divided evenly across the shards. A non-positive capacity falls back to the
// default so tests and the offline check path get a working cache.
func newDBCache(capacityBytes int64) *dbCache {
	if capacityBytes <= 0 {
		capacityBytes = defaultValueCacheBytes
	}
	perShard := capacityBytes / dbCacheShards
	if perShard < minShardBytes {
		perShard = minShardBytes
	}
	c := &dbCache{}
	for i := range c.shards {
		c.shards[i].entries = make(map[string]*cacheEntry)
		c.shards[i].budget = perShard
	}
	return c
}

// shardIdx returns the shard index for a []byte key. Using []byte avoids the
// string allocation that sharding by string would require on the read hot path.
func (c *dbCache) shardIdx(key []byte) int {
	h := uint64(14695981039346656037)
	for _, b := range key {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return int(h & (dbCacheShards - 1))
}

// shardIdxStr returns the shard index for a string key. Used by cput, which
// already holds the key as a string for map storage.
func (c *dbCache) shardIdxStr(key string) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return int(h & (dbCacheShards - 1))
}

// cget returns the cached body and header for key, and updates the entry's
// access timestamp atomically. ok is false on a miss.
//
// key is []byte so callers on the hot read path (HotGet) never allocate a
// string. The map lookup uses string(key) directly, which the Go compiler
// optimizes to a temporary that does not escape to the heap.
func (c *dbCache) cget(key []byte) ([]byte, ValueHeader, bool) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.RLock()
	e, ok := sh.entries[string(key)] // compiler: no alloc for map index with string([]byte)
	sh.mu.RUnlock()
	if !ok {
		return nil, ValueHeader{}, false
	}
	// Load the value atomically. cput may swap e.val at any time after we
	// released RLock, but atomic.Pointer.Load gives us a consistent snapshot
	// of whichever cachedValue was current at the moment of this load.
	cv := e.val.Load()
	// Update atime atomically without holding any lock. Even if cput replaces
	// cv between our Load and this Store, the write is to a still-live object
	// and is harmless (the new cv will get its own atime stamp on the next hit).
	// coarseSeconds reads the cron-cached clock so the GET hot path never pays a
	// time.Now syscall per hit.
	cv.atime.Store(coarseSeconds())
	return cv.body, cv.hdr, true
}

// cgetAtime returns the last-access time for key as recorded in the hot cache.
// Used by accessMetrics to prefer hot-cache atime over the potentially-stale
// access map entry for keys that are actively served via HotGet.
func (c *dbCache) cgetAtime(key []byte) (uint32, bool) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.RLock()
	e, ok := sh.entries[string(key)]
	sh.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return e.val.Load().atime.Load(), true
}

// cput stores body and header under key, evicting LRU entries when the shard is
// over its byte budget. key is a string because cput is only called from write
// paths where the string is already available (set path, wbPending warm-up).
//
// For an existing key, cput swaps in a new cachedValue atomically into the
// existing cacheEntry without allocating a new entry. For a new key, it
// allocates both a cacheEntry and a cachedValue.
//
// cput is version-aware: it never overwrites a cached value whose version is
// strictly newer than hdr.Version. Every write carries a unique increasing
// version from the global counter, so this keeps the cache monotonic across the
// concurrent paths that touch it. The async write-behind worker can apply an
// older staged write to the B-tree after a newer read-modify-write has already
// cached its result, and a read-path warm-up can land after a newer write
// staged its value. Without this guard the older cput would clobber the newer
// cache entry and the next read-modify-write would read a stale value and lose
// an update.
func (c *dbCache) cput(key string, body []byte, hdr ValueHeader) {
	sh := &c.shards[c.shardIdxStr(key)]
	cv := &cachedValue{body: body, hdr: hdr}
	cv.atime.Store(coarseSeconds())
	newCost := entryBytes(len(key), len(body))

	// Fast path: the key is already cached, which is the steady state under a
	// stream of writes to the same key. The update is only an atomic pointer swap
	// into the existing cacheEntry, so it needs no write lock; an RLock to find
	// the entry plus a version-guarded compare-and-swap is enough. This turns the
	// many connections that hammer one key in lockstep from serialized write-lock
	// holders into concurrent readers. The map read still takes the shard RLock so
	// it never races the slow path's map mutation.
	sh.mu.RLock()
	e, exists := sh.entries[key]
	sh.mu.RUnlock()
	if exists {
		for {
			cur := e.val.Load()
			if cur != nil && cur.hdr.Version > hdr.Version {
				// A strictly newer value is already cached; do not clobber it.
				return
			}
			if e.val.CompareAndSwap(cur, cv) {
				// Adjust the shard's byte total by the change in body size. The
				// key length is unchanged, so only the body delta matters.
				if cur != nil {
					sh.used.Add(int64(len(body)) - int64(len(cur.body)))
				}
				return
			}
			// Lost the swap to a concurrent cput; reload and re-check the version
			// guard so monotonicity holds, then retry.
		}
	}

	// Slow path: a new key needs the write lock to insert into the map, append to
	// the LRU ring, and evict down to the byte budget. The key may have been
	// inserted between the RLock above and this Lock, so the existing-key branch
	// here repeats the version-guarded swap rather than assuming the key is absent.
	sh.mu.Lock()
	if e, exists := sh.entries[key]; exists {
		if cur := e.val.Load(); cur != nil && cur.hdr.Version > hdr.Version {
			// A strictly newer value is already cached; do not clobber it.
			sh.mu.Unlock()
			return
		}
		// Key already in cache: swap the value pointer in-place. No new
		// cacheEntry allocation, saving one heap object per update write.
		if cur := e.val.Load(); cur != nil {
			sh.used.Add(int64(len(body)) - int64(len(cur.body)))
		}
		e.val.Store(cv)
		sh.mu.Unlock()
		return
	}
	// New key: allocate a cacheEntry, record its cost, and evict the oldest
	// entries until the shard is back under its byte budget.
	ne := &cacheEntry{key: key}
	ne.val.Store(cv)
	sh.entries[key] = ne
	sh.lru = append(sh.lru, key)
	sh.used.Add(newCost)
	sh.evictLocked()
	sh.mu.Unlock()
}

// evictLocked removes the oldest entries until the shard is back within its byte
// budget. The caller holds sh.mu. It never evicts the only entry below the floor,
// so a single oversized value can still be cached; the budget is a soft cap that a
// lone large entry may exceed, matching how a buffer pool always keeps at least the
// page it just faulted in.
func (sh *cacheShard) evictLocked() {
	for sh.used.Load() > sh.budget && sh.head < len(sh.lru) {
		if len(sh.entries) <= 1 {
			return
		}
		victim := sh.lru[sh.head]
		sh.head++
		e, present := sh.entries[victim]
		if !present {
			continue // already invalidated; its bytes were removed at delete time
		}
		delete(sh.entries, victim)
		if cur := e.val.Load(); cur != nil {
			sh.used.Add(-entryBytes(len(victim), len(cur.body)))
		}
	}
	sh.compactLRU()
}

// compactLRU drops the consumed prefix of the lru slice once it grows past half
// the slice, so a steady stream of evictions does not leak the backing array. The
// caller holds sh.mu.
func (sh *cacheShard) compactLRU() {
	if sh.head == 0 || sh.head < len(sh.lru)/2 {
		return
	}
	n := copy(sh.lru, sh.lru[sh.head:])
	sh.lru = sh.lru[:n]
	sh.head = 0
}

// cinvalidate removes key from the cache. Called after any write to that key.
//
// key is []byte so callers do not need to allocate a string on the hot write
// path. The map delete uses string(key) directly, which the Go compiler
// optimizes to a temporary that does not escape to the heap.
func (c *dbCache) cinvalidate(key []byte) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.Lock()
	if e, ok := sh.entries[string(key)]; ok {
		delete(sh.entries, string(key)) // compiler: no alloc for delete with string([]byte)
		if cur := e.val.Load(); cur != nil {
			sh.used.Add(-entryBytes(len(e.key), len(cur.body)))
		}
	}
	sh.mu.Unlock()
}

// cclear empties every shard. Called on FLUSHDB.
func (c *dbCache) cclear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.entries = make(map[string]*cacheEntry)
		sh.lru = sh.lru[:0]
		sh.head = 0
		sh.used.Store(0)
		sh.mu.Unlock()
	}
}
