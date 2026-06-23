package keyspace

import (
	"sync"
	"sync/atomic"
)

// dbCache is a small sharded cache that stores decoded values above the B-tree.
// A db.get() hit returns the body and header without walking the B-tree or
// decoding a cell. Invalidation on every write keeps consistency simple: any
// write to a key removes its cache entry so the next read falls through to the
// B-tree.
//
// The cache is per-DB, so the key is just the raw Redis key string. With 64
// shards each holding up to 256 entries (16 384 entries total per DB), a
// shard RLock on a hot key never needs to touch the eviction path.
//
// This is intentionally not a full W-TinyLFU cache. That upgrade (frequency
// sketch, admission filter, scan-aware eviction) is spec perf/03 and can be
// layered on later without changing the callsites.

const (
	dbCacheShards   = 64
	dbCacheCapacity = 16384
	dbCachePerShard = dbCacheCapacity / dbCacheShards // 256
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
	val *cachedValue // pointer so atime can be updated through a shard read lock
}

type cacheShard struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	// lru tracks insertion order so we can evict the oldest when the shard
	// is full. A simple slice is fast enough for 256-entry shards.
	lru []string
}

type dbCache struct {
	shards [dbCacheShards]cacheShard
}

func newDBCache() *dbCache {
	c := &dbCache{}
	for i := range c.shards {
		c.shards[i].entries = make(map[string]*cacheEntry, dbCachePerShard)
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
	// Update atime atomically without holding any lock. Even if the entry is
	// evicted from the map between RUnlock and this Store, the pointer e is
	// still live (Go GC is precise) and the write is harmless.
	e.val.atime.Store(nowSeconds())
	return e.val.body, e.val.hdr, true
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
	return e.val.atime.Load(), true
}

// cput stores body and header under key, evicting the LRU entry when the shard
// is full. key is a string because cput is only called from write paths where
// the string is already available (set path, wbPending warm-up).
func (c *dbCache) cput(key string, body []byte, hdr ValueHeader) {
	sh := &c.shards[c.shardIdxStr(key)]
	sh.mu.Lock()
	if _, exists := sh.entries[key]; !exists {
		sh.lru = append(sh.lru, key)
		if len(sh.entries) >= dbCachePerShard {
			// Evict the oldest entry whose key still exists in the map.
			for len(sh.lru) > 0 {
				victim := sh.lru[0]
				sh.lru = sh.lru[1:]
				if _, present := sh.entries[victim]; present {
					delete(sh.entries, victim)
					break
				}
			}
		}
	}
	cv := &cachedValue{body: body, hdr: hdr}
	cv.atime.Store(nowSeconds())
	sh.entries[key] = &cacheEntry{key: key, val: cv}
	sh.mu.Unlock()
}

// cinvalidate removes key from the cache. Called after any write to that key.
//
// key is []byte so callers do not need to allocate a string on the hot write
// path. The map delete uses string(key) directly, which the Go compiler
// optimizes to a temporary that does not escape to the heap.
func (c *dbCache) cinvalidate(key []byte) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.Lock()
	delete(sh.entries, string(key)) // compiler: no alloc for delete with string([]byte)
	sh.mu.Unlock()
}

// cclear empties every shard. Called on FLUSHDB.
func (c *dbCache) cclear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.entries = make(map[string]*cacheEntry, dbCachePerShard)
		sh.lru = sh.lru[:0]
		sh.mu.Unlock()
	}
}
