package keyspace

import (
	"sync"
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

type cachedValue struct {
	body []byte
	hdr  ValueHeader
}

type cacheEntry struct {
	key string
	val cachedValue
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

func (c *dbCache) shardIdx(key string) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return int(h & (dbCacheShards - 1))
}

// cget returns the cached body and header for key. ok is false on a miss.
func (c *dbCache) cget(key string) ([]byte, ValueHeader, bool) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()
	if !ok {
		return nil, ValueHeader{}, false
	}
	return e.val.body, e.val.hdr, true
}

// cput stores body and header under key, evicting the LRU entry when the shard
// is full.
func (c *dbCache) cput(key string, body []byte, hdr ValueHeader) {
	sh := &c.shards[c.shardIdx(key)]
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
	sh.entries[key] = &cacheEntry{key: key, val: cachedValue{body: body, hdr: hdr}}
	sh.mu.Unlock()
}

// cinvalidate removes key from the cache. Called after any write to that key.
func (c *dbCache) cinvalidate(key string) {
	sh := &c.shards[c.shardIdx(key)]
	sh.mu.Lock()
	delete(sh.entries, key)
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
