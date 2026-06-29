package keyspace

import (
	"fmt"
	"testing"
)

// TestValueCacheByteBudget checks that the cache evicts by bytes and keeps each
// shard within its budget. It builds a cache whose per-shard budget holds only a
// few entries, fills one shard well past that, and verifies the shard's accounted
// bytes stay at or under the budget while the newest keys survive and the oldest
// were evicted. This is the property that makes the cache hold a working set sized
// to the configured memory rather than a fixed entry count.
func TestValueCacheByteBudget(t *testing.T) {
	// minShardBytes (4 KiB) is the per-shard floor. With ~128-byte bodies each
	// entry costs body + key + entryOverhead, so a 4 KiB shard holds a handful.
	c := newDBCache(minShardBytes * dbCacheShards)
	body := make([]byte, 128)

	// Drive 500 distinct keys into one shard by hashing the key to a fixed shard.
	// Picking the shard for each key keeps the test independent of the hash.
	const target = 7
	put := 0
	var lastKeys []string
	for i := 0; put < 300; i++ {
		key := fmt.Sprintf("budget-key-%d", i)
		if c.shardIdxStr(key) != target {
			continue
		}
		c.cput(key, body, ValueHeader{Version: uint64(i)})
		lastKeys = append(lastKeys, key)
		put++
	}

	sh := &c.shards[target]
	sh.mu.RLock()
	used := sh.used.Load()
	live := len(sh.entries)
	sh.mu.RUnlock()

	if used > sh.budget {
		t.Fatalf("shard used %d exceeds budget %d", used, sh.budget)
	}
	if live == 0 || live >= put {
		t.Fatalf("expected eviction: live=%d of %d put", live, put)
	}
	// The most recently inserted key must still be present; an early one must be gone.
	if _, _, ok := c.cget([]byte(lastKeys[len(lastKeys)-1])); !ok {
		t.Fatal("most recent key was evicted; LRU order is wrong")
	}
	if _, _, ok := c.cget([]byte(lastKeys[0])); ok {
		t.Fatal("oldest key survived past the byte budget; eviction did not run")
	}

	// Accounting must return to zero once every live key is invalidated.
	sh.mu.RLock()
	keys := make([]string, 0, len(sh.entries))
	for k := range sh.entries {
		keys = append(keys, k)
	}
	sh.mu.RUnlock()
	for _, k := range keys {
		c.cinvalidate([]byte(k))
	}
	if got := sh.used.Load(); got != 0 {
		t.Fatalf("after invalidating every entry, shard used = %d want 0", got)
	}
}

// TestValueCacheUpdateAccounting checks that overwriting a key with a larger or
// smaller body adjusts the shard's accounted bytes by the body delta, so a stream
// of in-place updates does not drift the byte total.
func TestValueCacheUpdateAccounting(t *testing.T) {
	c := newDBCache(defaultValueCacheBytes)
	key := "acct"
	sh := &c.shards[c.shardIdxStr(key)]

	c.cput(key, make([]byte, 100), ValueHeader{Version: 1})
	base := sh.used.Load()
	want := entryBytes(len(key), 100)
	if base != want {
		t.Fatalf("after first put, used = %d want %d", base, want)
	}
	// Grow the body: used must rise by exactly the body delta.
	c.cput(key, make([]byte, 300), ValueHeader{Version: 2})
	if got := sh.used.Load(); got != want+200 {
		t.Fatalf("after grow, used = %d want %d", got, want+200)
	}
	// Shrink the body: used must fall back.
	c.cput(key, make([]byte, 50), ValueHeader{Version: 3})
	if got := sh.used.Load(); got != want-50 {
		t.Fatalf("after shrink, used = %d want %d", got, want-50)
	}
}

// TestHotCacheHitSkipsBTree confirms that a second Get on the same key is
// served from the hot cache without walking the B-tree. We verify this
// indirectly: populate a key, read it once (populates cache), overwrite the
// B-tree by calling Set again (which must invalidate the cache), then read
// again. The second read must return the new value, not the old cached one.
func TestHotCacheHitSkipsBTree(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	if err := db.Set([]byte("k"), []byte("v1"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	// First Get: cache miss, populates cache.
	b, _, found, _ := db.Get([]byte("k"))
	if !found || string(b) != "v1" {
		t.Fatalf("first Get = %q found=%v", b, found)
	}
	// Second Get: cache hit.
	b, _, found, _ = db.Get([]byte("k"))
	if !found || string(b) != "v1" {
		t.Fatalf("second Get = %q found=%v", b, found)
	}

	// Overwrite: must invalidate the cache entry.
	if err := db.Set([]byte("k"), []byte("v2"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	// Get after overwrite must return the new value, not the cached v1.
	b, _, found, _ = db.Get([]byte("k"))
	if !found || string(b) != "v2" {
		t.Fatalf("Get after overwrite = %q found=%v want v2", b, found)
	}
}

// TestHotCacheDeleteInvalidates checks that deleting a key removes it from the
// cache so a subsequent Get returns not-found.
func TestHotCacheDeleteInvalidates(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	_ = db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1)
	_, _, _, _ = db.Get([]byte("k")) // populate cache
	ok, err := db.Delete([]byte("k"))
	if !ok || err != nil {
		t.Fatalf("Delete = %v %v", ok, err)
	}
	_, _, found, _ := db.Get([]byte("k"))
	if found {
		t.Fatal("Get after Delete returned found; cache was not invalidated")
	}
}

// TestHotCacheFlushClears verifies that FlushDB empties the cache so a Get
// after the flush sees the database as empty.
func TestHotCacheFlushClears(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	_ = db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1)
	_, _, _, _ = db.Get([]byte("k")) // populate cache
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	_, _, found, _ := db.Get([]byte("k"))
	if found {
		t.Fatal("Get after Flush returned found; cache was not cleared")
	}
}

// TestHotCachePeekDoesNotPopulate confirms that Peek (touch=false) does not
// populate the cache. Set populates it for inline values; after evicting the
// entry manually, a Peek must leave the cache cold while Get warms it again.
func TestHotCachePeekDoesNotPopulate(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	_ = db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1)
	// Set populates the hot cache for inline values.
	if _, _, ok := db.hc.Load().cget([]byte("k")); !ok {
		t.Fatal("Set must populate the hot cache for inline values")
	}

	// Evict the entry to test Peek's non-populating property in isolation.
	db.hc.Load().cinvalidate([]byte("k"))

	// Peek should not re-populate the cache.
	b, _, found, _ := db.Peek([]byte("k"))
	if !found || string(b) != "v" {
		t.Fatalf("Peek = %q found=%v", b, found)
	}
	if _, _, ok := db.hc.Load().cget([]byte("k")); ok {
		t.Fatal("Peek must not populate the hot cache")
	}

	// Get re-warms the cache. Under the read-miss admission doorkeeper (note 247) a
	// key arriving fresh from the B-tree is admitted on its second sighting, not its
	// first, so the first Get after the cold cache records the sighting and the
	// second Get warms it. Set force-admits, but this key was invalidated above, so
	// the read path sees it as a first sighting.
	b, _, found, _ = db.Get([]byte("k"))
	if !found || string(b) != "v" {
		t.Fatalf("first Get after Peek = %q found=%v", b, found)
	}
	b, _, found, _ = db.Get([]byte("k"))
	if !found || string(b) != "v" {
		t.Fatalf("second Get after Peek = %q found=%v", b, found)
	}
	if _, _, ok := db.hc.Load().cget([]byte("k")); !ok {
		t.Fatal("Get must populate the hot cache on the second read")
	}
}

// TestValueCacheAdmissionDoorkeeper checks the read-miss admission filter: a gated
// put (cputRead, the B-tree read path) caches a brand-new key only on its second
// sighting within the epoch, while a force-admit put (cput, the write path) caches
// immediately. This is the filter that stops a uniform-random scan over a working
// set larger than the cache from thrashing it (note 247).
func TestValueCacheAdmissionDoorkeeper(t *testing.T) {
	c := newDBCache(defaultValueCacheBytes)
	body := []byte("v")

	// First gated sighting of a fresh key: recorded by the doorkeeper, not cached.
	c.cputRead("door", body, ValueHeader{Version: 1})
	if _, _, ok := c.cget([]byte("door")); ok {
		t.Fatal("first read-miss put admitted the key; doorkeeper did not gate it")
	}
	// Second gated sighting: admitted.
	c.cputRead("door", body, ValueHeader{Version: 1})
	if _, _, ok := c.cget([]byte("door")); !ok {
		t.Fatal("second read-miss put did not admit the key")
	}

	// A force-admit put (write path) caches a brand-new key on the first call.
	c.cput("force", body, ValueHeader{Version: 1})
	if _, _, ok := c.cget([]byte("force")); !ok {
		t.Fatal("force-admit put did not cache the key immediately")
	}
}

// TestGetUncachedReadsWithoutInitialProbe checks that GetUncached, the variant
// handleGet calls after a viewHotGet cache miss, returns the same value as Get and
// still warms the cache on the way out. It must not depend on the initial cache
// probe Get does, because its caller already probed and missed: GetUncached skips
// that probe and reads the write-behind overlay and the B-tree directly. We verify
// it reads a B-tree-backed key correctly, warms the cache, and reflects an
// overwrite, matching Get's contract.
func TestGetUncachedReadsWithoutInitialProbe(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	if err := db.Set([]byte("k"), []byte("v1"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	// Drop the entry Set warmed so the read goes through the B-tree, the path that
	// exercises the skipped-probe code rather than a cache hit.
	db.hc.Load().cinvalidate([]byte("k"))

	// GetUncached must read the B-tree value without the initial cache probe.
	b, _, found, err := db.GetUncached([]byte("k"))
	if err != nil || !found || string(b) != "v1" {
		t.Fatalf("GetUncached = %q found=%v err=%v want v1", b, found, err)
	}
	// Read-miss warming runs through the admission doorkeeper, so a one-hit-wonder
	// is not cached on its first sighting. A second GetUncached crosses the
	// doorkeeper and warms the cache, matching Get's gated read-miss contract.
	b, _, found, err = db.GetUncached([]byte("k"))
	if err != nil || !found || string(b) != "v1" {
		t.Fatalf("GetUncached (2nd) = %q found=%v err=%v want v1", b, found, err)
	}
	if _, _, ok := db.hc.Load().cget([]byte("k")); !ok {
		t.Fatal("GetUncached did not warm the cache after the doorkeeper admitted it")
	}

	// After an overwrite, GetUncached must return the new value, not the cached one.
	if err := db.Set([]byte("k"), []byte("v2"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	b, _, found, _ = db.GetUncached([]byte("k"))
	if !found || string(b) != "v2" {
		t.Fatalf("GetUncached after overwrite = %q want v2", b)
	}
}

// TestHotCacheSwap checks that after SWAPDB the hot caches move with the
// data, so a Get on db0 returns what was in db1 and vice versa.
func TestHotCacheSwap(t *testing.T) {
	ks, _, _ := newKS(t)
	db0 := mustDB(t, ks, 0)
	db1 := mustDB(t, ks, 1)

	_ = db0.Set([]byte("k"), []byte("in0"), TypeString, EncRaw, -1)
	_ = db1.Set([]byte("k"), []byte("in1"), TypeString, EncRaw, -1)
	// Warm the caches.
	_, _, _, _ = db0.Get([]byte("k"))
	_, _, _, _ = db1.Get([]byte("k"))

	if err := ks.Swap(0, 1); err != nil {
		t.Fatalf("Swap: %v", err)
	}
	b0, _, found0, _ := db0.Get([]byte("k"))
	b1, _, found1, _ := db1.Get([]byte("k"))
	if !found0 || string(b0) != "in1" {
		t.Fatalf("db0 after swap = %q found=%v want in1", b0, found0)
	}
	if !found1 || string(b1) != "in0" {
		t.Fatalf("db1 after swap = %q found=%v want in0", b1, found1)
	}
}
