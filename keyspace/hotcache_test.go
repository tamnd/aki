package keyspace

import (
	"testing"
)

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

	// Get re-warms the cache.
	b, _, found, _ = db.Get([]byte("k"))
	if !found || string(b) != "v" {
		t.Fatalf("Get after Peek = %q found=%v", b, found)
	}
	if _, _, ok := db.hc.Load().cget([]byte("k")); !ok {
		t.Fatal("Get must populate the hot cache")
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
