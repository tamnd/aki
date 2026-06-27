package findex

import (
	"runtime"
	"strconv"
	"testing"
)

// valLoc mirrors the v2 store's map value so the resident-size comparison is
// against the structure findex actually replaces.
type valLoc struct {
	addr uint64
	vlen uint32
}

func key(i int) []byte { return []byte("user:session:" + strconv.Itoa(i)) }

func TestPutGetBasic(t *testing.T) {
	ix := New(1024)
	const n = 5000
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte("v"+strconv.Itoa(i)))
	}
	if ix.Len() != n {
		t.Fatalf("Len = %d, want %d", ix.Len(), n)
	}
	for i := 0; i < n; i++ {
		got, ok := ix.Get(key(i))
		if !ok {
			t.Fatalf("missing key %d", i)
		}
		if want := "v" + strconv.Itoa(i); string(got) != want {
			t.Fatalf("key %d = %q, want %q", i, got, want)
		}
	}
	if _, ok := ix.Get([]byte("absent")); ok {
		t.Fatal("absent key reported present")
	}
}

func TestUpdateShadows(t *testing.T) {
	ix := New(1024)
	ix.Put([]byte("k"), []byte("one"))
	ix.Put([]byte("k"), []byte("two"))
	ix.Put([]byte("k"), []byte("three"))
	if ix.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after updates", ix.Len())
	}
	got, ok := ix.Get([]byte("k"))
	if !ok || string(got) != "three" {
		t.Fatalf("Get = %q,%v, want \"three\",true", got, ok)
	}
}

// TestGrowPreservesAll fills well past the initial capacity so several grows
// fire, then checks every key still resolves.
func TestGrowPreservesAll(t *testing.T) {
	ix := New(64) // tiny on purpose: forces many grows
	const n = 20000
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte(strconv.Itoa(i)))
	}
	if ix.Len() != n {
		t.Fatalf("Len = %d, want %d", ix.Len(), n)
	}
	for i := 0; i < n; i++ {
		got, ok := ix.Get(key(i))
		if !ok || string(got) != strconv.Itoa(i) {
			t.Fatalf("after grow key %d = %q,%v", i, got, ok)
		}
	}
}

func TestDelete(t *testing.T) {
	ix := New(1024)
	const n = 4000
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte(strconv.Itoa(i)))
	}
	// delete the even keys, then check odds survive and evens are gone.
	for i := 0; i < n; i += 2 {
		if !ix.Delete(key(i)) {
			t.Fatalf("delete %d returned false", i)
		}
	}
	if ix.Len() != n/2 {
		t.Fatalf("Len = %d, want %d", ix.Len(), n/2)
	}
	for i := 0; i < n; i++ {
		_, ok := ix.Get(key(i))
		if i%2 == 0 && ok {
			t.Fatalf("deleted key %d still present", i)
		}
		if i%2 == 1 && !ok {
			t.Fatalf("surviving key %d missing after deletes", i)
		}
	}
	if ix.Delete([]byte("never")) {
		t.Fatal("delete of absent key returned true")
	}
}

// TestResidentBytesPerKey is the headline measurement: the index's
// resident-when-spilled footprint per key versus map[string]valLoc, which
// keeps keys resident. Run with -v to see the numbers.
func TestResidentBytesPerKey(t *testing.T) {
	const n = 1_000_000

	// findex: the resident number is IndexBytes (the []uint64 table); keys and
	// values live in the log, which spills to disk in the real engine.
	ix := New(n)
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte("0123456789abcdef")) // 16-byte value
	}
	idxPerKey := float64(ix.IndexBytes()) / float64(n)
	logPerKey := float64(ix.LogBytes()) / float64(n)

	// map[string]valLoc: measure the live heap it occupies, keys included.
	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	mp := make(map[string]valLoc, n)
	for i := 0; i < n; i++ {
		mp[string(key(i))] = valLoc{addr: uint64(i), vlen: 16}
	}
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	mapPerKey := float64(m1.HeapInuse-m0.HeapInuse) / float64(n)
	if len(mp) != n {
		t.Fatalf("map len %d", len(mp))
	}

	t.Logf("keys=%d", n)
	t.Logf("findex index resident: %.1f bytes/key  (table %d bytes)", idxPerKey, ix.IndexBytes())
	t.Logf("findex log (spills):   %.1f bytes/key", logPerKey)
	t.Logf("map[string]valLoc:     %.1f bytes/key  (heapinuse delta %d bytes)", mapPerKey, m1.HeapInuse-m0.HeapInuse)
	t.Logf("resident ratio map/findex: %.1fx", mapPerKey/idxPerKey)

	// guard: the index must stay small and key-size-independent. At a 0.75
	// target load with power-of-two rounding the table lands near 8-17
	// bytes/key regardless of the 26-byte keys; anything well over that means
	// the table is mis-sized or broken.
	if idxPerKey > 24 {
		t.Fatalf("index resident %.1f bytes/key too high", idxPerKey)
	}
	// the index must also be far smaller than the key-resident map.
	if mapPerKey/idxPerKey < 3 {
		t.Fatalf("resident ratio map/findex only %.1fx, expected >=3x", mapPerKey/idxPerKey)
	}
	runtime.KeepAlive(mp)
}

// TestGCMarkCost makes doc 01's garbage-collection argument a real number: a
// pointer-free index is marked in O(1), while a map[string] holds a pointer
// per entry that the GC scans every cycle. It times a forced GC with each
// structure live and large. Run with -v to see the pause times.
func TestGCMarkCost(t *testing.T) {
	if testing.Short() {
		t.Skip("skip heavy GC measurement in -short")
	}
	const n = 5_000_000

	ix := New(n)
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte("0123456789abcdef"))
	}
	idxPause := timedGC()
	runtime.KeepAlive(ix)

	mp := make(map[string]valLoc, n)
	for i := 0; i < n; i++ {
		mp[string(key(i))] = valLoc{addr: uint64(i), vlen: 16}
	}
	mapPause := timedGC()
	runtime.KeepAlive(mp)

	t.Logf("keys=%d", n)
	t.Logf("findex GC pause (pointer-free index+log live): %v", idxPause)
	t.Logf("map[string]valLoc GC pause (pointer per entry): %v", mapPause)
	if mapPause > 0 && idxPause > 0 {
		t.Logf("map GC pause / findex GC pause: %.1fx", float64(mapPause)/float64(idxPause))
	}
}

// timedGC returns the wall time of the slower of two forced GCs (the second is
// warm), which is dominated by the mark phase over the live heap.
func timedGC() (worst int64) {
	var s runtime.MemStats
	for i := 0; i < 2; i++ {
		runtime.GC()
		runtime.ReadMemStats(&s)
		p := int64(s.PauseNs[(s.NumGC+255)%256])
		if p > worst {
			worst = p
		}
	}
	return worst
}

func benchKeys(n int) [][]byte {
	ks := make([][]byte, n)
	for i := range ks {
		ks[i] = key(i)
	}
	return ks
}

func BenchmarkGetFindex(b *testing.B) {
	const n = 1_000_000
	ix := New(n)
	for i := 0; i < n; i++ {
		ix.Put(key(i), []byte("0123456789abcdef"))
	}
	ks := benchKeys(n)
	b.ResetTimer()
	b.ReportAllocs()
	var sink int
	for i := 0; i < b.N; i++ {
		v, ok := ix.Get(ks[i%n])
		if ok {
			sink += len(v)
		}
	}
	_ = sink
}

func BenchmarkGetMap(b *testing.B) {
	const n = 1_000_000
	mp := make(map[string]valLoc, n)
	for i := 0; i < n; i++ {
		mp[string(key(i))] = valLoc{addr: uint64(i), vlen: 16}
	}
	ks := benchKeys(n)
	b.ResetTimer()
	b.ReportAllocs()
	var sink uint32
	for i := 0; i < b.N; i++ {
		// map lookup by []byte needs a string; Go optimizes map[string] with a
		// []byte key in a string() conversion to avoid the alloc.
		vl, ok := mp[string(ks[i%n])]
		if ok {
			sink += vl.vlen
		}
	}
	_ = sink
}
