package f2raw

import (
	"fmt"
	"sync"
	"testing"
)

// newStore builds a two-tier store sized for a test with hotKeys live-key ceiling and
// room for totalKeys across both tiers. The arenas are grow-only, so every key that
// passes through the hot tier consumes hot arena bytes even after it is evicted; the
// hot arena is sized for that churn so eviction is exercised through index reuse, not
// arena exhaustion.
func newStore(hotKeys, totalKeys int) *Store {
	return New(Config{
		HotKeys:          hotKeys,
		HotIndexBuckets:  hotKeys/2 + 16,
		HotArenaBytes:    totalKeys*256 + 1<<16,
		ColdIndexBuckets: totalKeys/2 + 16,
		ColdArenaBytes:   totalKeys*256 + 1<<16,
	})
}

func get(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, ok := s.Get([]byte(key), nil)
	return string(v), ok
}

func TestSetGetDelete(t *testing.T) {
	s := newStore(1024, 1024)
	if _, ok := get(t, s, "missing"); ok {
		t.Fatal("empty store returned a hit")
	}
	if err := s.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if v, ok := get(t, s, "a"); !ok || v != "1" {
		t.Fatalf("a = %q,%v want 1,true", v, ok)
	}
	// Overwrite in place (same size).
	if err := s.Set([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if v, ok := get(t, s, "a"); !ok || v != "2" {
		t.Fatalf("a = %q,%v want 2,true", v, ok)
	}
	// Grow the value past its slot: forces an append + index swap.
	if err := s.Set([]byte("a"), []byte("longer-value")); err != nil {
		t.Fatal(err)
	}
	if v, ok := get(t, s, "a"); !ok || v != "longer-value" {
		t.Fatalf("a = %q,%v want longer-value,true", v, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d want 1", s.Len())
	}
	if !s.Delete([]byte("a")) {
		t.Fatal("delete reported absent")
	}
	if _, ok := get(t, s, "a"); ok {
		t.Fatal("deleted key still present")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d want 0", s.Len())
	}
	// Re-insert after delete.
	if err := s.Set([]byte("a"), []byte("3")); err != nil {
		t.Fatal(err)
	}
	if v, ok := get(t, s, "a"); !ok || v != "3" {
		t.Fatalf("a = %q,%v want 3,true", v, ok)
	}
}

// TestLoadPromoteEvict drives the two-tier machinery directly: keys start cold via
// Load, a second read promotes one into hot, and a small hot ceiling forces eviction
// back to cold. Single residency must hold throughout: every key stays retrievable and
// Len stays exact.
func TestLoadPromoteEvict(t *testing.T) {
	s := newStore(8, 1000)
	const n = 1000
	for i := 0; i < n; i++ {
		if err := s.Load([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.Stats(); st.ColdKeys != n || st.HotKeys != 0 {
		t.Fatalf("after load hot=%d cold=%d want 0,%d", st.HotKeys, st.ColdKeys, n)
	}

	// First read of k5 arms the admission bit but does not promote.
	if v, ok := get(t, s, "k5"); !ok || v != "v5" {
		t.Fatalf("k5 = %q,%v want v5", v, ok)
	}
	if st := s.Stats(); st.Promotions != 0 || st.HotKeys != 0 {
		t.Fatalf("first cold read promoted early: %+v", st)
	}
	// Second read promotes it into hot and drops the cold copy.
	if v, ok := get(t, s, "k5"); !ok || v != "v5" {
		t.Fatalf("k5 = %q,%v want v5", v, ok)
	}
	if st := s.Stats(); st.Promotions != 1 || st.HotKeys != 1 {
		t.Fatalf("second cold read did not promote: %+v", st)
	}
	if s.Len() != n {
		t.Fatalf("Len = %d want %d after promote", s.Len(), n)
	}

	// Every key still reads back its own value through whichever tier it now lives in.
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("v%d", i)
		if v, ok := get(t, s, fmt.Sprintf("k%d", i)); !ok || v != want {
			t.Fatalf("k%d = %q,%v want %q", i, v, ok, want)
		}
	}
	if s.Len() != n {
		t.Fatalf("Len = %d want %d after full scan", s.Len(), n)
	}
	if st := s.Stats(); st.HotKeys > s.hotCap {
		t.Fatalf("hot tier %d over ceiling %d: eviction not keeping up", st.HotKeys, s.hotCap)
	}
}

func TestManyKeysAndChains(t *testing.T) {
	// A small hot ceiling and bucket count force eviction and collision chains in both
	// tiers; every key must survive somewhere.
	s := newStore(500, 5000)
	const n = 5000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		if err := s.Set([]byte(k), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if s.Len() != n {
		t.Fatalf("Len = %d want %d", s.Len(), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		if v, ok := get(t, s, k); !ok || v != want {
			t.Fatalf("%s = %q,%v want %q", k, v, ok, want)
		}
	}
}

func TestBothTiersFull(t *testing.T) {
	s := New(Config{
		HotKeys:          8,
		HotIndexBuckets:  8,
		HotArenaBytes:    256,
		ColdIndexBuckets: 8,
		ColdArenaBytes:   256,
	})
	var sawFull bool
	for i := 0; i < 1000; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("payload")); err == ErrFull {
			sawFull = true
			break
		}
	}
	if !sawFull {
		t.Fatal("expected ErrFull once both tiny tiers fill")
	}
}

// TestConcurrentSameKey hammers one key that stays resident in the hot tier, so it is
// the same seqlock stress as f1raw: a reader must always see a whole value a writer
// actually wrote. It skips under -race for the same reason f1raw does, the seqlock
// value memcpy is a benign race the detector cannot model.
func TestConcurrentSameKey(t *testing.T) {
	if raceEnabled {
		t.Skip("seqlock value memcpy is a benign race the detector cannot model; runs as a plain stress test")
	}
	s := newStore(1024, 1024)
	key := []byte("hot")
	if err := s.Set(key, []byte("v0000")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	writers, readers := 8, 8
	iters := 20000
	valid := map[string]bool{"v0000": true}
	var mu sync.Mutex
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				val := fmt.Sprintf("v%01d%03d", w, i%1000) // always 5 bytes -> in-place
				mu.Lock()
				valid[val] = true
				mu.Unlock()
				if err := s.Set(key, []byte(val)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for i := 0; i < iters; i++ {
				v, ok := s.Get(key, buf)
				if !ok {
					t.Error("hot key missing mid-run")
					return
				}
				buf = v
				mu.Lock()
				good := valid[string(v)]
				mu.Unlock()
				if !good {
					t.Errorf("read a value no writer wrote: %q", v)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentDistinctKeys runs eight goroutines over disjoint key spaces with a hot
// ceiling far below the key count, and reads each key twice so the second read promotes
// it and the small hot tier evicts under contention. No two goroutines touch the same
// key, so it stays clean under -race while driving the evictor's hot-to-cold migration
// against concurrent lock-free inserts, reads, and promotions. Every key must read back
// its own value, and Len must be exact at the end.
func TestConcurrentDistinctKeys(t *testing.T) {
	const groups = 8
	const keys = 2000
	s := newStore(256, groups*keys)
	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				k := []byte(fmt.Sprintf("g%d-k%d", g, i))
				v := []byte(fmt.Sprintf("v%d-%d", g, i))
				if err := s.Set(k, v); err != nil {
					t.Error(err)
					return
				}
				for r := 0; r < 2; r++ { // second read promotes into the small hot tier
					got, ok := s.Get(k, nil)
					if !ok || string(got) != string(v) {
						t.Errorf("key %s = %q,%v want %q", k, got, ok, v)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	if s.Len() != groups*keys {
		t.Fatalf("Len = %d want %d", s.Len(), groups*keys)
	}
	for g := 0; g < groups; g++ {
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("g%d-k%d", g, i)
			want := fmt.Sprintf("v%d-%d", g, i)
			if v, ok := get(t, s, k); !ok || v != want {
				t.Fatalf("key %s = %q,%v want %q", k, v, ok, want)
			}
		}
	}
}
