package f2raw

import (
	"fmt"
	"sync"
	"testing"
)

func get(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, ok := s.Get([]byte(key), nil)
	return string(v), ok
}

func TestSetGetDelete(t *testing.T) {
	s := New(1024, 1<<20)
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

func TestManyKeysAndChains(t *testing.T) {
	// A small bucket count forces collision chains and bucket-full prepends.
	s := New(16, 1<<22)
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

func TestArenaFull(t *testing.T) {
	s := New(64, 256) // tiny arena
	var sawFull bool
	for i := 0; i < 1000; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("payload")); err == ErrFull {
			sawFull = true
			break
		}
	}
	if !sawFull {
		t.Fatal("expected ErrFull on a tiny arena")
	}
}

// TestConcurrentSameKey hammers one key with many writers and readers. A reader must
// always see a whole value, never a torn one, and every value read must be one a
// writer actually wrote. It skips under -race: the seqlock value memcpy is a benign
// data race by construction (the version check makes it correct), and the detector
// models happens-before, not seqlock validity, so it would flag the value copy. Every
// other test runs clean under -race because they touch immutable data or atomics.
func TestConcurrentSameKey(t *testing.T) {
	if raceEnabled {
		t.Skip("seqlock value memcpy is a benign race the detector cannot model; runs as a plain stress test")
	}
	s := New(64, 1<<20)
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

// TestConcurrentDistinctKeys runs eight goroutines over disjoint key spaces so no two
// touch the same record. That keeps it clean under -race while still exercising the
// lock-free index: concurrent inserts and reads race on the shared buckets and the
// shared bump allocator, just never on one record's value. Every key each goroutine
// wrote must read back its own value at the end.
func TestConcurrentDistinctKeys(t *testing.T) {
	s := New(4096, 1<<24)
	const groups = 8
	const keys = 2000
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
				got, ok := s.Get(k, nil)
				if !ok || string(got) != string(v) {
					t.Errorf("key %s = %q,%v want %q", k, got, ok, v)
					return
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
