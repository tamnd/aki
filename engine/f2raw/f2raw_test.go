package f2raw

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New(1<<10, 1<<20)
	if _, ok := s.Get([]byte("missing"), nil); ok {
		t.Fatal("empty store returned a value")
	}
	if err := s.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	v, ok := s.Get([]byte("k1"), nil)
	if !ok || string(v) != "v1" {
		t.Fatalf("got %q %v, want v1 true", v, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
	// In-place update within reserved capacity.
	if err := s.Set([]byte("k1"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	v, _ = s.Get([]byte("k1"), nil)
	if string(v) != "v2" {
		t.Fatalf("after update got %q, want v2", v)
	}
	// Grow past reserved capacity forces a republish.
	big := []byte("a-much-longer-value-than-before")
	if err := s.Set([]byte("k1"), big); err != nil {
		t.Fatal(err)
	}
	v, _ = s.Get([]byte("k1"), nil)
	if string(v) != string(big) {
		t.Fatalf("after grow got %q, want %q", v, big)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d after grow, want 1", s.Len())
	}
	if !s.Delete([]byte("k1")) {
		t.Fatal("delete of present key returned false")
	}
	if _, ok := s.Get([]byte("k1"), nil); ok {
		t.Fatal("deleted key still present")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d after delete, want 0", s.Len())
	}
}

func TestManyKeys(t *testing.T) {
	s := New(1<<12, 1<<24)
	const n = 50000
	for i := 0; i < n; i++ {
		k := []byte(strconv.Itoa(i))
		if err := s.Set(k, k); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if s.Len() != n {
		t.Fatalf("Len = %d, want %d", s.Len(), n)
	}
	for i := 0; i < n; i++ {
		k := []byte(strconv.Itoa(i))
		v, ok := s.Get(k, nil)
		if !ok || string(v) != string(k) {
			t.Fatalf("get %d: %q %v", i, v, ok)
		}
	}
}

func TestIncr(t *testing.T) {
	s := New(1<<10, 1<<20)
	n, err := s.Incr([]byte("c"), 5)
	if err != nil || n != 5 {
		t.Fatalf("incr new: %d %v", n, err)
	}
	n, err = s.Incr([]byte("c"), 7)
	if err != nil || n != 12 {
		t.Fatalf("incr existing: %d %v", n, err)
	}
	// Force a width grow: 12 -> a value needing more digits.
	n, _ = s.Incr([]byte("c"), 999999999)
	if n != 1000000011 {
		t.Fatalf("incr grow: %d", n)
	}
	v, _ := s.Get([]byte("c"), nil)
	if string(v) != "1000000011" {
		t.Fatalf("value after grow: %q", v)
	}
	if err := s.Set([]byte("s"), []byte("notint")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Incr([]byte("s"), 1); err != ErrNotInt {
		t.Fatalf("incr non-int: want ErrNotInt, got %v", err)
	}
}

func TestConcurrentDistinctKeys(t *testing.T) {
	s := New(1<<14, 1<<26)
	const goroutines = 8
	const perG = 20000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				k := []byte(fmt.Sprintf("g%d-k%d", g, i))
				if err := s.Set(k, k); err != nil {
					t.Errorf("set: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if s.Len() != goroutines*perG {
		t.Fatalf("Len = %d, want %d", s.Len(), goroutines*perG)
	}
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			k := []byte(fmt.Sprintf("g%d-k%d", g, i))
			v, ok := s.Get(k, nil)
			if !ok || string(v) != string(k) {
				t.Fatalf("get %s: %q %v", k, v, ok)
			}
		}
	}
}
