package f1raw

import (
	"strconv"
	"sync"
	"testing"
)

func TestIncrBasic(t *testing.T) {
	s := New(1<<10, 1<<20)
	n, err := s.Incr([]byte("c"), 1)
	if err != nil || n != 1 {
		t.Fatalf("first incr = %d, %v; want 1, nil", n, err)
	}
	n, _ = s.Incr([]byte("c"), 1)
	if n != 2 {
		t.Fatalf("second incr = %d, want 2", n)
	}
	n, _ = s.Incr([]byte("c"), 40)
	if n != 42 {
		t.Fatalf("incrby 40 = %d, want 42", n)
	}
	n, _ = s.Incr([]byte("c"), -50)
	if n != -8 {
		t.Fatalf("incrby -50 = %d, want -8", n)
	}
}

func TestIncrNotInt(t *testing.T) {
	s := New(1<<10, 1<<20)
	if err := s.Set([]byte("k"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Incr([]byte("k"), 1); err != ErrNotInt {
		t.Fatalf("incr on non-int = %v, want ErrNotInt", err)
	}
}

func TestIncrGrowsAcrossWidth(t *testing.T) {
	s := New(1<<10, 1<<20)
	// Start from a SET value whose reserved capacity is one 8-byte word, then push
	// the counter past 8 digits so the in-place write no longer fits and the record
	// must be republished wider.
	if err := s.Set([]byte("k"), []byte("9")); err != nil {
		t.Fatal(err)
	}
	want := int64(9)
	for _, d := range []int64{90, 900, 9000, 90000, 900000, 9000000, 90000000, 900000000} {
		want += d
		got, err := s.Incr([]byte("k"), d)
		if err != nil {
			t.Fatalf("incr %d: %v", d, err)
		}
		if got != want {
			t.Fatalf("incr %d = %d, want %d", d, got, want)
		}
	}
	dst, _ := s.Get([]byte("k"), nil)
	if string(dst) != strconv.FormatInt(want, 10) {
		t.Fatalf("stored value = %q, want %q", dst, strconv.FormatInt(want, 10))
	}
}

// TestIncrConcurrentSameKeyCreate races many goroutines incrementing one key that
// starts absent, so the create path and the in-place path both run under contention.
// The final value must equal the total number of increments, proving no update is
// lost even when two goroutines try to create the key at once.
func TestIncrConcurrentSameKeyCreate(t *testing.T) {
	s := New(1<<12, 1<<22)
	const goroutines = 16
	const each = 2000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := s.Incr([]byte("shared"), 1); err != nil {
					t.Errorf("incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	// A trailing read (delta 0) returns the accumulated total, which must equal the
	// exact number of increments issued; any lower value means an update was lost.
	got, _ := s.Incr([]byte("shared"), 0)
	if want := int64(goroutines * each); got != want {
		t.Fatalf("final = %d, want %d (lost updates)", got, want)
	}
}

func TestResetClears(t *testing.T) {
	s := New(1<<10, 1<<20)
	for i := 0; i < 100; i++ {
		k := []byte("k" + strconv.Itoa(i))
		if err := s.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if s.Len() != 100 {
		t.Fatalf("len before reset = %d, want 100", s.Len())
	}
	s.Reset()
	if s.Len() != 0 {
		t.Fatalf("len after reset = %d, want 0", s.Len())
	}
	if _, ok := s.Get([]byte("k0"), nil); ok {
		t.Fatal("k0 still present after reset")
	}
	// Store is usable again after reset.
	if err := s.Set([]byte("fresh"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get([]byte("fresh"), nil); !ok || string(v) != "x" {
		t.Fatalf("post-reset get = %q,%v", v, ok)
	}
}
