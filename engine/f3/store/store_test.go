package store

import (
	"fmt"
	"testing"
)

// testStore builds a store big enough for the unit suite: a few segments of
// the floor size.
func testStore(t testing.TB, nSeg int) *Store {
	t.Helper()
	segSize := int(align8(maxRecordBytes))
	return New(8+(nSeg+1)*segSize, segSize)
}

func get(t *testing.T, s *Store, key string) (string, bool) {
	t.Helper()
	v, ok := s.Get([]byte(key), nil)
	return string(v), ok
}

func TestSetGetDelete(t *testing.T) {
	s := testStore(t, 2)
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
	// Grow the value past its reserved capacity: forces an append plus an
	// entry repoint.
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
	// Re-insert after delete: the emptied slot is a hole the probe tolerates
	// and the insert refills.
	if err := s.Set([]byte("a"), []byte("3")); err != nil {
		t.Fatal(err)
	}
	if v, ok := get(t, s, "a"); !ok || v != "3" {
		t.Fatalf("a = %q,%v want 3,true", v, ok)
	}
}

// TestManyKeysAndChains pushes well past one segment's capacity, so overflow
// chains form and segment splits run, then every key must still resolve.
func TestManyKeysAndChains(t *testing.T) {
	s := testStore(t, 8)
	const n = 20000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		if err := s.Set([]byte(k), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if s.Len() != n {
		t.Fatalf("Len = %d want %d", s.Len(), n)
	}
	if s.Splits() == 0 {
		t.Fatal("20000 keys grew no index segment; the split path never ran")
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		if v, ok := get(t, s, k); !ok || v != want {
			t.Fatalf("%s = %q,%v want %q", k, v, ok, want)
		}
	}
	// Delete every other key, then both halves must answer correctly.
	for i := 0; i < n; i += 2 {
		if !s.Delete([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("Delete key-%d: missing", i)
		}
	}
	if s.Len() != n/2 {
		t.Fatalf("Len = %d want %d", s.Len(), n/2)
	}
	for i := 0; i < n; i++ {
		v, ok := get(t, s, fmt.Sprintf("key-%d", i))
		if i%2 == 0 {
			if ok {
				t.Fatalf("deleted key-%d still present", i)
			}
			continue
		}
		if want := fmt.Sprintf("val-%d", i); !ok || v != want {
			t.Fatalf("key-%d = %q,%v want %q", i, v, ok, want)
		}
	}
}

func TestArenaFull(t *testing.T) {
	s := New(8+int(align8(maxRecordBytes)), 1) // one floor-size segment
	var sawFull bool
	for i := 0; i < 1<<20; i++ {
		err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("payload"))
		if err == ErrFull {
			sawFull = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !sawFull {
		t.Fatal("expected ErrFull on a one-segment arena")
	}
}

func TestKeyValueLimits(t *testing.T) {
	s := testStore(t, 2)
	if err := s.Set(nil, []byte("v")); err != errEmptyKey {
		t.Fatalf("empty key: %v", err)
	}
	if err := s.Set(make([]byte, maxKey+1), []byte("v")); err != ErrTooBig {
		t.Fatalf("oversize key: %v", err)
	}
	// The value ceiling is the 512MiB proto limit, pinned through SETRANGE so
	// the test never allocates a value that size.
	if _, err := s.SetRange([]byte("k"), maxValueLen, []byte("v"), 0); err != ErrTooBig {
		t.Fatalf("oversize value: %v", err)
	}
	// The widest key plus a separated-band-width value must fit.
	if err := s.Set(make([]byte, maxKey), make([]byte, maxVal)); err != nil {
		t.Fatalf("max-width record: %v", err)
	}
}

func TestResetRewinds(t *testing.T) {
	s := testStore(t, 3)
	for i := 0; i < 3000; i++ {
		if err := s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	s.Reset()
	if s.Len() != 0 {
		t.Fatalf("Reset left %d keys, want 0", s.Len())
	}
	if used, _ := s.ArenaBytes(); used != 0 {
		t.Fatalf("Reset left %d used bytes, want 0", used)
	}
	if s.arena.cur != 0 {
		t.Fatalf("Reset left current segment at %d, want 0", s.arena.cur)
	}
	if err := s.Set([]byte("post"), []byte("reset")); err != nil {
		t.Fatalf("Set after Reset: %v", err)
	}
	if v, ok := get(t, s, "post"); !ok || v != "reset" {
		t.Fatalf("post = %q,%v want reset,true", v, ok)
	}
}
