package store

import (
	"bytes"
	"fmt"
	"testing"
)

// TestEachVisitsEveryLiveKey is the enumerate contract: Each must hand back every
// key currently set, with its exact value, and nothing it has deleted.
func TestEachVisitsEveryLiveKey(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	want := map[string]string{}
	for i := 0; i < 2000; i++ {
		k := fmt.Sprintf("key:%d", i)
		v := fmt.Sprintf("val:%d", i)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		want[k] = v
	}
	// Delete the even keys so Each must skip them, not just report all records.
	for i := 0; i < 2000; i += 2 {
		s.Delete([]byte(fmt.Sprintf("key:%d", i)))
		delete(want, fmt.Sprintf("key:%d", i))
	}
	got := map[string]string{}
	err := s.Each(func(key, val []byte) bool {
		got[string(key)] = string(val)
		return true
	})
	if err != nil {
		t.Fatalf("Each: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Each visited %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Each key %q = %q, want %q", k, got[k], v)
		}
	}
}

// TestEachStopsEarly checks fn returning false ends the walk and Each returns nil.
func TestEachStopsEarly(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	for i := 0; i < 100; i++ {
		s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	seen := 0
	err := s.Each(func(key, val []byte) bool {
		seen++
		return seen < 10
	})
	if err != nil {
		t.Fatalf("Each: %v", err)
	}
	if seen != 10 {
		t.Fatalf("Each visited %d before stop, want 10", seen)
	}
}

// TestEachAfterSpill exercises the disk read-back branch of recordKV: with a tiny
// resident budget most pages spill, so Each must recover keys and values off the
// log file, not only from resident pages.
func TestEachAfterSpill(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, Tunables{Shards: 4, PageSize: 1 << 12, ResidentPagesPerShard: 2, Dir: dir})
	const n = 3000
	want := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		v := fmt.Sprintf("value-for-%06d-pad-pad-pad", i)
		if err := s.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		want[k] = v
	}
	if s.Spilled() == 0 {
		t.Fatal("no pages spilled; not exercising the disk read-back in Each")
	}
	got := 0
	err := s.Each(func(key, val []byte) bool {
		k := string(key)
		if w, ok := want[k]; !ok || w != string(val) {
			t.Fatalf("Each key %q = %q, want %q (ok=%v)", k, val, w, ok)
		}
		got++
		return true
	})
	if err != nil {
		t.Fatalf("Each after spill: %v", err)
	}
	if got != n {
		t.Fatalf("Each visited %d keys after spill, want %d", got, n)
	}
}

// TestClearEmptiesStore checks Clear drops every key and that the store is usable
// again afterward, reusing its shards.
func TestClearEmptiesStore(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	for i := 0; i < 500; i++ {
		s.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	if s.Len() != 500 {
		t.Fatalf("Len before clear = %d, want 500", s.Len())
	}
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Len() != 0 {
		t.Fatalf("Len after clear = %d, want 0", s.Len())
	}
	visited := 0
	s.Each(func(key, val []byte) bool { visited++; return true })
	if visited != 0 {
		t.Fatalf("Each after clear saw %d keys, want 0", visited)
	}
	// The store still works after a clear.
	if err := s.Set([]byte("fresh"), []byte("again")); err != nil {
		t.Fatalf("Set after clear: %v", err)
	}
	got, found, _ := s.Get([]byte("fresh"))
	if !found || !bytes.Equal(got, []byte("again")) {
		t.Fatalf("Get after clear+set = %q found=%v, want again", got, found)
	}
	if s.Len() != 1 {
		t.Fatalf("Len after clear+set = %d, want 1", s.Len())
	}
}

// TestClearWithSpillTruncates checks Clear resets the spilled-page bookkeeping so
// a store that had spilled reads back correctly after being refilled.
func TestClearWithSpillTruncates(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, Tunables{Shards: 2, PageSize: 1 << 12, ResidentPagesPerShard: 1, Dir: dir})
	for i := 0; i < 2000; i++ {
		s.Set([]byte(fmt.Sprintf("old%06d", i)), bytes.Repeat([]byte("x"), 40))
	}
	if s.Spilled() == 0 {
		t.Fatal("expected spill before clear")
	}
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Spilled() != 0 {
		t.Fatalf("Spilled after clear = %d, want 0", s.Spilled())
	}
	// Refill and confirm a fresh key reads back, so the truncated file did not
	// leave a stale offset behind.
	s.Set([]byte("new"), []byte("value"))
	got, found, err := s.Get([]byte("new"))
	if err != nil || !found || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get new after clear = %q found=%v err=%v", got, found, err)
	}
}
