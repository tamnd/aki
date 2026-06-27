package store

import (
	"bytes"
	"fmt"
	"testing"
)

func mustStore(t *testing.T, tn Tunables) *Store {
	t.Helper()
	s, err := New(tn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSetGetRoundTrip(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key:%d", i))
		v := []byte(fmt.Sprintf("val:%d", i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key:%d", i))
		want := []byte(fmt.Sprintf("val:%d", i))
		got, found, err := s.Get(k)
		if err != nil || !found {
			t.Fatalf("Get %d: found=%v err=%v", i, found, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d = %q, want %q", i, got, want)
		}
	}
	if got := s.Len(); got != 1000 {
		t.Fatalf("Len = %d, want 1000", got)
	}
}

func TestOverwrite(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	k := []byte("k")
	s.Set(k, []byte("first"))
	s.Set(k, []byte("second"))
	got, _, _ := s.Get(k)
	if !bytes.Equal(got, []byte("second")) {
		t.Fatalf("Get = %q, want second", got)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (overwrite double-counted)", got)
	}
}

func TestMissing(t *testing.T) {
	s := mustStore(t, DefaultTunables())
	_, found, err := s.Get([]byte("nope"))
	if err != nil {
		t.Fatalf("Get err: %v", err)
	}
	if found {
		t.Fatal("found a key that was never set")
	}
}

// TestLargerThanMemory is the core v2 guarantee: a working set far larger than
// the resident page budget must still read back correctly, which forces the spill
// path and the disk read-back. Small pages and a tiny per-shard cap make the
// budget a few KiB while the data is hundreds of KiB, so most pages spill to disk.
func TestLargerThanMemory(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, Tunables{
		Shards:                4,
		PageSize:              1 << 12, // 4 KiB pages
		ResidentPagesPerShard: 2,       // only 2 pages per shard stay in RAM
		Dir:                   dir,
	})
	const n = 5000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte(fmt.Sprintf("value-for-key-%06d-padding-padding", i))
		if err := s.Set(k, v); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if s.Spilled() == 0 {
		t.Fatal("no pages spilled; the budget was not exceeded, test is not exercising disk")
	}
	t.Logf("spilled %d pages with a %d-page-per-shard resident cap", s.Spilled(), 2)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		want := []byte(fmt.Sprintf("value-for-key-%06d-padding-padding", i))
		got, found, err := s.Get(k)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !found {
			t.Fatalf("Get %d: not found after spill", i)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %d = %q, want %q (spilled read-back wrong)", i, got, want)
		}
	}
}

// TestOverwriteAfterSpill checks that a key updated after its first record spilled
// reads back the new value: the index must point at the newest record wherever it
// lives.
func TestOverwriteAfterSpill(t *testing.T) {
	dir := t.TempDir()
	s := mustStore(t, Tunables{Shards: 2, PageSize: 1 << 12, ResidentPagesPerShard: 1, Dir: dir})
	k := []byte("hot")
	s.Set(k, []byte("old"))
	// Push enough other data to spill the page holding "old".
	for i := 0; i < 2000; i++ {
		s.Set([]byte(fmt.Sprintf("filler%06d", i)), bytes.Repeat([]byte("x"), 40))
	}
	s.Set(k, []byte("new"))
	got, found, err := s.Get(k)
	if err != nil || !found {
		t.Fatalf("Get hot: found=%v err=%v", found, err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("Get hot = %q, want new", got)
	}
}

func TestRejectOversizeRecord(t *testing.T) {
	s := mustStore(t, Tunables{Shards: 1, PageSize: 128})
	err := s.Set([]byte("k"), bytes.Repeat([]byte("v"), 200))
	if err == nil {
		t.Fatal("expected error for record larger than page")
	}
}

func TestBadShards(t *testing.T) {
	if _, err := New(Tunables{Shards: 3, PageSize: 1024}); err == nil {
		t.Fatal("expected error for non-power-of-two shards")
	}
}
