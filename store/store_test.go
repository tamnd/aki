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

// TestDelete checks the index drops a key (backward-shift deletion) while leaving
// the rest of the shard's keys probeable, including after a grow has reshuffled
// the table.
func TestDelete(t *testing.T) {
	s := mustStore(t, Tunables{Shards: 8, PageSize: 1 << 16, IndexHintPerShard: 64})
	const n = 20000
	for i := 0; i < n; i++ {
		if err := s.Set([]byte(fmt.Sprintf("key:%d", i)), []byte("v")); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	// delete the even keys.
	for i := 0; i < n; i += 2 {
		ok, err := s.Delete([]byte(fmt.Sprintf("key:%d", i)))
		if err != nil || !ok {
			t.Fatalf("Delete %d: ok=%v err=%v", i, ok, err)
		}
	}
	if got := s.Len(); got != n/2 {
		t.Fatalf("Len = %d, want %d after deleting half", got, n/2)
	}
	for i := 0; i < n; i++ {
		_, found, _ := s.Get([]byte(fmt.Sprintf("key:%d", i)))
		if i%2 == 0 && found {
			t.Fatalf("deleted key %d still present", i)
		}
		if i%2 == 1 && !found {
			t.Fatalf("surviving key %d missing after deletes", i)
		}
	}
	if ok, _ := s.Delete([]byte("never-set")); ok {
		t.Fatal("Delete of absent key returned true")
	}
}

// TestIndexResidentBytes is the larger-than-memory headline made a guard: the
// resident index is the []uint64 tables only, a small fixed cost per key that is
// independent of key length, so a dataset whose log spills to disk keeps only this
// in RAM. A map[string] would keep every key resident with nowhere to spill.
func TestIndexResidentBytes(t *testing.T) {
	const n = 500_000
	// size the per-shard hint to the real per-shard load, the way the engine is
	// configured from an expected key count; an over-large hint just rounds the
	// power-of-two table up and inflates bytes/key without being a structural cost.
	s := mustStore(t, Tunables{Shards: 64, PageSize: 1 << 16, IndexHintPerShard: n / 64})
	for i := 0; i < n; i++ {
		// long keys to show the index cost does not grow with key length.
		k := []byte(fmt.Sprintf("user:session:token:%040d", i))
		if err := s.Set(k, []byte("0123456789abcdef")); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	perKey := float64(s.IndexBytes()) / float64(n)
	t.Logf("index resident: %.1f bytes/key over %d keys (%d total table bytes)", perKey, n, s.IndexBytes())
	// at a 0.75 target load with power-of-two rounding the table lands near
	// 8-17 bytes/key regardless of the 58-byte keys.
	if perKey > 24 {
		t.Fatalf("index resident %.1f bytes/key too high (key-size leak?)", perKey)
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
