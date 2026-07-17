package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// TestReplayRebuildsIndex is the string vertical round trip: write a mix of
// inline, int, separated, and expiring values plus a delete, close the file so
// only its durable bytes remain, reopen it into a fresh store whose arena is
// empty, replay the record log, and assert every key reads back exactly as the
// original run left it. It is the first proof the durable arc closes: a crash
// leaves nothing in memory, and the log alone rebuilds the index.
func TestReplayRebuildsIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.aki")

	// A tiny resident cap forces the separated run past residency into the shared
	// value region, so its logged word is log-resident and replay can deref it.
	create := func() *akifile.File {
		f, err := akifile.Create(path, akifile.CreateOptions{
			ShardCount:   4,
			SepThreshold: 64,
			Sync:         akifile.SyncNo,
		})
		if err != nil {
			t.Fatalf("create aki: %v", err)
		}
		return f
	}
	openStore := func(f *akifile.File) *Store {
		s, err := Open(Options{
			ArenaBytes:       4 << 20,
			SegBytes:         1 << 20,
			AkiValueLog:      f,
			Shard:            1,
			ResidentCapBytes: 64,
		})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	big := make([]byte, strInlineMax+64)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	const ttlAt = int64(1_900_000_000_000)

	// First run: write the mix, delete one key, then flush and drop the store so
	// only the file's durable bytes carry into recovery.
	f := create()
	s := openStore(f)
	if err := s.SetString([]byte("greet"), []byte("hello"), 0, 0, false); err != nil {
		t.Fatalf("set greet: %v", err)
	}
	if err := s.SetString([]byte("count"), []byte("12345"), 0, 0, false); err != nil {
		t.Fatalf("set count: %v", err)
	}
	if err := s.SetString([]byte("wide"), big, 0, 0, false); err != nil {
		t.Fatalf("set wide: %v", err)
	}
	if err := s.SetString([]byte("ttl"), []byte("soon"), 0, ttlAt, false); err != nil {
		t.Fatalf("set ttl: %v", err)
	}
	if err := s.SetString([]byte("temp"), []byte("x"), 0, 0, false); err != nil {
		t.Fatalf("set temp: %v", err)
	}
	if !s.Del([]byte("temp"), 0) {
		t.Fatal("del temp reported absent for a live key")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen the file, open a fresh empty store over it, and rebuild
	// the index from the log alone.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	if err := s2.ReplayRecords(0); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Every surviving key reads back with the value the first run committed.
	wantVal := map[string][]byte{
		"greet": []byte("hello"),
		"count": []byte("12345"),
		"wide":  big,
		"ttl":   []byte("soon"),
	}
	for key, want := range wantVal {
		got, ok := s2.GetString([]byte(key), 0, nil)
		if !ok {
			t.Fatalf("key %q absent after replay", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %q read back %q, want %q", key, got, want)
		}
	}

	// The deleted key stays gone: its tombstone replayed as a clear, not the SET
	// before it.
	if _, ok := s2.GetString([]byte("temp"), 0, nil); ok {
		t.Fatal("deleted key temp came back after replay")
	}

	// The expiry rode the log, so the rebuilt record carries its deadline.
	if at, ok := s2.Deadline([]byte("ttl"), 0); !ok || at != ttlAt {
		t.Fatalf("ttl deadline after replay = (%d, %v), want (%d, true)", at, ok, ttlAt)
	}
}

// TestReplayIsShardScoped confirms a store replays only its own shard's records:
// two shards write distinct keys into the one shared file, then a fresh store on
// each shard replays and sees its own key but not the other shard's, so a
// multi-shard file recovers into disjoint per-shard indexes.
func TestReplayIsShardScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shards.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	openShard := func(f *akifile.File, shard uint16) *Store {
		s, err := Open(Options{
			ArenaBytes:  4 << 20,
			SegBytes:    1 << 20,
			AkiValueLog: f,
			Shard:       shard,
		})
		if err != nil {
			t.Fatalf("open shard %d: %v", shard, err)
		}
		return s
	}

	// Two shards share the one file; each logs a key the other never sees.
	s1 := openShard(f, 1)
	s2 := openShard(f, 2)
	if err := s1.SetString([]byte("one"), []byte("from-1"), 0, 0, false); err != nil {
		t.Fatalf("shard 1 set: %v", err)
	}
	if err := s2.SetString([]byte("two"), []byte("from-2"), 0, 0, false); err != nil {
		t.Fatalf("shard 2 set: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close shard 1: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close shard 2: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	r1 := openShard(f2, 1)
	r2 := openShard(f2, 2)
	t.Cleanup(func() { _ = r1.Close(); _ = r2.Close(); _ = f2.Close() })
	if err := r1.ReplayRecords(0); err != nil {
		t.Fatalf("replay shard 1: %v", err)
	}
	if err := r2.ReplayRecords(0); err != nil {
		t.Fatalf("replay shard 2: %v", err)
	}

	// Each rebuilt shard holds its own key and not the other's.
	if got, ok := r1.GetString([]byte("one"), 0, nil); !ok || string(got) != "from-1" {
		t.Fatalf("shard 1 read one = (%q, %v), want (from-1, true)", got, ok)
	}
	if _, ok := r1.GetString([]byte("two"), 0, nil); ok {
		t.Fatal("shard 1 replayed shard 2's key")
	}
	if got, ok := r2.GetString([]byte("two"), 0, nil); !ok || string(got) != "from-2" {
		t.Fatalf("shard 2 read two = (%q, %v), want (from-2, true)", got, ok)
	}
	if _, ok := r2.GetString([]byte("one"), 0, nil); ok {
		t.Fatal("shard 2 replayed shard 1's key")
	}
}

// TestReplayNoHandleIsNoop confirms a store with no record log replays nothing and
// reports no error, so the volatile-only configuration pays nothing for recovery.
func TestReplayNoHandleIsNoop(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.ReplayRecords(0); err != nil {
		t.Fatalf("replay on a store with no record log: %v", err)
	}
}
