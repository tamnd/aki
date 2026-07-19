package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
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

// TestReplaySkipsCollectionFrames pins the mixed-vertical recovery walk: a store
// that carries both string records and collection effect frames in the one shared
// record log must rebuild the string index without choking on the collection
// frames, which have no string value band and belong to WalkCollection instead. It
// is the regression for the arena-resident failure the first end-to-end restart
// surfaced: before the skip, the string replay fed a RecFlagCollectionOp frame into
// applyValueRow, which read its opaque effect payload as a value and failed closed.
func TestReplaySkipsCollectionFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	openStore := func(f *akifile.File) *Store {
		s, err := Open(Options{
			ArenaBytes:  4 << 20,
			SegBytes:    1 << 20,
			AkiValueLog: f,
			Shard:       1,
		})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	// First run: two string writes straddling a set effect and a set snapshot frame,
	// so the string walk must step over collection frames both before and after a
	// string record it must still recover.
	s := openStore(f)
	if err := s.SetString([]byte("before"), []byte("v1"), 0, 0, false); err != nil {
		t.Fatalf("set before: %v", err)
	}
	s.LogCollectionOp([]byte("myset"), akifile.CollKindSet, 0, []byte("member"), nil)
	s.LogCollectionSnap([]byte("myset"), akifile.CollKindSet, []byte("hdr"), []byte("run"))
	if err := s.SetString([]byte("after"), []byte("v2"), 0, 0, false); err != nil {
		t.Fatalf("set after: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: replay the log into a fresh store. The collection frames are
	// skipped and both string keys come back.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	if err := s2.ReplayRecords(0); err != nil {
		t.Fatalf("replay over collection frames: %v", err)
	}
	for key, want := range map[string]string{"before": "v1", "after": "v2"} {
		got, ok := s2.GetString([]byte(key), 0, nil)
		if !ok {
			t.Fatalf("string key %q absent after mixed replay", key)
		}
		if string(got) != want {
			t.Fatalf("string key %q read back %q, want %q", key, got, want)
		}
	}
	// The collection key never entered the string index: it rebuilds through
	// WalkCollection, not this walk, so a string GET must not resurrect it.
	if _, ok := s2.GetString([]byte("myset"), 0, nil); ok {
		t.Fatal("collection key myset leaked into the string index after replay")
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

// TestSharedWriterRecoveryRoundTrip is the end-to-end capstone for the group-commit
// seam: two shard stores share one .aki and one group-commit writer, and each runs
// real SetString commands from its own owner goroutine at the same time, so every
// record cut goes through the one writer that serializes the single-writer file
// rather than the direct path the earlier shard-scoped test took. It then drops both
// stores and the writer, reopens the file into fresh empty stores, and replays each
// shard's log, asserting every key the first run committed reads back at its shard.
// Run under -race this proves the writer produces a correct, recoverable durable log
// under concurrent multi-shard commit, which the direct-cut recovery tests cannot
// reach because they each own the file alone.
func TestSharedWriterRecoveryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sharedrec.aki")
	const shardA, shardB = 1, 2
	const per = 200

	openStore := func(f *akifile.File, shard uint16, gw *akifile.GroupWriter) *Store {
		s, err := Open(Options{
			ArenaBytes:     4 << 20,
			SegBytes:       1 << 20,
			AkiValueLog:    f,
			Shard:          shard,
			AkiGroupWriter: gw,
		})
		if err != nil {
			t.Fatalf("open shard %d: %v", shard, err)
		}
		return s
	}

	// First run: both shards commit through the one writer concurrently. A ring
	// smaller than the per-shard command count forces the submitWait backpressure
	// spin, so this exercises a full writer ring too.
	f, err := akifile.Create(path, akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	gw := akifile.NewGroupWriter(f, 4, 16)

	keyA := func(i int) []byte { return []byte(fmt.Sprintf("a-key-%04d", i)) }
	keyB := func(i int) []byte { return []byte(fmt.Sprintf("b-key-%04d", i)) }
	valA := func(i int) []byte { return []byte(fmt.Sprintf("a-val-%04d", i)) }
	valB := func(i int) []byte { return []byte(fmt.Sprintf("b-val-%04d", i)) }

	sa := openStore(f, shardA, gw)
	sb := openStore(f, shardB, gw)
	var wg sync.WaitGroup
	wg.Add(2)
	var setErr [2]error
	go func() {
		defer wg.Done()
		for i := 0; i < per; i++ {
			if err := sa.SetString(keyA(i), valA(i), 0, 0, false); err != nil {
				setErr[0] = err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < per; i++ {
			if err := sb.SetString(keyB(i), valB(i), 0, 0, false); err != nil {
				setErr[1] = err
				return
			}
		}
	}()
	wg.Wait()
	if setErr[0] != nil || setErr[1] != nil {
		t.Fatalf("concurrent sets: shardA=%v shardB=%v", setErr[0], setErr[1])
	}

	// The writer must quiesce before the file closes, then the borrowed handle
	// closes once, after both stores let go of it.
	gw.Stop()
	if err := sa.Close(); err != nil {
		t.Fatalf("close shard A: %v", err)
	}
	if err := sb.Close(); err != nil {
		t.Fatalf("close shard B: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen and rebuild each shard from the log the writer laid down.
	// Recovery does not log, so the fresh stores need no writer.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	ra := openStore(f2, shardA, nil)
	rb := openStore(f2, shardB, nil)
	t.Cleanup(func() { _ = ra.Close(); _ = rb.Close(); _ = f2.Close() })
	if err := ra.ReplayRecords(0); err != nil {
		t.Fatalf("replay shard A: %v", err)
	}
	if err := rb.ReplayRecords(0); err != nil {
		t.Fatalf("replay shard B: %v", err)
	}

	// Every key each shard committed reads back at its own value, and neither shard
	// recovered the other's keys.
	for i := 0; i < per; i++ {
		if got, ok := ra.GetString(keyA(i), 0, nil); !ok || !bytes.Equal(got, valA(i)) {
			t.Fatalf("shard A key %d = (%q, %v), want (%q, true)", i, got, ok, valA(i))
		}
		if got, ok := rb.GetString(keyB(i), 0, nil); !ok || !bytes.Equal(got, valB(i)) {
			t.Fatalf("shard B key %d = (%q, %v), want (%q, true)", i, got, ok, valB(i))
		}
	}
	if _, ok := ra.GetString(keyB(0), 0, nil); ok {
		t.Fatal("shard A recovered a shard B key")
	}
	if _, ok := rb.GetString(keyA(0), 0, nil); ok {
		t.Fatal("shard B recovered a shard A key")
	}
}

// TestReplayFromCheckpointRebuildsIndex closes the producer to consumer loop: a
// first run writes a mix, overwrites one key and deletes another, builds a full
// index checkpoint, and drops the store; a fresh store over the same file loads
// that checkpoint and reads every live key back at its newest value, with the
// deleted key gone. It proves recovery can rebuild the index from a checkpoint dump
// alone, without walking the whole log.
func TestReplayFromCheckpointRebuildsIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckptreplay.aki")

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

	// First run: write the mix, overwrite one key, delete another, then dump a
	// checkpoint before dropping the store so only durable bytes carry forward.
	f, err := akifile.Create(path, akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := openStore(f)
	if err := s.SetString([]byte("greet"), []byte("hi"), 0, 0, false); err != nil {
		t.Fatalf("set greet: %v", err)
	}
	if err := s.SetString([]byte("wide"), big, 0, 0, false); err != nil {
		t.Fatalf("set wide: %v", err)
	}
	if err := s.SetString([]byte("gone"), []byte("x"), 0, 0, false); err != nil {
		t.Fatalf("set gone: %v", err)
	}
	if err := s.SetString([]byte("greet"), []byte("hello"), 0, 0, false); err != nil {
		t.Fatalf("overwrite greet: %v", err)
	}
	if !s.Del([]byte("gone"), 0) {
		t.Fatal("del gone reported absent for a live key")
	}

	payload, _, err := s.BuildIndexCheckpoint(nil)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Second run: reopen the file into a fresh store and rebuild from the dump.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	if err := s2.ReplayFromCheckpoint(payload, 0); err != nil {
		t.Fatalf("replay from checkpoint: %v", err)
	}

	wantVal := map[string][]byte{
		"greet": []byte("hello"),
		"wide":  big,
	}
	for key, want := range wantVal {
		got, ok := s2.GetString([]byte(key), 0, nil)
		if !ok {
			t.Fatalf("key %q absent after checkpoint replay", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %q read back %q, want %q", key, got, want)
		}
	}
	if _, ok := s2.GetString([]byte("gone"), 0, nil); ok {
		t.Fatal("deleted key gone came back after checkpoint replay")
	}
}

// TestReplayFromCheckpointNoHandleIsNoop confirms a store with no record log takes
// a nil-safe no-op path, so the volatile-only configuration never touches the
// checkpoint loader.
func TestReplayFromCheckpointNoHandleIsNoop(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.ReplayFromCheckpoint(nil, 0); err != nil {
		t.Fatalf("checkpoint replay on a store with no record log: %v", err)
	}
}

// TestBoundedRecoveryComposesCheckpointAndTail is the bounded recovery loop: a
// first run writes a batch, builds a checkpoint over it and remembers the append
// offset, then writes a second batch that overwrites, deletes, and adds keys past
// the checkpoint. A fresh store loads the checkpoint and replays only the tail from
// that offset, and the composed state matches the first run exactly: the overwrite
// won, the delete stuck, the new key landed, and no prefix record was applied
// twice. It proves a restart can skip the settled prefix instead of walking the
// whole log.
func TestBoundedRecoveryComposesCheckpointAndTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.aki")

	openStore := func(f *akifile.File) *Store {
		s, err := Open(Options{
			ArenaBytes:  4 << 20,
			SegBytes:    1 << 20,
			AkiValueLog: f,
			Shard:       1,
		})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	f, err := akifile.Create(path, akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := openStore(f)

	// Batch one is the settled prefix the checkpoint captures.
	if err := s.SetString([]byte("keep"), []byte("k1"), 0, 0, false); err != nil {
		t.Fatalf("set keep: %v", err)
	}
	if err := s.SetString([]byte("edit"), []byte("e1"), 0, 0, false); err != nil {
		t.Fatalf("set edit: %v", err)
	}
	if err := s.SetString([]byte("drop"), []byte("d1"), 0, 0, false); err != nil {
		t.Fatalf("set drop: %v", err)
	}

	payload, _, err := s.BuildIndexCheckpoint(nil)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	// The append offset the instant the checkpoint was built is where the tail
	// starts: every later record is cut past it.
	tailFrom := s.akirlog.cursor()

	// Batch two is the tail: overwrite one prefix key, delete another, add a fresh
	// one. None of it is in the checkpoint.
	if err := s.SetString([]byte("edit"), []byte("e2"), 0, 0, false); err != nil {
		t.Fatalf("overwrite edit: %v", err)
	}
	if !s.Del([]byte("drop"), 0) {
		t.Fatal("del drop reported absent for a live key")
	}
	if err := s.SetString([]byte("new"), []byte("n1"), 0, 0, false); err != nil {
		t.Fatalf("set new: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	// Recovery: load the settled prefix from the dump, then replay only the tail.
	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	if err := s2.ReplayFromCheckpoint(payload, 0); err != nil {
		t.Fatalf("replay from checkpoint: %v", err)
	}
	if err := s2.ReplayTail(tailFrom, 0); err != nil {
		t.Fatalf("replay tail: %v", err)
	}

	wantVal := map[string][]byte{
		"keep": []byte("k1"), // untouched by the tail, survives from the checkpoint
		"edit": []byte("e2"), // the tail overwrite wins over the checkpoint value
		"new":  []byte("n1"), // added in the tail, absent from the checkpoint
	}
	for key, want := range wantVal {
		got, ok := s2.GetString([]byte(key), 0, nil)
		if !ok {
			t.Fatalf("key %q absent after bounded recovery", key)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %q read back %q, want %q", key, got, want)
		}
	}
	// The checkpoint carried drop, the tail deleted it: the composed state drops it.
	if _, ok := s2.GetString([]byte("drop"), 0, nil); ok {
		t.Fatal("deleted key drop survived bounded recovery")
	}
}

// TestReplayTailNoHandleIsNoop confirms the tail replay is a nil-safe no-op on a
// store with no record log.
func TestReplayTailNoHandleIsNoop(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.ReplayTail(akifile.PageSize, 0); err != nil {
		t.Fatalf("tail replay on a store with no record log: %v", err)
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
