package store

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// TestBuildIndexCheckpointDumpsLiveSet writes a mix of values, overwrites one key
// and deletes another, then builds a checkpoint and asserts it dumps exactly the
// live set: every surviving key at its newest record address, the deleted key
// absent, and each address dereferencing back to the key it is filed under.
func TestBuildIndexCheckpointDumpsLiveSet(t *testing.T) {
	f, err := akifile.Create(filepath.Join(t.TempDir(), "ckpt.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s, err := Open(Options{
		ArenaBytes:  4 << 20,
		SegBytes:    1 << 20,
		AkiValueLog: f,
		Shard:       1,
	})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = f.Close() })

	if err := s.SetString([]byte("alpha"), []byte("a1"), 0, 0, false); err != nil {
		t.Fatalf("set alpha: %v", err)
	}
	if err := s.SetString([]byte("beta"), []byte("b1"), 0, 0, false); err != nil {
		t.Fatalf("set beta: %v", err)
	}
	if err := s.SetString([]byte("gone"), []byte("g1"), 0, 0, false); err != nil {
		t.Fatalf("set gone: %v", err)
	}
	// Overwrite beta so the checkpoint must pick its newest address, and delete
	// gone so the dump drops it.
	if err := s.SetString([]byte("beta"), []byte("b2"), 0, 0, false); err != nil {
		t.Fatalf("overwrite beta: %v", err)
	}
	if !s.Del([]byte("gone"), 0) {
		t.Fatal("del gone reported absent for a live key")
	}

	payload, hdr, err := s.BuildIndexCheckpoint(nil)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	if hdr.FullOrDelta != akifile.CkptFull {
		t.Fatalf("checkpoint kind = %d, want full", hdr.FullOrDelta)
	}
	if hdr.EntryCount != 2 {
		t.Fatalf("entry count = %d, want 2 (alpha, beta)", hdr.EntryCount)
	}
	if hdr.BucketCount < hdr.EntryCount {
		t.Fatalf("bucket count %d smaller than entry count %d", hdr.BucketCount, hdr.EntryCount)
	}

	parsed, err := akifile.ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	entries, err := akifile.CkptEntries(payload, parsed)
	if err != nil {
		t.Fatalf("parse entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("parsed %d entries, want 2", len(entries))
	}

	// Every dumped address dereferences to the key it is filed under, so the dump
	// carries real durable frame addresses, not stale or invented ones.
	byHash := map[uint64]akifile.CkptEntry{}
	for _, e := range entries {
		row, err := f.ReadRecordAt(e.RecordAddr)
		if err != nil {
			t.Fatalf("read record at %#x: %v", e.RecordAddr, err)
		}
		if Hash(row.Key) != e.KeyHash {
			t.Fatalf("entry hash %#x points at key %q (hash %#x)", e.KeyHash, row.Key, Hash(row.Key))
		}
		byHash[e.KeyHash] = e
	}
	if _, ok := byHash[Hash([]byte("gone"))]; ok {
		t.Fatal("deleted key gone is still in the checkpoint")
	}

	// Beta's dumped address must be its newest record, the one holding b2.
	be, ok := byHash[Hash([]byte("beta"))]
	if !ok {
		t.Fatal("beta missing from checkpoint")
	}
	row, err := f.ReadRecordAt(be.RecordAddr)
	if err != nil {
		t.Fatalf("read beta record: %v", err)
	}
	if string(row.Value) != "b2" {
		t.Fatalf("beta checkpoint address holds %q, want the newest value b2", row.Value)
	}
}

// TestBuildIndexCheckpointEmpty confirms a store with nothing durable frames a
// valid empty full checkpoint rather than erroring, so a checkpoint of a fresh
// shard is well formed.
func TestBuildIndexCheckpointEmpty(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	payload, hdr, err := s.BuildIndexCheckpoint(nil)
	if err != nil {
		t.Fatalf("build checkpoint: %v", err)
	}
	if hdr.EntryCount != 0 {
		t.Fatalf("empty checkpoint entry count = %d, want 0", hdr.EntryCount)
	}
	parsed, err := akifile.ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse empty header: %v", err)
	}
	entries, err := akifile.CkptEntries(payload, parsed)
	if err != nil {
		t.Fatalf("parse empty entries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty checkpoint parsed %d entries, want 0", len(entries))
	}
}

// TestWriteIndexCheckpointPersistsDumpAndRow writes a mix, persists a checkpoint,
// and asserts the returned SRT row names a real index_ckpt segment: the row's live
// count and tail offset match the store, the segment reads back as a KindIndexCkpt
// for this shard, and its entries deref to the live keys at their newest addresses.
func TestWriteIndexCheckpointPersistsDumpAndRow(t *testing.T) {
	f, err := akifile.Create(filepath.Join(t.TempDir(), "persist.aki"), akifile.CreateOptions{
		ShardCount:   4,
		SepThreshold: 64,
		Sync:         akifile.SyncNo,
	})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = f.Close() })

	if err := s.SetString([]byte("alpha"), []byte("a1"), 0, 0, false); err != nil {
		t.Fatalf("set alpha: %v", err)
	}
	if err := s.SetString([]byte("beta"), []byte("b1"), 0, 0, false); err != nil {
		t.Fatalf("set beta: %v", err)
	}
	if err := s.SetString([]byte("gone"), []byte("g1"), 0, 0, false); err != nil {
		t.Fatalf("set gone: %v", err)
	}
	if err := s.SetString([]byte("beta"), []byte("b2"), 0, 0, false); err != nil {
		t.Fatalf("overwrite beta: %v", err)
	}
	if !s.Del([]byte("gone"), 0) {
		t.Fatal("del gone reported absent for a live key")
	}

	// The tail starts where the append cursor sits the instant before the persist,
	// so the row's tail offset must equal it.
	tailBefore := s.akirlog.cursor()
	row, err := s.WriteIndexCheckpoint()
	if err != nil {
		t.Fatalf("write index checkpoint: %v", err)
	}
	if row.LiveRecords != 2 {
		t.Fatalf("row live records = %d, want 2 (alpha, beta)", row.LiveRecords)
	}
	if row.FirstTailSeg != tailBefore {
		t.Fatalf("row tail offset = %#x, want the pre-append cursor %#x", row.FirstTailSeg, tailBefore)
	}
	if row.IndexCkptLen == 0 {
		t.Fatal("row names a zero-length checkpoint")
	}

	// The row points at a real index_ckpt segment for this shard.
	h, payload, err := f.ReadSegmentAt(row.IndexCkptOff - akifile.SegHeaderLen)
	if err != nil {
		t.Fatalf("read checkpoint segment: %v", err)
	}
	if h.Kind != akifile.KindIndexCkpt {
		t.Fatalf("segment kind = %d, want index_ckpt", h.Kind)
	}
	if h.Shard != 1 {
		t.Fatalf("segment shard = %d, want 1", h.Shard)
	}
	if uint64(len(payload)) != row.IndexCkptLen {
		t.Fatalf("segment payload %d bytes, row names %d", len(payload), row.IndexCkptLen)
	}

	hdr, err := akifile.ParseCkptHeader(payload)
	if err != nil {
		t.Fatalf("parse persisted header: %v", err)
	}
	entries, err := akifile.CkptEntries(payload, hdr)
	if err != nil {
		t.Fatalf("parse persisted entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("persisted %d entries, want 2", len(entries))
	}
	byHash := map[uint64]akifile.CkptEntry{}
	for _, e := range entries {
		rec, err := f.ReadRecordAt(e.RecordAddr)
		if err != nil {
			t.Fatalf("read record at %#x: %v", e.RecordAddr, err)
		}
		if Hash(rec.Key) != e.KeyHash {
			t.Fatalf("entry hash %#x points at key %q", e.KeyHash, rec.Key)
		}
		byHash[e.KeyHash] = e
	}
	if _, ok := byHash[Hash([]byte("gone"))]; ok {
		t.Fatal("deleted key gone is in the persisted checkpoint")
	}
	be := byHash[Hash([]byte("beta"))]
	rec, err := f.ReadRecordAt(be.RecordAddr)
	if err != nil {
		t.Fatalf("read beta record: %v", err)
	}
	if string(rec.Value) != "b2" {
		t.Fatalf("beta persisted address holds %q, want newest b2", rec.Value)
	}
}

// TestPersistedCheckpointDrivesRecovery closes the loop the coordinator will wire:
// a first run persists a checkpoint and writes a tail record past it, then a fresh
// store recovers from the persisted segment plus the row's tail offset alone, with
// no in-memory state carried over, and every key reads back.
func TestPersistedCheckpointDrivesRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persistrec.aki")
	openStore := func(f *akifile.File) *Store {
		s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
		if err != nil {
			t.Fatalf("open aki store: %v", err)
		}
		return s
	}

	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, SepThreshold: 64, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s := openStore(f)
	if err := s.SetString([]byte("keep"), []byte("k1"), 0, 0, false); err != nil {
		t.Fatalf("set keep: %v", err)
	}
	if err := s.SetString([]byte("edit"), []byte("e1"), 0, 0, false); err != nil {
		t.Fatalf("set edit: %v", err)
	}
	row, err := s.WriteIndexCheckpoint()
	if err != nil {
		t.Fatalf("write index checkpoint: %v", err)
	}
	// A tail record past the checkpoint: overwrite one prefix key and add a new one.
	if err := s.SetString([]byte("edit"), []byte("e2"), 0, 0, false); err != nil {
		t.Fatalf("overwrite edit: %v", err)
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

	f2, err := akifile.Open(path, akifile.OpenOptions{Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("reopen aki: %v", err)
	}
	s2 := openStore(f2)
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	// Read the dump straight from the segment the row names, the way a committed
	// root's IndexCkptOff will point recovery at it.
	_, payload, err := f2.ReadSegmentAt(row.IndexCkptOff - akifile.SegHeaderLen)
	if err != nil {
		t.Fatalf("read persisted checkpoint: %v", err)
	}
	if err := s2.ReplayFromCheckpoint(payload, 0); err != nil {
		t.Fatalf("replay from persisted checkpoint: %v", err)
	}
	if err := s2.ReplayTail(row.FirstTailSeg, 0); err != nil {
		t.Fatalf("replay tail: %v", err)
	}

	wantVal := map[string]string{"keep": "k1", "edit": "e2", "new": "n1"}
	for key, want := range wantVal {
		got, ok := s2.GetString([]byte(key), 0, nil)
		if !ok || string(got) != want {
			t.Fatalf("key %q read back (%q, %v), want (%q, true)", key, got, ok, want)
		}
	}
}

// TestWriteIndexCheckpointNoHandle confirms a store with no record log returns a
// zero row rather than erroring, so the volatile-only configuration takes a clean
// no-op through the persist path.
func TestWriteIndexCheckpointNoHandle(t *testing.T) {
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20})
	if err != nil {
		t.Fatalf("open plain store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	row, err := s.WriteIndexCheckpoint()
	if err != nil {
		t.Fatalf("write checkpoint on a store with no record log: %v", err)
	}
	if row != (akifile.SRTRow{}) {
		t.Fatalf("no-record-log store returned a non-zero row %+v", row)
	}
}

// TestPow2Ceil pins the bucket-count rounding the checkpoint header carries.
func TestPow2Ceil(t *testing.T) {
	cases := map[uint64]uint64{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 1000: 1024}
	for in, want := range cases {
		if got := pow2ceil(in); got != want {
			t.Fatalf("pow2ceil(%d) = %d, want %d", in, got, want)
		}
	}
}
