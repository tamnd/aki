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

// TestPow2Ceil pins the bucket-count rounding the checkpoint header carries.
func TestPow2Ceil(t *testing.T) {
	cases := map[uint64]uint64{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 1000: 1024}
	for in, want := range cases {
		if got := pow2ceil(in); got != want {
			t.Fatalf("pow2ceil(%d) = %d, want %d", in, got, want)
		}
	}
}
