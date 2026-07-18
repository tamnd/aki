package store

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/akifile"
)

// TestCollLiveFramesDropsSuperseded is the box-free core of set compaction (spec
// 2064/f3/M8-collection-durability-plan slice 4): the liveness rule the record-region
// compaction driver consumes. It builds a shard log where one key accretes effects, a
// snapshot folds them, and one more effect lands past the snapshot, plus a second key
// with no snapshot and a frame of another kind, then asserts CollLiveFrames keeps the
// snapshot and its tail, drops the effects the snapshot superseded, counts their bytes
// as dead, and ignores the other kind. The physical reclaim that consumes the live set
// is the box-gated integration, so this proves only the rule, the same box-free split
// slice 3 took for the snapshot.
func TestCollLiveFramesDropsSuperseded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "colllive.aki")
	f, err := akifile.Create(path, akifile.CreateOptions{ShardCount: 4, Sync: akifile.SyncNo})
	if err != nil {
		t.Fatalf("create aki: %v", err)
	}
	s, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f, Shard: 1})
	if err != nil {
		t.Fatalf("open aki store: %v", err)
	}

	const kind = akifile.CollKindSet
	hdr := make([]byte, 8) // a snapshot header, contents irrelevant to liveness

	// key a: two effects, a snapshot folding them, then one more effect past it.
	s.LogCollectionOp([]byte("a"), kind, 1, []byte("m1"), nil)
	s.LogCollectionOp([]byte("a"), kind, 1, []byte("m2"), nil)
	s.LogCollectionSnap([]byte("a"), kind, hdr, []byte("m1m2"))
	s.LogCollectionOp([]byte("a"), kind, 1, []byte("m3"), nil)
	// key b: one effect, never snapshotted, so it stays live for want of a base.
	s.LogCollectionOp([]byte("b"), kind, 1, []byte("x"), nil)
	// a frame of another kind must not enter the set's liveness set.
	s.LogCollectionOp([]byte("c"), akifile.CollKindHash, 1, []byte("f"), nil)
	if s.rlogErr != nil {
		t.Fatalf("cut a collection frame: %v", s.rlogErr)
	}

	// Reopen so the walk reads only durable bytes, the state a compaction sees.
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
	s2, err := Open(Options{ArenaBytes: 4 << 20, SegBytes: 1 << 20, AkiValueLog: f2, Shard: 1})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(); _ = f2.Close() })

	live, dead, err := s2.CollLiveFrames(kind)
	if err != nil {
		t.Fatalf("live frames: %v", err)
	}
	// Live is the snapshot, the a-effect after it, and b's lone effect. The two
	// pre-snapshot a-effects are superseded, so they are dead and out of the set.
	if len(live) != 3 {
		t.Fatalf("live frame count = %d, want 3 (snapshot + post-snap effect + unsnapshotted key)", len(live))
	}
	if dead == 0 {
		t.Fatal("dead bytes = 0, want the two superseded pre-snapshot effects counted")
	}
	// Live addresses come back in append order, the order a rewrite re-homes them.
	for i := 1; i < len(live); i++ {
		if live[i] <= live[i-1] {
			t.Fatalf("live addresses out of append order: %d then %d", live[i-1], live[i])
		}
	}
	// Read each survivor back and confirm the composition: key a keeps exactly one
	// snapshot and one effect, key b keeps its one effect, and nothing of another
	// kind leaked in.
	var snaps, ops int
	perKey := map[string]int{}
	for _, addr := range live {
		row, err := f2.ReadRecordAt(addr)
		if err != nil {
			t.Fatalf("read live frame at %d: %v", addr, err)
		}
		perKey[string(row.Key)]++
		switch {
		case row.Flags&akifile.RecFlagCollectionSnap != 0:
			snap, err := akifile.ParseCollSnap(row.Value)
			if err != nil || snap.Kind != kind {
				t.Fatalf("live snapshot at %d parses wrong: kind=%v err=%v", addr, snap.Kind, err)
			}
			snaps++
		case row.Flags&akifile.RecFlagCollectionOp != 0:
			op, err := akifile.ParseCollOp(row.Value)
			if err != nil || op.Kind != kind {
				t.Fatalf("live effect at %d parses wrong: kind=%v err=%v", addr, op.Kind, err)
			}
			ops++
		default:
			t.Fatalf("live frame at %d is neither a snapshot nor an effect", addr)
		}
	}
	if snaps != 1 || ops != 2 {
		t.Fatalf("live composition = %d snapshots %d effects, want 1 and 2", snaps, ops)
	}
	if perKey["a"] != 2 || perKey["b"] != 1 || perKey["c"] != 0 {
		t.Fatalf("live per-key = %v, want a:2 b:1 c:0", perKey)
	}
}

// TestCollLiveFramesNoLog proves CollLiveFrames is inert on a store with no record
// log: there is nothing durable to compact, so it returns an empty live set and no
// dead bytes rather than touching the in-memory registry.
func TestCollLiveFramesNoLog(t *testing.T) {
	s := New(16<<20, 1<<20)
	live, dead, err := s.CollLiveFrames(akifile.CollKindSet)
	if err != nil {
		t.Fatalf("live frames on a no-log store: %v", err)
	}
	if len(live) != 0 || dead != 0 {
		t.Fatalf("no-log store reported %d live and %d dead, want 0 and 0", len(live), dead)
	}
}
