package store

import (
	"fmt"
	"testing"
)

// The M7 reclamation half proven in isolation (spec 2064/f3/06 sections 2.4,
// 2.5, and the slice-1 plan): a drained segment retired at an epoch stamp stays
// resident and off the free list until the safe epoch moves strictly past the
// stamp, which is exactly when every bracket that could have named its bytes
// has exited. The bracket that produces the safe epoch is the F6 owner bracket
// (engine/f3/shard/epoch.go), proven in TestEpochBracketBump; its only output
// the reclaimer reads is safe(), a uint64, so these tests drive that number
// directly and tie each value to the bracket state it stands for.

// makeDeadSegment fills the current segment with separated records, spills into
// the next so the first is no longer current, then deletes every record in it,
// returning a fully dead non-current segment index.
func makeDeadSegment(t *testing.T, s *Store, prefix string) uint64 {
	t.Helper()
	seg, keys := fillSepSeg(t, s, prefix, 4096)
	// A second fill guarantees seg is comfortably behind the bump cursor.
	fillSepSeg(t, s, prefix+"pad", 4096)
	for _, k := range keys {
		if !s.Delete(k) {
			t.Fatalf("Delete %q: missing", k)
		}
	}
	if l := s.arena.segs[seg].live; l != 0 {
		t.Fatalf("segment %d live=%d after deleting its records, want 0", seg, l)
	}
	if s.arena.cur == seg {
		t.Fatalf("segment %d is still the bump target", seg)
	}
	return seg
}

// TestArenaRetireEpochGated pins the gate: a segment retired at a stamp is not
// reclaimed while the safe epoch sits at or below the stamp (a bracket in
// flight at retirement still pins safe there), and is reclaimed the moment safe
// passes it (that bracket has drained).
func TestArenaRetireEpochGated(t *testing.T) {
	s := testStore(t, 8)
	seg := makeDeadSegment(t, s, "a")

	const stamp = 5
	if !s.arena.retireSegment(seg, stamp) {
		t.Fatal("retireSegment refused a dead non-current segment")
	}
	if !s.arena.segs[seg].retired {
		t.Fatal("segment not marked retired")
	}
	freeBefore := s.arena.freeSegCount()
	fillBefore := s.arena.fillOf(seg)
	if fillBefore == 0 {
		t.Fatal("retired segment lost its bytes immediately")
	}

	// safe == stamp: the retiring bracket is still open (safe = its entry
	// epoch). Strictly-below means no free.
	if n := s.ReclaimSafe(stamp); n != 0 {
		t.Fatalf("ReclaimSafe(%d) freed %d with the bracket open, want 0", stamp, n)
	}
	// safe below the stamp: an even older bracket pins it. Still no free.
	if n := s.ReclaimSafe(stamp - 2); n != 0 {
		t.Fatalf("ReclaimSafe(%d) freed %d, want 0", stamp-2, n)
	}
	if !s.arena.segs[seg].retired || s.arena.freeSegCount() != freeBefore {
		t.Fatal("segment reclaimed before the safe epoch cleared it")
	}
	if got := s.arena.fillOf(seg); got != fillBefore {
		t.Fatalf("retired segment's bytes changed while gated: %d -> %d", fillBefore, got)
	}

	// safe past the stamp: the retiring bracket and every older one have
	// drained. The segment comes back.
	if n := s.ReclaimSafe(stamp + 1); n != 1 {
		t.Fatalf("ReclaimSafe(%d) freed %d, want 1", stamp+1, n)
	}
	if s.arena.segs[seg].retired {
		t.Fatal("segment still marked retired after reclaim")
	}
	if f := s.arena.fillOf(seg); f != 0 {
		t.Fatalf("reclaimed segment %d still holds %d bytes", seg, f)
	}
	if s.arena.freeSegCount() != freeBefore+1 {
		t.Fatalf("free count %d after reclaim, want %d", s.arena.freeSegCount(), freeBefore+1)
	}
	// A second call is a no-op: the retire list is empty.
	if n := s.ReclaimSafe(stamp + 100); n != 0 {
		t.Fatalf("second ReclaimSafe freed %d, want 0", n)
	}
}

// TestArenaRetiredSkippedByCompactor pins that the compaction and full-arena
// passes leave a retired segment alone even under the most aggressive
// threshold: its bytes are held for a reader, so only ReclaimSafe may take them
// back. Without the skip the fully-dead fast path would free it ungated.
func TestArenaRetiredSkippedByCompactor(t *testing.T) {
	s := testStore(t, 8)
	seg := makeDeadSegment(t, s, "a")
	if !s.arena.retireSegment(seg, 2) {
		t.Fatal("retireSegment refused")
	}

	// Every touched non-current segment is compaction-eligible now. The fill
	// helper leaves incidental dead bytes elsewhere, so prove the skip by the
	// delta: dropping the retire mark adds exactly the segment's dead bytes to
	// the reclaimable figure.
	s.TuneArenaReclaim(0, 1)
	segDead := s.arena.deadOf(seg)
	if segDead == 0 {
		t.Fatal("retired segment has no dead bytes; test setup broken")
	}
	withRetired := s.ArenaReclaimable()
	s.arena.segs[seg].retired = false
	without := s.ArenaReclaimable()
	s.arena.segs[seg].retired = true
	if without-withRetired != segDead {
		t.Fatalf("ArenaReclaimable counted %d of the retired segment's %d dead bytes, want 0", without-withRetired, segDead)
	}
	s.CompactArena()
	if !s.arena.segs[seg].retired {
		t.Fatal("CompactArena freed a retired segment")
	}
	if s.arena.fillOf(seg) == 0 {
		t.Fatal("CompactArena took a retired segment's bytes")
	}

	// The allocator never reuses it either (it is not on the free list): churn
	// several segments' worth and confirm nothing lands in it.
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("churn%06d", i))
		if err := s.Set(k, sepVal('z', 4096)); err != nil {
			t.Fatalf("Set churn %d: %v", i, err)
		}
		_, addr, _ := s.findEntry(Hash(k), k)
		if si, _ := s.arena.segOf(addr); si == seg {
			t.Fatalf("record %d allocated into the retired segment %d", i, seg)
		}
		s.Delete(k)
	}
	if !s.arena.segs[seg].retired {
		t.Fatal("retired segment lost its mark under churn")
	}

	// The epoch alone brings it back.
	if n := s.ReclaimSafe(3); n != 1 {
		t.Fatalf("ReclaimSafe took %d after the compactor was blocked, want 1", n)
	}
}

// TestArenaRetireRefusals pins the two refusals: the current bump segment
// cannot be retired (the next allocation would corrupt it), and a segment
// already retired is not double-listed.
func TestArenaRetireRefusals(t *testing.T) {
	s := testStore(t, 8)
	if s.arena.retireSegment(s.arena.cur, 1) {
		t.Fatal("retireSegment took the current segment")
	}
	seg := makeDeadSegment(t, s, "a")
	if !s.arena.retireSegment(seg, 1) {
		t.Fatal("first retire refused")
	}
	if s.arena.retireSegment(seg, 2) {
		t.Fatal("second retire double-listed the segment")
	}
	if got := len(s.arena.retired); got != 1 {
		t.Fatalf("retire list holds %d entries, want 1", got)
	}
}

// TestArenaResetClearsRetired pins that a flush drops the retire list and its
// per-segment marks, so a reused arena starts clean.
func TestArenaResetClearsRetired(t *testing.T) {
	s := testStore(t, 8)
	seg := makeDeadSegment(t, s, "a")
	if !s.arena.retireSegment(seg, 3) {
		t.Fatal("retire refused")
	}
	s.arena.reset()
	if len(s.arena.retired) != 0 {
		t.Fatalf("retire list survived reset: %d entries", len(s.arena.retired))
	}
	if s.arena.segs[seg].retired {
		t.Fatal("segment retired mark survived reset")
	}
	if n := s.ReclaimSafe(100); n != 0 {
		t.Fatalf("ReclaimSafe found %d segments after reset, want 0", n)
	}
}
