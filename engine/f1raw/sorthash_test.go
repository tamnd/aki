package f1raw

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// buildSnap makes a published snapshot from a map of arena-offset to member hash, the way a fold
// would leave it: ascending by (hash, off). Tests that exercise the merge want a controlled array,
// so they build the snapshot directly rather than through foldBatch.
func buildSnap(members map[uint64]uint64) *sortedSnap {
	type pair struct{ h, off uint64 }
	ps := make([]pair, 0, len(members))
	for off, h := range members {
		ps = append(ps, pair{h: h, off: off})
	}
	slices.SortFunc(ps, func(a, b pair) int {
		if a.h != b.h {
			if a.h < b.h {
				return -1
			}
			return 1
		}
		if a.off < b.off {
			return -1
		}
		if a.off > b.off {
			return 1
		}
		return 0
	})
	snap := &sortedSnap{h: make([]uint64, len(ps)), off: make([]uint64, len(ps))}
	for i, p := range ps {
		snap.h[i] = p.h
		snap.off[i] = p.off
	}
	return snap
}

// snapIsSorted asserts a snapshot is in strict ascending (hash, off) order, the invariant every
// fold must preserve and every merge relies on.
func snapIsSorted(t *testing.T, snap *sortedSnap) {
	t.Helper()
	if len(snap.h) != len(snap.off) {
		t.Fatalf("h/off length mismatch: %d vs %d", len(snap.h), len(snap.off))
	}
	for i := 1; i < len(snap.h); i++ {
		if snap.h[i-1] > snap.h[i] || (snap.h[i-1] == snap.h[i] && snap.off[i-1] >= snap.off[i]) {
			t.Fatalf("not ascending at %d: (%d,%d) then (%d,%d)", i, snap.h[i-1], snap.off[i-1], snap.h[i], snap.off[i])
		}
	}
}

// TestFoldBatchConverges runs a mixed add/remove workload in several batches and asserts the sorted
// array converges to exactly the live member-hash set, sorted, after each batch. This is slice 1's
// definition of done: the array is a correct maintained mirror of the live members.
func TestFoldBatchConverges(t *testing.T) {
	sh := newSortedHashes(0)
	rng := rand.New(rand.NewPCG(1, 2))

	// live maps a still-present arena offset to its member hash; it is the truth the array must match.
	live := map[uint64]uint64{}
	var nextOff uint64 = 1
	var gen uint64

	for batch := 0; batch < 40; batch++ {
		var deltas []foldDelta
		// Some adds.
		for i := 0; i < 1+rng.IntN(20); i++ {
			off := nextOff
			nextOff++
			h := rng.Uint64()
			deltas = append(deltas, foldDelta{hash: h, off: off, add: true})
			live[off] = h
		}
		// Some removes of currently-live offsets.
		if len(live) > 0 {
			offs := make([]uint64, 0, len(live))
			for off := range live {
				offs = append(offs, off)
			}
			for i := 0; i < rng.IntN(10) && len(offs) > 0; i++ {
				k := rng.IntN(len(offs))
				off := offs[k]
				offs[k] = offs[len(offs)-1]
				offs = offs[:len(offs)-1]
				h := live[off]
				delete(live, off)
				deltas = append(deltas, foldDelta{hash: h, off: off, add: false})
			}
		}

		gen++
		sh.foldBatch(deltas, gen)
		snap := sh.load()
		snapIsSorted(t, snap)
		if snap.gen != gen {
			t.Fatalf("batch %d: gen %d, want %d", batch, snap.gen, gen)
		}

		want := buildSnap(live)
		if !slices.Equal(snap.h, want.h) || !slices.Equal(snap.off, want.off) {
			t.Fatalf("batch %d: array diverged from live set (%d vs %d entries)", batch, len(snap.h), len(want.h))
		}
	}
}

// TestFoldBatchAddThenRemoveNets checks that a record added and removed within the same batch nets
// out to absent, since the remove set is keyed by offset and the add is skipped when its offset is
// also removed in the batch.
func TestFoldBatchAddThenRemoveNets(t *testing.T) {
	sh := newSortedHashes(0)
	sh.foldBatch([]foldDelta{
		{hash: 100, off: 1, add: true},
		{hash: 200, off: 2, add: true},
		{hash: 100, off: 1, add: false}, // cancels off 1
	}, 1)
	snap := sh.load()
	snapIsSorted(t, snap)
	if len(snap.h) != 1 || snap.h[0] != 200 || snap.off[0] != 2 {
		t.Fatalf("expected only (200,2), got h=%v off=%v", snap.h, snap.off)
	}
}

// TestFoldBatchEmptyRepublishesGen checks an empty batch still advances the published generation,
// which the folder uses to record it has caught up even when nothing changed.
func TestFoldBatchEmptyRepublishesGen(t *testing.T) {
	sh := newSortedHashes(0)
	sh.foldBatch([]foldDelta{{hash: 7, off: 9, add: true}}, 5)
	sh.foldBatch(nil, 8)
	snap := sh.load()
	if snap.gen != 8 {
		t.Fatalf("gen %d, want 8", snap.gen)
	}
	if len(snap.h) != 1 || snap.h[0] != 7 {
		t.Fatalf("empty batch changed contents: %v", snap.h)
	}
}

// TestFoldBatchSnapshotStableUnderRefold checks that a snapshot taken before a fold is not mutated
// by a later fold, the copy-on-write guarantee a concurrent merge depends on.
func TestFoldBatchSnapshotStableUnderRefold(t *testing.T) {
	sh := newSortedHashes(0)
	sh.foldBatch([]foldDelta{{hash: 10, off: 1, add: true}, {hash: 20, off: 2, add: true}}, 1)
	before := sh.load()
	beforeH := slices.Clone(before.h)
	beforeOff := slices.Clone(before.off)

	sh.foldBatch([]foldDelta{{hash: 15, off: 3, add: true}, {hash: 10, off: 1, add: false}}, 2)

	if !slices.Equal(before.h, beforeH) || !slices.Equal(before.off, beforeOff) {
		t.Fatalf("old snapshot mutated by later fold: %v/%v", before.h, before.off)
	}
	after := sh.load()
	if after == before {
		t.Fatalf("view pointer not swapped")
	}
}

// TestIntersectEmit checks the two-pointer intersection emits exactly the shared members in A order.
func TestIntersectEmit(t *testing.T) {
	// A offsets: 10..19 hashed to h=off*7; B shares even hashes.
	a := buildSnap(map[uint64]uint64{10: 70, 11: 77, 12: 84, 13: 91, 14: 98})
	// B shares hashes 70, 84, 98 (offsets differ, distinct confirm true so shared).
	b := buildSnap(map[uint64]uint64{50: 70, 51: 84, 52: 98, 53: 200})

	var got []uint64
	intersectEmit(a, b, func(offA, offB uint64) bool { return a.hashAt(offA) == b.hashAt(offB) }, func(offA uint64) {
		got = append(got, offA)
	})
	// Shared hashes are 70(off10), 84(off12), 98(off14).
	want := []uint64{10, 12, 14}
	if !slices.Equal(got, want) {
		t.Fatalf("intersect emitted %v, want %v", got, want)
	}
}

// TestIntersectHashCollisionRejected checks that two distinct members sharing a 64-bit hash are not
// counted as intersecting: the byte-confirm callback rejects the pair.
func TestIntersectHashCollisionRejected(t *testing.T) {
	a := buildSnap(map[uint64]uint64{1: 500})
	b := buildSnap(map[uint64]uint64{2: 500}) // same hash, different member

	// identity: offset 1 is "alpha", offset 2 is "beta"; they collide on hash but differ.
	ident := map[uint64]string{1: "alpha", 2: "beta"}
	confirm := func(offA, offB uint64) bool { return ident[offA] == ident[offB] }

	var got []uint64
	intersectEmit(a, b, confirm, func(offA uint64) { got = append(got, offA) })
	if len(got) != 0 {
		t.Fatalf("hash collision wrongly emitted %v", got)
	}
	// And a real match on the same hash IS emitted.
	ident[2] = "alpha"
	got = got[:0]
	intersectEmit(a, b, confirm, func(offA uint64) { got = append(got, offA) })
	if !slices.Equal(got, []uint64{1}) {
		t.Fatalf("real match on colliding hash not emitted: %v", got)
	}
}

// TestIntersectRunOnBothSides checks the equal-hash run handling when several members on both sides
// share one hash, only some of which cross-confirm.
func TestIntersectRunOnBothSides(t *testing.T) {
	// Three A members and two B members all on hash 42, with a single true cross pair.
	a := buildSnap(map[uint64]uint64{1: 42, 2: 42, 3: 42})
	b := buildSnap(map[uint64]uint64{4: 42, 5: 42})
	ident := map[uint64]string{1: "a1", 2: "a2", 3: "a3", 4: "b1", 5: "a2"} // off5 equals off2
	confirm := func(offA, offB uint64) bool { return ident[offA] == ident[offB] }

	var got []uint64
	intersectEmit(a, b, confirm, func(offA uint64) { got = append(got, offA) })
	if !slices.Equal(got, []uint64{2}) {
		t.Fatalf("run intersection emitted %v, want [2]", got)
	}
}

// TestIntersectCount checks the count variant and its LIMIT early stop.
func TestIntersectCount(t *testing.T) {
	a := buildSnap(map[uint64]uint64{1: 10, 2: 20, 3: 30, 4: 40})
	b := buildSnap(map[uint64]uint64{5: 20, 6: 30, 7: 40, 8: 99})
	confirm := func(offA, offB uint64) bool { return a.hashAt(offA) == b.hashAt(offB) }

	if n := intersectCount(a, b, confirm, 0); n != 3 {
		t.Fatalf("count %d, want 3", n)
	}
	if n := intersectCount(a, b, confirm, 2); n != 2 {
		t.Fatalf("limited count %d, want 2", n)
	}
	if n := intersectCount(a, b, confirm, 10); n != 3 {
		t.Fatalf("count with high limit %d, want 3", n)
	}
}

// TestDiffEmit checks the SDIFF merge keeps A members absent from B, including a bare hash collision
// where the hash matches but the byte-confirm fails so the A member survives.
func TestDiffEmit(t *testing.T) {
	a := buildSnap(map[uint64]uint64{1: 10, 2: 20, 3: 30, 4: 40})
	b := buildSnap(map[uint64]uint64{5: 20, 6: 40})
	confirm := func(offA, offB uint64) bool { return a.hashAt(offA) == b.hashAt(offB) }

	var got []uint64
	diffEmit(a, b, confirm, func(offA uint64) { got = append(got, offA) })
	// A minus B: hashes 10(off1) and 30(off3) remain.
	if !slices.Equal(got, []uint64{1, 3}) {
		t.Fatalf("diff emitted %v, want [1 3]", got)
	}

	// Now a bare collision: A off7 hash 55, B off8 hash 55 but different member, so A survives.
	a2 := buildSnap(map[uint64]uint64{7: 55})
	b2 := buildSnap(map[uint64]uint64{8: 55})
	ident := map[uint64]string{7: "x", 8: "y"}
	got = got[:0]
	diffEmit(a2, b2, func(offA, offB uint64) bool { return ident[offA] == ident[offB] }, func(offA uint64) {
		got = append(got, offA)
	})
	if !slices.Equal(got, []uint64{7}) {
		t.Fatalf("diff over bare collision emitted %v, want [7]", got)
	}
}

// TestIntersectEmptyOperands checks the merges terminate on empty inputs.
func TestIntersectEmptyOperands(t *testing.T) {
	empty := &sortedSnap{}
	full := buildSnap(map[uint64]uint64{1: 10, 2: 20})
	confirm := func(offA, offB uint64) bool { return true }

	intersectEmit(empty, full, confirm, func(uint64) { t.Fatal("emitted from empty A") })
	intersectEmit(full, empty, confirm, func(uint64) { t.Fatal("emitted from empty B") })
	if n := intersectCount(empty, empty, confirm, 0); n != 0 {
		t.Fatalf("empty count %d, want 0", n)
	}
	var got []uint64
	diffEmit(full, empty, confirm, func(offA uint64) { got = append(got, offA) })
	if !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("A minus empty emitted %v, want [1 2]", got)
	}
}

// hashAt resolves an arena offset back to its hash within a test snapshot, so a test confirm can
// compare member hashes without an arena. It is a linear scan, fine for the tiny test snapshots.
func (s *sortedSnap) hashAt(off uint64) uint64 {
	for i := range s.off {
		if s.off[i] == off {
			return s.h[i]
		}
	}
	return 0
}
