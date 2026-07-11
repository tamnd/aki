package main

import (
	"sort"
	"testing"
)

// TestKernelsAgree checks the three intersection kernels (probe, streaming
// merge, galloping merge) return the same match count across size ratios,
// overlaps, and member sizes, which is the lab's correctness bar: whatever the
// driver picks, it must produce the same result.
func TestKernelsAgree(t *testing.T) {
	for _, sz := range []int{8, 32, 64} {
		for _, k := range []int{1, 2, 7, 16, 64} {
			for _, ov := range []float64{0.0, 0.1, 0.5, 0.9, 1.0} {
				const small = 2000
				bigN := small * k
				bigIDs := make([]uint64, bigN)
				for i := range bigIDs {
					bigIDs[i] = uint64(i + 1)
				}
				big := newMemberSet(bigIDs, sz)
				tab := newTable(big)
				bigHS := buildHSet(big, tailCap)

				sm := makeSmall(small, bigN, ov, sz)
				smHS := buildHSet(sm, tailCap)

				want := int(ov*float64(small) + 0.5)
				op := probeIntersect(sm, tab)
				om := mergeIntersect(smHS, bigHS)
				og := gallopIntersect(smHS, bigHS)
				if op != want {
					t.Fatalf("sz=%d k=%d ov=%.2f: probe=%d want=%d", sz, k, ov, op, want)
				}
				if om != op || og != op {
					t.Fatalf("sz=%d k=%d ov=%.2f: probe=%d merge=%d gallop=%d", sz, k, ov, op, om, og)
				}
			}
		}
	}
}

// TestMergeMatchesReference checks the merge kernel against a brute-force set
// intersection on the actual member bytes, so a bug in the run-merge-tail cursor
// or the hash-tie confirm cannot hide behind agreement with the probe kernel.
func TestMergeMatchesReference(t *testing.T) {
	const small, bigN, sz = 1500, 9000, 32
	bigIDs := make([]uint64, bigN)
	for i := range bigIDs {
		bigIDs[i] = uint64(i + 1)
	}
	big := newMemberSet(bigIDs, sz)
	bigHS := buildHSet(big, tailCap)
	sm := makeSmall(small, bigN, 0.5, sz)
	smHS := buildHSet(sm, tailCap)

	present := map[string]bool{}
	for i := range big.ids {
		present[string(big.key(i))] = true
	}
	ref := 0
	for i := range sm.ids {
		if present[string(sm.key(i))] {
			ref++
		}
	}
	if got := mergeIntersect(smHS, bigHS); got != ref {
		t.Fatalf("merge=%d reference=%d", got, ref)
	}
	if got := gallopIntersect(smHS, bigHS); got != ref {
		t.Fatalf("gallop=%d reference=%d", got, ref)
	}
}

// TestBoundedTail checks that a bounded unsorted tail is handled correctly: the
// same members split differently between run and tail must merge to the same
// result, so the section-6.3 tail discipline does not change the answer.
func TestBoundedTail(t *testing.T) {
	const small, bigN, sz = 1000, 4000, 16
	bigIDs := make([]uint64, bigN)
	for i := range bigIDs {
		bigIDs[i] = uint64(i + 1)
	}
	big := newMemberSet(bigIDs, sz)
	sm := makeSmall(small, bigN, 0.6, sz)

	full := buildHSet(sm, 0) // everything in the run, empty tail
	bigFull := buildHSet(big, 0)
	partial := buildHSet(sm, tailCap) // last tailCap in the tail
	bigPart := buildHSet(big, tailCap)

	a := mergeIntersect(full, bigFull)
	b := mergeIntersect(partial, bigPart)
	if a != b {
		t.Fatalf("tail split changed result: run-only=%d run+tail=%d", a, b)
	}
}

// TestSwarMatch checks the SWAR group match property lookup relies on: it never
// misses a real tag match and never flags an empty control byte, the same
// invariant lab 01 pins.
func TestSwarMatch(t *testing.T) {
	words := []uint64{
		0x8080808080808080,
		0x0102030405060780,
		0x7f7f7f7f7f7f7f7f,
		0x807f00017e2a5501,
	}
	for _, w := range words {
		for tag := 0; tag < 128; tag++ {
			got := swarMatch(w, byte(tag))
			if got&^hi != 0 {
				t.Fatalf("swarMatch(%016x,%d)=%016x set a non-0x80 bit", w, tag, got)
			}
			for i := 0; i < 8; i++ {
				b := byte(w >> (8 * i))
				bit := uint64(0x80) << (8 * i)
				if b == byte(tag) && got&bit == 0 {
					t.Fatalf("swarMatch(%016x,%d) missed match at byte %d", w, tag, i)
				}
				if b == ctrlEmpty && got&bit != 0 {
					t.Fatalf("swarMatch(%016x,%d) flagged empty at byte %d", w, tag, i)
				}
			}
		}
	}
}

// TestTableFindReject checks the member table finds every inserted member and
// rejects disjoint keys at 7/8 load, the probe kernel's correctness floor.
func TestTableFindReject(t *testing.T) {
	const n, sz = 7000, 32
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	set := newMemberSet(ids, sz)
	tab := newTable(set)
	for i := range set.ids {
		if !tab.contains(set.key(i), mix(set.ids[i])) {
			t.Fatalf("member %d not found", i)
		}
	}
	miss := newMemberSet([]uint64{1 << 50, 1<<50 + 1, 1<<50 + 2}, sz)
	for i := range miss.ids {
		if tab.contains(miss.key(i), mix(miss.ids[i])) {
			t.Fatalf("disjoint key %d falsely found", i)
		}
	}
}

// sanity: the sort import is used by the reference sort-check below, keeping the
// test file honest that run arrays really are sorted by hash.
func TestRunSorted(t *testing.T) {
	const n, sz = 5000, 8
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	set := newMemberSet(ids, sz)
	hs := buildHSet(set, tailCap)
	if !sort.SliceIsSorted(hs.run, func(a, b int) bool { return hs.run[a].h < hs.run[b].h }) {
		t.Fatal("run not sorted by hash")
	}
}
