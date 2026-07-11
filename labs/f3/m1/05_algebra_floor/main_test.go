package main

import (
	"sort"
	"testing"
)

// TestKernelsAgree checks the merge and probe kernels return the same match
// count across cardinalities, overlaps, and member sizes, the correctness bar:
// whatever the driver picks above or below the floor, it must produce the same
// intersection.
func TestKernelsAgree(t *testing.T) {
	for _, sz := range []int{8, 16, 64} {
		for _, card := range []int{16, 128, 1000, 5000} {
			for _, ov := range []float64{0.0, 0.5, 1.0} {
				aIDs := make([]uint64, card)
				for i := range aIDs {
					aIDs[i] = uint64(i + 1)
				}
				a := newMemberSet(aIDs, sz)
				b := makeOperand(card, ov, sz)
				aHS := buildHSet(a, tailCap)
				bHS := buildHSet(b, tailCap)
				aTab := newTable(a)

				want := int(ov*float64(card) + 0.5)
				om := mergeIntersect(aHS, bHS)
				op := probeIntersect(b, aTab)
				if op != want {
					t.Fatalf("sz=%d card=%d ov=%.1f: probe=%d want=%d", sz, card, ov, op, want)
				}
				if om != op {
					t.Fatalf("sz=%d card=%d ov=%.1f: merge=%d probe=%d", sz, card, ov, om, op)
				}
			}
		}
	}
}

// TestMaintainerMatchesReference checks the incremental bounded-tail maintainer
// (sorted run plus tail) holds the same members as a direct sort of the whole
// set, so a bug in the tail flush or the run merge cannot hide behind the write
// timing. It merges the maintained run and tail and compares to a full sort.
func TestMaintainerMatchesReference(t *testing.T) {
	const n, sz = 3000, 16
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i*7 + 3) // spread so hashes are not monotone in insert order
	}
	set := newMemberSet(ids, sz)

	m := maintainer{cap: tailCap}
	for i := 0; i < n; i++ {
		m.add(mix(set.ids[i]), uint32(i))
	}
	// Fold the final partial tail into the run, then sort, matching how a reader
	// sees run-merge-tail as one logical sorted stream.
	all := append(append([]hpair{}, m.run...), m.tail...)
	sort.Slice(all, func(a, b int) bool { return all[a].h < all[b].h })

	ref := make([]hpair, n)
	for i := 0; i < n; i++ {
		ref[i] = hpair{h: mix(set.ids[i]), ord: uint32(i)}
	}
	sort.Slice(ref, func(a, b int) bool { return ref[a].h < ref[b].h })

	if len(all) != len(ref) {
		t.Fatalf("maintained %d entries, reference %d", len(all), len(ref))
	}
	for i := range ref {
		if all[i].h != ref[i].h || all[i].ord != ref[i].ord {
			t.Fatalf("entry %d: maintained {%d,%d} reference {%d,%d}",
				i, all[i].h, all[i].ord, ref[i].h, ref[i].ord)
		}
	}
}

// TestMaintainerRunSorted checks the run stays sorted after every flush, the
// invariant the merge kernel relies on.
func TestMaintainerRunSorted(t *testing.T) {
	const n = 2000
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i*13 + 1)
	}
	set := newMemberSet(ids, 8)
	m := maintainer{cap: tailCap}
	for i := 0; i < n; i++ {
		m.add(mix(set.ids[i]), uint32(i))
		if !sort.SliceIsSorted(m.run, func(a, b int) bool { return m.run[a].h < m.run[b].h }) {
			t.Fatalf("run not sorted after %d adds", i+1)
		}
		if len(m.tail) > tailCap {
			t.Fatalf("tail %d exceeded cap %d after %d adds", len(m.tail), tailCap, i+1)
		}
	}
}

// TestSwarMatch checks the SWAR group match property the probe relies on: no
// false negatives and no empty control byte flagged, the same invariant labs 01
// and 03 pin.
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
				bb := byte(w >> (8 * i))
				bit := uint64(0x80) << (8 * i)
				if bb == byte(tag) && got&bit == 0 {
					t.Fatalf("swarMatch(%016x,%d) missed match at byte %d", w, tag, i)
				}
				if bb == ctrlEmpty && got&bit != 0 {
					t.Fatalf("swarMatch(%016x,%d) flagged empty at byte %d", w, tag, i)
				}
			}
		}
	}
}
