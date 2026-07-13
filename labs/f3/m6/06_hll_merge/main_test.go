package main

import "testing"

// TestSwarByteMaxMatchesScalar checks the branchless byte-max against a plain
// per-lane max over every value pair the merge can see (registers 0..63).
func TestSwarByteMaxMatchesScalar(t *testing.T) {
	dst := make([]byte, 64*64/8*8) // a multiple of 8, room for every (a,b) pair
	src := make([]byte, len(dst))
	want := make([]byte, len(dst))
	k := 0
	for a := 0; a <= hllRegMax; a++ {
		for b := 0; b <= hllRegMax; b++ {
			dst[k] = byte(a)
			src[k] = byte(b)
			if a >= b {
				want[k] = byte(a)
			} else {
				want[k] = byte(b)
			}
			k++
		}
	}
	swarByteMaxInto(dst, src)
	for i := 0; i < k; i++ {
		if dst[i] != want[i] {
			t.Fatalf("byte %d: max(%d,%d) = %d, want %d", i, src[i], want[i], dst[i], want[i])
		}
	}
}

// TestUnpackRepackRoundtrip confirms the word unpack and the packed accessors
// are inverses over a filled sketch.
func TestUnpackRepackRoundtrip(t *testing.T) {
	regs := fill(7, 50000)
	back := repack(unpack(regs))
	if !bytesEqual(regs, back) {
		t.Fatal("unpack then repack changed the packed bytes")
	}
}

// TestMergeShapesAgree pins the lab's core claim: the packed per-register merge
// and the SWAR unpack-fold-repack merge produce the byte-identical sketch and
// the identical estimate, across cardinalities and fan-in.
func TestMergeShapesAgree(t *testing.T) {
	for _, card := range []int{100, 1000, 50000} {
		for _, n := range []int{2, 3, 8} {
			srcs := make([][]byte, n)
			for i := range srcs {
				srcs[i] = fill(i+1, card)
			}
			gn := naiveMergeAll(srcs)
			gs := swarMergeAll(srcs)
			if !bytesEqual(gn, gs) {
				t.Fatalf("card=%d n=%d: merged sketches differ", card, n)
			}
			if estimate(gn) != estimate(gs) {
				t.Fatalf("card=%d n=%d: estimates differ", card, n)
			}
		}
	}
}
