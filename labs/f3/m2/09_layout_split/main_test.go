package main

import (
	"bytes"
	"testing"
)

// TestArmsEmitIdenticalBytes is the correctness bar the decomposition rests on:
// the four layouts differ only in WHERE they read each member from, never in
// WHAT they emit, so a ZRANGE window must produce byte-identical RESP under all
// four. If any arm reordered or mis-sliced a member the comparison here would
// catch it, which is what lets the timing table attribute the whole delta to
// addressing.
//
// It stays small (a few thousand members, not the 1M sweep arm) so `go test`
// runs well under a second: the DRAM-scale sweep lives only in `go run .`.
func TestArmsEmitIdenticalBytes(t *testing.T) {
	for _, mlen := range []int{8, 32} {
		const card = 3000
		m := build(card, mlen)
		for _, w := range []int{1, 10, 100} {
			for _, lo := range []int{0, 7, card / 2, card - w} {
				base := append([]byte(nil), m.baseline(nil, lo, w)...)
				slabR := m.slabRankWalk(nil, lo, w)
				bothR := m.bothRankWalk(nil, lo, w)
				leaf := m.leafLocWalk(nil, lo, w)
				if !bytes.Equal(base, slabR) {
					t.Fatalf("mlen=%d w=%d lo=%d: slabRank differs from baseline", mlen, w, lo)
				}
				if !bytes.Equal(base, bothR) {
					t.Fatalf("mlen=%d w=%d lo=%d: bothRank differs from baseline", mlen, w, lo)
				}
				if !bytes.Equal(base, leaf) {
					t.Fatalf("mlen=%d w=%d lo=%d: leafLoc differs from baseline", mlen, w, lo)
				}
			}
		}
	}
}

// TestRankLayoutIsSequential guards the model itself: in the rank-order arms the
// member for rank p must sit at p*mlen in slabRank, the co-location the arms
// claim to measure. A build bug that left slabRank scattered would make the
// "sequential" arms secretly scattered and the split meaningless.
func TestRankLayoutIsSequential(t *testing.T) {
	const card, mlen = 500, 16
	m := build(card, mlen)
	for p := 0; p < card; p++ {
		if int(m.recsRank[p].loc) != p*mlen {
			t.Fatalf("rank %d: recsRank loc %d, want %d", p, m.recsRank[p].loc, p*mlen)
		}
		if int(m.leaf[p].loc) != p*mlen {
			t.Fatalf("rank %d: leaf loc %d, want %d", p, m.leaf[p].loc, p*mlen)
		}
		// Architecture A's insertion-order record for the member at rank p must
		// point into the rank slab at p*mlen (it reads the member there), while
		// the baseline record still points at the insertion slab.
		ord := m.perm[p]
		if int(m.recsA[ord].loc) != p*mlen {
			t.Fatalf("rank %d ord %d: recsA loc %d, want %d", p, ord, m.recsA[ord].loc, p*mlen)
		}
		if int(m.recsIns[ord].loc) != int(ord)*mlen {
			t.Fatalf("rank %d ord %d: recsIns loc %d, want %d", p, ord, m.recsIns[ord].loc, int(ord)*mlen)
		}
	}
}
