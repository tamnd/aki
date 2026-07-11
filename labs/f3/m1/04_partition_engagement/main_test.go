package main

import (
	"math"
	"testing"
)

// buildPSet inserts n distinct ids [1..n] into a partitioned set at P, from a
// small start so the sub-tables grow, exercising rehash on the way.
func buildPSet(P, n int) *pset {
	ps := newPSetSmall(P)
	for i := 0; i < n; i++ {
		ps.insert(uint64(i + 1))
	}
	return ps
}

// TestInsertContains checks that every inserted member is found and every
// disjoint key is rejected, at every swept P, the point-op correctness floor.
func TestInsertContains(t *testing.T) {
	const n = 20000
	for _, P := range pList {
		ps := buildPSet(P, n)
		if got := int(ps.total()); got != n {
			t.Fatalf("P=%d: total=%d want %d", P, got, n)
		}
		for i := 0; i < n; i++ {
			id := uint64(i + 1)
			p := route(mix(id), ps.pmask)
			if !ps.parts[p].contains(id) {
				t.Fatalf("P=%d: member %d not found", P, id)
			}
		}
		for i := 0; i < 2000; i++ {
			id := uint64(1)<<50 + uint64(i)
			p := route(mix(id), ps.pmask)
			if ps.parts[p].contains(id) {
				t.Fatalf("P=%d: disjoint key %d falsely found", P, id)
			}
		}
	}
}

// TestGrowPreservesMembership drives one sub-table through several doublings and
// checks nothing is lost, since the grow-pause numbers are only meaningful if
// the rehash is correct.
func TestGrowPreservesMembership(t *testing.T) {
	s := newSubset(0)
	const n = 50000
	for i := 0; i < n; i++ {
		s.insert(uint64(i + 1))
	}
	if s.rehashMax == 0 {
		t.Fatal("no rehash observed; the build never grew")
	}
	for i := 0; i < n; i++ {
		if !s.contains(uint64(i + 1)) {
			t.Fatalf("member %d lost across rehash", i+1)
		}
	}
}

// TestWeightedDrawBijection is the exact-uniformity proof made concrete: over
// r in [0,total) the weighted draw must return each member exactly once, which
// is section 4.3's claim that every member has probability 1/total regardless of
// partition skew. A bijection over the draw domain is exactly that property.
func TestWeightedDrawBijection(t *testing.T) {
	for _, P := range pList {
		const n = 5000
		ps := buildPSet(P, n)
		total := ps.total()
		seen := make(map[uint64]int, total)
		for r := uint64(0); r < total; r++ {
			id := ps.weightedDraw(r)
			seen[id]++
		}
		if len(seen) != n {
			t.Fatalf("P=%d: draw covered %d distinct members, want %d", P, len(seen), n)
		}
		for id, c := range seen {
			if c != 1 {
				t.Fatalf("P=%d: member %d drawn %d times over the domain, want 1", P, id, c)
			}
		}
	}
}

// TestWeightedDrawUniform checks the draw is statistically uniform under a random
// source, not just a bijection, with a loose chi-squared bound so F15 stays
// empirical (section 4.3, 5.4).
func TestWeightedDrawUniform(t *testing.T) {
	const n = 1000
	const draws = 400000
	ps := buildPSet(8, n)
	total := ps.total()
	rng := newPCG(0x1234)
	counts := make(map[uint64]int, n)
	for i := 0; i < draws; i++ {
		counts[ps.weightedDraw(rng.below(total))]++
	}
	if len(counts) != n {
		t.Fatalf("only %d of %d members ever drawn", len(counts), n)
	}
	exp := float64(draws) / float64(n)
	chi := 0.0
	for _, c := range counts {
		d := float64(c) - exp
		chi += d * d / exp
	}
	// n-1 = 999 dof; a wildly biased draw blows far past this. The bound is loose
	// on purpose: the bijection test above already proves exactness structurally.
	if chi > 1200 {
		t.Fatalf("chi-squared %.0f too high for %d members, draw looks biased", chi, n)
	}
}

// TestRouteBalanced checks member-hash routing spreads roughly evenly across
// partitions, so no partition carries an outsized share that would defeat the
// n/P maintenance bound (section 4.1).
func TestRouteBalanced(t *testing.T) {
	const n = 160000
	ps := buildPSet(16, n)
	exp := float64(n) / 16
	for i, s := range ps.parts {
		got := float64(s.n)
		if math.Abs(got-exp)/exp > 0.05 {
			t.Fatalf("partition %d holds %d, expected ~%.0f (>5%% skew)", i, s.n, exp)
		}
	}
}
