package main

import "testing"

// buildEsc builds an n-member partitioned set at its derived P and escalates it
// into k groups, exercising the two-level draw the sweep times.
func buildEsc(n, k int) *pset {
	P := derivP(n)
	ps := newPSet(P, n)
	for i := 0; i < n; i++ {
		ps.insert(uint64(i + 1))
	}
	ps.escalate(k)
	return ps
}

// TestEscLocateMatchesFlat is the exactness proof: over every draw position r in
// [0,total) the two-level escalated locate must return the same partition and slot
// the flat locate returns, which is section 5.4's claim that the scatter split
// does not disturb section 4.3's uniformity. A whole-domain match is exactly that.
func TestEscLocateMatchesFlat(t *testing.T) {
	// A small cardinality so its P is the floor 4; use k=2. Then a larger one at a
	// higher P with k=4, so the group span is exercised past one partition.
	for _, tc := range []struct{ n, k int }{{300000, 2}, {900000, 4}} {
		ps := buildEsc(tc.n, tc.k)
		total := ps.total()
		for r := uint64(0); r < total; r++ {
			fp, fl := ps.flatLocate(r)
			ep, el := ps.escLocate(r)
			if fp != ep || fl != el {
				t.Fatalf("n=%d k=%d r=%d: esc (%d,%d) != flat (%d,%d)", tc.n, tc.k, r, ep, el, fp, fl)
			}
		}
	}
}

// TestEscDrawBijection checks the escalated draw returns each member exactly once
// over the whole draw domain, the structural exactness of the two-level draw.
func TestEscDrawBijection(t *testing.T) {
	const n = 300000
	ps := buildEsc(n, 2)
	total := ps.total()
	seen := make(map[uint64]int, total)
	for r := uint64(0); r < total; r++ {
		seen[ps.drawEsc(r)]++
	}
	if len(seen) != n {
		t.Fatalf("draw covered %d distinct members, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("member %d drawn %d times over the domain, want 1", id, c)
		}
	}
}

// TestEscDrawUniform checks the two-level draw is statistically uniform under a
// random source, the empirical F15 gate (section 5.4), with a loose chi-squared
// bound since the bijection above already proves exactness.
func TestEscDrawUniform(t *testing.T) {
	const n = 100000
	const draws = 3000000
	ps := buildEsc(n, 4)
	total := ps.total()
	rng := newPCG(0x1234)
	counts := make(map[uint64]int, n)
	for i := 0; i < draws; i++ {
		counts[ps.drawEsc(rng.below(total))]++
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
	// n-1 degrees of freedom; a uniform draw lands near n, a biased one blows far
	// past. The bound is generous on purpose.
	if chi > 1.3*float64(n) {
		t.Fatalf("chi-squared %.0f too high for %d members, draw looks biased", chi, n)
	}
}

// TestPopHoldsCardinality checks the pop kernel keeps every ordinal live (it
// swap-removes then reinserts to hold cardinality across the measurement), so the
// benchmark draws over a stable population.
func TestPopHoldsCardinality(t *testing.T) {
	const n = 200000
	ps := buildEsc(n, 4)
	before := ps.total()
	rng := newPCG(9)
	for i := 0; i < 500000; i++ {
		ps.popEsc(rng.below(ps.total()))
	}
	if ps.total() != before {
		t.Fatalf("total drifted to %d from %d across pops", ps.total(), before)
	}
	// Every partition still holds its full set of distinct ordinals.
	for pi, s := range ps.parts {
		seen := make(map[uint32]bool, len(s.vec))
		for _, o := range s.vec {
			if seen[o] {
				t.Fatalf("partition %d has ordinal %d twice after pops", pi, o)
			}
			seen[o] = true
		}
		if len(seen) != len(s.ids) {
			t.Fatalf("partition %d holds %d live ordinals, want %d", pi, len(seen), len(s.ids))
		}
	}
}
