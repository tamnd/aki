package main

import (
	"bytes"
	"testing"
)

// TestWalksEmitIdenticalBytes is the correctness bar the trigger rests on: the
// scattered walk and the architecture-A walk differ only in where each member is
// read from, so a co-location must not change a single emitted byte. If it did,
// firing the trigger would corrupt a reply. Small card so `go test` stays under a
// second; the 1M timing lives only in `go run .`.
func TestWalksEmitIdenticalBytes(t *testing.T) {
	const card, mlen = 4000, 8
	m := build(card, mlen)
	for _, w := range []int{1, 10, 100} {
		for _, lo := range []int{0, 7, card / 2, card - w} {
			a := append([]byte(nil), m.scatWalk(nil, lo, w)...)
			b := m.archAWalk(nil, lo, w)
			if !bytes.Equal(a, b) {
				t.Fatalf("w=%d lo=%d: arch-A walk differs from scattered", w, lo)
			}
		}
	}
}

// TestReadGateStopsThrash is the lab's whole point, checked on synthetic costs so
// no DRAM-scale timing runs in `go test`. The divergence gate already makes
// reorders rare at moderate write fractions, so the thrash the read gate defends
// against is the EXTREME write-heavy regime: many more than card/8 writes between
// consecutive reads, so every read finds the slab freshly diverged. There the
// naive divergence-only predicate reorders on nearly every read and blows far
// past the never-reorder baseline, while the two-gate predicate's read gate holds
// its cost to the amortization bound, baseline plus at most one reorder element
// per read element (window*reorderNs). That bound, not "never worse than
// baseline", is the guarantee.
func TestReadGateStopsThrash(t *testing.T) {
	// Small card so card/8 (the divergence gate) is reachable within the op
	// stream; a reorder element is cheap, a scattered read is a cache miss, a
	// sequential read is cheapest (the lab-09 ordering).
	const card, window, reads = 8000, 100, 1000
	const scatNs, seqNs, reorderNs = 30.0, 7.0, 1.5
	// writeFrac so writes-between-reads (wf/(1-wf)) far exceeds card/8 = 1000.
	const thrashWF = 0.9995

	chosen := &predicate{name: "two-gate", divD: 8, readR: 1}
	naive := &predicate{name: "naive", divD: 8, readR: 0}

	base := simulate(card, window, thrashWF, nil, reads).nsPerRead(scatNs, seqNs, reorderNs)
	nv := simulate(card, window, thrashWF, naive, reads).nsPerRead(scatNs, seqNs, reorderNs)
	gt := simulate(card, window, thrashWF, chosen, reads).nsPerRead(scatNs, seqNs, reorderNs)

	if nv <= 3*base {
		t.Fatalf("write-heavy: naive %.2f did not thrash far above baseline %.2f", nv, base)
	}
	bound := base + float64(window)*reorderNs
	if gt > bound+1e-6 {
		t.Fatalf("write-heavy: two-gate %.2f broke the amortization bound %.2f (baseline %.2f)", gt, bound, base)
	}

	// Under pure reads (the gate shape, build then read) the two-gate predicate
	// reorders once early and then serves sequential, so it beats the baseline.
	pbase := simulate(card, window, 0, nil, reads).nsPerRead(scatNs, seqNs, reorderNs)
	pgt := simulate(card, window, 0, chosen, reads).nsPerRead(scatNs, seqNs, reorderNs)
	if pgt >= pbase {
		t.Fatalf("pure reads: two-gate %.2f did not beat baseline %.2f", pgt, pbase)
	}
}
