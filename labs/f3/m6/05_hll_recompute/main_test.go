package main

import (
	"math"
	"testing"
)

// TestRegisterRoundtrip checks the packed 6-bit get/set pair the whole lab rests
// on: every value 0..63 written to a register reads back unchanged and does not
// disturb its neighbours.
func TestRegisterRoundtrip(t *testing.T) {
	regs := newRegs()
	for i := 0; i < hllRegisters; i++ {
		setRegister(regs, i, byte(i%64))
	}
	for i := 0; i < hllRegisters; i++ {
		if got := getRegister(regs, i); got != byte(i%64) {
			t.Fatalf("register %d: got %d want %d", i, got, i%64)
		}
	}
}

// TestHistoBuildsAgree is the load-bearing claim: the word build and the naive
// build produce the identical histogram, so the fast path is a pure speedup with
// no change to the estimate.
func TestHistoBuildsAgree(t *testing.T) {
	for _, n := range []int{0, 1, 100, 5000, 200000} {
		regs := fill(n)
		naive := regHistoNaive(regs)
		word := regHistoWord(regs)
		if naive != word {
			t.Fatalf("n=%d: word histogram differs from naive", n)
		}
		total := 0
		for _, c := range word {
			total += c
		}
		if total != hllRegisters {
			t.Fatalf("n=%d: histogram sums to %d want %d", n, total, hllRegisters)
		}
	}
}

// TestEstimateWithinBounds checks the ported Ertl estimator lands within the HLL
// standard error (1.04/sqrt(m)) times a slack factor for a range of true
// cardinalities, so the reference the slice ports is sound, not just consistent.
func TestEstimateWithinBounds(t *testing.T) {
	stderr := 1.04 / math.Sqrt(float64(hllRegisters))
	for _, n := range []int{100, 1000, 10000, 100000, 1000000} {
		est := estimate(regHistoWord(fill(n)))
		relErr := math.Abs(float64(est)-float64(n)) / float64(n)
		if relErr > 4*stderr {
			t.Fatalf("n=%d: estimate %d relErr %.4f exceeds %.4f", n, est, relErr, 4*stderr)
		}
	}
}

// TestEmptyEstimate pins the corner: an all-zero register array estimates zero,
// the fresh-HLL answer PFCOUNT must give.
func TestEmptyEstimate(t *testing.T) {
	if est := estimate(regHistoWord(make([]byte, hllDenseSize-16))); est != 0 {
		t.Fatalf("empty estimate = %d want 0", est)
	}
}
