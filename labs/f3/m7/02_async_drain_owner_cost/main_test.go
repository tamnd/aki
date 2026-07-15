package main

import "testing"

// The lab's claims as invariants over the pure cost model, so CI catches a
// regression in the owner-time algebra without depending on box timings: moving
// the pwrite off the owner cuts the owner's critical-path time per drain, the
// reclaimed share is the pwrite net of the hand-off, a bigger drain reclaims
// more, and the serving-throughput uplift rises with drain intensity toward the
// per-drain owner-time ratio.

// fixed is a costs set with plausible, ordered magnitudes: framing and flip are
// small per-record CPU, the pwrite is a blocking per-record cost that dominates a
// drain, and the hand-off is a single fixed cost. The pwrite is the design case,
// a write that blocks (well past the warm-cache floor), so a drain of any real
// size clears the hand-off. The exact numbers do not matter to the invariants.
var fixed = costs{frameRec: 5, pwrite: 50, submit: 200, flipRec: 8}

// perDrain gives the whole-drain pwrite figure the model prices, so the fixture
// reads like the box measurement (a per-record blocking-write cost scaled by size).
func perDrain(c costs, recs int) costs {
	return costs{frameRec: c.frameRec, pwrite: c.pwrite * float64(recs), submit: c.submit, flipRec: c.flipRec}
}

// TestAsyncCutsOwnerTime pins the core claim: with the pwrite off the owner the
// async owner cost is below the sync owner cost whenever the pwrite exceeds the
// hand-off, which a drain of any real size clears.
func TestAsyncCutsOwnerTime(t *testing.T) {
	for _, recs := range []int{64, 256, 1024, 4096, 16384} {
		c := perDrain(fixed, recs)
		if c.ownerAsyncNs(recs) >= c.ownerSyncNs(recs) {
			t.Fatalf("recs=%d: async owner %.1f not below sync owner %.1f", recs, c.ownerAsyncNs(recs), c.ownerSyncNs(recs))
		}
		if f := c.reclaimedFrac(recs); f <= 0 || f >= 1 {
			t.Fatalf("recs=%d: reclaimed fraction %.4f out of (0,1)", recs, f)
		}
	}
}

// TestReclaimedIsPwriteNetOfHandoff pins that the time taken off the critical
// path is exactly the pwrite minus the hand-off the async form adds, the model's
// definition of the saving.
func TestReclaimedIsPwriteNetOfHandoff(t *testing.T) {
	recs := 1024
	c := perDrain(fixed, recs)
	saved := c.ownerSyncNs(recs) - c.ownerAsyncNs(recs)
	want := c.pwrite - c.submit // pwrite is already the whole-drain figure here
	if diff := saved - want; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("saved %.4f, want pwrite-handoff %.4f", saved, want)
	}
}

// TestBiggerDrainReclaimsMore pins sweep A: a larger drain is more pwrite
// wall-clock against fixed framing and hand-off, so the reclaimed share rises
// with the drain size.
func TestBiggerDrainReclaimsMore(t *testing.T) {
	prev := 0.0
	for _, recs := range []int{64, 256, 1024, 4096, 16384} {
		c := perDrain(fixed, recs)
		f := c.reclaimedFrac(recs)
		if f <= prev {
			t.Fatalf("recs=%d: reclaimed %.4f did not rise above %.4f", recs, f, prev)
		}
		prev = f
	}
}

// TestUpliftRisesWithDrainIntensity pins sweep B: fewer commands served per drain
// means the owner spends proportionally more of its time draining, so the async
// throughput uplift climbs toward the pure per-drain owner-time ratio as the
// command load between drains falls.
func TestUpliftRisesWithDrainIntensity(t *testing.T) {
	recs := 1024
	c := perDrain(fixed, recs)
	const serveNs = 80.0
	loads := []int{100000, 10000, 1000, 100} // falling commands per drain
	prev := 1.0
	for _, cmds := range loads {
		u := c.uplift(recs, cmds, serveNs)
		if u <= prev {
			t.Fatalf("cmds=%d: uplift %.4f did not rise above %.4f", cmds, u, prev)
		}
		prev = u
	}
	// The ceiling: at zero served commands the uplift is the pure owner-time ratio.
	ceiling := c.ownerSyncNs(recs) / c.ownerAsyncNs(recs)
	if prev > ceiling+1e-9 {
		t.Fatalf("uplift %.4f exceeded the per-drain ceiling %.4f", prev, ceiling)
	}
}
