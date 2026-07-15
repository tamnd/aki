package main

import "testing"

// The lab's claims as invariants over the deterministic simulation, so CI catches
// a regression in the policy model without depending on box timings: the
// second-chance policies resist a one-hit-wonder scan that plain FIFO does not,
// ghost readmission never hurts the hit ratio, S3-FIFO and SIEVE land in the same
// family (close hit ratios), and demotion CPU stays bounded. The seeds and sizes
// match main so the numbers are stable.

const (
	tCap  = 8192
	tWs   = 20000
	tLen  = 400_000
	tZipf = 1.07
)

func ghostParity() int { return tCap - tCap/10 }

// runAt builds a trace and runs the three policies at one tail rate and skew.
func runAt(tailRate, zipfS float64, ghostCap int, seed int64) (f, sv, s3 stats) {
	t := makeTrace(traceCfg{length: tLen, wsKeys: tWs, tailRate: tailRate, zipfS: zipfS, seed: seed})
	f = run(newFIFO(tCap), t, tWs)
	sv = run(newSieve(tCap, ghostCap), t, tWs)
	s3 = run(newS3FIFO(tCap, ghostCap), t, tWs)
	return
}

// TestSecondChanceResistsScan pins the core claim: under a heavy one-hit tail both
// second-chance policies keep more of the working set resident than plain FIFO,
// which the scan evicts, and S3-FIFO clears FIFO by a wide margin. This is why the
// migrator does not sink on a bare recency bit.
func TestSecondChanceResistsScan(t *testing.T) {
	f, sv, s3 := runAt(0.6, tZipf, ghostParity(), 10)
	if sv.wsHitRatio() <= f.wsHitRatio() {
		t.Fatalf("sieve wsHit %.4f not above fifo %.4f under a heavy tail", sv.wsHitRatio(), f.wsHitRatio())
	}
	if s3.wsHitRatio() < f.wsHitRatio()+0.1 {
		t.Fatalf("s3-fifo wsHit %.4f not at least 0.1 above fifo %.4f under a heavy tail", s3.wsHitRatio(), f.wsHitRatio())
	}
}

// TestFifoCollapsesWithTail pins that plain FIFO's working-set hit ratio falls as
// the one-hit tail grows (the scan-pollution failure), while the second-chance
// policies retain most of their no-tail hit ratio.
func TestFifoCollapsesWithTail(t *testing.T) {
	fLow, svLow, _ := runAt(0.0, tZipf, ghostParity(), 11)
	fHigh, svHigh, _ := runAt(0.6, tZipf, ghostParity(), 11)
	if fHigh.wsHitRatio() >= fLow.wsHitRatio() {
		t.Fatalf("fifo wsHit did not fall with the tail: %.4f -> %.4f", fLow.wsHitRatio(), fHigh.wsHitRatio())
	}
	// FIFO loses a large share; SIEVE keeps most of it.
	fifoDrop := 1 - fHigh.wsHitRatio()/fLow.wsHitRatio()
	sieveDrop := 1 - svHigh.wsHitRatio()/svLow.wsHitRatio()
	if sieveDrop >= fifoDrop {
		t.Fatalf("sieve drop %.4f not below fifo drop %.4f under the tail", sieveDrop, fifoDrop)
	}
}

// TestGhostNeverHurts pins that ghost readmission does not lower the working-set
// hit ratio for either policy: parity is at least as good as ghost-off. The spec
// leans on the ghost as a pure warming signal, so a regression that made it harm
// the hit ratio is a bug.
func TestGhostNeverHurts(t *testing.T) {
	_, svOff, s3Off := runAt(0.6, 1.05, 0, 12)
	_, svOn, s3On := runAt(0.6, 1.05, ghostParity(), 12)
	if svOn.wsHitRatio() < svOff.wsHitRatio()-1e-9 {
		t.Fatalf("sieve ghost parity %.4f below ghost-off %.4f", svOn.wsHitRatio(), svOff.wsHitRatio())
	}
	if s3On.wsHitRatio() < s3Off.wsHitRatio()-1e-9 {
		t.Fatalf("s3-fifo ghost parity %.4f below ghost-off %.4f", s3On.wsHitRatio(), s3Off.wsHitRatio())
	}
}

// TestS3FIFOWinsHeavyTail pins the verdict: under a heavy one-hit tail S3-FIFO's
// small probationary queue confines the tail to a tenth of the budget, so it holds
// a materially higher working-set hit ratio than SIEVE-plus-ghost across the skew
// range. The small queue is what the single-region SIEVE lacks, and the gap is the
// reason the choice goes to S3-FIFO rather than to the simpler policy.
func TestS3FIFOWinsHeavyTail(t *testing.T) {
	for _, s := range []float64{1.05, 1.1, 1.2, 1.4} {
		_, sv, s3 := runAt(0.6, s, ghostParity(), 13)
		if s3.wsHitRatio() <= sv.wsHitRatio() {
			t.Fatalf("s=%.2f: s3-fifo wsHit %.4f not above sieve %.4f under a heavy tail", s, s3.wsHitRatio(), sv.wsHitRatio())
		}
	}
}

// TestS3FIFODemotionCheaper pins the second half of the verdict: S3-FIFO also pays
// fewer survivor copies than SIEVE, because a one-hit-wonder sinks from the small
// queue without ever entering the main region, so it is never a survivor. SIEVE
// funnels all traffic through one region and re-appends more survivors. So the
// winner on hit ratio is also the cheaper one on demotion CPU, not a trade.
func TestS3FIFODemotionCheaper(t *testing.T) {
	_, sv, s3 := runAt(0.6, tZipf, ghostParity(), 14)
	if s3.copiesPerK() >= sv.copiesPerK() {
		t.Fatalf("s3-fifo copies/1k %.1f not below sieve %.1f", s3.copiesPerK(), sv.copiesPerK())
	}
	// Both stay bounded well under one copy per access.
	if sv.copiesPerK() >= 1000 {
		t.Fatalf("sieve copies/1k %.1f not below one-per-access", sv.copiesPerK())
	}
}
