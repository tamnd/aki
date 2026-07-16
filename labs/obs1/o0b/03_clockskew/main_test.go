package main

import (
	"testing"
	"time"
)

// TestOffsetInvariance: constant offsets at honest rate are invisible,
// the protocol only ever subtracts a clock from itself.
func TestOffsetInvariance(t *testing.T) {
	base := schedule(0, 1.0, 6*time.Second, 24*time.Second)
	if len(base.violations) != 0 || !base.takeover {
		t.Fatalf("baseline broken: %+v", base)
	}
	for _, off := range []time.Duration{250 * time.Millisecond, -2 * time.Second, 10 * time.Second} {
		r := schedule(off, 1.0, 6*time.Second, 24*time.Second)
		if r.acks != base.acks || r.zombieAcks != base.zombieAcks || r.windowMS != base.windowMS {
			t.Fatalf("offset %v differs from baseline: %+v vs %+v", off, r, base)
		}
		if len(r.violations) != 0 {
			t.Fatalf("offset %v violations: %v", off, r.violations)
		}
	}
}

// TestRateKnife: at honest rate the guard suspends a full second before
// the takeover can land, so zero zombie acks; well past the knife edge
// the window opens; safety holds on both sides.
func TestRateKnife(t *testing.T) {
	fast := schedule(0, 1.0, 6*time.Second, 24*time.Second)
	if fast.zombieAcks != 0 {
		t.Fatalf("honest clock produced %d zombie acks", fast.zombieAcks)
	}
	slow := schedule(0, 0.6, 8*time.Second, 32*time.Second)
	if slow.zombieAcks == 0 {
		t.Fatal("rate 0.6 is past the knife edge, want zombie acks")
	}
	for _, r := range []result{fast, slow} {
		if len(r.violations) != 0 || !r.takeover || !r.suspended {
			t.Fatalf("safety or shape broken: %+v", r)
		}
	}
}

// TestFrozenClockSafe: the VM-pause pathology acks to the end of the
// run and still cannot corrupt anything.
func TestFrozenClockSafe(t *testing.T) {
	r := schedule(0, 0, 30*time.Second, 40*time.Second)
	if len(r.violations) != 0 {
		t.Fatalf("violations: %v", r.violations)
	}
	if !r.takeover || r.zombieAcks == 0 || r.suspended {
		t.Fatalf("frozen clock shape wrong: %+v", r)
	}
}

// TestHealthyAnyClock: without a partition a wrong clock alone causes
// no suspension and no takeover.
func TestHealthyAnyClock(t *testing.T) {
	for _, c := range []struct {
		off  time.Duration
		rate float64
	}{{0, 1.0}, {-5 * time.Second, 0.5}, {5 * time.Second, 2.0}} {
		r := schedule(c.off, c.rate, 0, 20*time.Second)
		if r.suspended || r.takeover || len(r.violations) != 0 {
			t.Fatalf("healthy arm off=%v rate=%.1f broke: %+v", c.off, c.rate, r)
		}
	}
}
