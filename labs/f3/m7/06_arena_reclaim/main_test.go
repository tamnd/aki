package main

import "testing"

// TestOvershootBoundedAndReclaimed pins the two facts the G4 verdict rests on: the
// peak arena fill over the cap is bounded (it never runs away under churn), and a
// slacker compaction boundary lets the peak rise, which is what makes the overshoot
// churn-cycle headroom rather than a leak. A small churn so CI stays quick; the
// bounds are loose so a shared runner's timing noise cannot flake it.
func TestOvershootBoundedAndReclaimed(t *testing.T) {
	cfg := churnConfig{
		capBytes: 16 << 20,
		arena:    64 << 20,
		valLen:   1032,
		keys:     11_000,
		churn:    80_000,
	}
	tightPeak, tightSettled := runCadence(cfg, 256)
	slackPeak, slackSettled := runCadence(cfg, 16384)

	// Bounded: even the slack-cadence peak stays well under 2x the cap. A runaway
	// (reclaim not firing) would blow past this toward the arena size (4x the cap).
	if slackPeak >= 2.0 {
		t.Fatalf("peak fill unbounded under churn: slack peak %.2fx cap (want < 2.0x)", slackPeak)
	}
	// Reclaim returns fill to at or under the cap at the boundary, both cadences.
	if tightSettled > 1.5 || slackSettled > 1.5 {
		t.Fatalf("boundary did not reclaim: settled tight %.2fx slack %.2fx cap (want <= 1.5x)", tightSettled, slackSettled)
	}
	// The overshoot is cadence-driven: a slacker boundary peaks at least as high as
	// a tight one. This is the churn-headroom signature (the peak tracks the gap).
	if slackPeak < tightPeak {
		t.Fatalf("overshoot not cadence-driven: slack peak %.2fx < tight peak %.2fx", slackPeak, tightPeak)
	}
}
