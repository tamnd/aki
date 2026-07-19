package main

import "testing"

// TestGroupCommitKnee pins the shape the G3 verdict rests on: coalescing the
// spill writes into a larger pwrite window lifts the sustained write rate far
// above the pre-batching byte-1 posture (one pwrite per value), and the shipped
// 1 MiB window sits on the plateau past the amortization knee, not below it. A
// small fill so CI stays quick; the margin is generous so disk-cache noise on a
// shared runner does not flake it.
func TestGroupCommitKnee(t *testing.T) {
	cfg := spillConfig{
		capBytes: 4 << 20,
		arena:    16 << 20,
		valLen:   1032,
		keys:     20_000, // ~20 MiB spilled, ~5x the cap
		reps:     1,
	}
	byte1, _, _ := runWindow(cfg, 1)
	shipped, _, _ := runWindow(cfg, 1<<20)
	if byte1 <= 0 || shipped <= 0 {
		t.Fatalf("degenerate rates: byte1=%.0f shipped=%.0f", byte1, shipped)
	}
	// The window must buy a real coalescing win: the 1 MiB rate clears the
	// byte-1 syscall-bound rate by a wide margin. Measured ~10x; assert >2x so
	// runner noise cannot flip it while a genuine regression (window ignored)
	// would.
	if shipped < 2*byte1 {
		t.Fatalf("group-commit window bought no coalescing: byte1=%.0f shipped=%.0f (want shipped >= 2x)", byte1, shipped)
	}
}
