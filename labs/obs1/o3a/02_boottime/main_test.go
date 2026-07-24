package main

import "testing"

// TestBootSmoke runs the quick configuration end to end. The op-count
// invariants, StateSum agreement, rebuild stats, and WAL frame checks
// are all error paths in run(), so a nil return is the pass.
func TestBootSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("latency-bearing smoke")
	}
	if err := run(quickCfg()); err != nil {
		t.Fatal(err)
	}
}
