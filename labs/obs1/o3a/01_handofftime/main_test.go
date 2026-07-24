package main

import "testing"

// TestHandoffSmoke runs the quick configuration end to end: build, the
// warm windows with the op-count invariant, the rebuild arms with stat
// agreement, and the replay sweep. Every correctness check the scored
// run relies on is an error path in run(), so a nil return is the pass.
func TestHandoffSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("latency-bearing smoke")
	}
	if err := run(quickCfg()); err != nil {
		t.Fatal(err)
	}
}
