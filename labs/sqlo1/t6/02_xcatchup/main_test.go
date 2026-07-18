package main

import (
	"testing"
	"time"
)

// TestReplayOracle runs the three arms end to end at a small scale:
// build a stream, replay it cold with a COUNT that does not divide the
// run or page geometry, and run the pollute interleave. The replay's
// own ordinal check is the oracle; this test only has to drive it and
// fail on any error.
func TestReplayOracle(t *testing.T) {
	c := cfg{
		dir: t.TempDir(), n: 20000, elen: 64, count: 333, pipe: 128,
		wkeys: 500, wlen: 64, warmup: 2, probes: 50, gap: 50 * time.Microsecond,
	}
	if err := runBuild(c); err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, count := range []int{1, 333, 100000} {
		c.count = count
		if err := runCatchup(c); err != nil {
			t.Fatalf("catchup at COUNT %d: %v", count, err)
		}
	}
	c.count = 333
	if err := runPollute(c); err != nil {
		t.Fatalf("pollute: %v", err)
	}
}
