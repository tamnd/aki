package shard

import "testing"

// TestEpochBracket pins the F6 bracket contract the M7 reclaimer will build
// on: an open bracket holds the safe epoch at its entry value even when the
// global epoch moves (a retire mid-batch must not free under the batch), and
// exit releases it.
func TestEpochBracket(t *testing.T) {
	var e epoch
	e.init()

	if got := e.safe(); got != 1 {
		t.Fatalf("safe = %d before any bracket, want 1", got)
	}
	e.enter()
	// A segment retire bumps the global epoch; the in-flight bracket pins safe.
	e.global.Add(1)
	if got := e.safe(); got != 1 {
		t.Fatalf("safe = %d with a bracket open at epoch 1, want 1", got)
	}
	e.exit()
	if got := e.safe(); got != 2 {
		t.Fatalf("safe = %d after exit, want 2", got)
	}
	// The next bracket publishes the current epoch.
	e.enter()
	if got := e.safe(); got != 2 {
		t.Fatalf("safe = %d inside a fresh bracket, want 2", got)
	}
	e.exit()
}
