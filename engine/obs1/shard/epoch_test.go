package shard

import (
	"sync"
	"testing"
)

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

// TestEpochBracketBump pins the reclaimer's half: bump advances the global
// epoch and hands back the stamp a segment retired "now" carries, and a bracket
// open across that bump pins the safe epoch at or below the stamp until it
// exits. This is exactly the store.ReclaimSafe gate (a stamp reclaims once safe
// passes strictly beyond it), driven from the epoch that produces safe.
func TestEpochBracketBump(t *testing.T) {
	var e epoch
	e.init()

	// With no bracket open, bump returns the current stamp and advances safe.
	if got := e.bump(); got != 1 {
		t.Fatalf("bump returned %d, want the pre-advance stamp 1", got)
	}
	if got := e.safe(); got != 2 {
		t.Fatalf("safe = %d after a bump with no bracket, want 2", got)
	}

	// A retire under an open bracket: the owner enters, the reclaimer stamps
	// the current epoch and bumps, and the in-flight bracket holds safe at the
	// stamp so nothing retired at it reclaims yet.
	e.enter() // publishes owner = 2
	stamp := e.bump()
	if stamp != 2 {
		t.Fatalf("retire stamp %d, want 2", stamp)
	}
	if got := e.safe(); got > stamp {
		t.Fatalf("safe = %d passed the retire stamp %d with the bracket open", got, stamp)
	}
	// Exit drains the bracket; safe now clears the stamp and the gate opens.
	e.exit()
	if got := e.safe(); got <= stamp {
		t.Fatalf("safe = %d did not clear the retire stamp %d after exit", got, stamp)
	}
}

// TestEpochConcurrentSafeRead drives the one concurrent access the scheme ever
// has: a reader sampling safe() from its own goroutine (the reclaimer's read of
// the owner word) while the owner runs enter/bump/exit cycles. It is a -race
// gate on the atomic discipline; safe is always at least the initial epoch, so
// a zero read means a plain (non-atomic) load slipped in.
func TestEpochConcurrentSafeRead(t *testing.T) {
	var e epoch
	e.init()

	const iters = 50_000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if s := e.safe(); s == 0 {
				t.Errorf("safe read 0 under concurrency")
				return
			}
		}
	}()
	for i := 0; i < iters; i++ {
		e.enter()
		e.bump()
		e.exit()
	}
	wg.Wait()
}
