package shard

import "testing"

// The collection-demotion hook registration (runtime.go UseDemoter, worker.go
// demoteColl). The server layer, which imports the collection packages the shard
// cannot, hands the demote loop a func to shed one collection quantum under memory
// pressure. These tests hold the fixed-before-Start contract: the hook lands on
// every worker, a runtime that never registers one leaves the field nil so the loop
// skips it, and registering after Start panics like Use does.

// TestUseDemoterRegistersOnEveryWorker holds that the hook reaches all shards, so a
// key routed to any worker gets the same demote boundary.
func TestUseDemoterRegistersOnEveryWorker(t *testing.T) {
	rt := New(4, testArena, testSeg)
	defer rt.Stop()

	var calls int
	rt.UseDemoter(func(*Ctx) int { calls++; return 0 })

	for i, w := range rt.workers {
		if w.demoteColl == nil {
			t.Fatalf("worker %d has no demote hook after UseDemoter", i)
		}
		w.demoteColl(&w.cx)
	}
	if calls != len(rt.workers) {
		t.Fatalf("hook fired %d times across the workers, want %d", calls, len(rt.workers))
	}
}

// TestNoDemoterLeavesHookNil is the string-only default: a runtime that never
// registers a demoter leaves every worker's field nil, so the demote loop's nil
// check skips it and the M0 path stays byte-for-byte.
func TestNoDemoterLeavesHookNil(t *testing.T) {
	rt := New(2, testArena, testSeg)
	defer rt.Stop()

	for i, w := range rt.workers {
		if w.demoteColl != nil {
			t.Fatalf("worker %d has a demote hook without UseDemoter", i)
		}
	}
}

// TestUseDemoterAfterStartPanics holds the fixed-before-Start rule: the worker reads
// the hook with no synchronization on the hot path, so registering it after the
// goroutines are live is a programming error, the same panic Use raises.
func TestUseDemoterAfterStartPanics(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Start()
	defer rt.Stop()

	defer func() {
		if recover() == nil {
			t.Fatal("UseDemoter after Start did not panic")
		}
	}()
	rt.UseDemoter(func(*Ctx) int { return 0 })
}
