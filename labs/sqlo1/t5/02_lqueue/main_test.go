package main

import (
	"strings"
	"testing"
	"time"
)

// TestLQueueFIFO runs the harness single-connection against an
// in-process memory-backed server with the order oracle on: every pop
// must return exactly the next sequence, no pop may miss, and the
// drain must find the ledger's remainder.
func TestLQueueFIFO(t *testing.T) {
	addr, cleanup, err := serveInProc("mem", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	res, err := runBench(cfg{
		addr: addr, key: "lq", depth: 64, elem: 32, conns: 1,
		warm: 100 * time.Millisecond, dur: 400 * time.Millisecond,
		batch: 16, checkOrder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.misses != 0 || res.orderErrs != 0 {
		t.Fatalf("%d misses, %d order errors", res.misses, res.orderErrs)
	}
	if res.drained != res.expected {
		t.Fatalf("drained %d, ledger says %d", res.drained, res.expected)
	}
	if res.ops == 0 {
		t.Fatal("no operations recorded in the measured window")
	}
}

// TestLQueueConcurrent runs the paired workload wide over the real
// file store: no misses (the pairing bounds the depth away from
// zero), and the drain agrees with the ledger.
func TestLQueueConcurrent(t *testing.T) {
	addr, cleanup, err := serveInProc("file", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	res, err := runBench(cfg{
		addr: addr, key: "lq", depth: 500, elem: 200, conns: 4,
		warm: 100 * time.Millisecond, dur: 500 * time.Millisecond,
		batch: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.misses != 0 {
		t.Fatalf("%d misses on a depth-500 queue with 4 conns", res.misses)
	}
	if res.drained != res.expected {
		t.Fatalf("drained %d, ledger says %d", res.drained, res.expected)
	}
}

// TestLQueueDepthCeiling pins the documented pre-paging limit: a 200 B
// queue past ~3000 elements needs the fence paging slice, and the
// harness must surface the refusal as a loud error, not a hang or a
// silent truncation. When fence paging lands this test flips to a
// reminder: delete it and run the full depth sweep.
func TestLQueueDepthCeiling(t *testing.T) {
	addr, cleanup, err := serveInProc("mem", "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	_, err = runBench(cfg{
		addr: addr, key: "lq", depth: 4000, elem: 200, conns: 1,
		warm: 50 * time.Millisecond, dur: 50 * time.Millisecond,
		batch: 512,
	})
	if err == nil {
		t.Fatal("a depth-4000 200 B prefill succeeded; fence paging has landed, delete this test and run the full sweep")
	}
	if !strings.Contains(err.Error(), "fence") {
		t.Fatalf("expected the fence paging refusal, got: %v", err)
	}
}
