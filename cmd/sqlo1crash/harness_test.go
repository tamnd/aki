package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
)

// TestHelperServer is not a test: it is the process the crash loop kills.
// When re-execed with SQLO1CRASH_HELPER=1 it serves the placeholder store
// on the -addr passed after the -- separator and prints the same listen
// line sqlo1srv prints, so the harness under test launches a real process
// it can SIGKILL without building the sqlo1srv binary first.
func TestHelperServer(t *testing.T) {
	if os.Getenv("SQLO1CRASH_HELPER") != "1" {
		return
	}
	addr := "127.0.0.1:0"
	args := flag.Args()
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-addr" {
			addr = args[i+1]
		}
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("sqlo1crash helper listening on %s\n", l.Addr())
	srv, err := sqlo1.NewServer(sqlo1.NewMemStore())
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		os.Exit(1)
	}
	srv.Serve(l)
}

// TestCrashLoopAgainstMemStore is the skeleton's end to end proof: two full
// kill cycles against the placeholder store. The memory store forgets
// everything on SIGKILL, so the expected shape is zero corruption, zero
// resurrections, and every acked key counted as lost, which passes the
// non-durable criterion and must fail the durable one.
func TestCrashLoopAgainstMemStore(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		bin:        exe,
		args:       []string{"-test.run=^TestHelperServer$", "--"},
		env:        append(os.Environ(), "SQLO1CRASH_HELPER=1"),
		iterations: 2,
		workers:    2,
		keys:       32,
		killMin:    150 * time.Millisecond,
		killMax:    300 * time.Millisecond,
		seed:       42,
	}
	for iter := range cfg.iterations {
		res, err := runIteration(cfg, iter)
		if err != nil {
			t.Fatalf("iteration %d: %v", iter, err)
		}
		if res.ops == 0 {
			t.Fatalf("iteration %d: no ops acknowledged before the kill", iter)
		}
		if res.corrupt > 0 {
			t.Fatalf("iteration %d: %d corrupt keys %v", iter, res.corrupt, res.corruptKeys)
		}
		total := res.matched + res.pendingApplied + res.lost + res.corrupt
		if total != cfg.workers*cfg.keys {
			t.Fatalf("iteration %d: classified %d keys, want %d", iter, total, cfg.workers*cfg.keys)
		}
		if !res.pass(false) {
			t.Fatalf("iteration %d: non-durable criterion failed: %+v", iter, res)
		}
		if res.lost > 0 && res.pass(true) {
			t.Fatalf("iteration %d: durable criterion ignored %d lost writes", iter, res.lost)
		}
		if res.recovery <= 0 {
			t.Fatalf("iteration %d: recovery time not measured", iter)
		}
		// The restarted memory store is empty, so when nothing ambiguous
		// resolved to an applied write the observed digest must be the
		// empty keyspace and the oracle digest must differ as long as any
		// acked write was lost.
		if res.lost > 0 && res.oracleDigest == res.observedDigest {
			t.Fatalf("iteration %d: digests match despite %d lost keys", iter, res.lost)
		}
	}
}
