package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The T3 exit-gate kill matrix: SIGKILL against the set ladder over
// the Track B composite while inline, segmented, and paged sets all
// take traffic and the STORE cadence keeps bulk-build commits inside
// the kill windows, then the SCARD exactness oracle over the
// recovered image. The worker populates every rung before the kill
// window opens, self-checks SCARD against its shadow while alive, and
// logs its seed so a failure replays from the log line alone. The
// clean-shutdown control arm runs to a fixed op count, flushes and
// checkpoints, and demands the exact final state back, STORE
// destination included.
//
// Defaults keep CI fast; the full gate run is
// SQLO1_SET_KILL_ITERS=100 SQLO1_SET_CLEAN_OPS=60000 with -v.

func setMatrixSeed() uint64 {
	if v := os.Getenv("SQLO1_SET_SEED"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 1
}

// TestSetRungPreflight is the non-kill control on the keyset shape:
// after population, the three bands must actually sit on their rungs
// (the paged pair is sized ~150 segments, past the 128-segment
// fence-page boundary; TestSetStorePaged in the engine pins the paged
// machinery itself). If an engine threshold moves, this fails loudly
// instead of letting the kill matrix degrade into killing an
// all-inline keyset.
func TestSetRungPreflight(t *testing.T) {
	rig, err := newSetCrashRig(t.TempDir(), setMatrixSeed()+8_000_000)
	if err != nil {
		t.Fatal(err)
	}
	defer rig.db.Close()
	ctx := context.Background()
	for rig.ops < setPopOps() {
		if err := rig.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.selfCheckCounts(ctx); err != nil {
		t.Fatal(err)
	}
	for k := range 7 {
		enc, ok, err := rig.se.Encoding(ctx, setKeyName(k))
		if err != nil || !ok {
			t.Fatalf("Encoding(sk%d): %q %v %v", k, enc, ok, err)
		}
		want := "hashtable"
		if k < 4 {
			want = "listpack"
		}
		if enc != want {
			t.Fatalf("sk%d sits on %q after population, want %q", k, enc, want)
		}
	}
	t.Logf("preflight: %d population ops, 4 inline, 2 segmented, 2 paged", rig.ops)
}

// TestSetWorker is not a test: it is the process the kill arm
// SIGKILLs. Re-execed with SQLO1CRASH_SET_WORKER=1 it populates all
// three rungs, flushes, reports READY with the durable high-water
// mark, and runs until killed, flushing on a fixed cadence so the
// durability ratchet keeps moving under the kills. With
// SQLO1CRASH_SET_OPS set it instead stops at that op count, flushes,
// checkpoints, closes, and reports CLEAN.
func TestSetWorker(t *testing.T) {
	if os.Getenv("SQLO1CRASH_SET_WORKER") != "1" {
		return
	}
	dir := os.Getenv("SQLO1CRASH_SET_DIR")
	seed, err := strconv.ParseUint(os.Getenv("SQLO1CRASH_SET_SEED"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set worker seed: %v\n", err)
		os.Exit(3)
	}
	cleanOps := 0
	if v := os.Getenv("SQLO1CRASH_SET_OPS"); v != "" {
		cleanOps, err = strconv.Atoi(v)
		if err != nil || cleanOps <= setPopOps() {
			fmt.Fprintf(os.Stderr, "set worker ops: %q (population alone is %d)\n", v, setPopOps())
			os.Exit(3)
		}
	}
	runSetWorker(dir, seed, cleanOps)
}

func runSetWorker(dir string, seed uint64, cleanOps int) {
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "set worker: %v\n", err)
		os.Exit(3)
	}
	rig, err := newSetCrashRig(dir, seed)
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	step := func() {
		if err := rig.step(ctx); err != nil {
			fail(err)
		}
		if rig.ops%setFlushEvery == 0 {
			if err := rig.tr.Flush(ctx); err != nil {
				fail(fmt.Errorf("cadence Flush: %w", err))
			}
			if err := rig.selfCheckCounts(ctx); err != nil {
				fail(err)
			}
		}
		if rig.ops%setProgressEvery == 0 {
			fmt.Printf("SETWORKER PROGRESS %d\n", rig.ops)
		}
	}

	for rig.ops < setPopOps() {
		step()
	}
	if err := rig.tr.Flush(ctx); err != nil {
		fail(fmt.Errorf("population Flush: %w", err))
	}
	// The high-water mark printed here is durable (WAL synced at
	// ApplyBatch), so recovery after any later kill may only be at or
	// past it, and every population member not removed by the bounded
	// stream must survive.
	fmt.Printf("SETWORKER READY ops=%d hw=%d\n", rig.ops, rig.db.Stats().HighWater)

	if cleanOps > 0 {
		for rig.ops < cleanOps {
			step()
		}
		if err := rig.tr.Flush(ctx); err != nil {
			fail(fmt.Errorf("final Flush: %w", err))
		}
		if err := rig.db.Checkpoint(); err != nil {
			fail(fmt.Errorf("final Checkpoint: %w", err))
		}
		if err := rig.db.Close(); err != nil {
			fail(fmt.Errorf("Close: %w", err))
		}
		fmt.Printf("SETWORKER CLEAN %d\n", rig.ops)
		return
	}
	for {
		step()
	}
}

// TestSetKillMatrix is the kill arm: spawn the worker, wait for
// READY, let it run for a seeded slice of steady state, SIGKILL it,
// then recover the image and hold every key against the count oracle.
func TestSetKillMatrix(t *testing.T) {
	iters := matrixIters("SQLO1_SET_KILL_ITERS", 6, 2)
	base := setMatrixSeed()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for i := range iters {
		seed := base + 9_000_000 + uint64(i)
		// The window spread reaches past several cadence flushes and
		// STORE commits so cuts land before, inside, and between
		// drain batches, bulk builds included.
		runFor := time.Duration(1+int(seed%40)*12) * time.Millisecond
		t.Logf("iter %d seed %d kill after %v", i, seed, runFor)
		if err := runSetKillIteration(t, exe, seed, runFor); err != nil {
			t.Fatalf("iter %d seed %d: %v", i, seed, err)
		}
	}
}

func runSetKillIteration(t *testing.T, exe string, seed uint64, runFor time.Duration) error {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestSetWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_SET_WORKER=1",
		"SQLO1CRASH_SET_DIR="+dir,
		"SQLO1CRASH_SET_SEED="+strconv.FormatUint(seed, 10))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()

	// The reader tracks the last complete PROGRESS line; the worker's
	// true op count at the kill is at most one marker period past it,
	// well inside the simulation slack.
	var lastOps atomic.Int64
	type readyInfo struct {
		hw  int64
		err error
	}
	readyCh := make(chan readyInfo, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stdout)
		readySent := false
		for sc.Scan() {
			ln := strings.TrimSpace(sc.Text())
			switch {
			case strings.HasPrefix(ln, "SETWORKER PROGRESS "):
				if n, err := strconv.ParseInt(strings.TrimPrefix(ln, "SETWORKER PROGRESS "), 10, 64); err == nil {
					lastOps.Store(n)
				}
			case strings.HasPrefix(ln, "SETWORKER READY "):
				var ops, hw int64
				if _, err := fmt.Sscanf(ln, "SETWORKER READY ops=%d hw=%d", &ops, &hw); err != nil {
					if !readySent {
						readyCh <- readyInfo{err: fmt.Errorf("ready line %q: %w", ln, err)}
						readySent = true
					}
					continue
				}
				lastOps.Store(ops)
				if !readySent {
					readyCh <- readyInfo{hw: hw}
					readySent = true
				}
			}
		}
		if !readySent {
			readyCh <- readyInfo{err: fmt.Errorf("worker ended before ready: %v", sc.Err())}
		}
	}()

	// Population fsyncs a lot and the set keyset is the widest in the
	// suite, so the ready window is generous; CI boxes with slow
	// disks are the audience here.
	var ready readyInfo
	select {
	case ready = <-readyCh:
		if ready.err != nil {
			cmd.Wait()
			return ready.err
		}
	case <-time.After(180 * time.Second):
		return fmt.Errorf("worker never reported ready")
	}

	time.Sleep(runFor)
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill: %w", err)
	}
	<-done
	cmd.Wait()

	bound := int(lastOps.Load()) + setBoundSlack
	_, everRemoved := simulateSet(seed, bound)
	rec, err := verifySetRecovered(filepath.Join(dir, setDataFile), seed, everRemoved, nil, ready.hw)
	if err != nil {
		return err
	}
	t.Logf("recovered %d members across %d keys at high-water %d", rec.Members, setKeys, rec.HighWater)
	return nil
}

// TestSetCleanControl is the torn-free arm: the same worker, no kill,
// exiting through Flush plus Checkpoint plus Close. The stream's
// exact final state must come back member by member, destination
// included, with SCARD, the walk, and point reads all agreeing, which
// pins the zero-loss half the kill arm deliberately does not claim.
func TestSetCleanControl(t *testing.T) {
	target := matrixIters("SQLO1_SET_CLEAN_OPS", 25000, 21000)
	if target <= setPopOps() {
		t.Fatalf("clean target %d does not clear population %d", target, setPopOps())
	}
	seed := setMatrixSeed() + 10_000_000
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestSetWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_SET_WORKER=1",
		"SQLO1CRASH_SET_DIR="+dir,
		"SQLO1CRASH_SET_SEED="+strconv.FormatUint(seed, 10),
		"SQLO1CRASH_SET_OPS="+strconv.Itoa(target))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	total := -1
	var readyHW int64
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		ln := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(ln, "SETWORKER READY "):
			var ops int64
			if _, err := fmt.Sscanf(ln, "SETWORKER READY ops=%d hw=%d", &ops, &readyHW); err != nil {
				t.Fatalf("ready line %q: %v", ln, err)
			}
		case strings.HasPrefix(ln, "SETWORKER CLEAN "):
			n, err := strconv.Atoi(strings.TrimPrefix(ln, "SETWORKER CLEAN "))
			if err != nil {
				t.Fatalf("clean line %q: %v", ln, err)
			}
			total = n
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("clean worker exited: %v", err)
	}
	if total < 0 {
		t.Fatal("worker never reported CLEAN")
	}

	shadow, _ := simulateSet(seed, total)
	rec, err := verifySetRecovered(filepath.Join(dir, setDataFile), seed, nil, shadow, readyHW)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean control: %d ops, %d members back exactly at high-water %d", total, rec.Members, rec.HighWater)
}
