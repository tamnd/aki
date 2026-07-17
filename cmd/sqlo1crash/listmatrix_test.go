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

// The T5 exit-gate kill matrix: SIGKILL against the list ladder over
// the Track B composite while inline, noded, and paged lists all take
// queue and middle-op streams and the LMOVE plus LTRIM feed cadence
// keeps two-root frame groups and trim cuts inside the kill windows,
// then Verify plus the exact-state oracle over the recovered image.
// The worker populates every rung before the kill window opens,
// self-checks LLEN against its shadow while alive, and logs its seed
// so a failure replays from the log line alone. The clean-shutdown
// control arm runs to a fixed op count, flushes and checkpoints, and
// demands the exact final state back, element order included.
//
// Defaults keep CI fast; the full gate run is
// SQLO1_LIST_KILL_ITERS=100 SQLO1_LIST_CLEAN_OPS=60000 with -v.

func listMatrixSeed() uint64 {
	if v := os.Getenv("SQLO1_LIST_SEED"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 1
}

// TestListRungPreflight is the non-kill control on the keyset shape:
// after population, the bands must actually sit on their rungs, and
// Verify must pass on every key while the process is healthy. If an
// engine threshold moves, this fails loudly instead of letting the
// kill matrix degrade into killing an all-inline keyset. lk6's paged
// form is by construction: 10500 elements at 68 encoded bytes cut
// ~178 nodes of 59, past the 167-entry flat fence cap, so population
// would have died on errListFencePaged if paging were not live.
func TestListRungPreflight(t *testing.T) {
	rig, err := newListCrashRig(t.TempDir(), listMatrixSeed()+14_000_000)
	if err != nil {
		t.Fatal(err)
	}
	defer rig.db.Close()
	ctx := context.Background()
	for rig.ops < listPopOps() {
		if err := rig.step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.selfCheckLens(ctx); err != nil {
		t.Fatal(err)
	}
	for k := range listKeys - 1 {
		enc, ok, err := rig.l.Encoding(ctx, listKeyName(k))
		if err != nil || !ok {
			t.Fatalf("Encoding(lk%d): %q %v %v", k, enc, ok, err)
		}
		want := "quicklist"
		if k < 4 {
			want = "listpack"
		}
		if enc != want {
			t.Fatalf("lk%d sits on %q after population, want %q", k, enc, want)
		}
		if err := rig.l.Verify(ctx, listKeyName(k)); err != nil {
			t.Fatalf("Verify(lk%d) after population: %v", k, err)
		}
	}
	// The destination stays absent until the first feed fires in
	// steady state.
	if n, err := rig.l.Len(ctx, listKeyName(listDestIdx)); err != nil || n != 0 {
		t.Fatalf("ld0 holds %d elements before the first feed (%v)", n, err)
	}
	t.Logf("preflight: %d population ops, 4 inline, 2 noded, 1 paged, Verify green", rig.ops)
}

// TestListWorker is not a test: it is the process the kill arm
// SIGKILLs. Re-execed with SQLO1CRASH_LIST_WORKER=1 it populates the
// rungs, flushes, reports READY with the durable high-water mark, and
// runs until killed, flushing on a fixed cadence so the durability
// ratchet keeps moving under the kills. With SQLO1CRASH_LIST_OPS set
// it instead stops at that op count, flushes, checkpoints, closes,
// and reports CLEAN.
func TestListWorker(t *testing.T) {
	if os.Getenv("SQLO1CRASH_LIST_WORKER") != "1" {
		return
	}
	dir := os.Getenv("SQLO1CRASH_LIST_DIR")
	seed, err := strconv.ParseUint(os.Getenv("SQLO1CRASH_LIST_SEED"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list worker seed: %v\n", err)
		os.Exit(3)
	}
	cleanOps := 0
	if v := os.Getenv("SQLO1CRASH_LIST_OPS"); v != "" {
		cleanOps, err = strconv.Atoi(v)
		if err != nil || cleanOps <= listPopOps() {
			fmt.Fprintf(os.Stderr, "list worker ops: %q (population alone is %d)\n", v, listPopOps())
			os.Exit(3)
		}
	}
	runListWorker(dir, seed, cleanOps)
}

func runListWorker(dir string, seed uint64, cleanOps int) {
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "list worker: %v\n", err)
		os.Exit(3)
	}
	rig, err := newListCrashRig(dir, seed)
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	step := func() {
		if err := rig.step(ctx); err != nil {
			fail(err)
		}
		if rig.ops%listFlushEvery == 0 {
			if err := rig.tr.Flush(ctx); err != nil {
				fail(fmt.Errorf("cadence Flush: %w", err))
			}
			if err := rig.selfCheckLens(ctx); err != nil {
				fail(err)
			}
		}
		if rig.ops%listProgressEvery == 0 {
			fmt.Printf("LISTWORKER PROGRESS %d\n", rig.ops)
		}
	}

	for rig.ops < listPopOps() {
		step()
	}
	if err := rig.tr.Flush(ctx); err != nil {
		fail(fmt.Errorf("population Flush: %w", err))
	}
	// The high-water mark printed here is durable (WAL synced at
	// ApplyBatch), so recovery after any later kill may only be at or
	// past it, which pins every key at or past its population state.
	fmt.Printf("LISTWORKER READY ops=%d hw=%d\n", rig.ops, rig.db.Stats().HighWater)

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
		fmt.Printf("LISTWORKER CLEAN %d\n", rig.ops)
		return
	}
	for {
		step()
	}
}

// TestListKillMatrix is the kill arm: spawn the worker, wait for
// READY, let it run for a seeded slice of steady state, SIGKILL it,
// then recover the image and hold every key against Verify and the
// exact-state oracle.
func TestListKillMatrix(t *testing.T) {
	iters := matrixIters("SQLO1_LIST_KILL_ITERS", 6, 2)
	base := listMatrixSeed()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for i := range iters {
		seed := base + 15_000_000 + uint64(i)
		// The window spread reaches past several cadence flushes and
		// STORE commits so cuts land before, inside, and between drain
		// batches, feed moves and trim cuts included.
		runFor := time.Duration(1+int(seed%40)*12) * time.Millisecond
		t.Logf("iter %d seed %d kill after %v", i, seed, runFor)
		if err := runListKillIteration(t, exe, seed, runFor); err != nil {
			t.Fatalf("iter %d seed %d: %v", i, seed, err)
		}
	}
}

func runListKillIteration(t *testing.T, exe string, seed uint64, runFor time.Duration) error {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestListWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_LIST_WORKER=1",
		"SQLO1CRASH_LIST_DIR="+dir,
		"SQLO1CRASH_LIST_SEED="+strconv.FormatUint(seed, 10))
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
			case strings.HasPrefix(ln, "LISTWORKER PROGRESS "):
				if n, err := strconv.ParseInt(strings.TrimPrefix(ln, "LISTWORKER PROGRESS "), 10, 64); err == nil {
					lastOps.Store(n)
				}
			case strings.HasPrefix(ln, "LISTWORKER READY "):
				var ops, hw int64
				if _, err := fmt.Sscanf(ln, "LISTWORKER READY ops=%d hw=%d", &ops, &hw); err != nil {
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

	// Population writes the paged band one element per op, so the
	// ready window is generous; CI boxes with slow disks are the
	// audience.
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

	bound := int(lastOps.Load()) + listBoundSlack
	_, legal := simulateL(seed, bound)
	rec, err := verifyListRecovered(filepath.Join(dir, listDataFile), seed, legal, nil, ready.hw)
	if err != nil {
		return err
	}
	t.Logf("recovered %d elements across %d keys at high-water %d", rec.Elems, listKeys, rec.HighWater)
	return nil
}

// TestListCleanControl is the torn-free arm: the same worker, no
// kill, exiting through Flush plus Checkpoint plus Close. The
// stream's exact final state must come back element by element and
// position by position, destination included, with Verify, LLEN, the
// walk, and point reads all agreeing, which pins the zero-loss half
// the kill arm deliberately does not claim.
func TestListCleanControl(t *testing.T) {
	target := matrixIters("SQLO1_LIST_CLEAN_OPS", 25000, 13000)
	if target <= listPopOps() {
		t.Fatalf("clean target %d does not clear population %d", target, listPopOps())
	}
	seed := listMatrixSeed() + 16_000_000
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestListWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_LIST_WORKER=1",
		"SQLO1CRASH_LIST_DIR="+dir,
		"SQLO1CRASH_LIST_SEED="+strconv.FormatUint(seed, 10),
		"SQLO1CRASH_LIST_OPS="+strconv.Itoa(target))
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
		case strings.HasPrefix(ln, "LISTWORKER READY "):
			var ops int64
			if _, err := fmt.Sscanf(ln, "LISTWORKER READY ops=%d hw=%d", &ops, &readyHW); err != nil {
				t.Fatalf("ready line %q: %v", ln, err)
			}
		case strings.HasPrefix(ln, "LISTWORKER CLEAN "):
			n, err := strconv.Atoi(strings.TrimPrefix(ln, "LISTWORKER CLEAN "))
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

	shadow, _ := simulateL(seed, total)
	rec, err := verifyListRecovered(filepath.Join(dir, listDataFile), seed, nil, shadow, readyHW)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean control: %d ops, %d elements back exactly at high-water %d", total, rec.Elems, rec.HighWater)
}
