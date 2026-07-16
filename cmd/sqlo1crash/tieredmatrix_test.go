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

// The B3 exit-gate kill matrix: SIGKILL against the full Track B
// runtime composite, Tiered over sqlo1b, while drain, compaction, and
// eviction all run. The preflight proves in-process that the pressure
// knobs actually reach all three loops, the worker refuses to open a
// kill window until they have fired in its own process too, and every
// iteration logs its seed so a failure replays from the log line
// alone. The clean-shutdown control arm runs the same worker to a
// fixed op count, flushes and checkpoints, and demands zero loss.
//
// Defaults keep CI fast; the full gate run is
// SQLO1_TIERED_KILL_ITERS=200 SQLO1_TIERED_CLEAN_OPS=20000 with -v.

func tieredSeed() uint64 {
	if v := os.Getenv("SQLO1_TIERED_SEED"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 1
}

// TestTieredPressurePreflight is the non-kill control on the pressure
// knobs: a fixed slice of the stream, in process, must reach drain,
// eviction, and compaction. If a store or runtime change moves the
// thresholds, this fails loudly instead of letting the kill matrix
// quietly degrade into killing an idle composite.
func TestTieredPressurePreflight(t *testing.T) {
	ops := matrixIters("SQLO1_TIERED_PREFLIGHT_OPS", 12000, 6000)
	rig, err := newTieredRuntime(t.TempDir(), tieredSeed()+4_000_000)
	if err != nil {
		t.Fatal(err)
	}
	defer rig.db.Close()
	ctx := context.Background()
	for rig.ops < ops {
		if err := rig.step(ctx); err != nil {
			t.Fatal(err)
		}
		if rig.ops >= tieredWarmupMin && rig.pressureProven() {
			break
		}
	}
	if !rig.pressureProven() {
		t.Fatalf("pressure knobs never reached all three loops after %d ops: %s", rig.ops, rig.pressureReport())
	}
	t.Logf("preflight: %s", rig.pressureReport())
}

// TestTieredWorker is not a test: it is the process the kill arm
// SIGKILLs. Re-execed with SQLO1CRASH_TIERED_WORKER=1 it drives the
// stream through the composite, warms up until drain, eviction, and
// compaction have all fired, reports READY with the durable
// high-water mark, and then runs until killed. With
// SQLO1CRASH_TIERED_OPS set it instead stops at that op count,
// flushes, checkpoints, closes, and reports CLEAN, which is the
// zero-loss control arm.
func TestTieredWorker(t *testing.T) {
	if os.Getenv("SQLO1CRASH_TIERED_WORKER") != "1" {
		return
	}
	dir := os.Getenv("SQLO1CRASH_TIERED_DIR")
	seed, err := strconv.ParseUint(os.Getenv("SQLO1CRASH_TIERED_SEED"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tiered worker seed: %v\n", err)
		os.Exit(3)
	}
	cleanOps := 0
	if v := os.Getenv("SQLO1CRASH_TIERED_OPS"); v != "" {
		cleanOps, err = strconv.Atoi(v)
		if err != nil || cleanOps <= 0 {
			fmt.Fprintf(os.Stderr, "tiered worker ops: %q\n", v)
			os.Exit(3)
		}
	}
	runTieredWorker(dir, seed, cleanOps)
}

func runTieredWorker(dir string, seed uint64, cleanOps int) {
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "tiered worker: %v\n", err)
		os.Exit(3)
	}
	rig, err := newTieredRuntime(dir, seed)
	if err != nil {
		fail(err)
	}
	// Shed markers land on stdout before the op is counted done, so
	// the clean arm's parent can replay the exact landed set. The kill
	// arm never needs them: its bound is one-sided.
	rig.onShed = func(op int) {
		fmt.Printf("TIEREDWORKER SHED %d\n", op)
	}
	ctx := context.Background()
	step := func() {
		if err := rig.step(ctx); err != nil {
			fail(err)
		}
		if rig.ops%tieredProgressEvery == 0 {
			fmt.Printf("TIEREDWORKER PROGRESS %d\n", rig.ops)
		}
	}

	// Warmup: no kill window opens on a composite that is not yet
	// draining, evicting, and compacting. Failing to get there inside
	// the cap is a rig bug and must be loud, never a weaker matrix.
	for rig.ops < tieredWarmupCap {
		if rig.ops >= tieredWarmupMin && rig.pressureProven() {
			break
		}
		step()
	}
	if !rig.pressureProven() {
		fail(fmt.Errorf("warmup never reached all three loops after %d ops: %s", rig.ops, rig.pressureReport()))
	}
	// The high-water mark printed here is durable (WAL synced at
	// ApplyBatch), so recovery after any later kill may only be at or
	// past it.
	fmt.Printf("TIEREDWORKER READY ops=%d hw=%d evictions=%d compactions=%d\n",
		rig.ops, rig.db.Stats().HighWater, rig.tr.Stats().Evictions, rig.db.DebtStats().Compactions)

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
		fmt.Printf("TIEREDWORKER CLEAN %d\n", rig.ops)
		return
	}
	for {
		step()
	}
}

// TestTieredKillMatrix is the kill arm: spawn the worker, wait for
// READY, let it run for a seeded slice of steady state, SIGKILL it,
// then recover the image on the bare store and hold it against
// invariants recomputed from the seed.
func TestTieredKillMatrix(t *testing.T) {
	iters := matrixIters("SQLO1_TIERED_KILL_ITERS", 8, 3)
	base := tieredSeed()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for i := range iters {
		seed := base + 2_000_000 + uint64(i)
		runFor := time.Duration(1+int(seed%40)) * time.Millisecond
		t.Logf("iter %d seed %d kill after %v", i, seed, runFor)
		if err := runTieredKillIteration(t, exe, seed, runFor); err != nil {
			t.Fatalf("iter %d seed %d: %v", i, seed, err)
		}
	}
}

func runTieredKillIteration(t *testing.T, exe string, seed uint64, runFor time.Duration) error {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestTieredWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_TIERED_WORKER=1",
		"SQLO1CRASH_TIERED_DIR="+dir,
		"SQLO1CRASH_TIERED_SEED="+strconv.FormatUint(seed, 10))
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

	// The reader tracks the last complete PROGRESS line; a line is in
	// the parent's memory only if the worker fully wrote it, so the
	// worker's true op count at the kill is at most one marker period
	// past it, well inside the simulation slack.
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
			case strings.HasPrefix(ln, "TIEREDWORKER PROGRESS "):
				if n, err := strconv.ParseInt(strings.TrimPrefix(ln, "TIEREDWORKER PROGRESS "), 10, 64); err == nil {
					lastOps.Store(n)
				}
			case strings.HasPrefix(ln, "TIEREDWORKER READY "):
				var ops, hw, ev, cp int64
				if _, err := fmt.Sscanf(ln, "TIEREDWORKER READY ops=%d hw=%d evictions=%d compactions=%d",
					&ops, &hw, &ev, &cp); err != nil {
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

	// Warmup fsyncs a lot, so the ready window is generous; CI boxes
	// with slow disks are the audience here.
	var ready readyInfo
	select {
	case ready = <-readyCh:
		if ready.err != nil {
			cmd.Wait()
			return ready.err
		}
	case <-time.After(120 * time.Second):
		return fmt.Errorf("worker never reported ready")
	}

	time.Sleep(runFor)
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill: %w", err)
	}
	<-done
	cmd.Wait()

	// The bound is generous on purpose: overshooting only widens the
	// per-key version ceilings, and the regeneration check already
	// rejects invented bytes, so slack costs nothing but simulation
	// time.
	bound := int(lastOps.Load()) + tieredBoundSlack
	_, maxVer := simulateTiered(seed, bound, nil)
	rec, err := verifyTieredRecovered(filepath.Join(dir, tieredDataFile), seed, maxVer, nil, ready.hw)
	if err != nil {
		return err
	}
	t.Logf("recovered %d keys at high-water %d", rec.Keys, rec.HighWater)
	return nil
}

// TestTieredCleanControl is the torn-free arm: the same worker, no
// kill, exiting through Flush plus Checkpoint plus Close. Everything
// the stream landed (sheds replayed out from the markers) must come
// back exactly, which pins the zero-loss half of the seam contract
// the kill arm deliberately does not claim.
func TestTieredCleanControl(t *testing.T) {
	target := matrixIters("SQLO1_TIERED_CLEAN_OPS", 8000, 4000)
	seed := tieredSeed() + 3_000_000
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestTieredWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_TIERED_WORKER=1",
		"SQLO1CRASH_TIERED_DIR="+dir,
		"SQLO1CRASH_TIERED_SEED="+strconv.FormatUint(seed, 10),
		"SQLO1CRASH_TIERED_OPS="+strconv.Itoa(target))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	sheds := map[int]bool{}
	total := -1
	var readyHW int64
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		ln := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(ln, "TIEREDWORKER SHED "):
			n, err := strconv.Atoi(strings.TrimPrefix(ln, "TIEREDWORKER SHED "))
			if err != nil {
				t.Fatalf("shed line %q: %v", ln, err)
			}
			sheds[n] = true
		case strings.HasPrefix(ln, "TIEREDWORKER READY "):
			var ops, ev, cp int64
			if _, err := fmt.Sscanf(ln, "TIEREDWORKER READY ops=%d hw=%d evictions=%d compactions=%d",
				&ops, &readyHW, &ev, &cp); err != nil {
				t.Fatalf("ready line %q: %v", ln, err)
			}
		case strings.HasPrefix(ln, "TIEREDWORKER CLEAN "):
			n, err := strconv.Atoi(strings.TrimPrefix(ln, "TIEREDWORKER CLEAN "))
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

	landed, _ := simulateTiered(seed, total, sheds)
	rec, err := verifyTieredRecovered(filepath.Join(dir, tieredDataFile), seed, nil, landed, readyHW)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("clean control: %d ops, %d sheds, %d keys back exactly at high-water %d",
		total, len(sheds), rec.Keys, rec.HighWater)
}
