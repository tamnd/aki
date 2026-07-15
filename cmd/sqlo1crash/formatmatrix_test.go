package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/sqlo1b"
)

// The B1 exit-gate crash matrix: torn-write and kill -9 injection at
// every superblock, extent, group, WAL, and checkpoint boundary. The
// torn arm cycles through five boundary flavors so each one is hit by
// construction, then cuts power with a seeded sector mask; the kill
// arm SIGKILLs a worker process at a random point in the same op
// stream. Every iteration logs its seed, so a failure replays from
// the log line alone.
//
// Defaults keep CI fast; the full 1000-iteration gate run is
// SQLO1_FORMAT_TORN_ITERS=800 SQLO1_FORMAT_KILL_ITERS=200 with -v.

func matrixIters(env string, def, short int) int {
	if v := os.Getenv(env); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	if testing.Short() {
		return short
	}
	return def
}

func matrixSeed() uint64 {
	if v := os.Getenv("SQLO1_FORMAT_SEED"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 1
}

// TestFormatTornMatrix is the torn arm. Iteration i runs a random op
// stream over a FaultFile, injects at boundary flavor i%5, crashes
// with KeepRandom(seed), and requires the durable image to recover
// consistently against the oracle.
func TestFormatTornMatrix(t *testing.T) {
	iters := matrixIters("SQLO1_FORMAT_TORN_ITERS", 250, 60)
	base := matrixSeed()
	flavors := []string{"superblock", "extent", "group", "wal", "checkpoint"}
	for i := range iters {
		seed := base + uint64(i)
		flavor := i % len(flavors)
		t.Logf("iter %d seed %d boundary %s", i, seed, flavors[flavor])
		if err := runTornIteration(t, seed, flavor); err != nil {
			t.Fatalf("iter %d seed %d boundary %s: %v", i, seed, flavors[flavor], err)
		}
	}
}

func runTornIteration(t *testing.T, seed uint64, flavor int) error {
	t.Helper()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "m.aki")
	walPath := filepath.Join(dir, "m.aki-wal")
	base, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	defer base.Close()
	fault := sqlo1b.NewFaultFile(base)
	rig, err := newFormatRig(seed, fault, base, walPath)
	if err != nil {
		return err
	}
	defer rig.wal.Close()

	for range 20 + rig.rng.IntN(60) {
		if err := rig.step(); err != nil {
			return fmt.Errorf("op stream: %w", err)
		}
	}

	switch flavor {
	case 0:
		// Superblock boundary: a commit whose sync never finished.
		// Steps 1-4 run for real, then the successor superblock write
		// sits unsynced in the cache when power cuts, so any subset of
		// its sectors may land. Fully landed reopens on the successor,
		// anything less reopens on the survivor; both must verify. The
		// oracle does not advance because the commit was never acked.
		frozen := rig.wal.LastSeq()
		roots, err := rig.Snapshot(frozen)
		if err != nil {
			return fmt.Errorf("torn commit snapshot: %w", err)
		}
		next := *rig.sb
		next.Seq++
		next.WALTrimSeq = frozen
		next.AllocmapRoot = roots.Allocmap
		off := int64(0)
		if next.Seq%2 == 1 {
			off = sqlo1b.SuperblockSize
		}
		if _, err := fault.WriteAt(next.Encode(), off); err != nil {
			return err
		}
	case 1:
		// Extent boundary: a seal header hitting the platter without
		// its sync. The extent is still active as far as anything
		// acked knows; a surviving sealed header over half-landed
		// groups must be caught by structure checks, never trusted.
		for range 1 + rig.rng.IntN(2) {
			if err := rig.opGroup(0); err != nil {
				return err
			}
		}
		if st := rig.streams[0]; st != nil {
			h := sqlo1b.ExtentHeader{
				Kind:       sqlo1b.KindVlog,
				EFlags:     sqlo1b.EFlagSealed,
				SealSeq:    ^uint64(0),
				PayloadLen: st.payload,
				GroupCount: st.groups,
			}
			if _, err := fault.WriteAt(h.Encode(), int64(st.ext)*rigExtentSize); err != nil {
				return err
			}
		}
	case 2:
		// Group boundary: fresh group writes with no sync behind them,
		// torn at sector grain by the crash mask.
		for range 3 {
			if err := rig.opGroup(uint16(rig.rng.IntN(2))); err != nil {
				return err
			}
		}
	case 3:
		// WAL boundary: tear the sidecar's unacked tail, either a
		// mid-frame sector flip or a lost run to the segment end.
		if err := mangleWALTail(walPath, rig.o.lastAcked, rig.rng); err != nil {
			return fmt.Errorf("wal mangle: %w", err)
		}
	case 4:
		// Checkpoint boundary: abandon the protocol after a random
		// numbered step via the failpoint, then cut power there.
		if err := rig.opCheckpoint(1 + rig.rng.IntN(6)); err != nil && err != errRigCrash {
			return fmt.Errorf("checkpoint failpoint: %w", err)
		}
	}

	if err := fault.Crash(sqlo1b.KeepRandom(seed)); err != nil {
		return fmt.Errorf("crash: %w", err)
	}
	return verifyIteration(base, walPath, seed, rig.o)
}

// TestFormatWorker is not a test: it is the process the kill arm
// SIGKILLs. Re-execed with SQLO1CRASH_FORMAT_WORKER=1 it runs the rig
// op stream against real files forever, acking durable state through
// the log the parent verifies against.
func TestFormatWorker(t *testing.T) {
	if os.Getenv("SQLO1CRASH_FORMAT_WORKER") != "1" {
		return
	}
	dir := os.Getenv("SQLO1CRASH_FORMAT_DIR")
	seed, err := strconv.ParseUint(os.Getenv("SQLO1CRASH_FORMAT_SEED"), 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "format worker seed: %v\n", err)
		os.Exit(3)
	}
	runFormatWorker(dir, seed)
}

func runFormatWorker(dir string, seed uint64) {
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "format worker: %v\n", err)
		os.Exit(3)
	}
	f, err := os.Create(filepath.Join(dir, "k.aki"))
	if err != nil {
		fail(err)
	}
	ack, err := os.OpenFile(filepath.Join(dir, "ack.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fail(err)
	}
	rig, err := newFormatRig(seed, f, f, filepath.Join(dir, "k.aki-wal"))
	if err != nil {
		fail(err)
	}
	// Ack lines land only after the state they report is durable, so
	// the log the parent reads back never overclaims. A kill between
	// durability and the log line just underclaims, which is the safe
	// direction.
	rig.onAck = func(data []uint64, seals []rigSeal) {
		var b []byte
		for _, seq := range data {
			b = fmt.Appendf(b, "D %d\n", seq)
		}
		for _, s := range seals {
			b = fmt.Appendf(b, "S %d %d %d\n", s.walSeq, s.ext, s.sum)
		}
		if _, err := ack.Write(b); err != nil {
			fail(err)
		}
	}
	rig.onCommit = func(seq uint64) {
		if _, err := fmt.Fprintf(ack, "C %d\n", seq); err != nil {
			fail(err)
		}
	}
	rig.onFree = func(ext uint64) {
		if _, err := fmt.Fprintf(ack, "F %d\n", ext); err != nil {
			fail(err)
		}
	}
	fmt.Println("FORMATWORKER READY")
	for {
		if err := rig.step(); err != nil {
			fail(err)
		}
	}
}

// TestFormatKillMatrix is the kill arm: spawn the worker, let it run
// for a random slice of its op stream, SIGKILL it, then recover the
// files it left and hold them against the ack log.
func TestFormatKillMatrix(t *testing.T) {
	iters := matrixIters("SQLO1_FORMAT_KILL_ITERS", 25, 6)
	base := matrixSeed()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for i := range iters {
		seed := base + 1_000_000 + uint64(i)
		runFor := time.Duration(1+int(seed%40)) * time.Millisecond
		t.Logf("iter %d seed %d kill after %v", i, seed, runFor)
		if err := runKillIteration(t, exe, seed, runFor); err != nil {
			t.Fatalf("iter %d seed %d: %v", i, seed, err)
		}
	}
}

func runKillIteration(t *testing.T, exe string, seed uint64, runFor time.Duration) error {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=^TestFormatWorker$")
	cmd.Env = append(os.Environ(),
		"SQLO1CRASH_FORMAT_WORKER=1",
		"SQLO1CRASH_FORMAT_DIR="+dir,
		"SQLO1CRASH_FORMAT_SEED="+strconv.FormatUint(seed, 10))
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

	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "FORMATWORKER READY" {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("worker ended before ready: %v", sc.Err())
	}()
	select {
	case err := <-ready:
		if err != nil {
			cmd.Wait()
			return err
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("worker never reported ready")
	}

	time.Sleep(runFor)
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill: %w", err)
	}
	cmd.Wait()

	oracle, err := parseAckLog(filepath.Join(dir, "ack.log"))
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "k.aki"), os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	defer f.Close()
	return verifyIteration(f, filepath.Join(dir, "k.aki-wal"), seed, oracle)
}

// TestMangleWALTailDeterminism pins the mangler itself: same seed,
// same tear, and acked frames stay untouched. One WAL is built once
// (its db_id is random), then each tear works on a fresh copy.
func TestMangleWALTailDeterminism(t *testing.T) {
	dir := t.TempDir()
	base, err := os.Create(filepath.Join(dir, "d.aki"))
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	walPath := filepath.Join(dir, "d.aki-wal")
	rig, err := newFormatRig(7, base, base, walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rig.wal.Close()
	for range 12 {
		if err := rig.opPut(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.opBarrier(); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if err := rig.opPut(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rig.wal.Flush(); err != nil {
		t.Fatal(err)
	}
	pristine, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// The acked region is the frame chain up to the last acked seq;
	// the segment is pre-truncated so file size says nothing.
	ackedEnd := int64(0)
	for off := int64(0); off+28 <= int64(len(pristine)); {
		flen := int64(binary.LittleEndian.Uint32(pristine[off:]))
		if flen < 28 || binary.LittleEndian.Uint64(pristine[off+8:]) > rig.o.lastAcked {
			break
		}
		off += flen
		ackedEnd = off
	}
	if ackedEnd == 0 {
		t.Fatal("no acked frames found in the pristine WAL")
	}

	tear := func(seed uint64) []byte {
		t.Helper()
		p := filepath.Join(dir, fmt.Sprintf("copy-%d.aki-wal", seed))
		if err := os.WriteFile(p, pristine, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := mangleWALTail(p, rig.o.lastAcked, rand.New(rand.NewPCG(seed, 0))); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, b := tear(3), tear(3)
	if string(a) != string(b) {
		t.Fatal("same seed produced different tears")
	}
	if string(a) == string(pristine) {
		t.Fatal("mangler left the unacked tail untouched")
	}
	if string(a[:ackedEnd]) != string(pristine[:ackedEnd]) {
		t.Fatal("mangler touched bytes below the ack barrier")
	}
	var differs bool
	for seed := uint64(4); seed < 12; seed++ {
		if string(tear(seed)) != string(a) {
			differs = true
			break
		}
	}
	if !differs {
		t.Fatal("eight different seeds all produced seed 3's tear")
	}
}
