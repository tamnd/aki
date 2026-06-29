package command

import (
	"math"
	"runtime"
	"runtime/debug"
	"testing"
)

// restoreGC snapshots the process GC settings and GOMAXPROCS and restores them when
// the test ends, so tuning the global runtime here does not leak into other tests.
func restoreGC(t *testing.T) {
	t.Helper()
	prevPct := debug.SetGCPercent(100)
	prevLimit := debug.SetMemoryLimit(math.MaxInt64)
	prevProcs := runtime.GOMAXPROCS(0)
	t.Cleanup(func() {
		debug.SetGCPercent(prevPct)
		debug.SetMemoryLimit(prevLimit)
		runtime.GOMAXPROCS(prevProcs)
	})
}

// TestApplyGCTuning checks go-gogc and go-memlimit reach the runtime, including
// the 0 specials: go-gogc 0 turns the collector off and go-memlimit 0 means no
// limit.
func TestApplyGCTuning(t *testing.T) {
	restoreGC(t)
	d := New(Config{})

	// A plain percentage and a byte limit are applied verbatim.
	d.conf.set("go-gogc", "50")
	d.conf.set("go-memlimit", "1048576")
	d.ApplyGCTuning()
	if got := debug.SetGCPercent(100); got != 50 {
		t.Fatalf("go-gogc 50 gave SetGCPercent %d", got)
	}
	if got := debug.SetMemoryLimit(math.MaxInt64); got != 1048576 {
		t.Fatalf("go-memlimit 1mb gave SetMemoryLimit %d", got)
	}

	// go-gogc 0 disables the collector, which the runtime reports as -1.
	d.conf.set("go-gogc", "0")
	d.applyGOGC()
	if got := debug.SetGCPercent(100); got != -1 {
		t.Fatalf("go-gogc 0 gave SetGCPercent %d want -1", got)
	}

	// go-memlimit 0 means no limit, expressed as math.MaxInt64.
	d.conf.set("go-memlimit", "0")
	d.applyMemLimit()
	if got := debug.SetMemoryLimit(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("go-memlimit 0 gave SetMemoryLimit %d want MaxInt64", got)
	}
}

// TestApplyMaxProcs checks go-maxprocs reaches the runtime: a positive value pins
// GOMAXPROCS, and 0 leaves it untouched.
func TestApplyMaxProcs(t *testing.T) {
	restoreGC(t)
	d := New(Config{})

	// A positive value pins GOMAXPROCS to exactly that.
	d.conf.set("go-maxprocs", "3")
	d.applyMaxProcs()
	if got := runtime.GOMAXPROCS(0); got != 3 {
		t.Fatalf("go-maxprocs 3 gave GOMAXPROCS %d want 3", got)
	}

	// 0 is the leave-default special: it must not change the current setting.
	d.conf.set("go-maxprocs", "0")
	d.applyMaxProcs()
	if got := runtime.GOMAXPROCS(0); got != 3 {
		t.Fatalf("go-maxprocs 0 changed GOMAXPROCS to %d want it left at 3", got)
	}
}

// TestGCTuningConfigSet checks CONFIG SET go-gogc parses, stores the canonical
// value, and re-tunes the runtime live.
func TestGCTuningConfigSet(t *testing.T) {
	restoreGC(t)
	r, c := startData(t)

	if got := sendLine(t, r, c, "CONFIG SET go-gogc 200"); got != "+OK" {
		t.Fatalf("CONFIG SET go-gogc = %q", got)
	}
	if got := debug.SetGCPercent(100); got != 200 {
		t.Fatalf("CONFIG SET go-gogc 200 left SetGCPercent at %d", got)
	}

	// A memory size with a suffix is canonicalized to a byte count.
	if got := sendLine(t, r, c, "CONFIG SET go-memlimit 64mb"); got != "+OK" {
		t.Fatalf("CONFIG SET go-memlimit = %q", got)
	}
	if got := debug.SetMemoryLimit(math.MaxInt64); got != 64*1024*1024 {
		t.Fatalf("CONFIG SET go-memlimit 64mb left SetMemoryLimit at %d", got)
	}
	val := configGet(t, r, c, "go-memlimit")
	if val != "67108864" {
		t.Fatalf("CONFIG GET go-memlimit = %q want 67108864", val)
	}

	// go-maxprocs pins GOMAXPROCS live and reports back the value it stored.
	if got := sendLine(t, r, c, "CONFIG SET go-maxprocs 2"); got != "+OK" {
		t.Fatalf("CONFIG SET go-maxprocs = %q", got)
	}
	if got := runtime.GOMAXPROCS(0); got != 2 {
		t.Fatalf("CONFIG SET go-maxprocs 2 left GOMAXPROCS at %d", got)
	}
	if v := configGet(t, r, c, "go-maxprocs"); v != "2" {
		t.Fatalf("CONFIG GET go-maxprocs = %q want 2", v)
	}
}
