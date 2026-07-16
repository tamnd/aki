package shard

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The doc 04 section 6 park-reason taxonomy: registration (names, stat rows)
// and attribution (the arena-full park counts as resident, the unraised
// reasons stay at zero). The park machinery itself is proven in
// backpressure_test.go; these tests cover only what the taxonomy added.

// TestParkReasonNames pins the doc 04 names and the off-taxonomy fallback.
func TestParkReasonNames(t *testing.T) {
	for _, c := range []struct {
		r    ParkReason
		name string
	}{
		{ParkResident, "resident"},
		{ParkFlushlag, "flushlag"},
		{ParkLease, "lease"},
		{numParkReasons, "unknown"},
	} {
		if got := c.r.String(); got != c.name {
			t.Fatalf("ParkReason(%d).String() = %q, want %q", c.r, got, c.name)
		}
	}
}

// TestParkReasonStatRows proves every reason has both INFO rows registered:
// a wait row and a stall row, named backpressure_{waits,stalls}_<reason>, so
// the park-storm lab can read the split without a schema change.
func TestParkReasonStatRows(t *testing.T) {
	rows := map[string]bool{}
	for i := 0; i < NumStats; i++ {
		if statNames[i] == "" {
			t.Fatalf("stat %d has no INFO name", i)
		}
		rows[statNames[i]] = true
	}
	for r := ParkReason(0); r < numParkReasons; r++ {
		for _, kind := range []string{"waits", "stalls"} {
			want := "backpressure_" + kind + "_" + r.String()
			if !rows[want] {
				t.Fatalf("INFO row %q is not registered", want)
			}
		}
	}
}

// TestParkCountsAsResident drives the existing arena-full park and proves it
// is attributed to the resident reason while flushlag and lease stay at zero:
// the wait counts under resident at park time, and the stall-out counts under
// resident too.
func TestParkCountsAsResident(t *testing.T) {
	rt := New(1, testArena, testSeg)
	rt.Use([]Handler{
		opBpParkAlways: func(cx *Ctx, args [][]byte, r Reply) {
			cx.ParkFull(store.ErrFull)
		},
	})
	c := rt.NewConn()
	w := rt.workers[0]

	if err := c.Do(opBpParkAlways, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()

	if w.bpReasonWaits != [numParkReasons]uint64{ParkResident: 1} {
		t.Fatalf("bpReasonWaits = %v, want the one park under resident", w.bpReasonWaits)
	}
	if got := w.cx.ParkWaits(ParkResident); got != 1 {
		t.Fatalf("ParkWaits(resident) = %d, want 1", got)
	}
	if got := w.cx.ParkWaits(ParkFlushlag) + w.cx.ParkWaits(ParkLease); got != 0 {
		t.Fatalf("unraised reasons show %d waits, want 0", got)
	}

	for len(w.fullWaiters) > 0 {
		w.retryFull()
	}
	collect(t, c, 1)
	if w.bpReasonStalls != [numParkReasons]uint64{ParkResident: 1} {
		t.Fatalf("bpReasonStalls = %v, want the one stall under resident", w.bpReasonStalls)
	}
	if got := w.cx.ParkStalls(ParkResident); got != 1 {
		t.Fatalf("ParkStalls(resident) = %d, want 1", got)
	}
	if got := w.cx.ParkStalls(ParkFlushlag) + w.cx.ParkStalls(ParkLease); got != 0 {
		t.Fatalf("unraised reasons show %d stalls, want 0", got)
	}
}

// TestParkCountersOnBareCtx keeps the accessor contract total: a Ctx built
// outside a runtime reports zero for every reason, as BackpressureWaits does.
func TestParkCountersOnBareCtx(t *testing.T) {
	var cx Ctx
	for r := ParkReason(0); r <= numParkReasons; r++ {
		if cx.ParkWaits(r) != 0 || cx.ParkStalls(r) != 0 {
			t.Fatalf("bare Ctx reports nonzero park counters for %v", r)
		}
	}
}
