package shard

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The flushlag gate (worker.go executeCmd, backpressure.go, doc 04 section
// 6): a write handler must not run while the WAL buffer sits over cap, so
// the gate parks the command before the handler and the retry runs it for
// the first time once the lag clears. These drive the worker directly with
// the fake log's lag flag, the test goroutine as owner.

const (
	opFlSet byte = iota + 1
	opFlGet
	opFlParkAlways
	opFlMSet
	opFlMax
)

// flushlagRuntime builds a single-shard runtime with the fake log wired and
// the write bits installed: opFlSet and opFlMSet are writes behind the gate,
// opFlGet is a read that must flow through a park storm, opFlParkAlways is a
// resident park for the isolation tests. setCalls counts opFlSet handler
// entries, the pre-execution proof.
func flushlagRuntime() (*Runtime, *fakeLog, *int) {
	fl := newFakeLog()
	setCalls := new(int)
	handlers := make([]Handler, opFlMax)
	handlers[opFlSet] = func(cx *Ctx, a [][]byte, r Reply) {
		*setCalls++
		r.Status("OK")
	}
	handlers[opFlGet] = func(cx *Ctx, a [][]byte, r Reply) { r.Bulk(a[0]) }
	handlers[opFlParkAlways] = func(cx *Ctx, a [][]byte, r Reply) {
		cx.ParkFull(store.ErrFull)
	}
	handlers[opFlMSet] = func(cx *Ctx, a [][]byte, r Reply) { r.FanOK() }
	writes := make([]bool, opFlMax)
	writes[opFlSet] = true
	writes[opFlMSet] = true
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	rt.UseWriteOps(writes)
	rt.SetWriteLog(fl)
	return rt, fl, setCalls
}

// TestFlushlagGateParksWriteBeforeHandler proves the gate's core contract: a
// write arriving under lag parks on the flushlag reason without its handler
// ever running (the mutation must not happen while the buffer is over cap),
// a read on another connection flows meanwhile, and once the lag clears the
// retry runs the handler for the first time and the reply delivers.
func TestFlushlagGateParksWriteBeforeHandler(t *testing.T) {
	rt, fl, setCalls := flushlagRuntime()
	w := rt.workers[0]
	fl.lagged.Store(true)

	c := rt.NewConn()
	if err := c.Do(opFlSet, false, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	c2 := rt.NewConn()
	if err := c2.Do(opFlGet, false, args("hi")); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	for w.drainAndExecute() > 0 {
	}

	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	if got := w.fullWaiters[0].reason; got != ParkFlushlag {
		t.Fatalf("park reason = %v, want flushlag", got)
	}
	if *setCalls != 0 {
		t.Fatalf("write handler ran %d times under lag, want 0: the gate parks before execution", *setCalls)
	}
	if w.bpReasonWaits[ParkFlushlag] != 1 {
		t.Fatalf("flushlag waits = %d, want 1", w.bpReasonWaits[ParkFlushlag])
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %d replies on the parked connection, want 0", len(got))
	}
	// The read on the other connection flowed through the park storm.
	got := collect(t, c2, 1)
	if !bytes.Equal(got[0], []byte("$2\r\nhi\r\n")) {
		t.Fatalf("read under lag = %q, want $2 hi", got[0])
	}

	// A retry while still lagged keeps the park and still never runs the
	// handler; clearing the flag lets the next retry run it once.
	w.retryFull()
	if len(w.fullWaiters) != 1 || *setCalls != 0 {
		t.Fatalf("after lagged retry: waiters = %d, handler calls = %d, want 1 and 0", len(w.fullWaiters), *setCalls)
	}
	fl.lagged.Store(false)
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after the lag cleared, want 0", len(w.fullWaiters))
	}
	if *setCalls != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", *setCalls)
	}
	got = collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("released write reply = %q, want +OK", got[0])
	}
}

// TestFlushlagGateParksFanSub gates a fan sub-command: the sub's op carries
// the verb's write bit, so it parks pre-execution like a point write and the
// gather folds its +OK only after the lag clears.
func TestFlushlagGateParksFanSub(t *testing.T) {
	rt, fl, _ := flushlagRuntime()
	w := rt.workers[0]
	fl.lagged.Store(true)

	c := rt.NewConn()
	if err := c.DoFan(opFlMSet, FanOK, args("k0", "k1"), args("v0", "v1")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	if got := w.fullWaiters[0].reason; got != ParkFlushlag {
		t.Fatalf("park reason = %v, want flushlag", got)
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %d replies while the fan sub was parked, want 0", len(got))
	}
	fl.lagged.Store(false)
	w.retryFull()
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("folded reply = %q, want +OK", got[0])
	}
}

// TestFlushlagStallPacedAndReset pins the stall window's two flushlag
// properties. Pacing: retry passes at an idle boundary spin far faster than
// a WAL PUT completes, so raw passes must not advance the counter; only a
// check bpFlushPollMs after the last one does. Progress: a completed flush
// (FlushCount moved) resets the window however close it was. Crossing 64
// paced fruitless checks surfaces the flush-stalled reply.
func TestFlushlagStallPacedAndReset(t *testing.T) {
	rt, fl, setCalls := flushlagRuntime()
	w := rt.workers[0]
	fl.lagged.Store(true)

	c := rt.NewConn()
	if err := c.Do(opFlSet, false, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}

	// Rapid passes: the first check charges one (nothing was ever seen),
	// the rest sit inside the pacing interval and charge nothing.
	for range 20 {
		w.retryFull()
	}
	if w.bpFlushStall != 1 {
		t.Fatalf("bpFlushStall = %d after rapid passes, want 1: unpaced passes must not climb the window", w.bpFlushStall)
	}
	// Backdate the pace clock to charge checks without sleeping; part-way
	// through, a completed flush resets the whole window.
	for range 10 {
		w.bpFlushCheckMs -= bpFlushPollMs + 1
		w.retryFull()
	}
	if w.bpFlushStall != 11 {
		t.Fatalf("bpFlushStall = %d after 10 backdated checks, want 11", w.bpFlushStall)
	}
	fl.flushes.Add(1)
	w.retryFull()
	if w.bpFlushStall != 0 {
		t.Fatalf("bpFlushStall = %d after a completed flush, want 0", w.bpFlushStall)
	}
	passes := 0
	for len(w.fullWaiters) > 0 {
		if passes > bpStallWindow+4 {
			t.Fatalf("still parked after %d backdated checks, window is %d", passes, bpStallWindow)
		}
		w.bpFlushCheckMs -= bpFlushPollMs + 1
		w.retryFull()
		passes++
	}
	if *setCalls != 0 {
		t.Fatalf("handler ran %d times across the stall, want 0", *setCalls)
	}
	if w.bpReasonStalls[ParkFlushlag] != 1 {
		t.Fatalf("flushlag stalls = %d, want 1", w.bpReasonStalls[ParkFlushlag])
	}
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("-ERR store: flush stalled\r\n")) {
		t.Fatalf("stalled reply = %q, want the flush-stalled text", got[0])
	}
}

// TestFlushlagAndResidentWindowsAreIndependent parks one write on each
// reason and crosses only the resident window with rapid passes: the
// resident waiter takes the OOM reply while the flushlag waiter survives,
// because its paced window barely moved, and it completes once the lag
// clears. This is the isolation that keeps a spinning resident stall from
// killing a write whose flush was milliseconds away.
func TestFlushlagAndResidentWindowsAreIndependent(t *testing.T) {
	rt, fl, setCalls := flushlagRuntime()
	w := rt.workers[0]
	fl.lagged.Store(true)

	cRes := rt.NewConn()
	if err := cRes.Do(opFlParkAlways, false, nil); err != nil {
		t.Fatal(err)
	}
	cRes.Flush()
	cLag := rt.NewConn()
	if err := cLag.Do(opFlSet, false, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	cLag.Flush()
	for w.drainAndExecute() > 0 {
	}
	if len(w.fullWaiters) != 2 {
		t.Fatalf("fullWaiters = %d, want 2", len(w.fullWaiters))
	}
	if !w.residentParked() {
		t.Fatal("residentParked reports false with a resident waiter in the FIFO")
	}

	passes := 0
	for w.bpReasonStalls[ParkResident] == 0 {
		if passes > bpStallWindow+4 {
			t.Fatalf("resident waiter still parked after %d passes, window is %d", passes, bpStallWindow)
		}
		w.retryFull()
		passes++
	}
	gotRes := collect(t, cRes, 1)
	if want := []byte("-ERR " + store.ErrFull.Error() + " (no larger-than-memory tier)\r\n"); !bytes.Equal(gotRes[0], want) {
		t.Fatalf("resident stall reply = %q, want %q", gotRes[0], want)
	}
	// The flushlag waiter survived the resident stall-out with its own
	// window barely started, and its handler still never ran.
	if len(w.fullWaiters) != 1 || w.fullWaiters[0].reason != ParkFlushlag {
		t.Fatalf("surviving waiters = %d, want the one flushlag waiter", len(w.fullWaiters))
	}
	if w.residentParked() {
		t.Fatal("residentParked reports true with only a flushlag waiter left")
	}
	if w.bpFlushStall >= bpStallWindow {
		t.Fatalf("flushlag window = %d, crossed alongside the resident one", w.bpFlushStall)
	}
	if w.bpReasonStalls[ParkFlushlag] != 0 {
		t.Fatalf("flushlag stalls = %d, want 0", w.bpReasonStalls[ParkFlushlag])
	}
	fl.lagged.Store(false)
	w.retryFull()
	if len(w.fullWaiters) != 0 || *setCalls != 1 {
		t.Fatalf("after the lag cleared: waiters = %d, handler calls = %d, want 0 and 1", len(w.fullWaiters), *setCalls)
	}
	gotLag := collect(t, cLag, 1)
	if !bytes.Equal(gotLag[0], []byte("+OK\r\n")) {
		t.Fatalf("released write reply = %q, want +OK", gotLag[0])
	}
}
