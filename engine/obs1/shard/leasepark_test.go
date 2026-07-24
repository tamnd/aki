package shard

import (
	"bytes"
	"strconv"
	"testing"
)

// The lease gate (worker.go leaseGate, backpressure.go stallCheckLease,
// doc 02 section 3.5): a write for a suspended group parks before its
// handler runs, a write for a demoted group takes the doc 07 MOVED redirect,
// and 64 paced fruitless renewal checks surface the CLUSTERDOWN reply. These
// drive the worker directly with a fake view, the test goroutine as owner.

const (
	opLsSet byte = iota + 1
	opLsGet
	opLsFlushAll
	opLsMax
)

// fakeLeases is a settable LeaseView. Plain fields: the view is only read
// from the owner goroutine, which is the test goroutine here.
type fakeLeases struct {
	gated     bool
	suspended map[uint16]bool
	anySusp   bool
	demoted   map[uint16]string
	renewals  uint64
}

func newFakeLeases() *fakeLeases {
	return &fakeLeases{gated: true, suspended: map[uint16]bool{}, demoted: map[uint16]string{}}
}

func (f *fakeLeases) Gated(int64) bool                 { return f.gated }
func (f *fakeLeases) Suspended(g uint16, _ int64) bool { return f.suspended[g] }
func (f *fakeLeases) AnySuspended(int64) bool          { return f.anySusp }
func (f *fakeLeases) Renewals() uint64                 { return f.renewals }
func (f *fakeLeases) Demoted(g uint16) (string, bool) {
	ep, ok := f.demoted[g]
	return ep, ok
}

// leaseGroup is the group the gate derives for key on a default runtime,
// the same route leaseGate takes.
func leaseGroup(key string) uint16 {
	return uint16(groupOfSlot(HashSlot([]byte(key)), DefaultSlotGroups))
}

// leaseRuntime builds a single-shard runtime with the fake view wired:
// opLsSet is a keyed write behind the gate, opLsGet a read that must flow,
// opLsFlushAll a keyless write for the AnySuspended path. setCalls counts
// opLsSet handler entries, the pre-execution proof.
func leaseRuntime() (*Runtime, *fakeLeases, *int) {
	fv := newFakeLeases()
	setCalls := new(int)
	handlers := make([]Handler, opLsMax)
	handlers[opLsSet] = func(cx *Ctx, a [][]byte, r Reply) {
		*setCalls++
		r.Status("OK")
	}
	handlers[opLsGet] = func(cx *Ctx, a [][]byte, r Reply) { r.Bulk(a[0]) }
	handlers[opLsFlushAll] = func(cx *Ctx, a [][]byte, r Reply) { r.Status("OK") }
	writes := make([]bool, opLsMax)
	writes[opLsSet] = true
	writes[opLsFlushAll] = true
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	rt.UseWriteOps(writes)
	rt.UseLeaseView(fv)
	return rt, fv, setCalls
}

// TestLeaseGateParksWriteBeforeHandler proves the core contract: a write for
// a suspended group parks on the lease reason without its handler running (a
// suspended ack could race a takeover), a read flows meanwhile, and once a
// renewal un-suspends the group the retry runs the handler for the first
// time and the reply delivers.
func TestLeaseGateParksWriteBeforeHandler(t *testing.T) {
	rt, fv, setCalls := leaseRuntime()
	w := rt.workers[0]
	fv.suspended[leaseGroup("k")] = true

	c := rt.NewConn()
	if err := c.Do(opLsSet, true, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	c2 := rt.NewConn()
	if err := c2.Do(opLsGet, true, args("hi")); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	for w.drainAndExecute() > 0 {
	}

	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	if got := w.fullWaiters[0].reason; got != ParkLease {
		t.Fatalf("park reason = %v, want lease", got)
	}
	if *setCalls != 0 {
		t.Fatalf("write handler ran %d times while suspended, want 0: the gate parks before execution", *setCalls)
	}
	if w.bpReasonWaits[ParkLease] != 1 {
		t.Fatalf("lease waits = %d, want 1", w.bpReasonWaits[ParkLease])
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %d replies on the parked connection, want 0", len(got))
	}
	// Reads keep flowing while the group is suspended, only flagged stale
	// upstream; the gate never touches a read.
	got := collect(t, c2, 1)
	if !bytes.Equal(got[0], []byte("$2\r\nhi\r\n")) {
		t.Fatalf("read while suspended = %q, want $2 hi", got[0])
	}

	// A retry while still suspended keeps the park; a renewal lets the next
	// retry run the handler exactly once.
	w.retryFull()
	if len(w.fullWaiters) != 1 || *setCalls != 0 {
		t.Fatalf("after suspended retry: waiters = %d, handler calls = %d, want 1 and 0", len(w.fullWaiters), *setCalls)
	}
	delete(fv.suspended, leaseGroup("k"))
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after the renewal, want 0", len(w.fullWaiters))
	}
	if *setCalls != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", *setCalls)
	}
	got = collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("released write reply = %q, want +OK", got[0])
	}
}

// TestLeaseGateRedirectsDemoted pins the doc 07 redirect on both paths: a
// fresh write for a demoted group takes MOVED at once with the key's real
// slot and the taker's endpoint, and a write parked on suspension resolves
// to MOVED through the retry when its group demotes mid-wait, never an
// error and never the handler.
func TestLeaseGateRedirectsDemoted(t *testing.T) {
	rt, fv, setCalls := leaseRuntime()
	w := rt.workers[0]
	slot := HashSlot([]byte("k"))
	moved := "-MOVED " + strconv.Itoa(slot) + " 10.0.0.9:7000\r\n"

	// Fresh write, already demoted: the redirect lands immediately.
	fv.demoted[leaseGroup("k")] = "10.0.0.9:7000"
	c := rt.NewConn()
	if err := c.Do(opLsSet, true, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d, want 0: a demoted write redirects, never parks", len(w.fullWaiters))
	}
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte(moved)) {
		t.Fatalf("demoted write reply = %q, want %q", got[0], moved)
	}

	// Parked on suspension first, then demoted: the retrying gate resolves
	// the waiter with the redirect.
	rt2, fv2, setCalls2 := leaseRuntime()
	w2 := rt2.workers[0]
	fv2.suspended[leaseGroup("k")] = true
	c2 := rt2.NewConn()
	if err := c2.Do(opLsSet, true, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c2.Flush()
	for w2.drainAndExecute() > 0 {
	}
	if len(w2.fullWaiters) != 1 || w2.fullWaiters[0].reason != ParkLease {
		t.Fatalf("waiters = %d, want the one lease waiter", len(w2.fullWaiters))
	}
	delete(fv2.suspended, leaseGroup("k"))
	fv2.demoted[leaseGroup("k")] = ":6380"
	w2.retryFull()
	if len(w2.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after the demotion, want 0", len(w2.fullWaiters))
	}
	if *setCalls2 != 0 || *setCalls != 0 {
		t.Fatalf("handler ran for a demoted write: %d, %d, want 0", *setCalls2, *setCalls)
	}
	wantEmpty := "-MOVED " + strconv.Itoa(slot) + " :6380\r\n"
	got = collect(t, c2, 1)
	if !bytes.Equal(got[0], []byte(wantEmpty)) {
		t.Fatalf("parked-then-demoted reply = %q, want %q (the empty-host doc 07 form)", got[0], wantEmpty)
	}
}

// TestLeaseKeylessWriteParksOnAnySuspended gates a keyless write: it touches
// every group at once, so it parks while any group is suspended and runs
// once none is.
func TestLeaseKeylessWriteParksOnAnySuspended(t *testing.T) {
	rt, fv, _ := leaseRuntime()
	w := rt.workers[0]
	fv.anySusp = true

	c := rt.NewConn()
	if err := c.Do(opLsFlushAll, false, nil); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if len(w.fullWaiters) != 1 || w.fullWaiters[0].reason != ParkLease {
		t.Fatalf("waiters = %d, want the one lease waiter", len(w.fullWaiters))
	}
	fv.anySusp = false
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after every group renewed, want 0", len(w.fullWaiters))
	}
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("released keyless reply = %q, want +OK", got[0])
	}
}

// TestLeaseStallPacedAndReset pins the lease window's properties: rapid
// retry passes must not climb it (a renewal rides a chain append, which
// takes milliseconds), a renewal resets it however close it was, and
// crossing 64 paced fruitless checks surfaces the doc 07 CLUSTERDOWN reply
// under the lease stall counter.
func TestLeaseStallPacedAndReset(t *testing.T) {
	rt, fv, setCalls := leaseRuntime()
	w := rt.workers[0]
	fv.suspended[leaseGroup("k")] = true

	c := rt.NewConn()
	if err := c.Do(opLsSet, true, args("k", "v")); err != nil {
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
	if w.bpLeaseStall != 1 {
		t.Fatalf("bpLeaseStall = %d after rapid passes, want 1: unpaced passes must not climb the window", w.bpLeaseStall)
	}
	// Backdate the pace clock to charge checks without sleeping; part-way
	// through, a renewal resets the whole window.
	for range 10 {
		w.bpLeaseCheckMs -= bpLeasePollMs + 1
		w.retryFull()
	}
	if w.bpLeaseStall != 11 {
		t.Fatalf("bpLeaseStall = %d after 10 backdated checks, want 11", w.bpLeaseStall)
	}
	fv.renewals++
	w.retryFull()
	if w.bpLeaseStall != 0 {
		t.Fatalf("bpLeaseStall = %d after a renewal, want 0", w.bpLeaseStall)
	}
	passes := 0
	for len(w.fullWaiters) > 0 {
		if passes > bpLeaseStallWindow+4 {
			t.Fatalf("still parked after %d backdated checks, window is %d", passes, bpLeaseStallWindow)
		}
		w.bpLeaseCheckMs -= bpLeasePollMs + 1
		w.retryFull()
		passes++
	}
	if *setCalls != 0 {
		t.Fatalf("handler ran %d times across the stall, want 0", *setCalls)
	}
	if w.bpReasonStalls[ParkLease] != 1 {
		t.Fatalf("lease stalls = %d, want 1", w.bpReasonStalls[ParkLease])
	}
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("-CLUSTERDOWN Hash slot not served\r\n")) {
		t.Fatalf("stalled reply = %q, want the CLUSTERDOWN text", got[0])
	}
}

// TestLeaseOutranksFlushlag parks a write that is both suspended and behind
// a lagged WAL under the lease reason: fencing outranks capacity, so the
// gate order puts the takeover question first, and once the group renews
// the same write re-parks on flushlag through the retry, the reason
// traveling with the waiter.
func TestLeaseOutranksFlushlag(t *testing.T) {
	rt, fv, setCalls := leaseRuntime()
	fl := newFakeLog()
	rt.SetWriteLog(fl)
	w := rt.workers[0]
	fv.suspended[leaseGroup("k")] = true
	fl.lagged.Store(true)

	c := rt.NewConn()
	if err := c.Do(opLsSet, true, args("k", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	if len(w.fullWaiters) != 1 || w.fullWaiters[0].reason != ParkLease {
		t.Fatalf("park reason = %v with both gates raised, want lease first", w.fullWaiters[0].reason)
	}
	delete(fv.suspended, leaseGroup("k"))
	w.retryFull()
	if len(w.fullWaiters) != 1 || w.fullWaiters[0].reason != ParkFlushlag {
		t.Fatalf("park reason = %v after the renewal, want flushlag: the lag still holds", w.fullWaiters[0].reason)
	}
	if *setCalls != 0 {
		t.Fatal("handler ran while a gate was raised")
	}
	fl.lagged.Store(false)
	w.retryFull()
	if *setCalls != 1 {
		t.Fatalf("handler ran %d times after both gates cleared, want 1", *setCalls)
	}
	got := collect(t, c, 1)
	if !bytes.Equal(got[0], []byte("+OK\r\n")) {
		t.Fatalf("released reply = %q, want +OK", got[0])
	}
}
