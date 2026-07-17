package shard

import (
	"errors"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The strict-ack transport (strict.go, writelog.go noteMark): a write on a
// strict connection emits, replies, and the reply parks on the emitted
// marks' commit instead of emitting, delivered later through the same
// loopback a blocking verb rides. These tests drive the worker directly
// with a fake log whose commit callbacks fire on demand, so the properties
// proven are the shard layer's alone; the composed pipeline's end of the
// contract (Watermarks.Notify, barrier demand) is proven in the obs1 and
// drivers tests.

// fakeNotify is one registration the fake log took: the mark and the
// callback strictHold handed it.
type fakeNotify struct {
	group uint16
	seq   uint64
	fn    func()
}

// fakeLog implements the WriteLog seam over nothing: per-group seq
// counters, a group derived from the key's first byte, and a manual
// commit switchboard. err, when set, fails every emission with it.
type fakeLog struct {
	next    map[uint16]uint64
	pending []fakeNotify
	err     error
}

func newFakeLog() *fakeLog {
	return &fakeLog{next: make(map[uint16]uint64)}
}

func (f *fakeLog) group(key []byte) uint16 { return uint16(key[0]) % 4 }

func (f *fakeLog) emit(key []byte) (uint16, uint64, error) {
	if f.err != nil {
		return 0, 0, f.err
	}
	g := f.group(key)
	f.next[g]++
	return g, f.next[g], nil
}

func (f *fakeLog) StrSet(key, value []byte, expireAtMs int64, counter bool) (uint16, uint64, error) {
	return f.emit(key)
}

func (f *fakeLog) KeyDel(key []byte) (uint16, uint64, error) {
	return f.emit(key)
}

func (f *fakeLog) NotifyCommitted(group uint16, seq uint64, fn func()) {
	f.pending = append(f.pending, fakeNotify{group: group, seq: seq, fn: fn})
}

// newStrictRuntime builds a single-shard runtime with the fake log wired
// and one extra op past the test surface: a keyed write that stores its
// value and emits one strset frame per key argument, so a two-key call
// exercises the multi-mark countdown and a repeated key the per-group
// coalescing.
func newStrictRuntime() (*Runtime, *fakeLog, byte) {
	fl := newFakeLog()
	handlers := testHandlers()
	emitOp := byte(len(handlers))
	handlers = append(handlers, func(cx *Ctx, a [][]byte, r Reply) {
		for _, key := range a {
			if err := cx.St.Set(key, []byte("v")); err != nil {
				r.Err("ERR " + err.Error())
				return
			}
			if err := cx.LogStrSet(key, []byte("v"), 0, false); err != nil {
				r.Err(err.Error())
				return
			}
		}
		r.Status("OK")
	})
	rt := New(1, testArena, testSeg)
	rt.Use(handlers)
	rt.SetWriteLog(fl)
	return rt, fl, emitOp
}

func drainAll(c *Conn, w *worker) [][]byte {
	c.Flush()
	for w.drainAndExecute() > 0 {
	}
	var got [][]byte
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	return got
}

// TestStrictHoldsReplyUntilCommit proves the core contract: on a strict
// connection the write's reply does not emit at execution, later pipelined
// commands execute but their replies stall behind the held slot, and the
// commit callback delivers everything in pipeline order.
func TestStrictHoldsReplyUntilCommit(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()
	c.SetStrictAck(true)

	if err := c.Do(emitOp, true, args("a")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, args("a")); err != nil { // seq 1
		t.Fatal(err)
	}
	if got := drainAll(c, w); len(got) != 0 {
		t.Fatalf("emitted %d replies with the strict slot held: %q", len(got), got)
	}
	if len(fl.pending) != 1 {
		t.Fatalf("registered %d commit callbacks, want 1", len(fl.pending))
	}
	if n := fl.pending[0]; n.group != fl.group([]byte("a")) || n.seq != 1 {
		t.Fatalf("callback mark = (%d, %d), want the emitted frame", n.group, n.seq)
	}
	// The GET executed already even though nothing emitted: the store holds
	// the write and only the output waits.
	if v, ok := rt.workers[0].cx.St.Get([]byte("a"), nil); !ok || string(v) != "v" {
		t.Fatalf("store after held write = %q ok=%v", v, ok)
	}
	fl.pending[0].fn()
	var got [][]byte
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	if len(got) != 2 || string(got[0]) != "+OK\r\n" || string(got[1]) != "$1\r\nv\r\n" {
		t.Fatalf("replies after commit = %q, want OK then the GET", got)
	}
}

// TestRelaxedConnectionUnaffected proves the default path never parks and
// never registers: same runtime, same write, no strict flag.
func TestRelaxedConnectionUnaffected(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()

	if err := c.Do(emitOp, true, args("a")); err != nil {
		t.Fatal(err)
	}
	got := drainAll(c, w)
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("relaxed reply = %q", got)
	}
	if len(fl.pending) != 0 {
		t.Fatalf("relaxed write registered %d commit callbacks", len(fl.pending))
	}
}

// TestStrictMultiGroupCountdown proves a command that emitted to several
// groups acks on the last covering commit, not the first.
func TestStrictMultiGroupCountdown(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()
	c.SetStrictAck(true)

	// "a" and "b" land on different fake groups (97%4 != 98%4).
	if err := c.Do(emitOp, true, args("a", "b")); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(c, w); len(got) != 0 {
		t.Fatalf("emitted %q with both marks pending", got)
	}
	if len(fl.pending) != 2 {
		t.Fatalf("registered %d commit callbacks, want one per group", len(fl.pending))
	}
	fl.pending[0].fn()
	var got [][]byte
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	if len(got) != 0 {
		t.Fatalf("acked %q on the first of two marks", got)
	}
	fl.pending[1].fn()
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("replies after the second mark = %q", got)
	}
}

// TestStrictSameGroupCoalesces proves marks coalesce per group at the
// group's highest seq: a command emitting twice to one group registers one
// callback, on the later frame.
func TestStrictSameGroupCoalesces(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()
	c.SetStrictAck(true)

	if err := c.Do(emitOp, true, args("a", "a")); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(c, w); len(got) != 0 {
		t.Fatalf("emitted %q with the mark pending", got)
	}
	if len(fl.pending) != 1 || fl.pending[0].seq != 2 {
		t.Fatalf("callbacks = %+v, want one at the group's last seq 2", fl.pending)
	}
	fl.pending[0].fn()
	var got [][]byte
	c.DrainReplies(func(rep []byte) { got = append(got, append([]byte(nil), rep...)) })
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("replies after commit = %q", got)
	}
}

// strictFanProbe backs a synthetic strict MSET sub-command: pairs commit
// through the store and the log exactly as str.MSetShard's do, and the probe
// parks on command at a chosen pair, so the waiter-captured marks are driven
// deterministically.
type strictFanProbe struct {
	parkAt    int  // arg index to park at, -1 never
	parkEvery bool // keep parking there on every pass, for the stall tests
	parked    bool
}

func strictFanHandler(p *strictFanProbe) Handler {
	return func(cx *Ctx, a [][]byte, r Reply) {
		for i := cx.ResumeIndex(); i+1 < len(a); i += 2 {
			if i == p.parkAt && (p.parkEvery || !p.parked) {
				p.parked = true
				cx.ParkFullAt(store.ErrFull, i)
				return
			}
			if err := cx.St.Set(a[i], a[i+1]); err != nil {
				r.FanErrString("ERR " + err.Error())
				return
			}
			if err := cx.LogStrSet(a[i], a[i+1], 0, false); err != nil {
				r.FanErrString(err.Error())
				return
			}
		}
		r.FanOK()
	}
}

// newStrictFanRuntime builds a runtime with the fake log wired and the probe
// handler registered as a fan-capable write op.
func newStrictFanRuntime(shards int, p *strictFanProbe) (*Runtime, *fakeLog, byte) {
	fl := newFakeLog()
	handlers := testHandlers()
	op := byte(len(handlers))
	handlers = append(handlers, strictFanHandler(p))
	rt := New(shards, testArena, testSeg)
	rt.Use(handlers)
	rt.SetWriteLog(fl)
	return rt, fl, op
}

// drainWorkers runs every shard's drain to a fixpoint, then takes whatever
// replies are ready, the multi-shard sibling of drainAll.
func drainWorkers(c *Conn, rt *Runtime) [][]byte {
	c.Flush()
	for _, w := range rt.workers {
		for w.drainAndExecute() > 0 {
		}
	}
	return drainAvail(c)
}

// TestStrictFanHoldsGatherUntilCommit proves the fan half of the contract: a
// multi-key write scattered over two shards executes everywhere, its partials
// gather, and the assembled reply parks on both sub-commands' marks, acking
// on the last covering commit with everything pipelined behind it in order.
func TestStrictFanHoldsGatherUntilCommit(t *testing.T) {
	p := &strictFanProbe{parkAt: -1}
	rt, fl, op := newStrictFanRuntime(2, p)
	c := rt.NewConn()
	c.SetStrictAck(true)

	// One key per shard, and on distinct fake-log groups so the gather holds
	// two marks rather than one coalesced.
	var k0, k1 []byte
	for ch := byte('a'); ch <= 'z' && (k0 == nil || k1 == nil); ch++ {
		k := []byte{ch}
		if k0 == nil && rt.ShardOf(k) == 0 {
			k0 = k
			continue
		}
		if k1 == nil && rt.ShardOf(k) == 1 && (k0 == nil || fl.group(k) != fl.group(k0)) {
			k1 = k
		}
	}
	if k0 == nil || k1 == nil {
		t.Fatal("no key pair split across the two shards and two groups")
	}

	if err := c.DoFan(op, FanOK, [][]byte{k0, k1}, args("v", "v")); err != nil { // seq 0
		t.Fatal(err)
	}
	if err := c.Do(opGet, true, [][]byte{k0}); err != nil { // seq 1
		t.Fatal(err)
	}
	if got := drainWorkers(c, rt); len(got) != 0 {
		t.Fatalf("emitted %q with the fan gather held", got)
	}
	if len(fl.pending) != 2 {
		t.Fatalf("registered %d commit callbacks, want one per sub-command's group", len(fl.pending))
	}
	// Both writes applied already: only the output waits.
	if v, ok := rt.workers[rt.ShardOf(k1)].cx.St.Get(k1, nil); !ok || string(v) != "v" {
		t.Fatalf("store after held fan write = %q ok=%v", v, ok)
	}
	fl.pending[0].fn()
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("acked %q on the first of two marks", got)
	}
	fl.pending[1].fn()
	got := drainAvail(c)
	if len(got) != 2 || string(got[0]) != "+OK\r\n" || string(got[1]) != "$1\r\nv\r\n" {
		t.Fatalf("replies after both commits = %q, want OK then the GET", got)
	}
}

// TestStrictFanSameGroupCoalesces proves a sub-command whose pairs mark one
// group registers a single callback at the group's highest seq, the noteMark
// rule carried through the partial.
func TestStrictFanSameGroupCoalesces(t *testing.T) {
	p := &strictFanProbe{parkAt: -1}
	rt, fl, op := newStrictFanRuntime(1, p)
	c := rt.NewConn()
	c.SetStrictAck(true)

	// "a" and "e" share fake group 1 (97%4 == 101%4).
	if err := c.DoFan(op, FanOK, args("a", "e"), args("v", "v")); err != nil {
		t.Fatal(err)
	}
	if got := drainWorkers(c, rt); len(got) != 0 {
		t.Fatalf("emitted %q with the mark pending", got)
	}
	if len(fl.pending) != 1 || fl.pending[0].seq != 2 {
		t.Fatalf("callbacks = %+v, want one at the group's last seq 2", fl.pending)
	}
	fl.pending[0].fn()
	got := drainAvail(c)
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("replies after commit = %q", got)
	}
}

// TestRelaxedFanUnaffected proves the default fan path is untouched: the
// gathered reply delivers at merge time and nothing registers.
func TestRelaxedFanUnaffected(t *testing.T) {
	p := &strictFanProbe{parkAt: -1}
	rt, fl, op := newStrictFanRuntime(1, p)
	c := rt.NewConn()

	if err := c.DoFan(op, FanOK, args("a", "b"), args("v", "v")); err != nil {
		t.Fatal(err)
	}
	got := drainWorkers(c, rt)
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("relaxed fan reply = %q", got)
	}
	if len(fl.pending) != 0 {
		t.Fatalf("relaxed fan registered %d commit callbacks", len(fl.pending))
	}
}

// TestStrictFanRetryCarriesMarks is the waiter-capture proof: a sub-command
// commits its first pair, parks on the second, and completes on retry. The
// gather must wait on the marks of BOTH passes; losing the first pass's mark
// would ack the command with its first pair's frame still unflushed.
func TestStrictFanRetryCarriesMarks(t *testing.T) {
	p := &strictFanProbe{parkAt: 2}
	rt, fl, op := newStrictFanRuntime(1, p)
	c := rt.NewConn()
	c.SetStrictAck(true)
	w := rt.workers[0]

	// "a" and "b" mark distinct fake groups; the probe parks at the "b" pair
	// after committing and logging the "a" pair.
	if err := c.DoFan(op, FanOK, args("a", "b"), args("v", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()
	if len(w.fullWaiters) != 1 {
		t.Fatalf("fullWaiters = %d, want 1", len(w.fullWaiters))
	}
	// The committed prefix's mark rode onto the waiter at park time.
	fw := w.fullWaiters[0]
	if len(fw.marks) != 1 || fw.marks[0].Group != fl.group([]byte("a")) || fw.marks[0].Seq != 1 {
		t.Fatalf("waiter marks = %+v, want the first pair's frame", fw.marks)
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("delivered %q while the sub was parked", got)
	}

	// Retry completes the second pair; the gather then holds on both groups.
	w.retryFull()
	if len(w.fullWaiters) != 0 {
		t.Fatalf("fullWaiters = %d after retry, want 0", len(w.fullWaiters))
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("emitted %q with both marks pending", got)
	}
	if len(fl.pending) != 2 {
		t.Fatalf("registered %d commit callbacks after retry, want both passes' groups", len(fl.pending))
	}
	fl.pending[0].fn()
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("acked %q on the first of two marks", got)
	}
	fl.pending[1].fn()
	got := drainAvail(c)
	if len(got) != 1 || string(got[0]) != "+OK\r\n" {
		t.Fatalf("replies after both commits = %q", got)
	}
}

// TestStrictFanStallHoldsCommittedPrefix proves the stall-out answer keeps
// the strict contract too: a sub-command that committed one pair and then
// genuinely stalls takes the OOM error, and even that error holds until the
// committed pair's frame is covered, so no reply on a strict connection ever
// races the frames behind it.
func TestStrictFanStallHoldsCommittedPrefix(t *testing.T) {
	p := &strictFanProbe{parkAt: 2, parkEvery: true}
	rt, fl, op := newStrictFanRuntime(1, p)
	c := rt.NewConn()
	c.SetStrictAck(true)
	w := rt.workers[0]

	if err := c.DoFan(op, FanOK, args("a", "b"), args("v", "v")); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	w.drainAndExecute()
	for passes := 0; len(w.fullWaiters) > 0; passes++ {
		if passes > bpStallWindow+4 {
			t.Fatalf("fan sub still parked after %d passes, window is %d", passes, bpStallWindow)
		}
		w.retryFull()
	}
	if w.bpStalls != 1 {
		t.Fatalf("bpStalls = %d, want 1", w.bpStalls)
	}
	if got := drainAvail(c); len(got) != 0 {
		t.Fatalf("stall reply %q emitted before the prefix's commit", got)
	}
	if len(fl.pending) != 1 || fl.pending[0].group != fl.group([]byte("a")) {
		t.Fatalf("callbacks = %+v, want the committed pair's group", fl.pending)
	}
	fl.pending[0].fn()
	got := drainAvail(c)
	want := "-ERR " + store.ErrFull.Error() + " (no larger-than-memory tier)\r\n"
	if len(got) != 1 || string(got[0]) != want {
		t.Fatalf("held stall reply = %q, want %q", got, want)
	}
}

// TestIntentClosureEmissionRegistersNoMark proves the curConn hygiene in
// applyIntentOp: a tier-two owner closure runs between commands, where
// curConn still names the previous command's connection, so an emission
// inside it must register no mark. Without the clear, the strict GET below
// would merge a mark it never emitted and hold on a commit forever.
func TestIntentClosureEmissionRegistersNoMark(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()
	c.SetStrictAck(true)

	if err := c.Do(emitOp, true, args("a")); err != nil {
		t.Fatal(err)
	}
	if got := drainAll(c, w); len(got) != 0 {
		t.Fatalf("strict write answered before commit: %q", got)
	}
	if len(fl.pending) != 1 {
		t.Fatalf("registered %d commit callbacks, want 1", len(fl.pending))
	}
	fl.pending[0].fn()
	c.DrainReplies(func([]byte) {})

	// The worker's curConn still names the strict connection. Run an
	// emitting owner closure, the shape a cross-shard command's t.Do hop
	// or a PostOwner side-hop takes.
	rt.PostOwner(0, func(cx *Ctx) {
		if err := cx.LogStrSet([]byte("b"), []byte("v"), 0, false); err != nil {
			t.Errorf("closure emission failed: %v", err)
		}
	})
	if w.advanceIntents() != 1 {
		t.Fatal("owner closure did not run")
	}

	if err := c.Do(opGet, true, args("a")); err != nil {
		t.Fatal(err)
	}
	got := drainAll(c, w)
	if len(got) != 1 || string(got[0]) != "$1\r\nv\r\n" {
		t.Fatalf("GET after closure emission = %q, want an immediate answer", got)
	}
	if len(fl.pending) != 1 {
		t.Fatalf("callbacks = %d, want the strict write's alone", len(fl.pending))
	}
}

// TestStrictEmissionErrorAnswersNow proves a failed emission on a strict
// connection replies immediately: no frame was buffered, so there is
// nothing to wait for and the error takes the slot.
func TestStrictEmissionErrorAnswersNow(t *testing.T) {
	rt, fl, emitOp := newStrictRuntime()
	w := rt.workers[0]
	c := rt.NewConn()
	c.SetStrictAck(true)
	fl.err = errors.New("ERR store: flush stalled")

	if err := c.Do(emitOp, true, args("a")); err != nil {
		t.Fatal(err)
	}
	got := drainAll(c, w)
	if len(got) != 1 || string(got[0]) != "-ERR store: flush stalled\r\n" {
		t.Fatalf("failed strict write replied %q", got)
	}
	if len(fl.pending) != 0 {
		t.Fatalf("failed emission registered %d commit callbacks", len(fl.pending))
	}
}
