package shard

import (
	"errors"
	"testing"
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
