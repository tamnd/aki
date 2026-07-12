package stream

import (
	"runtime"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The blocking XREAD suite (spec 2064/f3/14 section 6.4). It drives the real
// handlers on a one-shard runtime and covers the ways a BLOCK read resolves: an
// immediate serve when a stream already has entries above the after-ID, a park
// that a later XADD wakes, the "$" bound that sees only entries added after the
// park, a multi-key wait whose sibling nodes unlink together, COUNT on the woken
// reply, the null-array timeout, and BLOCK 0 that waits until served. The reorder
// ring defers the parked reply here the same way the driver's reader barrier does
// in production, so the harness needs no ArmBlock.

// park sends a blocking XREAD routed to the one shard and returns at once without
// waiting for a reply, the way a client does after a BLOCK that blocks.
func park(t *testing.T, c *shard.Conn, a ...string) {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(opXread, 0, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
}

// drainN polls the connection until it has emitted want whole replies, then
// returns them in order.
func drainN(t *testing.T, c *shard.Conn, want int) [][]byte {
	t.Helper()
	var reps [][]byte
	deadline := time.Now().Add(10 * time.Second)
	for len(reps) < want {
		c.DrainReplies(func(b []byte) { reps = append(reps, append([]byte(nil), b...)) })
		if len(reps) < want {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %d replies, got %d", want, len(reps))
			}
			runtime.Gosched()
		}
	}
	return reps
}

func drainOne(t *testing.T, c *shard.Conn) []byte {
	t.Helper()
	return drainN(t, c, 1)[0]
}

// noReply fails if the connection emits anything within dur, the way a still
// blocked waiter must stay silent.
func noReply(t *testing.T, c *shard.Conn, dur time.Duration) {
	t.Helper()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		got := false
		c.DrainReplies(func(b []byte) { got = true })
		if got {
			t.Fatal("connection emitted a reply while it should still block")
		}
		runtime.Gosched()
	}
}

// --- immediate serve ------------------------------------------------------

func TestXreadBlockImmediateServe(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// The stream already holds an entry above id 0, so BLOCK never parks: the read
	// serves at once from the same collection loop the non-blocking form runs.
	wantStreams(t, do(t, c, opXread, "BLOCK", "100", "STREAMS", "s", "0"),
		sw("s", e("1-0", "f", "v")))
}

// --- park then serve ------------------------------------------------------

func TestXreadBlockParkThenServe(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	// "$" on a missing stream resolves to 0-0, so the read parks; a later XADD on
	// another connection wakes it with the appended entry.
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	noReply(t, a, 30*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "v")))
}

func TestXreadBlockExplicitID(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	do(t, b, opXadd, "s", "5-0", "f", "old")
	// An explicit after-ID above the current tail parks even though the stream
	// exists, and only an entry past that ID wakes it.
	park(t, a, "BLOCK", "0", "STREAMS", "s", "5-0")
	do(t, b, opXadd, "s", "6-0", "f", "new")
	wantStreams(t, drainOne(t, a), sw("s", e("6-0", "f", "new")))
}

func TestXreadBlockDollarSeesOnlyNew(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	do(t, b, opXadd, "s", "1-0", "f", "a")
	do(t, b, opXadd, "s", "2-0", "f", "b")
	// "$" resolves to the current last ID at park, so the two existing entries are
	// invisible and only the next add is delivered.
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "3-0", "f", "c")
	wantStreams(t, drainOne(t, a), sw("s", e("3-0", "f", "c")))
}

func TestXreadBlockUnrelatedAddDoesNotWake(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	// An add to a different stream must not wake the waiter.
	do(t, b, opXadd, "other", "1-0", "f", "v")
	noReply(t, a, 50*time.Millisecond)
	// The add on the blocked key does.
	do(t, b, opXadd, "s", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "v")))
}

// --- multi-key ------------------------------------------------------------

func TestXreadBlockMultiKey(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	// Park on two streams; an add to the second wakes the waiter with that stream's
	// entry, the other stream omitted since it produced nothing.
	park(t, a, "BLOCK", "0", "STREAMS", "s1", "s2", "$", "$")
	do(t, b, opXadd, "s2", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a), sw("s2", e("1-0", "f", "v")))
}

func TestXreadBlockMultiKeySiblingUnlink(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, "BLOCK", "0", "STREAMS", "s1", "s2", "$", "$")
	// The add on s1 serves the waiter and unlinks its sibling on s2.
	do(t, b, opXadd, "s1", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a), sw("s1", e("1-0", "f", "v")))
	// A later add on s2 must not wake the already served waiter.
	do(t, b, opXadd, "s2", "1-0", "f", "w")
	noReply(t, a, 60*time.Millisecond)
}

func TestXreadBlockServesAllWaiters(t *testing.T) {
	rt := newHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	// A stream read is a fan-out: two clients blocked on the same key both receive
	// the one appended entry, unlike a BLPOP hand-off.
	park(t, a1, "BLOCK", "0", "STREAMS", "s", "$")
	park(t, a2, "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a1), sw("s", e("1-0", "f", "v")))
	wantStreams(t, drainOne(t, a2), sw("s", e("1-0", "f", "v")))
}

func TestXreadBlockParkServeParkAgain(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	// A second BLOCK after the first served reuses the recycled waiter node and
	// the resident list, so a busy blocked key never grows its slab past its peak.
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "1-0", "f", "a")
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "a")))
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "2-0", "f", "b")
	wantStreams(t, drainOne(t, a), sw("s", e("2-0", "f", "b")))
}

// --- COUNT with BLOCK -----------------------------------------------------

func TestXreadBlockCountAccepted(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	// COUNT parses in either order with BLOCK and is carried into the park. A single
	// XADD wakes the reader with that one entry, the way Redis serves a blocked
	// XREAD on the first new entry; COUNT bounds the wake without truncating a
	// smaller result.
	park(t, a, "COUNT", "2", "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "1-0", "f", "a")
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "a")))
}

// --- timeout --------------------------------------------------------------

func TestXreadBlockTimeoutNullArray(t *testing.T) {
	c := newHarness(t).NewConn()
	// A finite BLOCK with no serving add fires the timer and delivers the null
	// array. do waits for the deferred reply, so no separate drain is needed.
	wantStreams(t, do(t, c, opXread, "BLOCK", "50", "STREAMS", "s", "$"))
}

func TestXreadBlockZeroWaitsUntilServed(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	// BLOCK 0 arms no timer, so the waiter stays silent until an add serves it.
	noReply(t, a, 80*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "v")))
}

func TestXreadBlockTimeoutErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	// A negative timeout and a non-integer one are Redis's two BLOCK errors.
	wantErr(t, do(t, c, opXread, "BLOCK", "-1", "STREAMS", "s", "$"),
		"ERR timeout is negative")
	wantErr(t, do(t, c, opXread, "BLOCK", "x", "STREAMS", "s", "$"),
		"ERR timeout is not an integer or out of range")
}

func TestXreadBlockWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	// A wrong-typed key fails the read before it can park.
	wantErr(t, do(t, c, opXread, "BLOCK", "0", "STREAMS", "k", "$"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}

// --- reorder stall --------------------------------------------------------

// TestXreadBlockReorderStall proves the reorder ring holds a reply pipelined
// behind a parked XREAD until the block resolves, then emits both in request
// order.
func TestXreadBlockReorderStall(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	if err := a.DoAt(opXread, 0, [][]byte{[]byte("BLOCK"), []byte("0"), []byte("STREAMS"), []byte("s"), []byte("$")}); err != nil {
		t.Fatal(err)
	}
	if err := a.DoAt(opXlen, 0, [][]byte{[]byte("other")}); err != nil {
		t.Fatal(err)
	}
	a.Flush()
	// Neither reply may emit while the XREAD is parked.
	noReply(t, a, 80*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	reps := drainN(t, a, 2)
	wantStreams(t, reps[0], sw("s", e("1-0", "f", "v")))
	wantInt(t, reps[1], 0)
}

// --- race cleanliness -----------------------------------------------------

// TestXreadBlockServeRaceClean drives the cross-goroutine wake the race detector
// guards: the owner running an XADD completes a reply on a foreign connection
// while that connection's reader drains.
func TestXreadBlockServeRaceClean(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, "BLOCK", "0", "STREAMS", "s", "$")
	go func() {
		_ = b.DoAt(opXadd, 0, [][]byte{[]byte("s"), []byte("1-0"), []byte("f"), []byte("v")})
		b.Flush()
	}()
	wantStreams(t, drainOne(t, a), sw("s", e("1-0", "f", "v")))
}
