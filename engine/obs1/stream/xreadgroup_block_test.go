package stream

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// The blocking XREADGROUP suite (spec 2064/f3/14 section 7.5). Unlike XREAD, a
// group `>` wake is a hand-off: an XADD delivers the appended entry to exactly one
// parked consumer, whose delivery advances the group cursor and records the PEL
// entry, so a second consumer parked on the same group stays parked. Different
// groups each deliver independently. The reorder ring defers the parked reply the
// way the driver's reader barrier does in production, so the harness needs no
// ArmBlock.

// parkGroup sends a blocking XREADGROUP routed to the one shard and returns at once,
// the way a client does after a BLOCK that parks.
func parkGroup(t *testing.T, c *shard.Conn, a ...string) {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(opXreadgroup, 0, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
}

// emptyGroup creates an empty native stream s with group g at the tail, so a `>`
// read parks until a later XADD, the setup every block test starts from.
func emptyGroup(t *testing.T, c *shard.Conn) {
	t.Helper()
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "$", "MKSTREAM"), "OK")
}

// oneEntry asserts a woken group reply carried exactly one entry on key with the
// given id and single field pair.
func oneEntry(t *testing.T, raw []byte, key, id, f, v string) {
	t.Helper()
	got := readGroupReply(t, raw)
	es := got[key]
	if len(es) != 1 || es[0].id != id {
		t.Fatalf("reply[%s] = %v, want one entry %s", key, got, id)
	}
	if len(es[0].fields) != 2 || es[0].fields[0] != f || es[0].fields[1] != v {
		t.Fatalf("entry fields = %v, want [%s %s]", es[0].fields, f, v)
	}
}

// --- park then serve ------------------------------------------------------

func TestXreadgroupBlockParkThenServe(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	// The group cursor sits at the tail, so `>` parks; a later XADD wakes the
	// consumer with the appended entry.
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	noReply(t, a, 30*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
	// The woken delivery recorded a pending entry owned by c.
	got := decodeReply(t, do(t, b, opXpending, "s", "g")).([]any)
	if got[0].(string) != "1" {
		t.Fatalf("pending count = %v, want 1 after the woken delivery", render(got))
	}
}

func TestXreadgroupBlockUnrelatedAddNoWake(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	// An add to a different stream must not wake the waiter.
	do(t, b, opXadd, "other", "1-0", "f", "v")
	noReply(t, a, 50*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
}

// --- hand-off within a group ----------------------------------------------

func TestXreadgroupBlockHandoffOneConsumer(t *testing.T) {
	rt := newHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a1)
	// Two consumers of the same group park. One XADD is a hand-off, not a fan-out:
	// the first-parked consumer takes the entry and advances the group cursor, so the
	// second stays parked.
	parkGroup(t, a1, "GROUP", "g", "c1", "BLOCK", "0", "STREAMS", "s", ">")
	parkGroup(t, a2, "GROUP", "g", "c2", "BLOCK", "0", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a1), "s", "1-0", "f", "v")
	noReply(t, a2, 50*time.Millisecond)
	// A second XADD wakes the still-parked consumer with the next entry.
	do(t, b, opXadd, "s", "2-0", "f", "w")
	oneEntry(t, drainOne(t, a2), "s", "2-0", "f", "w")
	// Each consumer owns the entry it took.
	rows := pendingRows(t, do(t, b, opXpending, "s", "g", "-", "+", "10"))
	if len(rows) != 2 || rows[0][1] != "c1" || rows[1][1] != "c2" {
		t.Fatalf("pending owners = %v, want 1-0->c1 and 2-0->c2", rows)
	}
}

func TestXreadgroupBlockTwoGroupsBothServed(t *testing.T) {
	rt := newHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	wantStatus(t, do(t, a1, opXgroup, "CREATE", "s", "g1", "$", "MKSTREAM"), "OK")
	wantStatus(t, do(t, a1, opXgroup, "CREATE", "s", "g2", "$"), "OK")
	// Two different groups park on the same key. Each keeps its own cursor, so one
	// XADD delivers the entry to both groups' waiting consumers.
	parkGroup(t, a1, "GROUP", "g1", "c", "BLOCK", "0", "STREAMS", "s", ">")
	parkGroup(t, a2, "GROUP", "g2", "c", "BLOCK", "0", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a1), "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a2), "s", "1-0", "f", "v")
}

// --- mixed with a plain XREAD waiter --------------------------------------

func TestXreadgroupBlockMixedWithPlainRead(t *testing.T) {
	rt := newHarness(t)
	g := rt.NewConn()
	p := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, g)
	// A plain XREAD and a group `>` both park on the same key. One XADD fans out to
	// the plain reader and hands off to the group consumer, both from one serve walk.
	parkGroup(t, g, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	park(t, p, "BLOCK", "0", "STREAMS", "s", "$")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, g), "s", "1-0", "f", "v")
	wantStreams(t, drainOne(t, p), sw("s", e("1-0", "f", "v")))
}

// --- NOACK ----------------------------------------------------------------

func TestXreadgroupBlockNoackNoPending(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	// A woken NOACK delivery hands out the entry but records no pending entry.
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "NOACK", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
	got := decodeReply(t, do(t, b, opXpending, "s", "g")).([]any)
	if got[0].(string) != "0" {
		t.Fatalf("pending count = %v, want 0 under NOACK", render(got))
	}
}

// --- COUNT ----------------------------------------------------------------

func TestXreadgroupBlockCountBoundsWake(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	// COUNT parses with BLOCK and bounds the woken delivery. A single XADD wakes the
	// reader with that one entry.
	parkGroup(t, a, "GROUP", "g", "c", "COUNT", "2", "BLOCK", "0", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
}

// --- node reuse -----------------------------------------------------------

func TestXreadgroupBlockParkServeParkAgain(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "1-0", "f", "a")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "a")
	// A second park reuses the recycled waiter node, and delivers from the advanced
	// cursor so it sees only the next add.
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	do(t, b, opXadd, "s", "2-0", "f", "b")
	oneEntry(t, drainOne(t, a), "s", "2-0", "f", "b")
}

// --- timeout --------------------------------------------------------------

func TestXreadgroupBlockTimeoutNullArray(t *testing.T) {
	c := newHarness(t).NewConn()
	emptyGroup(t, c)
	// A finite BLOCK with no serving add fires the timer and delivers the null array,
	// the same timeout shape XREAD gives.
	if got := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">")); got != nil {
		t.Fatalf("timeout reply = %v, want the null array", got)
	}
}

func TestXreadgroupBlockZeroWaitsUntilServed(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	// BLOCK 0 arms no timer, so the waiter stays silent until an add serves it.
	noReply(t, a, 80*time.Millisecond)
	do(t, b, opXadd, "s", "1-0", "f", "v")
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
}

// --- explicit ID never parks ----------------------------------------------

func TestXreadgroupBlockExplicitIDNeverParks(t *testing.T) {
	c := newHarness(t).NewConn()
	emptyGroup(t, c)
	// An explicit-ID history read is always present in the reply (an empty entry list
	// is the drained-history answer), so BLOCK on it returns at once, never parking.
	got := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", "0"))
	if es, ok := got["s"]; !ok || len(es) != 0 {
		t.Fatalf("explicit-id reply = %v, want an empty entry list for s", got)
	}
}

// --- errors before the park -----------------------------------------------

func TestXreadgroupBlockNogroupBeforePark(t *testing.T) {
	c := newHarness(t).NewConn()
	// A missing group fails the read before it can park, even with BLOCK.
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "nokey", ">"),
		nogroupRead([]byte("nokey"), []byte("g")))
}

func TestXreadgroupBlockTimeoutErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	emptyGroup(t, c)
	// A negative timeout and a non-integer one are the two BLOCK errors, reported
	// before any park.
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "-1", "STREAMS", "s", ">"),
		"ERR timeout is negative")
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "x", "STREAMS", "s", ">"),
		"ERR timeout is not an integer or out of range")
}

// --- race cleanliness -----------------------------------------------------

// TestXreadgroupBlockServeRaceClean drives the cross-goroutine wake the race
// detector guards: the owner running an XADD completes a reply on a foreign
// connection while that connection's reader drains.
func TestXreadgroupBlockServeRaceClean(t *testing.T) {
	rt := newHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	emptyGroup(t, a)
	parkGroup(t, a, "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
	go func() {
		_ = b.DoAt(opXadd, 0, [][]byte{[]byte("s"), []byte("1-0"), []byte("f"), []byte("v")})
		b.Flush()
	}()
	oneEntry(t, drainOne(t, a), "s", "1-0", "f", "v")
}
