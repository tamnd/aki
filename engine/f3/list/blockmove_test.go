package list

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// BLMOVE, BRPOPLPUSH, and BLMPOP (spec 2064/f3/13 M3 slice 8), the blocking move
// and blocking multi-pop. The suite drives the real handlers on the shared block
// harness and covers the immediate serve through the non-blocking core, the park
// that a later push wakes, the LMOVE-destination serve hook that lets a plain move
// wake a blocked client, the serve-chain worklist (A->B->C and the A<->B ping-pong
// both drained in one push), the self-move, the two per-kind timeout shapes
// ($-1 for a move, *-1 for a pop), the direction and timeout parse errors before
// any side effect, the dest-wrong-type-at-serve that keeps the source element in
// place, BLMPOP's count budget and multi-key sibling unlink, and the reorder-ring
// stall for both new verbs. A byte-exact live replay guards the wire form.

// --- reply helpers --------------------------------------------------------

// wantKeyElems asserts a [key, [elem, ...]] reply, the shape a served BLMPOP
// returns, the same shape LMPOP builds.
func wantKeyElems(t *testing.T, raw []byte, key string, elems ...string) {
	t.Helper()
	got := decodeReply(t, raw)
	arr, ok := got.([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("reply = %v, want [key, [elems]]", render(got))
	}
	if s, ok := arr[0].(string); !ok || s != key {
		t.Fatalf("reply key = %v, want %q", arr[0], key)
	}
	inner, ok := arr[1].([]any)
	if !ok || len(inner) != len(elems) {
		t.Fatalf("reply elems = %v, want %v", render(got), elems)
	}
	for i := range elems {
		if s, ok := inner[i].(string); !ok || s != elems[i] {
			t.Fatalf("elem[%d] = %v, want %q", i, inner[i], elems[i])
		}
	}
}

// wantRawNullBulk pins the exact $-1 wire form, the null bulk a timed-out BLMOVE
// returns, distinct from the *-1 null array (decodeReply flattens both to nil, so
// the raw bytes are checked here).
func wantRawNullBulk(t *testing.T, raw []byte) {
	t.Helper()
	if string(raw) != "$-1\r\n" {
		t.Fatalf("reply = %q, want $-1 null bulk", raw)
	}
}

// wantRawNullArray pins the exact *-1 wire form, the null array a timed-out
// BLMPOP returns.
func wantRawNullArray(t *testing.T, raw []byte) {
	t.Helper()
	if string(raw) != "*-1\r\n" {
		t.Fatalf("reply = %q, want *-1 null array", raw)
	}
}

// --- immediate serve ------------------------------------------------------

func TestBlmoveImmediateServe(t *testing.T) {
	cases := []struct {
		name           string
		from, to       string
		moved          string
		wantSrc, wantD []string
	}{
		{"LEFT LEFT", "LEFT", "LEFT", "a", []string{"b", "c"}, []string{"a", "d", "e"}},
		{"LEFT RIGHT", "LEFT", "RIGHT", "a", []string{"b", "c"}, []string{"d", "e", "a"}},
		{"RIGHT LEFT", "RIGHT", "LEFT", "c", []string{"a", "b"}, []string{"c", "d", "e"}},
		{"RIGHT RIGHT", "RIGHT", "RIGHT", "c", []string{"a", "b"}, []string{"d", "e", "c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := newBlockHarness(t)
			c := rt.NewConn()
			wantInt(t, do(t, c, bkRpush, "src", "a", "b", "c"), 3)
			wantInt(t, do(t, c, bkRpush, "dst", "d", "e"), 2)
			wantBulk(t, do(t, c, bkBlmove, "src", "dst", tc.from, tc.to, "0"), tc.moved)
			wantArray(t, do(t, c, bkLrange, "src", "0", "-1"), tc.wantSrc...)
			wantArray(t, do(t, c, bkLrange, "dst", "0", "-1"), tc.wantD...)
		})
	}
}

func TestBrpoplpushImmediateServe(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "src", "a", "b", "c"), 3)
	// RPOPLPUSH pops the source tail and pushes the destination head.
	wantBulk(t, do(t, c, bkBrpoplpush, "src", "dst", "0"), "c")
	wantArray(t, do(t, c, bkLrange, "src", "0", "-1"), "a", "b")
	wantArray(t, do(t, c, bkLrange, "dst", "0", "-1"), "c")
	// Self-move rotates the tail to the head on one list.
	wantBulk(t, do(t, c, bkBrpoplpush, "src", "src", "0"), "b")
	wantArray(t, do(t, c, bkLrange, "src", "0", "-1"), "b", "a")
}

func TestBlmpopImmediateServe(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k", "a", "b", "c", "d"), 4)
	// LEFT with COUNT pops from the head in order.
	wantKeyElems(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "2"), "k", "a", "b")
	// RIGHT without COUNT pops one off the tail.
	wantKeyElems(t, do(t, c, bkBlmpop, "0", "1", "k", "RIGHT"), "k", "d")
	// First non-empty across a missing key, and a COUNT that clamps to the rest,
	// draining and dropping the key.
	wantKeyElems(t, do(t, c, bkBlmpop, "0", "2", "missing", "k", "LEFT", "COUNT", "9"), "k", "c")
	wantInt(t, do(t, c, bkLlen, "k"), 0)
}

// --- park then serve ------------------------------------------------------

func TestBlmoveParkThenServeByPush(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlmove, "src", "dst", "LEFT", "RIGHT", "0")
	// An unrelated push must not wake the move waiter.
	wantInt(t, do(t, b, bkRpush, "other", "x"), 1)
	noReply(t, a, 50*time.Millisecond)
	// A push on the source serves the move: the element leaves the source and is
	// pushed onto the destination tail, and the client gets the moved bulk.
	wantInt(t, do(t, b, bkRpush, "src", "v"), 1)
	wantBulk(t, drainOne(t, a), "v")
	wantInt(t, do(t, b, bkLlen, "src"), 0)
	wantArray(t, do(t, b, bkLrange, "dst", "0", "-1"), "v")
}

// A plain LMOVE into a key with a parked BLPOP serves the blocked client through
// the lmove-destination hook, the reason the hook exists.
func TestLmoveDestServesBlockedBlpop(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	wantInt(t, do(t, b, bkRpush, "src", "x"), 1)
	park(t, a, bkBlpop, "d", "0")
	// LMOVE src head to d tail: the push onto d wakes the BLPOP parked on d.
	wantBulk(t, do(t, b, bkLmove, "src", "d", "LEFT", "RIGHT"), "x")
	wantArray(t, drainOne(t, a), "d", "x")
	// The element left the source and was consumed by the woken client, so both
	// keys are empty.
	wantInt(t, do(t, b, bkLlen, "src"), 0)
	wantInt(t, do(t, b, bkLlen, "d"), 0)
}

func TestBlmoveSelfMoveServe(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlmove, "k", "k", "LEFT", "LEFT", "0")
	// A push on the self-move key serves it: the element is popped off the head and
	// pushed back onto the head, so the key keeps it and the client gets it.
	wantInt(t, do(t, b, bkRpush, "k", "x"), 1)
	wantBulk(t, drainOne(t, a), "x")
	wantArray(t, do(t, b, bkLrange, "k", "0", "-1"), "x")
}

// --- serve chain ----------------------------------------------------------

// TestBlmoveServeChain proves the worklist drains a chain in one push: a BLMOVE
// A->B and a BLMOVE B->C both parked, a single element pushed onto A walks A->B->C
// and lands in C, waking both movers.
func TestBlmoveServeChain(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	park(t, a1, bkBlmove, "A", "B", "LEFT", "RIGHT", "0")
	park(t, a2, bkBlmove, "B", "C", "LEFT", "RIGHT", "0")
	wantInt(t, do(t, b, bkRpush, "A", "x"), 1)
	wantBulk(t, drainOne(t, a1), "x")
	wantBulk(t, drainOne(t, a2), "x")
	wantInt(t, do(t, b, bkLlen, "A"), 0)
	wantInt(t, do(t, b, bkLlen, "B"), 0)
	wantArray(t, do(t, b, bkLrange, "C", "0", "-1"), "x")
}

// TestBlmoveServeChainPingPong proves the drain terminates on a cycle: a BLMOVE
// A->B and a BLMOVE B->A parked, one element pushed onto A satisfies both moves
// and the chain settles with the element back in A.
func TestBlmoveServeChainPingPong(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	park(t, a1, bkBlmove, "A", "B", "LEFT", "RIGHT", "0")
	park(t, a2, bkBlmove, "B", "A", "LEFT", "RIGHT", "0")
	wantInt(t, do(t, b, bkRpush, "A", "x"), 1)
	wantBulk(t, drainOne(t, a1), "x")
	wantBulk(t, drainOne(t, a2), "x")
	// Both waiters are gone and the element rests in A; a later push on either key
	// wakes no one.
	wantArray(t, do(t, b, bkLrange, "A", "0", "-1"), "x")
	wantInt(t, do(t, b, bkLlen, "B"), 0)
	wantInt(t, do(t, b, bkRpush, "B", "y"), 1)
	noReply(t, a1, 50*time.Millisecond)
	noReply(t, a2, 50*time.Millisecond)
}

// TestBlmoveChainEndsInMpop mixes the two servable pop shapes into one chain: a
// BLMOVE A->B feeds a BLMPOP parked on B, drained in one push.
func TestBlmoveChainEndsInMpop(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	park(t, a1, bkBlmove, "A", "B", "LEFT", "RIGHT", "0")
	park(t, a2, bkBlmpop, "0", "1", "B", "LEFT", "COUNT", "5")
	wantInt(t, do(t, b, bkRpush, "A", "x"), 1)
	wantBulk(t, drainOne(t, a1), "x")
	wantKeyElems(t, drainOne(t, a2), "B", "x")
	wantInt(t, do(t, b, bkLlen, "A"), 0)
	wantInt(t, do(t, b, bkLlen, "B"), 0)
}

// --- BLMPOP park then serve ----------------------------------------------

func TestBlmpopParkThenServe(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn()
	a2 := rt.NewConn()
	b := rt.NewConn()
	// Two waiters, each with a two-element budget, park in FIFO order.
	park(t, a1, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "2")
	park(t, a2, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "2")
	// Three elements arrive: the first waiter takes its two, the second takes the
	// one that is left, and the key drains.
	wantInt(t, do(t, b, bkRpush, "k", "v1", "v2", "v3"), 3)
	wantKeyElems(t, drainOne(t, a1), "k", "v1", "v2")
	wantKeyElems(t, drainOne(t, a2), "k", "v3")
	wantInt(t, do(t, b, bkLlen, "k"), 0)
}

func TestBlmpopMultiKeyFifoAndSiblingUnlink(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlmpop, "0", "3", "k1", "k2", "k3", "LEFT", "COUNT", "2")
	// A push on the middle key serves the waiter and unlinks it from k1 and k3.
	wantInt(t, do(t, b, bkRpush, "k2", "v1", "v2", "v3"), 3)
	wantKeyElems(t, drainOne(t, a), "k2", "v1", "v2")
	// Later pushes on the other two keys must not wake the already served waiter.
	wantInt(t, do(t, b, bkRpush, "k1", "w"), 1)
	wantInt(t, do(t, b, bkRpush, "k3", "z"), 1)
	noReply(t, a, 100*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, "k1", "0", "-1"), "w")
	wantArray(t, do(t, b, bkLrange, "k2", "0", "-1"), "v3")
	wantArray(t, do(t, b, bkLrange, "k3", "0", "-1"), "z")
}

// TestBlmovePreservesLengthInvariant shows the invariant the whole design rests
// on: after a serve leaves a list non-empty, that list carries no waiter. A
// BLMPOP that takes only part of a push leaves the rest in the key, and a later
// push wakes no one, so a non-empty list never coexists with a parked waiter.
func TestBlmovePreservesLengthInvariant(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	park(t, a, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "1")
	wantInt(t, do(t, b, bkRpush, "k", "v1", "v2", "v3"), 3)
	wantKeyElems(t, drainOne(t, a), "k", "v1") // took one, k=[v2,v3]
	// k is non-empty (holds v2, v3) and now carries no waiter: another push grows
	// it to three and wakes no one.
	wantInt(t, do(t, b, bkRpush, "k", "v4"), 3)
	noReply(t, a, 50*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, "k", "0", "-1"), "v2", "v3", "v4")
	// And an immediate blocking pop on the non-empty key serves at once.
	wantKeyElems(t, do(t, b, bkBlmpop, "0", "1", "k", "RIGHT", "COUNT", "1"), "k", "v4")
}

// --- timeouts -------------------------------------------------------------

func TestBlmoveTimeoutNullBulk(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	park(t, c, bkBlmove, "src", "dst", "LEFT", "RIGHT", "0.1")
	// The armed timer fires and delivers the RESP2 null bulk, the move's timeout
	// shape.
	wantRawNullBulk(t, drainOne(t, c))
}

func TestBrpoplpushTimeoutNullBulk(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	park(t, c, bkBrpoplpush, "src", "dst", "0.1")
	wantRawNullBulk(t, drainOne(t, c))
}

func TestBlmpopTimeoutNullArray(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	park(t, c, bkBlmpop, "0.1", "2", "m1", "m2", "LEFT")
	// BLMPOP times out to the RESP2 null array, its pop-shaped timeout.
	wantRawNullArray(t, drainOne(t, c))
}

// --- parse errors ---------------------------------------------------------

func TestBlmoveTimeoutErrors(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "-1"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "-0.5"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "notanumber"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "nan"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "inf"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBrpoplpush, "s", "d", "-1"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBrpoplpush, "s", "d", "notafloat"), errTimeoutFloat)
	wantErr(t, do(t, c, bkBlmpop, "-1", "1", "k", "LEFT"), errTimeoutNeg)
	wantErr(t, do(t, c, bkBlmpop, "notafloat", "1", "k", "LEFT"), errTimeoutFloat)
}

// TestBlmoveBadDirection proves an invalid direction token is the syntax error
// before any side effect: the timeout is not parsed, the source is untouched, and
// the destination is never created.
func TestBlmoveBadDirection(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "src", "a", "b"), 2)
	wantErr(t, do(t, c, bkBlmove, "src", "dst", "UP", "RIGHT", "0"), errSyntax)
	wantErr(t, do(t, c, bkBlmove, "src", "dst", "LEFT", "SIDEWAYS", "0"), errSyntax)
	// A bad direction wins even over a bad timeout, since directions parse first.
	wantErr(t, do(t, c, bkBlmove, "src", "dst", "UP", "RIGHT", "notanumber"), errSyntax)
	// Nothing moved and no destination was born.
	wantArray(t, do(t, c, bkLrange, "src", "0", "-1"), "a", "b")
	wantInt(t, do(t, c, bkLlen, "dst"), 0)
}

func TestBlmpopParseErrors(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k", "a", "b"), 2)

	const errNumkeys = "ERR numkeys should be greater than 0"
	const errCount = "ERR count should be greater than 0"

	wantErr(t, do(t, c, bkBlmpop, "0", "0", "k", "LEFT"), errNumkeys)
	wantErr(t, do(t, c, bkBlmpop, "0", "-1", "k", "LEFT"), errNumkeys)
	wantErr(t, do(t, c, bkBlmpop, "0", "x", "k", "LEFT"), errNumkeys)
	wantErr(t, do(t, c, bkBlmpop, "0", "5", "k", "LEFT"), errSyntax)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "UP"), errSyntax)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "0"), errCount)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "-2"), errCount)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "x"), errCount)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "EXTRA"), errSyntax)
	wantErr(t, do(t, c, bkBlmpop, "0", "1", "k", "LEFT", "COUNT", "1", "TAIL"), errSyntax)
}

// TestBlmpopMatchesLmpopTail is the differential that pins BLMPOP's tail parse to
// LMPOP's byte-for-byte: every malformed tail must produce the identical error
// through both verbs, since they share parseLmpopTail.
func TestBlmpopMatchesLmpopTail(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantInt(t, do(t, c, bkRpush, "k", "a", "b", "c"), 3)
	tails := [][]string{
		{"0", "k", "LEFT"},
		{"-1", "k", "LEFT"},
		{"x", "k", "LEFT"},
		{"5", "k", "LEFT"},
		{"1", "k", "UP"},
		{"1", "k", "LEFT", "COUNT", "0"},
		{"1", "k", "LEFT", "COUNT", "-2"},
		{"1", "k", "LEFT", "COUNT", "x"},
		{"1", "k", "LEFT", "EXTRA"},
		{"1", "k", "LEFT", "COUNT", "1", "TAIL"},
	}
	for _, tl := range tails {
		lm := decodeReply(t, do(t, c, bkLmpop, tl...))
		bl := decodeReply(t, do(t, c, bkBlmpop, append([]string{"0"}, tl...)...))
		if !equalReply(lm, bl) {
			t.Fatalf("tail %v: LMPOP %v, BLMPOP %v", tl, render(lm), render(bl))
		}
	}
}

// --- wrong type -----------------------------------------------------------

func TestBlmoveSourceWrongTypeImmediate(t *testing.T) {
	rt := newBlockHarness(t)
	c := rt.NewConn()
	wantStatus(t, do(t, c, bkSet, "s", "v"), "OK")
	// A wrong-typed source is reported at once, never parked.
	wantErr(t, do(t, c, bkBlmove, "s", "d", "LEFT", "RIGHT", "0"), wrongType)
}

// TestBlmoveDestWrongTypeAtServe pins the deferred dest-type check: a BLMOVE parks
// on a missing source whose destination is a string key, and when a push finally
// serves it the client gets WRONGTYPE while the source element stays in place (the
// pop runs only after the check passes). The orchestrator live-verifies the exact
// text and that Redis leaves the source element intact.
func TestBlmoveDestWrongTypeAtServe(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	wantStatus(t, do(t, b, bkSet, "d", "astring"), "OK")
	park(t, a, bkBlmove, "src", "d", "LEFT", "RIGHT", "0")
	// The push makes the source servable; the serve finds the string destination
	// and fails the blocked client without moving anything.
	wantInt(t, do(t, b, bkRpush, "src", "x"), 1)
	wantErr(t, drainOne(t, a), wrongType)
	// The source element was never popped.
	wantArray(t, do(t, b, bkLrange, "src", "0", "-1"), "x")
}

// --- reorder stall --------------------------------------------------------

// TestBlmoveReorderStall proves a command pipelined behind a parked BLMOVE cannot
// reply until the block resolves, then both land in request order.
func TestBlmoveReorderStall(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	if err := a.DoAt(bkBlmove, 0, [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"), []byte("0")}); err != nil {
		t.Fatal(err)
	}
	if err := a.DoAt(bkLlen, 0, [][]byte{[]byte("other")}); err != nil {
		t.Fatal(err)
	}
	a.Flush()
	noReply(t, a, 100*time.Millisecond)
	wantInt(t, do(t, b, bkRpush, "src", "v"), 1)
	reps := drainN(t, a, 2)
	wantBulk(t, reps[0], "v")
	wantInt(t, reps[1], 0)
}

// TestBlmpopReorderStall is the same stall for BLMPOP, which routes through DoAt
// (keyAt past its leading timeout), so it also exercises the ArmBlock barrier the
// DoAt path arms.
func TestBlmpopReorderStall(t *testing.T) {
	rt := newBlockHarness(t)
	a := rt.NewConn()
	b := rt.NewConn()
	if err := a.DoAt(bkBlmpop, 0, [][]byte{[]byte("0"), []byte("1"), []byte("k"), []byte("LEFT")}); err != nil {
		t.Fatal(err)
	}
	if err := a.DoAt(bkLlen, 0, [][]byte{[]byte("other")}); err != nil {
		t.Fatal(err)
	}
	a.Flush()
	noReply(t, a, 100*time.Millisecond)
	wantInt(t, do(t, b, bkRpush, "k", "v"), 1)
	reps := drainN(t, a, 2)
	wantKeyElems(t, reps[0], "k", "v")
	wantInt(t, reps[1], 0)
}

// --- race cleanliness -----------------------------------------------------

// TestBlmoveServeChainRaceClean drives the cross-goroutine wake through a chain
// that drains a BLMOVE, another BLMOVE, a BLMPOP, and a BLPOP in one burst, so the
// race detector guards the owner completing replies on foreign connections while
// their readers drain.
func TestBlmoveServeChainRaceClean(t *testing.T) {
	rt := newBlockHarness(t)
	a1 := rt.NewConn() // BLMOVE A->B
	a2 := rt.NewConn() // BLMOVE B->C
	a3 := rt.NewConn() // BLMPOP on C
	a4 := rt.NewConn() // BLPOP on D
	b := rt.NewConn()
	park(t, a1, bkBlmove, "A", "B", "LEFT", "RIGHT", "0")
	park(t, a2, bkBlmove, "B", "C", "LEFT", "RIGHT", "0")
	park(t, a3, bkBlmpop, "0", "1", "C", "LEFT", "COUNT", "1")
	park(t, a4, bkBlpop, "D", "0")
	go func() {
		_ = b.DoAt(bkRpush, 0, [][]byte{[]byte("A"), []byte("x")})
		_ = b.DoAt(bkRpush, 0, [][]byte{[]byte("D"), []byte("y")})
		b.Flush()
	}()
	wantBulk(t, drainOne(t, a1), "x")
	wantBulk(t, drainOne(t, a2), "x")
	wantKeyElems(t, drainOne(t, a3), "C", "x")
	wantArray(t, drainOne(t, a4), "D", "y")
}

// --- zero / one alloc park ------------------------------------------------

// blmoveDstSink is a package-level mutable byte slice so string(blmoveDstSink) in
// the alloc test cannot be folded to a constant string; it mirrors the real
// handler, where the destination key is dynamic wire bytes the move must copy.
var blmoveDstSink = []byte("dst")

// TestBlmoveParkZeroAllocs documents that a warm BLMOVE park pays exactly one
// allocation, the dstKey string copy the handler makes; the park itself reuses a
// recycled node and the parkWaiter/unlink machinery allocates nothing, so the one
// alloc is only the destination-key copy a move must carry, against the zero a
// BLPOP park holds.
func TestBlmoveParkZeroAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("AllocsPerRun counts race-runtime allocations under -race")
	}
	g := &reg{m: make(map[string]*list), waiters: make(map[string]*waitList)}
	c := &shard.Conn{}
	src := [][]byte{[]byte("k")}
	anchor := waitSpec{kind: kindMove, dstKey: string(blmoveDstSink)}
	_ = parkWaiter(g, src, anchor, c, 0) // anchor: keeps the waiter list alive
	for i := 0; i < 8; i++ {             // warm the node slab and the free stack
		g.unlinkAll(nil, parkWaiter(g, src, waitSpec{kind: kindMove, dstKey: string(blmoveDstSink)}, c, 1))
	}
	allocs := testing.AllocsPerRun(200, func() {
		spec := waitSpec{kind: kindMove, dstKey: string(blmoveDstSink)}
		g.unlinkAll(nil, parkWaiter(g, src, spec, c, 1))
	})
	if allocs != 1 {
		t.Errorf("warm BLMOVE park allocated %v times per run, want 1 (the dstKey copy)", allocs)
	}
}
