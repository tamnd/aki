package list

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
)

// Cross-shard blocking moves (blockmovecross.go): BLMOVE and BRPOPLPUSH whose
// source and destination land on different owners. The differential suite holds
// the DoBlockCross path byte-identical to the co-located point handler for every
// case that resolves at once, a move off a non-empty source or a WRONGTYPE, so a
// client cannot tell the cross move from the co-located one. The Redis-exactness of
// the co-located move is blockmove_test.go and lmove_test.go's job; here the
// co-located reply is the oracle. The park suite then proves what the co-located
// form never exercises: a source-only park whose serving push spawns a coordinator
// to reach the far destination, the timer cancelled at serve so it cannot fire the
// reply twice, the destination's own blocked waiters woken by the moved element,
// and the next source waiter re-driven after the coordinator commits.

// crossBlmoveFire sends a cross-shard BLMOVE the way dispatchBlockCross does:
// DoBlockCross holds an intent on the source and the destination while BlmoveCross
// runs the serve-or-park decision, and ArmBlock guards the reorder slot. It returns
// without waiting, so an immediate serve is read with drainOne and a park is left
// open for a serving push or the timeout.
func crossBlmoveFire(t *testing.T, c *shard.Conn, src, dst, from, to, timeout string) {
	t.Helper()
	tail := strBytes([]string{src, dst, from, to, timeout})
	err := c.DoBlockCross(tail[:2], func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		return BlmoveCross(tx, conn, seq, tail)
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	c.Flush()
}

// crossBrpoplpushFire sends a cross-shard BRPOPLPUSH the same way: the tail is
// source destination timeout, the older spelling of BLMOVE source destination
// RIGHT LEFT.
func crossBrpoplpushFire(t *testing.T, c *shard.Conn, src, dst, timeout string) {
	t.Helper()
	tail := strBytes([]string{src, dst, timeout})
	err := c.DoBlockCross(tail[:2], func(tx *shard.Txn, conn *shard.Conn, seq uint32) []byte {
		return BrpoplpushCross(tx, conn, seq, tail)
	})
	if err != nil {
		t.Fatal(err)
	}
	c.ArmBlock()
	c.Flush()
}

// seedMove plants one key's contents, a list or the planted string that makes a
// key WRONGTYPE, on both a co-located and a cross-shard key so the two move paths
// run over identical data.
func seedMove(t *testing.T, c *shard.Conn, coKey, xKey string, vals []string, str bool) {
	t.Helper()
	if str {
		do(t, c, bkSet, coKey, "notalist")
		do(t, c, bkSet, xKey, "notalist")
		return
	}
	for _, v := range vals {
		do(t, c, bkRpush, coKey, v)
		do(t, c, bkRpush, xKey, v)
	}
}

type moveCase struct {
	name    string
	srcVals []string
	dstVals []string
	srcStr  bool
	dstStr  bool
}

// moveImmediateCases are the seedings that resolve at once on both paths: the
// source is non-empty (a move) or a WRONGTYPE. An empty source parks, which the
// park suite covers, so it is not here.
var moveImmediateCases = []moveCase{
	{name: "onto empty dst", srcVals: []string{"a", "b", "c"}},
	{name: "onto nonempty dst", srcVals: []string{"a", "b"}, dstVals: []string{"x", "y"}},
	{name: "single element", srcVals: []string{"solo"}},
	{name: "src wrongtype", srcStr: true},
	{name: "dst wrongtype with src present", srcVals: []string{"a"}, dstStr: true},
	{name: "native band source", srcVals: bigVals("s", 200)},
}

var moveDirs = []struct{ from, to string }{
	{"LEFT", "LEFT"},
	{"LEFT", "RIGHT"},
	{"RIGHT", "LEFT"},
	{"RIGHT", "RIGHT"},
}

// TestBlmoveCrossImmediateServeDifferential replays every resolve-at-once move
// shape on both paths across the four direction pairs: co-located source and
// destination on one shard through the point handler, cross-shard source and
// destination on distinct owners through BlmoveCross under DoBlockCross. The reply
// bytes and the post-move LRANGE of both key pairs must agree exactly.
func TestBlmoveCrossImmediateServeDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()

	for ci, tc := range moveImmediateCases {
		for di, d := range moveDirs {
			t.Run(fmt.Sprintf("%s %s%s", tc.name, d.from, d.to), func(t *testing.T) {
				p := fmt.Sprintf("mv%d_%d", ci, di)
				coSrc := keyOnShard(t, rt, 3, p+"cosrc")
				coDst := keyOnShard(t, rt, 3, p+"codst")
				xSrc := keyOnShard(t, rt, 0, p+"xsrc")
				xDst := keyOnShard(t, rt, 1, p+"xdst")
				seedMove(t, c, coSrc, xSrc, tc.srcVals, tc.srcStr)
				seedMove(t, c, coDst, xDst, tc.dstVals, tc.dstStr)

				coRep := do(t, c, bkBlmove, coSrc, coDst, d.from, d.to, "0")
				crossBlmoveFire(t, c, xSrc, xDst, d.from, d.to, "0")
				xRep := drainOne(t, c)
				if !bytes.Equal(coRep, xRep) {
					t.Fatalf("reply drift: co-located %q, cross-shard %q", coRep, xRep)
				}
				eqList(t, c, "source", coSrc, xSrc)
				eqList(t, c, "destination", coDst, xDst)
			})
		}
	}
}

// TestBrpoplpushCrossImmediateServeDifferential is the BRPOPLPUSH arm: the fixed
// RIGHT LEFT move must match the co-located point handler's reply and post-move
// state for a non-empty source and the WRONGTYPE shapes.
func TestBrpoplpushCrossImmediateServeDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()

	for ci, tc := range moveImmediateCases {
		t.Run(tc.name, func(t *testing.T) {
			p := fmt.Sprintf("rp%d", ci)
			coSrc := keyOnShard(t, rt, 3, p+"cosrc")
			coDst := keyOnShard(t, rt, 3, p+"codst")
			xSrc := keyOnShard(t, rt, 0, p+"xsrc")
			xDst := keyOnShard(t, rt, 1, p+"xdst")
			seedMove(t, c, coSrc, xSrc, tc.srcVals, tc.srcStr)
			seedMove(t, c, coDst, xDst, tc.dstVals, tc.dstStr)

			coRep := do(t, c, bkBrpoplpush, coSrc, coDst, "0")
			crossBrpoplpushFire(t, c, xSrc, xDst, "0")
			xRep := drainOne(t, c)
			if !bytes.Equal(coRep, xRep) {
				t.Fatalf("reply drift: co-located %q, cross-shard %q", coRep, xRep)
			}
			eqList(t, c, "source", coSrc, xSrc)
			eqList(t, c, "destination", coDst, xDst)
		})
	}
}

// TestBlmoveCrossErrorsDifferential proves the argument guards answer in place on
// the cross path too, byte-identical to the co-located point handler and with no
// park: an invalid direction token is a syntax error before the timeout or the keys
// are touched, and a negative or non-numeric timeout is a timeout error.
func TestBlmoveCrossErrorsDifferential(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	c := rt.NewConn()
	src := keyOnShard(t, rt, 0, "errsrc")
	dst := keyOnShard(t, rt, 1, "errdst")

	dirCases := []struct{ from, to string }{{"UP", "LEFT"}, {"LEFT", "SIDEWAYS"}}
	for _, d := range dirCases {
		coRep := do(t, c, bkBlmove, src, dst, d.from, d.to, "0")
		crossBlmoveFire(t, c, src, dst, d.from, d.to, "0")
		xRep := drainOne(t, c)
		if !bytes.Equal(coRep, xRep) {
			t.Fatalf("dir %s/%s drift: co-located %q, cross-shard %q", d.from, d.to, coRep, xRep)
		}
	}
	for _, bad := range []string{"-1", "-0.5", "notanumber", "nan"} {
		coRep := do(t, c, bkBlmove, src, dst, "LEFT", "RIGHT", bad)
		crossBlmoveFire(t, c, src, dst, "LEFT", "RIGHT", bad)
		xRep := drainOne(t, c)
		if !bytes.Equal(coRep, xRep) {
			t.Fatalf("timeout %q drift: co-located %q, cross-shard %q", bad, coRep, xRep)
		}
	}
}

// TestBlmoveCrossParkThenServe parks a cross-shard BLMOVE on an empty source, then
// a push on the source owner wakes it: a coordinator reaches the far destination,
// moves the head element to the destination tail, and completes the reply, with the
// source and destination left exactly as a co-located BLMOVE would leave them.
func TestBlmoveCrossParkThenServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	src := keyOnShard(t, rt, 0, "pssrc")
	dst := keyOnShard(t, rt, 1, "psdst")

	crossBlmoveFire(t, a, src, dst, "LEFT", "RIGHT", "0")
	noReply(t, a, 30*time.Millisecond) // source empty, the waiter is parked

	wantInt(t, do(t, b, bkRpush, src, "v1", "v2"), 2)
	wantBulk(t, drainOne(t, a), "v1") // LEFT pops the head
	wantArray(t, do(t, b, bkLrange, src, "0", "-1"), "v2")
	wantArray(t, do(t, b, bkLrange, dst, "0", "-1"), "v1")
}

// TestBrpoplpushCrossParkThenServe parks a cross BRPOPLPUSH and proves the serving
// push moves the source tail to the destination head, the ends the fixed RIGHT LEFT
// move threads through the waiter node to the coordinator.
func TestBrpoplpushCrossParkThenServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	src := keyOnShard(t, rt, 0, "rpsrc")
	dst := keyOnShard(t, rt, 1, "rpdst")

	crossBrpoplpushFire(t, a, src, dst, "0")
	noReply(t, a, 30*time.Millisecond)

	wantInt(t, do(t, b, bkRpush, src, "x", "y", "z"), 3)
	wantBulk(t, drainOne(t, a), "z") // RIGHT pops the tail
	wantArray(t, do(t, b, bkLrange, src, "0", "-1"), "x", "y")
	wantArray(t, do(t, b, bkLrange, dst, "0", "-1"), "z")
}

// TestBlmoveCrossTimeoutNullBulk parks a cross BLMOVE with a finite timeout on an
// empty source and proves the timer on the source owner fires and delivers the RESP2
// null bulk (BLMOVE's timeout form, distinct from BLPOP's null array). A push after
// the timeout stays put: the waiter is gone.
func TestBlmoveCrossTimeoutNullBulk(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	src := keyOnShard(t, rt, 0, "tosrc")
	dst := keyOnShard(t, rt, 1, "todst")

	start := time.Now()
	crossBlmoveFire(t, a, src, dst, "LEFT", "RIGHT", "0.1")
	wantNil(t, drainOne(t, a))
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("timeout fired after %v, want at least ~100ms", elapsed)
	}
	wantInt(t, do(t, b, bkRpush, src, "late"), 1)
	noReply(t, a, 40*time.Millisecond)
	wantArray(t, do(t, b, bkLrange, src, "0", "-1"), "late")
	wantEmptyArray(t, do(t, b, bkLrange, dst, "0", "-1"))
}

// TestBlmoveCrossServeWrongDestination proves the destination type is checked at
// serve, not at park, on the cross path: a BLMOVE parks on an empty source whose
// destination is already a string, and the waking push spawns a coordinator that
// finds the wrong-typed destination, fails the client with WRONGTYPE, and leaves the
// source element in place, exactly as the co-located serveMove does.
func TestBlmoveCrossServeWrongDestination(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	a := rt.NewConn()
	b := rt.NewConn()
	src := keyOnShard(t, rt, 0, "wdsrc")
	dst := keyOnShard(t, rt, 1, "wddst")

	do(t, b, bkSet, dst, "notalist")
	crossBlmoveFire(t, a, src, dst, "LEFT", "RIGHT", "0")
	noReply(t, a, 30*time.Millisecond)

	wantInt(t, do(t, b, bkRpush, src, "v"), 1)
	wantErr(t, drainOne(t, a), wrongType)
	wantArray(t, do(t, b, bkLrange, src, "0", "-1"), "v") // source element kept
}

// keyInRegistry reports whether a key holds a live list in its shard's registry. It
// reads registry(cx).m directly under a one-key barrier, so it tells an absent
// (dropped) key from an empty-but-present one, the distinction LRANGE and LLEN both
// blur to the same empty answer. The registry-hygiene assertions use it to prove a
// serve that empties a destination drops it, matching the co-located lmove.
func keyInRegistry(t *testing.T, rt *shard.Runtime, key string) bool {
	t.Helper()
	tx := rt.Begin([][]byte{[]byte(key)})
	tx.Acquire()
	var present bool
	tx.Do([]byte(key), func(cx *shard.Ctx) {
		present = registry(cx).m[key] != nil
	})
	tx.Release()
	return present
}

// TestBlmoveCrossServeWakesDestinationWaiter proves the moved element serves the
// destination's own blocked client: a BLPOP parks on the destination, a cross
// BLMOVE parks on the source, and the source push moves an element to the
// destination, whose coordinator hop then wakes the BLPOP with that element. The
// mover still gets the moved bulk, and the serve that emptied the destination drops
// it from the registry, so a cross move keeps the same registry hygiene as the
// co-located lmove.
func TestBlmoveCrossServeWakesDestinationWaiter(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	mover := rt.NewConn()
	popper := rt.NewConn()
	b := rt.NewConn()
	src := keyOnShard(t, rt, 0, "wksrc")
	dst := keyOnShard(t, rt, 1, "wkdst")

	park(t, popper, bkBlpop, dst, "0") // BLPOP blocks on the destination
	noReply(t, popper, 30*time.Millisecond)
	crossBlmoveFire(t, mover, src, dst, "LEFT", "RIGHT", "0")
	noReply(t, mover, 30*time.Millisecond)

	wantInt(t, do(t, b, bkRpush, src, "v"), 1)
	wantBulk(t, drainOne(t, mover), "v")        // the move reply
	wantArray(t, drainOne(t, popper), dst, "v") // the BLPOP woken by the moved element
	wantEmptyArray(t, do(t, b, bkLrange, dst, "0", "-1"))
	if keyInRegistry(t, rt, dst) {
		t.Fatalf("destination %q left in the registry after the serve emptied it", dst)
	}
}

// TestLmoveCrossWakesDestinationWaiter is the non-blocking twin of
// TestBlmoveCrossServeWakesDestinationWaiter: a plain cross LMOVE (and RPOPLPUSH)
// whose destination has a client parked on it must wake that client with the moved
// element, exactly as the co-located lmove distinct-key branch and the blocking cross
// move do. Without the serveWaiters hook in LmoveCross's destination hop a BLPOP
// parked on the destination would hang forever after the move pushed onto it. The
// mover still gets its moved bulk, the source drains to empty, and the destination
// the serve emptied is dropped from the registry, matching the co-located lmove.
func TestLmoveCrossWakesDestinationWaiter(t *testing.T) {
	moves := []struct {
		name string
		run  func(t *testing.T, c *shard.Conn, src, dst string) []byte
	}{
		{"LMOVE", func(t *testing.T, c *shard.Conn, src, dst string) []byte {
			return crossMove(t, c, src, dst, true, false) // LEFT source, RIGHT dest
		}},
		{"RPOPLPUSH", func(t *testing.T, c *shard.Conn, src, dst string) []byte {
			return crossRpoplpush(t, c, src, dst)
		}},
	}
	for _, mv := range moves {
		t.Run(mv.name, func(t *testing.T) {
			rt := crossBlockRuntime(t, 4)
			mover := rt.NewConn()
			popper := rt.NewConn()
			b := rt.NewConn()
			src := keyOnShard(t, rt, 0, mv.name+"nbsrc")
			dst := keyOnShard(t, rt, 1, mv.name+"nbdst")

			park(t, popper, bkBlpop, dst, "0") // BLPOP blocks on the empty destination
			noReply(t, popper, 30*time.Millisecond)
			wantInt(t, do(t, b, bkRpush, src, "v"), 1) // source non-empty, the move runs at once

			wantBulk(t, mv.run(t, mover, src, dst), "v") // the move reply
			wantArray(t, drainOne(t, popper), dst, "v")  // the BLPOP woken by the moved element
			wantEmptyArray(t, do(t, b, bkLrange, src, "0", "-1"))
			wantEmptyArray(t, do(t, b, bkLrange, dst, "0", "-1"))
			if keyInRegistry(t, rt, dst) {
				t.Fatalf("destination %q left in the registry after the serve emptied it", dst)
			}
		})
	}
}

// TestBlmoveCrossParkServesFifoAcrossReDrive parks three cross BLMOVE waiters on one
// source, each moving to its own far destination, then one push feeds the source
// three elements. Each waiter must be served exactly once in FIFO order off the head,
// the re-drive the coordinator runs after it commits proving the next waiter behind
// the one it served is picked up.
func TestBlmoveCrossParkServesFifoAcrossReDrive(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	src := keyOnShard(t, rt, 0, "fifosrc")
	const n = 3
	conns := make([]*shard.Conn, n)
	dsts := make([]string, n)
	for i := 0; i < n; i++ {
		conns[i] = rt.NewConn()
		dsts[i] = keyOnShard(t, rt, 1, fmt.Sprintf("fifodst%d", i))
		crossBlmoveFire(t, conns[i], src, dsts[i], "LEFT", "RIGHT", "0")
	}
	noReply(t, conns[0], 30*time.Millisecond) // all parked

	b := rt.NewConn()
	wantInt(t, do(t, b, bkRpush, src, "e0", "e1", "e2"), 3)

	// FIFO: waiter i is served element i off the head, into its own destination.
	for i := 0; i < n; i++ {
		wantBulk(t, drainOne(t, conns[i]), fmt.Sprintf("e%d", i))
		wantArray(t, do(t, b, bkLrange, dsts[i], "0", "-1"), fmt.Sprintf("e%d", i))
	}
	wantEmptyArray(t, do(t, b, bkLrange, src, "0", "-1"))
}

// TestBlmoveCrossTimeoutRaceServe pits a finite timeout against a push arriving at
// about the same moment. Both the timer and the serving push run on the source
// owner, so they serialize: exactly one completes the waiter. The client sees the
// moved bulk or the null bulk, never both, and the source and destination settle to
// match whichever won. Repeated under the race detector this covers the coordinator
// spawn racing the timer cancel.
func TestBlmoveCrossTimeoutRaceServe(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	for iter := 0; iter < 60; iter++ {
		a := rt.NewConn()
		b := rt.NewConn()
		src := keyOnShard(t, rt, 0, fmt.Sprintf("mr%d_s", iter))
		dst := keyOnShard(t, rt, 1, fmt.Sprintf("mr%d_d", iter))

		crossBlmoveFire(t, a, src, dst, "LEFT", "RIGHT", "0.03")
		done := make(chan struct{})
		go func() { pushDrain(b, src, "v"); close(done) }()

		got := decodeReply(t, drainOne(t, a))
		<-done
		switch got {
		case nil:
			// Timeout won: the element stays in the source, the destination is empty.
			wantArray(t, do(t, b, bkLrange, src, "0", "-1"), "v")
			wantEmptyArray(t, do(t, b, bkLrange, dst, "0", "-1"))
		case "v":
			// Serve won: the element moved to the destination, the source is empty.
			wantEmptyArray(t, do(t, b, bkLrange, src, "0", "-1"))
			wantArray(t, do(t, b, bkLrange, dst, "0", "-1"), "v")
		default:
			t.Fatalf("iter %d reply = %v, want null bulk or moved element", iter, render(got))
		}
		noReply(t, a, 5*time.Millisecond) // never a second reply
	}
}

// TestBlmoveCrossConcurrentExactlyOnce parks many cross BLMOVE waiters, each on its
// own source and destination pair spanning shards, then feeds every source one
// element at once. Every waiter must be served exactly once and every element land
// in its destination, with no reply lost or duplicated. Under the race detector this
// exercises many move coordinators acquiring their two owners at contention.
func TestBlmoveCrossConcurrentExactlyOnce(t *testing.T) {
	rt := crossBlockRuntime(t, 4)
	const n = 40
	conns := make([]*shard.Conn, n)
	srcs := make([]string, n)
	dsts := make([]string, n)
	for i := 0; i < n; i++ {
		conns[i] = rt.NewConn()
		srcs[i] = keyOnShard(t, rt, 0, fmt.Sprintf("cc%d_s", i))
		dsts[i] = keyOnShard(t, rt, 1, fmt.Sprintf("cc%d_d", i))
		crossBlmoveFire(t, conns[i], srcs[i], dsts[i], "LEFT", "RIGHT", "0")
	}
	noReply(t, conns[0], 30*time.Millisecond)

	pushers := make([]*shard.Conn, n)
	for i := 0; i < n; i++ {
		pushers[i] = rt.NewConn()
	}
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			pushDrain(pushers[i], srcs[i], fmt.Sprintf("e%d", i))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		want := fmt.Sprintf("e%d", i)
		wantBulk(t, drainOne(t, conns[i]), want)
		noReply(t, conns[i], 2*time.Millisecond) // exactly one delivery
		wantArray(t, do(t, pushers[i], bkLrange, dsts[i], "0", "-1"), want)
		wantEmptyArray(t, do(t, pushers[i], bkLrange, srcs[i], "0", "-1"))
	}
}
