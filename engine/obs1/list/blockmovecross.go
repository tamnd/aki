package list

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// Cross-shard blocking moves (spec 2064/f3/13 M3 slice 8, PR 6): BLMOVE and
// BRPOPLPUSH whose source and destination land on different owners. The co-located
// forms (blockmove.go) read both keys from the one shard the command routed to and
// serve a waking push inline; this file handles the split case the dispatcher
// sends through DoBlockCross with an intent on both keys.
//
// A move is not a pop. Where a cross BLPOP parks a claim shared across many owners
// (blockcross.go), a cross BLMOVE blocks on exactly one key, the source, so it
// needs no claim: the single source owner still serializes a serving push against
// the timeout, D2 as before. What is new is the serve. The source owner cannot
// push onto a destination it does not own, so when a push finally wakes the waiter
// the source owner cannot finish the move alone. It cancels the timer, marks the
// node serving (waiter.go serveMoveRemote), and spawns a coordinator that acquires
// both keys and runs the peek-push-pop under a fresh barrier, exactly the plan the
// non-blocking LmoveCross runs, then completes the parked reply.
//
// The one race worth stating plainly is the gap between the source owner deciding
// to serve and the spawned coordinator acquiring the source. In that gap another
// command on the source owner can drain the source (an LPOP), so the coordinator
// must re-check under the barrier: if the source is empty again it re-parks the
// same node, re-arming the timer it cancels at serve from the node's stored
// deadline, and the waiter blocks on. Cancelling the timer at serve is what keeps
// the timeout from firing during that gap and completing the reply a second time;
// the serving flag keeps a second push from spawning a duplicate coordinator. The
// two together make the serve exactly-once.

// moveCrossOnce runs the barrier-held peek(src) then push(dst) then pop(src) move
// for a BLMOVE/BRPOPLPUSH whose keys span shards, and wakes the destination's own
// blocked waiters after the push (parity with the co-located lmove, whose hook
// serves them; a destination with no waiters makes serveWaiters a no-op). Both
// intents are held by t. It returns the finished reply and a sourceEmpty flag: a
// missing or empty source moves nothing and replies neither, leaving the caller to
// park. The element is cloned off the source owner before it is carried to the
// destination hop, since it aliases the source's chunk storage; the source pop runs
// last so the transient cross-shard state is element-in-both, never the phantom
// element-in-neither the L15 lesson warns against.
func moveCrossOnce(t *shard.Txn, src, dst []byte, srcLeft, dstLeft bool) (rep []byte, sourceEmpty bool) {
	var srcWrong, have bool
	var elem []byte
	t.Do(src, func(cx *shard.Ctx) {
		s, w := registry(cx).lookup(cx, src)
		if w {
			srcWrong = true
			return
		}
		if s == nil || s.length() == 0 {
			return
		}
		elem = cloneBytes(peekEnd(s, srcLeft))
		have = true
	})
	if srcWrong {
		return resp.AppendError(nil, wrongType), false
	}
	if !have {
		return nil, true
	}
	var dstWrong bool
	t.Do(dst, func(cx *shard.Ctx) {
		g := registry(cx)
		d, w := g.lookup(cx, dst)
		if w {
			dstWrong = true
			return
		}
		if d == nil {
			d = newList()
			g.m[string(dst)] = d
		}
		pushEnd(d, elem, dstLeft)
		serveWaiters(cx, g, dst, d)
		// A serve that consumed the moved element leaves the destination empty, so
		// drop it, matching the co-located lmove (lmove.go) so EXISTS/TYPE/OBJECT
		// ENCODING stay in step. g.drop touches only the list index; waiters still
		// parked on dst live in the separate waiter set and stay for a future push.
		if d.length() == 0 {
			g.drop(dst)
		} else {
			g.note(d)
		}
	})
	if dstWrong {
		return resp.AppendError(nil, wrongType), false
	}
	t.Do(src, func(cx *shard.Ctx) {
		g := registry(cx)
		s, _ := g.lookup(cx, src)
		popEnd(s, srcLeft)
		if s.length() == 0 {
			g.drop(src)
		} else {
			g.note(s)
		}
	})
	return resp.AppendBulk(nil, elem), false
}

// BlmoveCross answers a cross-shard BLMOVE through DoBlockCross: a is the argument
// tail, source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout. The two direction
// tokens parse first so an invalid one is a syntax error before the timeout or the
// keys are touched, the order Redis and the co-located Blmove use.
func BlmoveCross(t *shard.Txn, conn *shard.Conn, seq uint32, a [][]byte) []byte {
	from, ok1 := parseDir(a[2])
	to, ok2 := parseDir(a[3])
	if !ok1 || !ok2 {
		return resp.AppendError(nil, errSyntax)
	}
	return blockMoveCross(t, conn, seq, a[0], a[1], from, to, a[4])
}

// BrpoplpushCross answers a cross-shard BRPOPLPUSH: a is source destination
// timeout, the older spelling of BLMOVE source destination RIGHT LEFT.
func BrpoplpushCross(t *shard.Txn, conn *shard.Conn, seq uint32, a [][]byte) []byte {
	return blockMoveCross(t, conn, seq, a[0], a[1], false, true, a[2])
}

// blockMoveCross runs the cross-shard blocking move core: parse the timeout, try an
// immediate move under the barrier, and on a missing or empty source park a
// kindMove waiter on the source only, with the destination end recorded for the
// serve-time push and the deadline recorded for a possible re-arm. It returns nil
// to leave the reply open for a serving push or the timeout. The destination type
// is not checked at park, only when a push wakes the waiter, the way Redis defers
// it and serveMoveRemote's coordinator reproduces.
func blockMoveCross(t *shard.Txn, conn *shard.Conn, seq uint32, src, dst []byte, srcLeft, dstLeft bool, timeoutArg []byte) []byte {
	timeout, ok := parseTimeout(timeoutArg)
	if !ok {
		return resp.AppendError(nil, errTimeoutFloat)
	}
	if timeout < 0 {
		return resp.AppendError(nil, errTimeoutNeg)
	}
	rep, empty := moveCrossOnce(t, src, dst, srcLeft, dstLeft)
	if !empty {
		return rep
	}
	t.Do(src, func(cx *shard.Ctx) {
		g := registry(cx)
		spec := waitSpec{kind: kindMove, front: srcLeft, dstKey: string(dst), dstLeft: dstLeft}
		head := parkWaiter(g, [][]byte{src}, spec, conn, seq)
		var deadline int64
		if timeout > 0 {
			deadline = cx.NowMs + int64(timeout*1000)
			g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
		}
		g.wpool.nodes[head].deadline = deadline
	})
	return nil
}

// runMoveCross is the coordinator serveMoveRemote spawns for a cross BLMOVE whose
// destination is on another shard. It acquires the source and destination and runs
// the move under the barrier, then completes the parked reply. It is the deferred
// twin of moveCrossOnce with the waiter lifecycle folded in: the first source hop
// re-checks the node is still the serving waiter and the source still has an
// element, the destination hop pushes and wakes the destination's own waiters, and
// the final source hop pops, unlinks the node, and re-drives the source so the next
// waiter behind this one is served. head is the node index on the source owner;
// deadline is its stored timeout instant, re-armed if the source drained in the
// window between the serve decision and this acquisition.
func runMoveCross(rt *shard.Runtime, src, dst []byte, head uint32, srcLeft, dstLeft bool, deadline int64) {
	tx := rt.Begin([][]byte{src, dst})
	tx.Acquire()

	var elem []byte
	var conn *shard.Conn
	var seq uint32
	proceed := false
	tx.Do(src, func(cx *shard.Ctx) {
		g := registry(cx)
		if int(head) >= len(g.wpool.nodes) {
			return
		}
		nd := &g.wpool.nodes[head]
		if !nd.live || !nd.serving {
			// The node was torn down or handed to another path; nothing to serve.
			return
		}
		s, w := g.lookup(cx, src)
		if w || s == nil || s.length() == 0 {
			// The source drained (or turned non-list) in the window since the serve
			// decision: keep the waiter parked and re-arm the timer cancelled at serve.
			nd.serving = false
			if deadline > 0 {
				nd.timer = cx.ArmTimer(deadline, makeFire(g, head))
			}
			return
		}
		elem = cloneBytes(peekEnd(s, srcLeft))
		conn = nd.conn
		seq = nd.seq
		proceed = true
	})
	if !proceed {
		tx.Release()
		return
	}

	var dstWrong bool
	tx.Do(dst, func(cx *shard.Ctx) {
		g := registry(cx)
		d, w := g.lookup(cx, dst)
		if w {
			dstWrong = true
			return
		}
		if d == nil {
			d = newList()
			g.m[string(dst)] = d
		}
		pushEnd(d, elem, dstLeft)
		serveWaiters(cx, g, dst, d)
		// A serve that consumed the moved element leaves the destination empty, so
		// drop it, matching the co-located lmove (lmove.go) so EXISTS/TYPE/OBJECT
		// ENCODING stay in step. g.drop touches only the list index; waiters still
		// parked on dst live in the separate waiter set and stay for a future push.
		if d.length() == 0 {
			g.drop(dst)
		} else {
			g.note(d)
		}
	})

	var rep []byte
	tx.Do(src, func(cx *shard.Ctx) {
		g := registry(cx)
		if dstWrong {
			// A wrong-typed destination fails the client and leaves the source element
			// in place, exactly as serveMove does at the co-located serve.
			g.unlinkAll(cx, head)
			rep = resp.AppendError(nil, wrongType)
		} else {
			s, _ := g.lookup(cx, src)
			popEnd(s, srcLeft)
			if s.length() == 0 {
				g.drop(src)
			}
			g.unlinkAll(cx, head)
			rep = resp.AppendBulk(nil, elem)
		}
		if s, _ := g.lookup(cx, src); s != nil && s.length() > 0 {
			// The served head is gone; drive the next waiter behind it off whatever the
			// source still holds, the re-drive serveKey deferred when it stopped for the
			// spawned coordinator. That re-drive can drain the source further, so
			// reconcile its footprint once after: drop it if the deferred waiters emptied
			// it, otherwise note the settled byte count.
			serveWaiters(cx, g, src, s)
			if s.length() == 0 {
				g.drop(src)
			} else {
				g.note(s)
			}
		}
	})
	tx.Release()
	conn.CompleteBlocked(seq, rep)
}
