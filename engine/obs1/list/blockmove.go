package list

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// BLMOVE, BRPOPLPUSH, and BLMPOP (spec 2064/f3/13 M3 slice 8), the blocking move
// and blocking multi-pop, built on the PR-4 waiter set. Each is the blocking twin
// of a non-blocking verb: BLMOVE of LMOVE, BRPOPLPUSH of RPOPLPUSH, BLMPOP of
// LMPOP. When the source is already servable the command runs at once through the
// same core its non-blocking twin uses and never touches the waiter set or a
// timer; otherwise it parks the connection through the deferred-reply seam and a
// later serving push or a firing timeout completes the reply at the parked
// sequence. A finite timeout arms one timer at park on the sibling-ring head.
//
// This slice reads the source and destination from the one shard the command
// routed to, the co-located convention LMOVE and LMPOP already ship: BLMOVE and
// BRPOPLPUSH route on the source, BLMPOP on its first key, and every other key is
// read from that owner's registry. A cross-shard move parks and serves on the
// source owner only; the destination hop across shards is a later slice (PR 6),
// recorded here rather than papered over, exactly as pre-slice-7 LMOVE recorded
// its own co-located-only gap.

// Blmove answers BLMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout: the
// blocking LMOVE. The two direction tokens are parsed first, so an invalid one is
// a syntax error before the timeout, the keys, or the waiter set are touched,
// matching Redis's argument order.
func Blmove(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	from, ok1 := parseDir(args[2])
	to, ok2 := parseDir(args[3])
	if !ok1 || !ok2 {
		r.Err(errSyntax)
		return
	}
	blmoveDir(cx, args[0], args[1], from, to, args[4], r)
}

// Brpoplpush answers BRPOPLPUSH source destination timeout, the older spelling of
// BLMOVE source destination RIGHT LEFT: pop the source tail and push the
// destination head, blocking when the source is missing or empty.
func Brpoplpush(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	blmoveDir(cx, args[0], args[1], false, true, args[2], r)
}

// blmoveDir runs the blocking move core: parse the timeout, try an immediate move
// through lmove (which also serves any destination waiters via its own hook), and
// on a missing or empty source park a kindMove waiter on the source with the
// destination end recorded for the deferred push. The destination type is not
// checked at park; Redis checks it only when a serving push wakes the waiter, and
// serveMove reproduces that.
func blmoveDir(cx *shard.Ctx, src, dst []byte, srcLeft, dstLeft bool, timeoutArg []byte, r shard.Reply) {
	timeout, ok := parseTimeout(timeoutArg)
	if !ok {
		r.Err(errTimeoutFloat)
		return
	}
	if timeout < 0 {
		r.Err(errTimeoutNeg)
		return
	}
	g := registry(cx)
	moved, ok, wrong, err := lmove(g, cx, src, dst, srcLeft, dstLeft)
	if wrong {
		r.Err(wrongType)
		return
	}
	if err != nil {
		r.Err(err.Error())
		return
	}
	if ok {
		r.Bulk(moved)
		return
	}
	spec := waitSpec{kind: kindMove, front: srcLeft, dstKey: string(dst), dstLeft: dstLeft}
	head := parkWaiter(g, [][]byte{src}, spec, cx.CurConn(), cx.CurSeq())
	var deadline int64
	if timeout > 0 {
		deadline = cx.NowMs + int64(timeout*1000)
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
	}
	g.wpool.nodes[head].deadline = deadline
	r.Park()
}

// Blmpop answers BLMPOP timeout numkeys key [key ...] <LEFT|RIGHT> [COUNT count],
// the blocking LMPOP. The timeout is parsed first, then the shared numkeys/keys/
// direction/COUNT tail (byte-identical to LMPOP through parseLmpopTail). An
// immediate pop off the first non-empty key runs through the lmpop core; when
// every key is missing or empty the connection parks a kindMpop waiter on every
// key, carrying the pop end and count so a serving push delivers up to count
// elements to this waiter and leaves the rest for the next.
func Blmpop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	timeout, ok := parseTimeout(args[0])
	if !ok {
		r.Err(errTimeoutFloat)
		return
	}
	if timeout < 0 {
		r.Err(errTimeoutNeg)
		return
	}
	keys, front, count, emsg := parseLmpopTail(args[1:])
	if emsg != "" {
		r.Err(emsg)
		return
	}
	g := registry(cx)
	out, ok, wrong, err := lmpop(g, cx, cx.Aux[:0], keys, front, count)
	if wrong {
		r.Err(wrongType)
		return
	}
	if err != nil {
		r.Err(err.Error())
		return
	}
	if ok {
		cx.Aux = out
		r.Raw(out)
		return
	}
	spec := waitSpec{kind: kindMpop, front: front, count: count}
	head := parkWaiter(g, keys, spec, cx.CurConn(), cx.CurSeq())
	if timeout > 0 {
		deadline := cx.NowMs + int64(timeout*1000)
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
	}
	r.Park()
}
