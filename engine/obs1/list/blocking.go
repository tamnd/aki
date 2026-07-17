package list

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// BLPOP and BRPOP (spec 2064/f3/13 M3 slice 8), the first list verbs that can
// block. Each takes one or more keys and a trailing timeout in seconds. When any
// key already holds a non-empty list the command serves at once, exactly like
// LPOP/RPOP on the first non-empty key, and never touches the waiter set or a
// timer. Otherwise it parks the connection on every key through the deferred-
// reply seam: the handler writes no reply now (Reply.Park), and a later serving
// push or a firing timeout delivers the reply at the parked sequence through
// conn.CompleteBlocked. A finite timeout arms one timer at park and cancels it on
// serve (D7). This slice reads every listed key from the one shard the command
// routed to, the co-located convention; a cross-shard multi-key wait is a later
// slice.

const (
	// errTimeoutNeg and errTimeoutFloat are Redis's exact BLPOP timeout errors,
	// verified against redis-server 8.8.0. The float check covers a non-number,
	// NaN, and infinity; the sign check covers a well-formed negative.
	errTimeoutNeg   = "ERR timeout is negative"
	errTimeoutFloat = "ERR timeout is not a float or out of range"
)

// Blpop answers BLPOP key [key ...] timeout: block until the head of a listed
// key can be popped, and reply [key, element].
func Blpop(cx *shard.Ctx, args [][]byte, r shard.Reply) { blockpop(cx, args, r, true) }

// Brpop answers BRPOP key [key ...] timeout: block until the tail of a listed
// key can be popped, and reply [key, element].
func Brpop(cx *shard.Ctx, args [][]byte, r shard.Reply) { blockpop(cx, args, r, false) }

func blockpop(cx *shard.Ctx, args [][]byte, r shard.Reply, front bool) {
	keys := args[:len(args)-1]
	timeout, ok := parseTimeout(args[len(args)-1])
	if !ok {
		r.Err(errTimeoutFloat)
		return
	}
	if timeout < 0 {
		r.Err(errTimeoutNeg)
		return
	}
	g := registry(cx)

	// Immediate serve: the first listed key that holds a non-empty list is
	// popped and returned, and a wrong-typed key reached before any poppable one
	// aborts with WRONGTYPE, the same order Redis probes the keys in.
	for _, key := range keys {
		l, wrong := g.lookup(cx, key)
		if wrong {
			r.Err(wrongType)
			return
		}
		if l == nil || l.length() == 0 {
			continue
		}
		elem := popOne(l, front)
		dropped := l.length() == 0
		if dropped {
			g.drop(key)
		} else {
			g.note(l)
		}
		if err := cx.LogListPop(key, front, 1, dropped); err != nil {
			r.Err(err.Error())
			return
		}
		out := appendReply(cx.Aux[:0], key, elem)
		cx.Aux = out
		r.Raw(out)
		return
	}

	// Park on every key. A finite timeout arms one timer on the sibling-ring
	// head; a zero timeout blocks forever and arms nothing.
	head := parkWaiter(g, keys, waitSpec{kind: kindPop, front: front}, cx.CurConn(), cx.CurSeq())
	if timeout > 0 {
		deadline := cx.NowMs + int64(timeout*1000)
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
	}
	r.Park()
}

// makeFire builds the timeout callback for the waiter whose sibling-ring head is
// head. It runs on the owner when the deadline passes with no serving push. The
// live guard makes it idempotent against a serve that already tore the waiter
// down: a served waiter always cancels this timer first, so a live head here is
// this waiter's own still-parked node. The timeout reply shape is the waiter's:
// BLMOVE/BRPOPLPUSH time out to the RESP2 null bulk ($-1), the shape Reply.Null
// emits, and BLPOP/BRPOP/BLMPOP to the RESP2 null array (*-1).
func makeFire(g *reg, head uint32) func(*shard.Ctx) {
	return func(cx *shard.Ctx) {
		nd := &g.wpool.nodes[head]
		if !nd.live {
			return
		}
		conn := nd.conn
		seq := nd.seq
		kind := nd.kind
		nd.timer = nil // the firing timer is off the heap already
		g.unlinkAll(cx, head)
		if kind == kindMove {
			conn.CompleteBlocked(seq, resp.AppendNull(nil))
		} else {
			conn.CompleteBlocked(seq, resp.AppendNullArray(nil))
		}
	}
}

// appendReply appends the [key, element] two-bulk array a served BLPOP/BRPOP
// returns, the reply shape Redis sends both on an immediate serve and on a
// deferred wake.
func appendReply(dst, key, elem []byte) []byte {
	dst = resp.AppendArrayHeader(dst, 2)
	dst = resp.AppendBulk(dst, key)
	dst = resp.AppendBulk(dst, elem)
	return dst
}

// parseTimeout parses the trailing timeout argument as a double in seconds, the
// same grammar Redis's getTimeoutFromObject accepts: a plain or fractional
// number, rejecting a non-number, NaN, and infinity. It does not range-check the
// sign; the caller does, so the two error texts stay separate.
func parseTimeout(b []byte) (float64, bool) {
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}
