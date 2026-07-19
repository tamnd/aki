package zset

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// BZPOPMIN, BZPOPMAX, and BZMPOP (spec 2064/f3/12 section 6.7), the blocking
// sorted-set pops, the zset twins of the list's BLPOP/BRPOP/BLMPOP. Each takes one
// or more keys and blocks until a member can be popped from one of them. When any
// key already holds a non-empty zset the command serves at once, exactly like
// ZPOPMIN/ZPOPMAX/ZMPOP on the first non-empty key, and never touches the waiter set
// or a timer. Otherwise it parks the connection on every key through the
// deferred-reply seam (Reply.Park), and a later serving ZADD/ZINCRBY (or a firing
// timeout) delivers the reply at the parked sequence through conn.CompleteBlocked.
// A finite timeout arms one timer at park and cancels it on serve. This slice reads
// every listed key from the one shard the command routed to, the co-located
// convention the zset pops already keep; a cross-shard multi-key wait is a later
// slice (the F17 intent route BLPOP took in its own follow-on).

const (
	// errTimeoutNeg and errTimeoutFloat are Redis's exact blocking-timeout errors,
	// the same texts the list blocking verbs answer. The float check covers a
	// non-number, NaN, and infinity; the sign check covers a well-formed negative.
	errTimeoutNeg   = "ERR timeout is negative"
	errTimeoutFloat = "ERR timeout is not a float or out of range"
)

// Bzpopmin answers BZPOPMIN key [key ...] timeout: block until a member can be
// popped from the lowest-scored end of a listed key, and reply [key, member,
// score].
func Bzpopmin(cx *shard.Ctx, args [][]byte, r shard.Reply) { bzpop(cx, args, r, true) }

// Bzpopmax answers BZPOPMAX key [key ...] timeout: the highest-scored-end twin.
func Bzpopmax(cx *shard.Ctx, args [][]byte, r shard.Reply) { bzpop(cx, args, r, false) }

func bzpop(cx *shard.Ctx, args [][]byte, r shard.Reply, min bool) {
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

	// Immediate serve: the first listed key that holds a non-empty zset is popped
	// and returned, and a wrong-typed key reached before any poppable one aborts
	// with WRONGTYPE, the same order Redis probes the keys in.
	for _, key := range keys {
		z, wrong := g.lookup(cx, key)
		if wrong {
			r.Err(wrongType)
			return
		}
		if z == nil || z.card() == 0 {
			continue
		}
		out := resp.AppendArrayHeader(cx.Aux[:0], 3)
		out = resp.AppendBulk(out, key)
		var sc [40]byte
		z.pop(min, 1, func(m []byte, s float64) {
			logRemove(cx, key, m)
			out = resp.AppendBulk(out, m)
			out = resp.AppendBulk(out, resp.FormatScore(sc[:0], s))
		})
		if z.card() == 0 {
			g.drop(key)
		} else {
			g.note(z)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}

	if cx.ExecNoBlock() {
		// Inside EXEC a blocking pop never parks; it answers the would-block
		// reply, the RESP2 null array BZPOPMIN/BZPOPMAX time out to.
		r.Raw(resp.AppendNullArray(nil))
		return
	}

	// Park on every key. A finite timeout arms one timer on the sibling-ring head;
	// a zero timeout blocks forever and arms nothing.
	head := parkWaiter(g, keys, waitSpec{kind: kindPop, min: min}, cx.CurConn(), cx.CurSeq())
	if timeout > 0 {
		deadline := cx.NowMs + int64(timeout*1000)
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
	}
	r.Park()
}

// Bzmpop answers BZMPOP timeout numkeys key [key ...] <MIN|MAX> [COUNT count]: the
// blocking ZMPOP. It leads with a timeout, then a numkeys-counted key list, then
// the MIN|MAX end and an optional COUNT, and pops up to count members off the named
// end of the first non-empty key. On a serve it replies [key, [[member, score],
// ...]]; parked, it wakes on the first listed key a later ZADD grows.
func Bzmpop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	timeout, ok := parseTimeout(args[0])
	if !ok {
		r.Err(errTimeoutFloat)
		return
	}
	if timeout < 0 {
		r.Err(errTimeoutNeg)
		return
	}
	numkeys, okn := parseIndex(args[1])
	if !okn || numkeys <= 0 {
		r.Err("ERR numkeys should be greater than 0")
		return
	}
	// After timeout and numkeys come exactly numkeys keys, then MIN|MAX, then the
	// optional COUNT.
	if len(args) < 2+numkeys+1 {
		r.Err("ERR syntax error")
		return
	}
	keys := args[2 : 2+numkeys]
	tail := args[2+numkeys:]
	var min bool
	switch {
	case eqFold(tail[0], "MIN"):
		min = true
	case eqFold(tail[0], "MAX"):
		min = false
	default:
		r.Err("ERR syntax error")
		return
	}
	count := 1
	rest := tail[1:]
	switch len(rest) {
	case 0:
	case 2:
		if !eqFold(rest[0], "COUNT") {
			r.Err("ERR syntax error")
			return
		}
		c, okc := parseIndex(rest[1])
		if !okc || c <= 0 {
			r.Err("ERR count should be greater than 0")
			return
		}
		count = c
	default:
		r.Err("ERR syntax error")
		return
	}

	g := registry(cx)
	for _, key := range keys {
		z, wrong := g.lookup(cx, key)
		if wrong {
			r.Err(wrongType)
			return
		}
		if z == nil || z.card() == 0 {
			continue
		}
		npop := count
		if npop > z.card() {
			npop = z.card()
		}
		out := resp.AppendArrayHeader(cx.Aux[:0], 2)
		out = resp.AppendBulk(out, key)
		out = resp.AppendArrayHeader(out, npop)
		var sc [40]byte
		z.pop(min, count, func(m []byte, s float64) {
			logRemove(cx, key, m)
			out = resp.AppendArrayHeader(out, 2)
			out = resp.AppendBulk(out, m)
			out = resp.AppendBulk(out, resp.FormatScore(sc[:0], s))
		})
		if z.card() == 0 {
			g.drop(key)
		} else {
			g.note(z)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}

	if cx.ExecNoBlock() {
		// Inside EXEC a blocking pop never parks; it answers the would-block
		// reply, the RESP2 null array BZMPOP times out to.
		r.Raw(resp.AppendNullArray(nil))
		return
	}

	head := parkWaiter(g, keys, waitSpec{kind: kindMpop, min: min, count: count}, cx.CurConn(), cx.CurSeq())
	if timeout > 0 {
		deadline := cx.NowMs + int64(timeout*1000)
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeFire(g, head))
	}
	r.Park()
}

// makeFire builds the timeout callback for the waiter whose sibling-ring head is
// head. It runs on the owner when the deadline passes with no serving push. The
// live guard makes it idempotent against a serve that already tore the waiter down:
// a served waiter always cancels this timer first, so a live head here is this
// waiter's own still-parked node. Both blocking-pop shapes time out to the RESP2
// null array (*-1), the shape Redis sends BZPOPMIN/BZPOPMAX/BZMPOP on timeout.
func makeFire(g *reg, head uint32) func(*shard.Ctx) {
	return func(cx *shard.Ctx) {
		nd := &g.wpool.nodes[head]
		if !nd.live {
			return
		}
		conn := nd.conn
		seq := nd.seq
		nd.timer = nil // the firing timer is off the heap already
		g.unlinkAll(cx, head)
		conn.CompleteBlocked(seq, resp.AppendNullArray(nil))
	}
}

// parseTimeout parses a blocking verb's timeout argument as a double in seconds,
// the same grammar Redis's getTimeoutFromObject accepts: a plain or fractional
// number, rejecting a non-number, NaN, and infinity. It does not range-check the
// sign; the caller does, so the two error texts stay separate.
func parseTimeout(b []byte) (float64, bool) {
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}
