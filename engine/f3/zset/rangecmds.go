package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The range-by-bound command surface (spec 2064/f3/12 sections 6.4, 6.5):
// ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZCOUNT,
// ZLEXCOUNT, and the ZRANGE BYSCORE/BYLEX forms. Every one resolves its bounds
// to a forward-rank window with two counted descents, then streams that window
// with the seek-and-walk machinery the index range already uses (skiplist.go
// walkRange/walkRangeRev), so a far range is a seek plus a bounded walk and
// ZCOUNT is pure count arithmetic. Replies are built in the shard scratch and
// handed over whole through Reply.Raw, the one-pass shape the rest of the zset
// surface uses.

const (
	errScoreBound = "ERR min or max is not a float"
	errLexBound   = "ERR min or max not valid string range item"
	errLimitOnly  = "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
	errLexScores  = "ERR syntax error, WITHSCORES not supported in combination with BYLEX"
	errNotInt     = "ERR value is not an integer or out of range"
)

// Zrangebyscore answers ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset
// count]: the ascending score band, streamed.
func Zrangebyscore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	zrangebyscoreImpl(cx, args, r, false)
}

// Zrevrangebyscore answers ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT
// offset count]: the same band high-to-low, bounds given max first. A low-first
// call intersects to nothing and returns an empty array, matching Redis.
func Zrevrangebyscore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	zrangebyscoreImpl(cx, args, r, true)
}

func zrangebyscoreImpl(cx *shard.Ctx, args [][]byte, r shard.Reply, rev bool) {
	minArg, maxArg := args[1], args[2]
	if rev {
		minArg, maxArg = args[2], args[1]
	}
	min, ok1 := parseScoreBound(minArg)
	max, ok2 := parseScoreBound(maxArg)
	if !ok1 || !ok2 {
		r.Err(errScoreBound)
		return
	}
	ws, limit, offset, count, errMsg := parseRangeOpts(args[3:], true)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	execByScore(cx, r, args[0], min, max, rev, ws, limit, offset, count)
}

// Zrangebylex answers ZRANGEBYLEX key min max [LIMIT offset count]: the lex band
// at equal scores (section 3.2). WITHSCORES is not a legal option here.
func Zrangebylex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	zrangebylexImpl(cx, args, r, false)
}

// Zrevrangebylex answers ZREVRANGEBYLEX key max min [LIMIT offset count]: the
// same band high-to-low, bounds given max first.
func Zrevrangebylex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	zrangebylexImpl(cx, args, r, true)
}

func zrangebylexImpl(cx *shard.Ctx, args [][]byte, r shard.Reply, rev bool) {
	minArg, maxArg := args[1], args[2]
	if rev {
		minArg, maxArg = args[2], args[1]
	}
	min, ok1 := parseLexBound(minArg)
	max, ok2 := parseLexBound(maxArg)
	if !ok1 || !ok2 {
		r.Err(errLexBound)
		return
	}
	_, limit, offset, count, errMsg := parseRangeOpts(args[3:], false)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	execByLex(cx, r, args[0], min, max, rev, limit, offset, count)
}

// Zcount answers ZCOUNT key min max: the number of members in the score band,
// two counted descents and no walk (section 6.4).
func Zcount(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	min, ok1 := parseScoreBound(args[1])
	max, ok2 := parseScoreBound(args[2])
	if !ok1 || !ok2 {
		r.Err(errScoreBound)
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Int(0)
		return
	}
	lo, hiExcl := z.scoreWindow(min, max)
	r.Int(int64(hiExcl - lo))
}

// Zlexcount answers ZLEXCOUNT key min max: the number of members in the lex
// band, the same count arithmetic on the tie-broken tree (section 6.4).
func Zlexcount(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	min, ok1 := parseLexBound(args[1])
	max, ok2 := parseLexBound(args[2])
	if !ok1 || !ok2 {
		r.Err(errLexBound)
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Int(0)
		return
	}
	lo, hiExcl := z.lexWindow(min, max)
	r.Int(int64(hiExcl - lo))
}

// execByScore resolves the score band to a rank window, applies LIMIT, and
// streams the window (ascending, or descending when rev).
func execByScore(cx *shard.Ctx, r shard.Reply, key []byte, min, max scoreBound, rev, ws, limit bool, offset, count int) {
	g := registry(cx)
	z, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	lo, hiExcl := z.scoreWindow(min, max)
	streamWindow(cx, r, z, lo, hiExcl, rev, ws, limit, offset, count)
}

// execByLex resolves the lex band to a rank window and streams it. Lex ranges
// never carry scores.
func execByLex(cx *shard.Ctx, r shard.Reply, key []byte, min, max lexBound, rev, limit bool, offset, count int) {
	g := registry(cx)
	z, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	lo, hiExcl := z.lexWindow(min, max)
	streamWindow(cx, r, z, lo, hiExcl, rev, false, limit, offset, count)
}

// streamWindow applies LIMIT to the forward-rank window [lo, hiExcl), sizes the
// array header from the resulting count (F19), and streams the elements into the
// shard scratch. It is the shared tail of every by-bound range.
func streamWindow(cx *shard.Ctx, r shard.Reply, z *zset, lo, hiExcl int, rev, ws, limit bool, offset, count int) {
	a, b, empty := applyLimit(lo, hiExcl, rev, limit, offset, count)
	if empty {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	n := b - a + 1
	if ws {
		n *= 2
	}
	out := z.rangeByRankWindow(resp.AppendArrayHeader(cx.Aux[:0], n), a, b, rev, ws)
	cx.Aux = out
	r.Raw(out)
}

// applyLimit turns the forward-rank window [lo, hiExcl) into the inclusive rank
// window [a, b] to emit, honoring LIMIT offset/count and the emit direction. A
// negative offset or a zero count yields an empty result, and a negative count
// means every element from the offset, both matching Redis. Forward ranges skip
// offset from the low end; reverse ranges skip it from the high end, since the
// offset counts along the reply's own order.
func applyLimit(lo, hiExcl int, rev, limit bool, offset, count int) (a, b int, empty bool) {
	if hiExcl <= lo {
		return 0, 0, true
	}
	if !limit {
		return lo, hiExcl - 1, false
	}
	if offset < 0 {
		return 0, 0, true
	}
	if rev {
		top := hiExcl - 1 - offset
		if top < lo {
			return 0, 0, true
		}
		bottom := lo
		if count >= 0 {
			bottom = top - count + 1
			if bottom < lo {
				bottom = lo
			}
		}
		if bottom > top {
			return 0, 0, true
		}
		return bottom, top, false
	}
	start := lo + offset
	if start >= hiExcl {
		return 0, 0, true
	}
	end := hiExcl - 1
	if count >= 0 {
		end = start + count - 1
		if end >= hiExcl {
			end = hiExcl - 1
		}
	}
	if end < start {
		return 0, 0, true
	}
	return start, end, false
}

// parseRangeOpts reads the trailing [WITHSCORES] [LIMIT offset count] options a
// by-bound range command takes, in any order. withScoresOK is false for the lex
// commands, where WITHSCORES is a syntax error. It returns the parsed options
// and a non-empty Redis error string on a malformed tail.
func parseRangeOpts(tail [][]byte, withScoresOK bool) (ws, limit bool, offset, count int, errMsg string) {
	for i := 0; i < len(tail); {
		switch {
		case withScoresOK && eqFold(tail[i], "WITHSCORES"):
			ws = true
			i++
		case eqFold(tail[i], "LIMIT"):
			if i+2 >= len(tail) {
				return false, false, 0, 0, "ERR syntax error"
			}
			o, ok1 := parseIndex(tail[i+1])
			c, ok2 := parseIndex(tail[i+2])
			if !ok1 || !ok2 {
				return false, false, 0, 0, errNotInt
			}
			offset, count, limit = o, c, true
			i += 3
		default:
			return false, false, 0, 0, "ERR syntax error"
		}
	}
	return ws, limit, offset, count, ""
}
