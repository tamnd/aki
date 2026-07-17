package zset

// ZRANGESTORE writes a ZRANGE selection into a destination sorted set (section
// 6.4). It runs the same index, score, and lex selection the read path does, but
// instead of framing a reply it materializes the selected members with their
// original source scores as a fresh sorted set, the way ZDIFFSTORE and
// GEOSEARCHSTORE materialize theirs. WITHSCORES is not part of the grammar: the
// destination is always a sorted set, so the scores travel regardless.
//
// Destination and source are two keys. The co-located case reads the source and
// writes the destination on one owner through the registry; a destination and
// source that span shards take the F17 intent route, the selection on the
// source's owner and the placement on the destination's, both inside the
// transaction that holds the two keys.

import (
	"math"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// collectIndexWindow gathers the inclusive index window [lo,hi] of the forward or
// reversed sequence as member/score pairs, the ZRANGESTORE counterpart to
// rangeByIndex. The members alias live storage until buildDest copies them.
func (z *zset) collectIndexWindow(lo, hi int, rev bool) []scoredMember {
	if hi < lo {
		return nil
	}
	out := make([]scoredMember, 0, hi-lo+1)
	emit := func(m []byte, bits uint64) {
		out = append(out, scoredMember{member: m, score: math.Float64frombits(bits)})
	}
	if z.enc == encSkiplist {
		if rev {
			// The window indexes the reversed sequence, so [lo,hi] maps to the
			// forward-rank window [card-1-hi, card-1-lo], the same remap rangeByIndex
			// makes.
			card := z.nat.card()
			z.nat.walkRangeRev(card-1-hi, card-1-lo, emit)
		} else {
			z.nat.walkRange(lo, hi, emit)
		}
		return out
	}
	ev := z.entries()
	if rev {
		reverse(ev)
	}
	for j := lo; j <= hi; j++ {
		emit(ev[j].member, math.Float64bits(ev[j].score))
	}
	return out
}

// collectRankWindow gathers the inclusive forward-rank window [a,hi] as
// member/score pairs, the ZRANGESTORE counterpart to rangeByRankWindow. The stored
// order comes from buildDest's sort, so the walk is always forward: REV and LIMIT
// have already chosen which ranks land in [a,hi].
func (z *zset) collectRankWindow(a, hi int) []scoredMember {
	if hi < a {
		return nil
	}
	out := make([]scoredMember, 0, hi-a+1)
	emit := func(m []byte, bits uint64) {
		out = append(out, scoredMember{member: m, score: math.Float64frombits(bits)})
	}
	if z.enc == encSkiplist {
		z.nat.walkRange(a, hi, emit)
		return out
	}
	ev := z.entries()
	for j := a; j <= hi; j++ {
		emit(ev[j].member, math.Float64bits(ev[j].score))
	}
	return out
}

// zrangeStoreResult parses the ZRANGESTORE selection (args is destination,
// source, min, max, then the ZRANGE option tail minus WITHSCORES) against the
// source registry g and returns the selected members with their original scores,
// score-sorted for buildDest, or a Redis error string. A missing source yields nil
// pairs, which place turns into a destination delete. The parse order mirrors
// ZRANGE: an option syntax error outranks a bound-parse error, which outranks the
// wrong-type source.
func zrangeStoreResult(g *reg, cx *shard.Ctx, args [][]byte) ([]scoredMember, string) {
	var byScore, byLex, rev, limit bool
	var offset, count int
	for i := 4; i < len(args); {
		switch {
		case eqFold(args[i], "BYSCORE"):
			byScore = true
			i++
		case eqFold(args[i], "BYLEX"):
			byLex = true
			i++
		case eqFold(args[i], "REV"):
			rev = true
			i++
		case eqFold(args[i], "LIMIT"):
			if i+2 >= len(args) {
				return nil, "ERR syntax error"
			}
			o, ok1 := parseIndex(args[i+1])
			c, ok2 := parseIndex(args[i+2])
			if !ok1 || !ok2 {
				return nil, errNotInt
			}
			offset, count, limit = o, c, true
			i += 3
		default:
			return nil, "ERR syntax error"
		}
	}
	if byScore && byLex {
		return nil, "ERR syntax error"
	}
	if limit && !byScore && !byLex {
		return nil, errLimitOnly
	}

	src, loArg, hiArg := args[1], args[2], args[3]
	switch {
	case byScore:
		lo, hi := loArg, hiArg
		if rev {
			lo, hi = hiArg, loArg
		}
		min, ok1 := parseScoreBound(lo)
		max, ok2 := parseScoreBound(hi)
		if !ok1 || !ok2 {
			return nil, errScoreBound
		}
		z, wrong := g.lookup(cx, src)
		if wrong {
			return nil, wrongType
		}
		if z == nil {
			return nil, ""
		}
		lo2, hiExcl := z.scoreWindow(min, max)
		a, b, empty := applyLimit(lo2, hiExcl, rev, limit, offset, count)
		if empty {
			return nil, ""
		}
		return sorted(z.collectRankWindow(a, b)), ""
	case byLex:
		lo, hi := loArg, hiArg
		if rev {
			lo, hi = hiArg, loArg
		}
		min, ok1 := parseLexBound(lo)
		max, ok2 := parseLexBound(hi)
		if !ok1 || !ok2 {
			return nil, errLexBound
		}
		z, wrong := g.lookup(cx, src)
		if wrong {
			return nil, wrongType
		}
		if z == nil {
			return nil, ""
		}
		lo2, hiExcl := z.lexWindow(min, max)
		a, b, empty := applyLimit(lo2, hiExcl, rev, limit, offset, count)
		if empty {
			return nil, ""
		}
		return sorted(z.collectRankWindow(a, b)), ""
	default:
		start, ok1 := parseIndex(loArg)
		stop, ok2 := parseIndex(hiArg)
		if !ok1 || !ok2 {
			return nil, errNotInt
		}
		z, wrong := g.lookup(cx, src)
		if wrong {
			return nil, wrongType
		}
		if z == nil {
			return nil, ""
		}
		lo, hi, empty := clampRange(start, stop, z.card())
		if empty {
			return nil, ""
		}
		return sorted(z.collectIndexWindow(lo, hi, rev)), ""
	}
}

// sorted score-sorts the selected pairs in place for buildDest and returns them.
func sorted(pairs []scoredMember) []scoredMember {
	sortByScore(pairs)
	return pairs
}

// Zrangestore answers ZRANGESTORE destination source min max [BYSCORE|BYLEX] [REV]
// [LIMIT offset count] on co-located keys: select from the source, materialize the
// selection into destination, and reply the stored cardinality. An empty selection
// deletes the destination and replies zero.
func Zrangestore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	pairs, errMsg := zrangeStoreResult(g, cx, args)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	r.Int(int64(place(cx, g, args[0], buildDest(pairs))))
}

// ZrangestoreCross is the F17 route for a destination and source on different
// shards: select on the source's owner, then place the result on the
// destination's owner, both under the intent transaction that holds the two keys.
func ZrangestoreCross(t *shard.Txn, args [][]byte) []byte {
	dest, src := args[0], args[1]
	var (
		pairs  []scoredMember
		errMsg string
	)
	t.Do(src, func(cx *shard.Ctx) {
		pairs, errMsg = zrangeStoreResult(registry(cx), cx, args)
	})
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}
	var n int
	t.Do(dest, func(cx *shard.Ctx) {
		n = place(cx, registry(cx), dest, buildDest(pairs))
	})
	return resp.AppendInt(nil, int64(n))
}
