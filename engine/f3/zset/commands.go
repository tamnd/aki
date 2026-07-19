package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The zset command surface over the inline band (spec 2064/f3/12). Every handler
// runs on its shard's owner goroutine, so the registry and every zset in it are
// plain single-owner state. Array replies are built in the shard scratch
// (cx.Aux) with the resp emitters and handed over whole through Reply.Raw, the
// same one-pass shape the set commands use. Scores are formatted with
// resp.FormatScore (Redis's d2string) into a small stack buffer, then copied
// into the reply, so a formatted score never escapes.

// Zadd answers ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member
// ...]: apply each pair under the flag matrix (section 6.1), reply the number
// added, or added-plus-changed with CH, or the new score (nil when suppressed)
// with INCR.
func Zadd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	fl, rest, errMsg := parseZaddFlags(args[1:])
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	// Parse every score before any write so a malformed pair aborts the whole
	// command with no partial effect (section 6.1).
	npairs := len(rest) / 2
	scores := make([]float64, npairs)
	for p := 0; p < npairs; p++ {
		s, ok := parseScore(rest[2*p])
		if !ok {
			r.Err("ERR value is not a valid float")
			return
		}
		scores[p] = s
	}

	g := registry(cx)
	// The create funnel: a lazily-expired zset is dropped and treated as absent, so
	// a fresh ZADD builds a new zset that carries no stale TTL, never resurrects the
	// expired one (the create-path hazard the rollout plan calls out).
	z := g.live(cx, key)
	created := false
	if z == nil {
		if cx.St.Exists(key, cx.NowMs) {
			r.Err(wrongType)
			return
		}
		z = newZset()
		created = true
	}

	var added, changed int64
	for p := 0; p < npairs; p++ {
		member := rest[2*p+1]
		gotAdded, gotChanged, newScore, applied, nan := z.update(member, scores[p], fl)
		if nan {
			// A NaN result rejects before the write. INCR is the only multi-flag
			// path that can reach here and it carries one pair, so a freshly
			// created zset is still empty and there is no key to leave behind.
			r.Err("ERR resulting score is not a number (NaN)")
			return
		}
		// Log the resolved score whenever a value was written, a new member or a
		// moved score, covering the plain and INCR forms alike before the INCR
		// early-return below. An idempotent re-add or a flag-suppressed pair writes
		// nothing and logs nothing.
		if gotAdded || gotChanged {
			logAdd(cx, key, member, newScore)
		}
		if gotAdded {
			added++
		}
		if gotChanged {
			changed++
		}
		if fl.incr {
			// INCR carries exactly one pair; reply the new score or nil.
			if created && z.card() == 0 {
				// XX/NX/GT/LT suppressed the only write; do not leave an empty key.
				g.drop(key)
			} else {
				if created {
					g.install(cx, key, z)
				}
				g.grewNote(cx, key, z)
			}
			if !applied {
				r.Null()
				return
			}
			r.Double(newScore)
			return
		}
	}

	if z.card() == 0 {
		if !created {
			g.drop(key)
		}
	} else {
		if created {
			g.install(cx, key, z)
		}
		g.grewNote(cx, key, z)
	}
	if fl.ch {
		r.Int(added + changed)
		return
	}
	r.Int(added)
}

// parseZaddFlags reads the leading NX/XX/GT/LT/CH/INCR options off a ZADD tail
// (the arguments after the key) and returns the parsed flags, the remaining
// score-member pairs, and a non-empty Redis error string when the options or
// pair count are invalid. Splitting it out keeps the option grammar testable
// without the reply arena.
func parseZaddFlags(tail [][]byte) (fl flags, rest [][]byte, errMsg string) {
	i := 0
flagsLoop:
	for ; i < len(tail); i++ {
		switch {
		case eqFold(tail[i], "NX"):
			fl.nx = true
		case eqFold(tail[i], "XX"):
			fl.xx = true
		case eqFold(tail[i], "GT"):
			fl.gt = true
		case eqFold(tail[i], "LT"):
			fl.lt = true
		case eqFold(tail[i], "CH"):
			fl.ch = true
		case eqFold(tail[i], "INCR"):
			fl.incr = true
		default:
			break flagsLoop
		}
	}
	if fl.nx && fl.xx {
		return fl, nil, "ERR XX and NX options at the same time are not compatible"
	}
	if (fl.gt && fl.lt) || (fl.nx && (fl.gt || fl.lt)) {
		return fl, nil, "ERR GT, LT, and/or NX options at the same time are not compatible"
	}
	rest = tail[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return fl, nil, "ERR syntax error"
	}
	if fl.incr && len(rest) != 2 {
		return fl, nil, "ERR INCR option supports a single increment-element pair"
	}
	return fl, rest, ""
}

// Zincrby answers ZINCRBY key increment member: ZADD INCR on one pair, reply the
// new score.
func Zincrby(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	delta, ok := parseScore(args[1])
	if !ok {
		r.Err("ERR value is not a valid float")
		return
	}
	key := args[0]
	member := args[2]
	g := registry(cx)
	// See Zadd: the create funnel drops an expired zset so ZINCRBY builds a fresh
	// one with no stale TTL.
	z := g.live(cx, key)
	created := false
	if z == nil {
		if cx.St.Exists(key, cx.NowMs) {
			r.Err(wrongType)
			return
		}
		z = newZset()
		created = true
	}
	gotAdded, gotChanged, newScore, _, nan := z.update(member, delta, flags{incr: true})
	if nan {
		r.Err("ERR resulting score is not a number (NaN)")
		return
	}
	if gotAdded || gotChanged {
		logAdd(cx, key, member, newScore)
	}
	if created {
		g.install(cx, key, z)
	}
	g.grewNote(cx, key, z)
	r.Double(newScore)
}

// Zscore answers ZSCORE key member: the member's score, a RESP2 bulk string of
// the formatted digits or a RESP3 double, nil when the member or key is absent.
// It never allocates on the present path over an inline band.
func Zscore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Null()
		return
	}
	s, ok := z.score(args[1])
	if !ok {
		r.Null()
		return
	}
	r.Double(s)
}

// Zmscore answers ZMSCORE key member [member ...]: an array of scores in
// argument order, nil in a position whose member is absent.
func Zmscore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	members := args[1:]
	resp3 := r.Resp3()
	out := resp.AppendArrayHeader(cx.Aux[:0], len(members))
	var sc [40]byte
	appendNull := func(out []byte) []byte {
		if resp3 {
			return resp.AppendNull3(out)
		}
		return resp.AppendNull(out)
	}
	for _, m := range members {
		if z == nil {
			out = appendNull(out)
			continue
		}
		if s, ok := z.score(m); ok {
			out = appendScore(out, s, resp3, sc[:])
		} else {
			out = appendNull(out)
		}
	}
	cx.Aux = out
	r.Raw(out)
}

// Zcard answers ZCARD key: the member count, 0 when absent. O(1) in every band.
func Zcard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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
	r.Int(int64(z.card()))
}

// Zrem answers ZREM key member [member ...]: remove each, reply the count
// removed, and delete the key when the last member leaves.
func Zrem(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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
	var removed int64
	for _, m := range args[1:] {
		if z.rem(m) {
			removed++
			logRemove(cx, args[0], m)
		}
	}
	if z.card() == 0 {
		g.drop(args[0])
	} else if removed > 0 {
		g.note(z)
	}
	r.Int(removed)
}

// Zrank answers ZRANK key member [WITHSCORE]: the 0-based rank, or nil when the
// member is absent. WITHSCORE replies [rank, score]; a missing member with
// WITHSCORE is a null array.
func Zrank(cx *shard.Ctx, args [][]byte, r shard.Reply) { zrankImpl(cx, args, r, false) }

// Zrevrank answers ZREVRANK key member [WITHSCORE]: the rank counting from the
// high end.
func Zrevrank(cx *shard.Ctx, args [][]byte, r shard.Reply) { zrankImpl(cx, args, r, true) }

func zrankImpl(cx *shard.Ctx, args [][]byte, r shard.Reply, rev bool) {
	withScore := false
	if len(args) == 3 {
		if !eqFold(args[2], "WITHSCORE") {
			r.Err("ERR syntax error")
			return
		}
		withScore = true
	} else if len(args) != 2 {
		r.Err("ERR syntax error")
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	var (
		rank  int
		score float64
		ok    bool
	)
	if z != nil {
		rank, score, ok = z.rank(args[1])
	}
	if !ok {
		if withScore {
			r.Raw(resp.AppendNullArray(cx.Aux[:0]))
			return
		}
		r.Null()
		return
	}
	if rev {
		rank = z.card() - 1 - rank
	}
	if !withScore {
		r.Int(int64(rank))
		return
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], 2)
	out = resp.AppendInt(out, int64(rank))
	var sc [40]byte
	out = resp.AppendBulk(out, resp.FormatScore(sc[:0], score))
	cx.Aux = out
	r.Raw(out)
}

// Zrange answers ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset
// count] [WITHSCORES] (section 6.4), the umbrella that subsumes ZRANGEBYSCORE,
// ZRANGEBYLEX, and their reverse forms. It parses the options, rejects the
// illegal combinations with Redis's exact strings, then dispatches to the score,
// lex, or index plan; each resolves to a rank window and streams it. When REV is
// set with BYSCORE or BYLEX the start and stop are the high and low bounds, so
// they swap before parsing, matching Redis.
func Zrange(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	var byScore, byLex, rev, withScores, limit bool
	var offset, count int
	for i := 3; i < len(args); {
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
		case eqFold(args[i], "WITHSCORES"):
			withScores = true
			i++
		case eqFold(args[i], "LIMIT"):
			if i+2 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			o, ok1 := parseIndex(args[i+1])
			c, ok2 := parseIndex(args[i+2])
			if !ok1 || !ok2 {
				r.Err(errNotInt)
				return
			}
			offset, count, limit = o, c, true
			i += 3
		default:
			r.Err("ERR syntax error")
			return
		}
	}
	if byScore && byLex {
		r.Err("ERR syntax error")
		return
	}
	if byLex && withScores {
		r.Err(errLexScores)
		return
	}
	if limit && !byScore && !byLex {
		r.Err(errLimitOnly)
		return
	}

	switch {
	case byScore:
		lo, hi := args[1], args[2]
		if rev {
			lo, hi = args[2], args[1]
		}
		min, ok1 := parseScoreBound(lo)
		max, ok2 := parseScoreBound(hi)
		if !ok1 || !ok2 {
			r.Err(errScoreBound)
			return
		}
		execByScore(cx, r, args[0], min, max, rev, withScores, limit, offset, count)
	case byLex:
		lo, hi := args[1], args[2]
		if rev {
			lo, hi = args[2], args[1]
		}
		min, ok1 := parseLexBound(lo)
		max, ok2 := parseLexBound(hi)
		if !ok1 || !ok2 {
			r.Err(errLexBound)
			return
		}
		execByLex(cx, r, args[0], min, max, rev, limit, offset, count)
	default:
		zrangeByIndex(cx, args, r, rev, withScores)
	}
}

// Zrevrange answers ZREVRANGE key start stop [WITHSCORES], the deprecated alias
// that indexes the high-to-low order (section 6.4). It shares the ZRANGE REV
// plan; only its tail grammar differs, taking WITHSCORES and no REV token.
func Zrevrange(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	withScores := false
	for _, opt := range args[3:] {
		if !eqFold(opt, "WITHSCORES") {
			r.Err("ERR syntax error")
			return
		}
		withScores = true
	}
	zrangeByIndex(cx, args, r, true, withScores)
}

// zrangeByIndex is the shared index-range plan for ZRANGE and ZREVRANGE: parse
// the bounds, clamp per Redis, and stream the window into the shard scratch. rev
// selects the high-to-low order.
func zrangeByIndex(cx *shard.Ctx, args [][]byte, r shard.Reply, rev, withScores bool) {
	start, ok1 := parseIndex(args[1])
	stop, ok2 := parseIndex(args[2])
	if !ok1 || !ok2 {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	lo, hi, empty := clampRange(start, stop, z.card())
	if empty {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	resp3 := r.Resp3()
	n := hi - lo + 1
	if withScores && !resp3 {
		// RESP2 flattens each member/score into two elements; RESP3 nests each
		// as a 2-element pair, so the outer count stays the member count.
		n *= 2
	}
	out := z.rangeByIndex(resp.AppendArrayHeader(cx.Aux[:0], n), lo, hi, rev, withScores, resp3)
	cx.Aux = out
	r.Raw(out)
}

// clampRange normalizes Redis index semantics: negatives count from the end,
// bounds clamp to the set, and an empty or inverted window reports empty.
func clampRange(start, stop, card int) (lo, hi int, empty bool) {
	if start < 0 {
		start += card
	}
	if stop < 0 {
		stop += card
	}
	if start < 0 {
		start = 0
	}
	if stop >= card {
		stop = card - 1
	}
	if start > stop || start >= card || card == 0 {
		return 0, 0, true
	}
	return start, stop, false
}

func reverse(ev []entryView) {
	for a, b := 0, len(ev)-1; a < b; a, b = a+1, b-1 {
		ev[a], ev[b] = ev[b], ev[a]
	}
}

// parseIndex parses a signed decimal range bound. It accepts the ordinary
// integer forms ZRANGE takes; anything else is the not-an-integer error.
func parseIndex(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '+' || b[0] == '-' {
		neg = b[0] == '-'
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	n := 0
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

// eqFold reports whether b equals the uppercase option name s, ASCII case
// insensitive, without allocating.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}
