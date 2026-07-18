package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// The ZREMRANGEBY* surface (spec 2064/f3/12 section 6.9). Each command resolves
// its bounds to a forward-rank window exactly as the read ranges do (BYRANK with
// Redis's negative-index normalization and clamping, BYSCORE and BYLEX through the
// same two counted bound seeks ZCOUNT and ZLEXCOUNT use), then deletes that window
// as one bounded operation and replies the removed count, which is known before
// the surgery. The deletion is inline: it completes before the reply, with no
// deferred teardown, so a native removal has no post-reply folder to shoulder a
// p99 (lab 04 priced v1's ZREM p99 shoulder to exactly such a deferral). An empty
// window removes nothing; the key is deleted the moment its last member leaves.

// Zremrangebyrank answers ZREMRANGEBYRANK key start stop: delete the members at
// the inclusive rank window and reply the count removed.
func Zremrangebyrank(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	start, ok1 := parseIndex(args[1])
	stop, ok2 := parseIndex(args[2])
	if !ok1 || !ok2 {
		r.Err(errNotInt)
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
	lo, hi, empty := clampRange(start, stop, z.card())
	if empty {
		r.Int(0)
		return
	}
	logRemoveWindow(cx, args[0], z, lo, hi+1)
	removed := z.removeRange(lo, hi+1)
	if z.card() == 0 {
		g.drop(args[0])
	} else if removed > 0 {
		g.note(z)
	}
	r.Int(int64(removed))
}

// Zremrangebyscore answers ZREMRANGEBYSCORE key min max: delete the members whose
// score falls in the band and reply the count removed.
func Zremrangebyscore(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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
	logRemoveWindow(cx, args[0], z, lo, hiExcl)
	removed := z.removeRange(lo, hiExcl)
	if z.card() == 0 {
		g.drop(args[0])
	} else if removed > 0 {
		g.note(z)
	}
	r.Int(int64(removed))
}

// Zremrangebylex answers ZREMRANGEBYLEX key min max: delete the members in the lex
// band (defined at equal scores, section 3.2) and reply the count removed.
func Zremrangebylex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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
	logRemoveWindow(cx, args[0], z, lo, hiExcl)
	removed := z.removeRange(lo, hiExcl)
	if z.card() == 0 {
		g.drop(args[0])
	} else if removed > 0 {
		g.note(z)
	}
	r.Int(int64(removed))
}
