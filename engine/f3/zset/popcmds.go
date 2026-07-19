package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The zset pop surface (spec 2064/f3/12 section 6.7): ZPOPMIN, ZPOPMAX, and
// ZMPOP, all riding the fused single-descent tree pop (struct/tree_pop.go) over
// the native band and an ordered-blob trim over the inline band. The blocking
// forms BZPOPMIN, BZPOPMAX, and BZMPOP are deferred: they need the per-shard
// waiter registry the F17 intent path introduces (section 6.7), which spans
// slices and is not part of this pops-and-random structural slice.
//
// Reply shapes are pinned by section 6.14. ZPOPMIN/ZPOPMAX with no count reply a
// flat two-element array [member, score]; with a count they reply a flat
// alternating array [m, s, m, s, ...]; an absent key replies the empty array in
// both forms. ZMPOP replies a two-element array [keyname, pair-list] where the
// pair-list is an array of [member, score] pairs, or a null array when every
// listed key is empty or absent.

// Zpopmin answers ZPOPMIN key [count]: pop the lowest-scored members.
func Zpopmin(cx *shard.Ctx, args [][]byte, r shard.Reply) { zpopImpl(cx, args, r, true) }

// Zpopmax answers ZPOPMAX key [count]: pop the highest-scored members.
func Zpopmax(cx *shard.Ctx, args [][]byte, r shard.Reply) { zpopImpl(cx, args, r, false) }

func zpopImpl(cx *shard.Ctx, args [][]byte, r shard.Reply, min bool) {
	count := 1
	countGiven := false
	if len(args) == 2 {
		c, ok := parseIndex(args[1])
		if !ok {
			r.Err(errNotInt)
			return
		}
		if c < 0 {
			r.Err("ERR value is out of range, must be positive")
			return
		}
		count = c
		countGiven = true
	} else if len(args) != 1 {
		r.Err("ERR syntax error")
		return
	}
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if z == nil || count == 0 {
		// Absent key or an explicit zero count: the empty flat array, both forms.
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	npop := count
	if npop > z.card() {
		npop = z.card()
	}
	// RESP3 nests each [member, score] pair only for the counted form; the no-count
	// form and every RESP2 reply stay a flat alternating array. The score itself is
	// a RESP3 double or a RESP2 bulk under both forms.
	resp3 := r.Resp3()
	nested := countGiven && resp3
	header := npop * 2
	if nested {
		header = npop
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], header)
	var sc [40]byte
	z.pop(min, count, func(m []byte, s float64) {
		logRemove(cx, args[0], m)
		if nested {
			out = resp.AppendArrayHeader(out, 2)
		}
		out = resp.AppendBulk(out, m)
		out = appendScore(out, s, resp3, sc[:])
	})
	cx.Aux = out
	// One event per command from the popped end, then the generic del if the pop
	// emptied the key. A reached-here pop always removes at least one member.
	cx.NotifyKeyspaceEvent(shard.NotifyZset, popEvent(min), args[0])
	if z.card() == 0 {
		g.drop(args[0])
		cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", args[0])
	} else {
		g.note(z)
	}
	r.Raw(out)
}

// popEvent names the keyspace event for a pop from the low end (zpopmin) or the
// high end (zpopmax), shared by ZPOPMIN/ZPOPMAX and ZMPOP.
func popEvent(min bool) string {
	if min {
		return "zpopmin"
	}
	return "zpopmax"
}

// Zmpop answers ZMPOP numkeys key [key ...] <MIN|MAX> [COUNT count]: pop from the
// first of the listed keys that has members, up to count of them from the named
// end. Keys are read from one shard registry (the co-located-operand convention
// the set algebra already documents); a cross-shard key set needs the F17 intent
// path. The reply is [firstNonEmptyKey, [[m,s], ...]] or a null array when every
// key is empty.
func Zmpop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	numkeys, ok := parseIndex(args[0])
	if !ok || numkeys <= 0 {
		r.Err("ERR numkeys should be greater than 0")
		return
	}
	// After numkeys come exactly numkeys keys, then MIN|MAX, then optional COUNT.
	if len(args) < 1+numkeys+1 {
		r.Err("ERR syntax error")
		return
	}
	keys := args[1 : 1+numkeys]
	tail := args[1+numkeys:]
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
		resp3 := r.Resp3()
		var sc [40]byte
		z.pop(min, count, func(m []byte, s float64) {
			logRemove(cx, key, m)
			out = resp.AppendArrayHeader(out, 2)
			out = resp.AppendBulk(out, m)
			out = appendScore(out, s, resp3, sc[:])
		})
		cx.Aux = out
		cx.NotifyKeyspaceEvent(shard.NotifyZset, popEvent(min), key)
		if z.card() == 0 {
			g.drop(key)
			cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", key)
		} else {
			g.note(z)
		}
		r.Raw(out)
		return
	}
	// Every listed key was empty or absent.
	r.Raw(resp.AppendNullArray(cx.Aux[:0]))
}
