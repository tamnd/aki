package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The multi-key set-algebra command surface (spec 2064/f3/11 section 6):
// SINTER, SUNION, SDIFF, and SINTERCARD. Every operand key routes to one shard
// (the dispatch table keys SINTER/SUNION/SDIFF on their first key and SINTERCARD
// on the key after numkeys), so a handler reads every operand from its own
// owner-local registry with no cross-shard hop. Dispatch guarantees that: a
// command whose operands are co-located comes here, and one whose operands
// span shards takes the F17 gather route instead (gathercross.go), which
// clones the remote operands under the intent barrier and runs these same
// drivers on the first key's owner. The STORE forms (setstore.go) still
// assume co-located sources, the remaining recorded deferral.
//
// The reply is buffer-then-encode (doc 11 section 6.4, the setalgebra lab):
// members land in the shard's value scratch as they are found, the exact count
// falls out of the walk, and the array header plus the page are handed over
// whole through Reply.Raw, the same one-span shape SSCAN uses.

// gather resolves every operand key against the local registry. wrong is true
// when any key holds a string value (WRONGTYPE); a missing key resolves to a nil
// set, which the drivers read as empty.
func gather(g *reg, cx *shard.Ctx, keys [][]byte) (sets []*set, wrong bool) {
	sets = make([]*set, len(keys))
	for i, k := range keys {
		s, w := g.lookup(cx, k)
		if w {
			return nil, true
		}
		sets[i] = s
	}
	return sets, false
}

// emitArray runs the driver's emit into the shard scratch, counting members, and
// hands the flat multi-bulk reply over whole. It is the shared reply shape for
// SINTER, SUNION, and SDIFF.
func emitArray(cx *shard.Ctx, r shard.Reply, drive func(emit func(m []byte))) {
	page := cx.Val[:0]
	n := 0
	drive(func(m []byte) {
		page = resp.AppendBulk(page, m)
		n++
	})
	cx.Val = page
	out := resp.AppendArrayHeader(cx.Aux[:0], n)
	out = append(out, page...)
	cx.Aux = out
	r.Raw(out)
}

// Sinter answers SINTER key [key ...]: the intersection of every set, a flat
// array. A missing key is an empty set, so it empties the result.
func Sinter(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sets, wrong := gather(registry(cx), cx, args)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitArray(cx, r, func(emit func(m []byte)) { sinter(cx, sets, emit) })
}

// Sunion answers SUNION key [key ...]: the distinct union of every set, a flat
// array. Missing keys contribute nothing.
func Sunion(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sets, wrong := gather(registry(cx), cx, args)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitArray(cx, r, func(emit func(m []byte)) { sunion(cx, sets, emit) })
}

// Sdiff answers SDIFF key [key ...]: the members of the first set not in any
// later set, a flat array. A missing first key is empty; missing later keys
// exclude nothing.
func Sdiff(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sets, wrong := gather(registry(cx), cx, args)
	if wrong {
		r.Err(wrongType)
		return
	}
	emitArray(cx, r, func(emit func(m []byte)) { sdiff(cx, sets, emit) })
}

// Sintercard answers SINTERCARD numkeys key [key ...] [LIMIT limit]: the size of
// the intersection, an integer, with LIMIT capping the count and stopping the
// walk early. LIMIT 0 means unlimited (Redis). The command keys on the first
// operand (args[1]) via the dispatch keyAt route, so args[0] here is numkeys.
func Sintercard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	keys, limit, msg := sintercardArgs(args)
	if msg != "" {
		r.Err(msg)
		return
	}
	sets, wrong := gather(registry(cx), cx, keys)
	if wrong {
		r.Err(wrongType)
		return
	}
	r.Int(int64(sintercard(cx, sets, limit)))
}

// sintercardArgs parses the SINTERCARD tail (numkeys, the keys, the LIMIT
// option): the operand keys, the limit (0 unlimited), and the error reply
// text when the tail is malformed. The point handler and the cross route
// share it, and dispatch reads the keys through SintercardKeys for the
// co-location check.
func sintercardArgs(args [][]byte) (keys [][]byte, limit int, msg string) {
	numkeys, ok := store.ParseInt(args[0])
	if !ok || numkeys <= 0 {
		return nil, 0, "ERR numkeys should be greater than 0"
	}
	nk := int(numkeys)
	if nk > len(args)-1 {
		return nil, 0, "ERR Number of keys can't be greater than number of args"
	}
	keys = args[1 : 1+nk]
	for i := 1 + nk; i < len(args); {
		if !eqFold(args[i], "LIMIT") || i+1 >= len(args) {
			return nil, 0, "ERR syntax error"
		}
		lv, ok := store.ParseInt(args[i+1])
		if !ok || lv < 0 {
			return nil, 0, "ERR LIMIT can't be negative"
		}
		limit = int(lv)
		i += 2
	}
	return keys, limit, ""
}
