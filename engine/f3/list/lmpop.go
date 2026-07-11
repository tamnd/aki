package list

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// LMPOP, the non-blocking multi-key list pop (spec 2064/f3/13 M3 slice 8). LMPOP
// numkeys key [key ...] <LEFT|RIGHT> [COUNT count] walks the listed keys in order,
// finds the first that holds elements, and pops up to count of them off the named
// end (the head for LEFT, the tail for RIGHT), replying [key, [elem, ...]] with
// the popped key as a bulk string and the elements as bulk strings in pop order.
// When every listed key is missing or empty the reply is the null array. It is
// the exact list twin of ZMPOP (zset/popcmds.go): same numkeys and COUNT grammar,
// same first-non-empty-key selection, same co-located-operand convention that
// reads every key from one shard registry. A key set spanning shards needs the
// F17 intent path, deferred here exactly as ZMPOP defers it.
//
// LMPOP is the only non-blocking member of the slice-8 family; the blocking forms
// BLPOP, BRPOP, BLMOVE, and BLMPOP land in later slice-8 PRs on the deferred-reply
// seam, the per-shard waiter set, and the timer heap that slice builds. LMPOP
// needs none of that substrate, so it ships first and de-risks the numkeys, COUNT,
// and direction tail parse the blocking forms reuse.

// lmpop runs the LMPOP core over the local registry: it walks keys in order, pops
// min(count, length) elements off the chosen end of the first non-empty key, and
// appends the two-element reply [key, [elem, ...]] to dst with the resp emitters,
// returning the extended buffer. front picks the end the pop takes, the head when
// true and the tail when false. ok is false when every key was missing or empty,
// so the caller replies the null array; wrong reports a WRONGTYPE on a probed key.
// Each popped element is appended to dst as it leaves the list, so it is copied
// into the reply before the next pop can overwrite the storage it aliased, the
// same one-pass shape ZMPOP builds.
func lmpop(g *reg, cx *shard.Ctx, dst []byte, keys [][]byte, front bool, count int) (out []byte, ok, wrong bool) {
	for _, key := range keys {
		l, w := g.lookup(cx, key)
		if w {
			return dst, false, true
		}
		if l == nil || l.length() == 0 {
			continue
		}
		npop := count
		if npop > l.length() {
			npop = l.length()
		}
		out = resp.AppendArrayHeader(dst, 2)
		out = resp.AppendBulk(out, key)
		out = resp.AppendArrayHeader(out, npop)
		for i := 0; i < npop; i++ {
			out = resp.AppendBulk(out, popOne(l, front))
		}
		if l.length() == 0 {
			g.drop(key)
		}
		return out, true, false
	}
	return dst, false, false
}

// Lmpop answers LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count]. It mirrors
// ZMPOP's arity and tail validation exactly: numkeys must be a positive integer,
// then come exactly numkeys keys, then the LEFT or RIGHT token, then an optional
// COUNT count whose count must be positive. Any malformed tail is the syntax
// error.
func Lmpop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	numkeys, ok := store.ParseInt(args[0])
	if !ok || numkeys <= 0 {
		r.Err("ERR numkeys should be greater than 0")
		return
	}
	// After numkeys come exactly numkeys keys, then LEFT|RIGHT, then an optional
	// COUNT. A numkeys past the argument tail leaves no room for the direction
	// token, so it is a syntax error, the short-tail case ZMPOP rejects the same
	// way; bounding it against len(args) first keeps the slice below in range.
	if numkeys > int64(len(args)) {
		r.Err(errSyntax)
		return
	}
	nk := int(numkeys)
	if len(args) < 1+nk+1 {
		r.Err(errSyntax)
		return
	}
	keys := args[1 : 1+nk]
	tail := args[1+nk:]
	var front bool
	switch {
	case eqFold(tail[0], "LEFT"):
		front = true
	case eqFold(tail[0], "RIGHT"):
		front = false
	default:
		r.Err(errSyntax)
		return
	}
	count := 1
	rest := tail[1:]
	switch len(rest) {
	case 0:
	case 2:
		if !eqFold(rest[0], "COUNT") {
			r.Err(errSyntax)
			return
		}
		c, okc := store.ParseInt(rest[1])
		if !okc || c <= 0 {
			r.Err("ERR count should be greater than 0")
			return
		}
		count = int(c)
	default:
		r.Err(errSyntax)
		return
	}

	g := registry(cx)
	out, ok, wrong := lmpop(g, cx, cx.Aux[:0], keys, front, count)
	if wrong {
		r.Err(wrongType)
		return
	}
	if !ok {
		r.Raw(resp.AppendNullArray(cx.Aux[:0]))
		return
	}
	cx.Aux = out
	r.Raw(out)
}
