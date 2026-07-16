package list

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
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
		} else {
			g.note(l)
		}
		return out, true, false
	}
	return dst, false, false
}

// parseLmpopTail parses the numkeys/keys/direction/COUNT tail shared by LMPOP and
// BLMPOP: for LMPOP a is the whole argument list, for BLMPOP it is the arguments
// after the leading timeout. It mirrors ZMPOP's validation exactly: numkeys must
// be a positive integer, then come exactly numkeys keys, then the LEFT or RIGHT
// token, then an optional COUNT count whose count must be positive. keys are the
// key views, front is true for LEFT, count is 1 without COUNT, and emsg is "" on
// success or one of the three exact Redis error texts. Factoring it out keeps the
// two verbs byte-identical on every malformed tail (a differential test pins it).
func parseLmpopTail(a [][]byte) (keys [][]byte, front bool, count int, emsg string) {
	numkeys, ok := store.ParseInt(a[0])
	if !ok || numkeys <= 0 {
		return nil, false, 0, "ERR numkeys should be greater than 0"
	}
	// After numkeys come exactly numkeys keys, then LEFT|RIGHT, then an optional
	// COUNT. A numkeys past the argument tail leaves no room for the direction
	// token, so it is a syntax error, the short-tail case ZMPOP rejects the same
	// way; bounding it against len(a) first keeps the slice below in range.
	if numkeys > int64(len(a)) {
		return nil, false, 0, errSyntax
	}
	nk := int(numkeys)
	if len(a) < 1+nk+1 {
		return nil, false, 0, errSyntax
	}
	keys = a[1 : 1+nk]
	tail := a[1+nk:]
	switch {
	case eqFold(tail[0], "LEFT"):
		front = true
	case eqFold(tail[0], "RIGHT"):
		front = false
	default:
		return nil, false, 0, errSyntax
	}
	count = 1
	rest := tail[1:]
	switch len(rest) {
	case 0:
	case 2:
		if !eqFold(rest[0], "COUNT") {
			return nil, false, 0, errSyntax
		}
		c, okc := store.ParseInt(rest[1])
		if !okc || c <= 0 {
			return nil, false, 0, "ERR count should be greater than 0"
		}
		count = int(c)
	default:
		return nil, false, 0, errSyntax
	}
	return keys, front, count, ""
}

// Lmpop answers LMPOP numkeys key [key ...] <LEFT|RIGHT> [COUNT count]. It is a
// thin wrapper over parseLmpopTail and the lmpop core, sharing its tail parse
// byte-for-byte with BLMPOP.
func Lmpop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	keys, front, count, emsg := parseLmpopTail(args)
	if emsg != "" {
		r.Err(emsg)
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
