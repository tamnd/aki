package zset

import (
	"math"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// Zrandmember answers ZRANDMEMBER key [count [WITHSCORES]] (spec 2064/f3/12
// section 6.8): draw members without removing them, exactly uniform (F15). With
// no count it replies one bulk member, nil when the key is absent. A positive
// count replies up to count distinct members; a negative count replies exactly
// -count members drawn with replacement (repeats allowed). WITHSCORES pairs each
// member with its score in the flat reply [m, s, m, s, ...]. Draws run over the
// shard's owner-local PCG, so the path takes no lock.
func Zrandmember(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if len(args) == 1 {
		if z == nil {
			r.Null()
			return
		}
		m, _ := z.at(g.next(z.card()))
		r.Bulk(m)
		return
	}
	count, ok := parseIndex(args[1])
	if !ok {
		r.Err(errNotInt)
		return
	}
	withScores := false
	if len(args) == 3 {
		if !eqFold(args[2], "WITHSCORES") {
			r.Err("ERR syntax error")
			return
		}
		withScores = true
	} else if len(args) != 2 {
		r.Err("ERR syntax error")
		return
	}
	if z == nil || count == 0 {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}

	// WITHSCORES flattens each member/score into two elements under RESP2; RESP3
	// nests each as a 2-element pair, so the outer count stays the member count and
	// the score rides as a native double.
	resp3 := r.Resp3()
	nested := withScores && resp3
	var sc [40]byte
	emit := func(out []byte, m []byte, bits uint64) []byte {
		if nested {
			out = resp.AppendArrayHeader(out, 2)
		}
		out = resp.AppendBulk(out, m)
		if withScores {
			out = appendScore(out, math.Float64frombits(bits), resp3, sc[:])
		}
		return out
	}

	if count < 0 {
		// With replacement: exactly -count draws, repeats allowed.
		want := -count
		out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withScores, nested))
		z.randWithReplacement(g, want, func(m []byte, bits uint64) { out = emit(out, m, bits) })
		cx.Aux = out
		r.Raw(out)
		return
	}
	// Distinct: up to count members, no repeats. The delivered count caps at the
	// cardinality.
	want := count
	if want > z.card() {
		want = z.card()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withScores, nested))
	z.randDistinct(g, want, func(m []byte, bits uint64) { out = emit(out, m, bits) })
	cx.Aux = out
	r.Raw(out)
}

// perElem is the array element count for n members: n when each member is one
// element (no scores, or RESP3's nested [member, score] pairs), n*2 when RESP2
// flattens each member and score into two top-level elements.
func perElem(n int, withScores, nested bool) int {
	if withScores && !nested {
		return n * 2
	}
	return n
}
