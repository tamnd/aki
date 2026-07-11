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

	var sc [40]byte
	emit := func(out []byte, m []byte, bits uint64) []byte {
		out = resp.AppendBulk(out, m)
		if withScores {
			out = resp.AppendBulk(out, resp.FormatScore(sc[:0], math.Float64frombits(bits)))
		}
		return out
	}

	if count < 0 {
		// With replacement: exactly -count draws, repeats allowed.
		want := -count
		out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withScores))
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
	out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withScores))
	z.randDistinct(g, want, func(m []byte, bits uint64) { out = emit(out, m, bits) })
	cx.Aux = out
	r.Raw(out)
}

// perElem is the array element count for n members, doubled when each carries a
// score.
func perElem(n int, withScores bool) int {
	if withScores {
		return n * 2
	}
	return n
}
