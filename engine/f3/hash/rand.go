package hash

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// Hrandfield answers HRANDFIELD key [count [WITHVALUES]] (spec 2064/f3/10 section
// 7.4): draw fields without removing them, exactly uniform (F15). With no count it
// replies one bulk field, nil when the key is absent. A positive count replies up
// to count distinct fields; a negative count replies exactly -count fields drawn
// with replacement (repeats allowed). WITHVALUES pairs each field with its value
// in the flat reply [f, v, f, v, ...]. Draws run over the shard's owner-local PCG,
// so the path takes no lock.
func Hrandfield(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if len(args) == 1 {
		if h == nil {
			r.Null()
			return
		}
		f, _ := h.at(g.next(h.card()))
		r.Bulk(f)
		return
	}
	count, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	withValues := false
	if len(args) == 3 {
		if !eqFold(args[2], "WITHVALUES") {
			r.Err("ERR syntax error")
			return
		}
		withValues = true
	} else if len(args) != 2 {
		r.Err("ERR syntax error")
		return
	}
	if h == nil || count == 0 {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}

	emit := func(out, f, v []byte) []byte {
		out = resp.AppendBulk(out, f)
		if withValues {
			out = resp.AppendBulk(out, v)
		}
		return out
	}

	if count < 0 {
		// With replacement: exactly -count draws, repeats allowed.
		want := int(-count)
		out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withValues))
		h.randWithReplacement(g, want, func(f, v []byte) { out = emit(out, f, v) })
		cx.Aux = out
		r.Raw(out)
		return
	}
	// Distinct: up to count fields, no repeats. The delivered count caps at the
	// cardinality.
	want := int(count)
	if want > h.card() {
		want = h.card()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], perElem(want, withValues))
	h.randDistinct(g, want, func(f, v []byte) { out = emit(out, f, v) })
	cx.Aux = out
	r.Raw(out)
}

// perElem is the array element count for n fields, doubled when each carries its
// value.
func perElem(n int, withValues bool) int {
	if withValues {
		return n * 2
	}
	return n
}
