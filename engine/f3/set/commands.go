package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The set command surface over the inline band (spec 2064/f3/11). Every
// handler runs on its shard's owner goroutine, so the registry and every set
// in it are plain single-owner state. Replies that are arrays are built in the
// shard scratch (cx.Aux) with the resp emitters and handed over whole through
// Reply.Raw, the same one-pass, one-span shape the MGET gather uses.

// Sadd answers SADD key member [member ...]: add each member, reply the count
// newly added. A missing key creates the set from the first member's shape
// (integer opens an intset), and a key the string store owns answers
// WRONGTYPE.
func Sadd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	key := args[0]
	s := g.m[string(key)]
	if s == nil {
		if cx.St.Exists(key, cx.NowMs) {
			r.Err(wrongType)
			return
		}
		s = newSet(args[1])
		g.m[string(key)] = s
	}
	var added int64
	for _, m := range args[1:] {
		if s.add(m) {
			added++
		}
	}
	r.Int(added)
}

// Srem answers SREM key member [member ...]: remove each, reply the count
// removed, and delete the key when the last member leaves.
func Srem(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Int(0)
		return
	}
	var removed int64
	for _, m := range args[1:] {
		if s.rem(m) {
			removed++
		}
	}
	if s.card() == 0 {
		g.drop(args[0])
	}
	r.Int(removed)
}

// Sismember answers SISMEMBER key member: 1 when present, 0 otherwise.
func Sismember(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s != nil && s.has(args[1]) {
		r.Int(1)
		return
	}
	r.Int(0)
}

// Smismember answers SMISMEMBER key member [member ...]: an array of 0/1 in
// argument order.
func Smismember(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	members := args[1:]
	out := resp.AppendArrayHeader(cx.Aux[:0], len(members))
	for _, m := range members {
		if s != nil && s.has(m) {
			out = resp.AppendInt(out, 1)
		} else {
			out = resp.AppendInt(out, 0)
		}
	}
	cx.Aux = out
	r.Raw(out)
}

// Scard answers SCARD key: the member count, 0 when absent.
func Scard(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Int(0)
		return
	}
	r.Int(int64(s.card()))
}

// Smembers answers SMEMBERS key: an array of every member, an empty array when
// absent. Each member is copied straight from its inline storage into the
// reply.
func Smembers(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	n := 0
	if s != nil {
		n = s.card()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], n)
	if s != nil {
		s.each(func(m []byte) { out = resp.AppendBulk(out, m) })
	}
	cx.Aux = out
	r.Raw(out)
}

// Spop answers SPOP key [count]: draw and remove members uniformly. Without a
// count it draws one and replies a bulk (nil when absent); with a count it
// replies an array of up to count members. The key is deleted when it empties.
func Spop(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	key := args[0]
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if len(args) == 1 {
		if s == nil {
			r.Null()
			return
		}
		var sc [64]byte
		m := append(sc[:0], s.at(g.next(s.card()), sc[:])...)
		s.rem(m)
		if s.card() == 0 {
			g.drop(key)
		}
		r.Bulk(m)
		return
	}
	count, ok := store.ParseInt(args[1])
	if !ok || count < 0 {
		r.Err("ERR value is out of range, must be positive")
		return
	}
	if s == nil || count == 0 {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	popped := int(count)
	if popped > s.card() {
		popped = s.card()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], popped)
	var sc [64]byte
	for i := 0; i < popped; i++ {
		m := append(sc[:0], s.at(g.next(s.card()), sc[:])...)
		out = resp.AppendBulk(out, m)
		s.rem(m)
	}
	cx.Aux = out
	r.Raw(out)
	if s.card() == 0 {
		g.drop(key)
	}
}

// Srandmember answers SRANDMEMBER key [count]: draw members without removing
// them. Without a count it replies one bulk (nil when absent). A positive
// count replies up to count distinct members; a negative count replies exactly
// -count members drawn with replacement (repeats allowed).
func Srandmember(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if len(args) == 1 {
		if s == nil {
			r.Null()
			return
		}
		var sc [64]byte
		r.Bulk(s.at(g.next(s.card()), sc[:]))
		return
	}
	count, ok := store.ParseInt(args[1])
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if s == nil || count == 0 {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	if count < 0 {
		// With replacement: exactly -count draws, repeats allowed.
		want := int(-count)
		out := resp.AppendArrayHeader(cx.Aux[:0], want)
		var sc [64]byte
		for i := 0; i < want; i++ {
			out = resp.AppendBulk(out, s.at(g.next(s.card()), sc[:]))
		}
		cx.Aux = out
		r.Raw(out)
		return
	}
	// Distinct: up to count members, no repeats. A partial Fisher-Yates over a
	// snapshot of the members gives an unbiased sample without disturbing the
	// set. The snapshot is bounded by the set's cardinality.
	want := int(count)
	snap := snapshot(s)
	if want > len(snap) {
		want = len(snap)
	}
	for i := 0; i < want; i++ {
		j := i + g.next(len(snap)-i)
		snap[i], snap[j] = snap[j], snap[i]
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], want)
	for i := 0; i < want; i++ {
		out = resp.AppendBulk(out, snap[i])
	}
	cx.Aux = out
	r.Raw(out)
}

// snapshot returns a fresh copy of every member's bytes, so a caller can shuffle
// and emit without touching the live set. Used only by the count draw forms.
func snapshot(s *set) [][]byte {
	out := make([][]byte, 0, s.card())
	s.each(func(m []byte) { out = append(out, append([]byte(nil), m...)) })
	return out
}
