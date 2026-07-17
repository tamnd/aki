package set

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
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
	created := s == nil
	if created {
		if cx.St.Exists(key, cx.NowMs) {
			r.Err(wrongType)
			return
		}
		s = newSet(args[1])
		g.m[string(key)] = s
	}
	// Compact the members that actually joined over the argument tail in
	// place (forward-safe): the sadd frame carries post-decision effects,
	// only what was added, so a duplicate-only SADD frames nothing.
	memb := args[1:]
	added := 0
	for _, m := range memb {
		if s.add(m) {
			memb[added] = m
			added++
		}
	}
	g.note(s)
	if added > 0 {
		if err := cx.LogSetAdd(key, created, memb[:added]); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Int(int64(added))
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
	// Compact the members that actually left in place, the same
	// post-decision rule SADD keeps: the srem frame lists only removals.
	memb := args[1:]
	removed := 0
	for _, m := range memb {
		if s.rem(m) {
			memb[removed] = m
			removed++
		}
	}
	dropped := s.card() == 0
	if dropped {
		g.drop(args[0])
	} else {
		g.note(s)
	}
	if removed > 0 {
		if err := cx.LogSetRem(args[0], memb[:removed], dropped); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Int(int64(removed))
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
// absent. A small reply is materialized in the shard scratch and handed over
// whole; a large native-band reply is streamed frame by frame through the ring
// (smembers.go) so a million-member set holds a bounded window, not one giant
// buffer. The stream cutover is the same chunk width the string band streams at.
func Smembers(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Raw(resp.AppendArrayHeader(cx.Aux[:0], 0))
		return
	}
	switch s.enc {
	case encHashtable:
		if total := s.ht.membersTotal(); total > store.ChunkSize {
			r.StreamRaw(total, s.ht.pinMembersStream())
			return
		}
	case encPartitioned:
		// A partitioned set is always past the engagement threshold, so its reply
		// is far wider than a chunk: stream it across every partition (partition.go).
		if total := s.part.membersTotal(); total > store.ChunkSize {
			r.StreamRaw(total, s.part.pinMembersStream())
			return
		}
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], s.card())
	s.each(func(m []byte) { out = resp.AppendBulk(out, m) })
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
		m := s.popOne(g, sc[:])
		dropped := s.card() == 0
		if dropped {
			g.drop(key)
		} else {
			g.note(s)
		}
		// SPOP frames as the srem it is (doc 04 section 2, no spop
		// sub-kind): the drawn member is the post-decision effect.
		if err := cx.LogSetRem(key, [][]byte{m}, dropped); err != nil {
			r.Err(err.Error())
			return
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
	// The drawn members are collected as copies for the srem frame when a
	// log is wired: the draw hands out scratch-backed slices that do not
	// survive the next draw.
	var drawn [][]byte
	logging := cx.Log != nil
	emit := func(m []byte) {
		out = resp.AppendBulk(out, m)
		if logging {
			drawn = append(drawn, append([]byte(nil), m...))
		}
	}
	if popped < s.card() && s.fanDraws(cx, popped) {
		// The escalated aggregate (drawfan.go): indices drawn serially on the
		// owner, the resolve fanned to donated workers, removal back on the
		// owner. Exact uniform without replacement either way (F15).
		popFan(cx, g, s, popped, emit)
	} else {
		var sc [64]byte
		for i := 0; i < popped; i++ {
			emit(s.popOne(g, sc[:]))
		}
	}
	cx.Aux = out
	dropped := s.card() == 0
	if dropped {
		g.drop(key)
	} else {
		g.note(s)
	}
	if logging {
		if err := cx.LogSetRem(key, drawn, dropped); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Raw(out)
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
		r.Bulk(s.drawOne(g, sc[:]))
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
		// With replacement: exactly -count draws, repeats allowed. The escalated
		// aggregate fans the resolve (drawfan.go); the indices are the same
		// serial owner draws either way.
		want := int(-count)
		out := resp.AppendArrayHeader(cx.Aux[:0], want)
		emit := func(m []byte) { out = resp.AppendBulk(out, m) }
		if s.fanDraws(cx, want) {
			drawFanReplacement(cx, g, s, want, emit)
		} else {
			s.sampleWithReplacement(g, want, emit)
		}
		cx.Aux = out
		r.Raw(out)
		return
	}
	// Distinct: up to count members, no repeats, each an exact uniform sample
	// without replacement (doc 11 section 5.2). The header carries the delivered
	// count, which caps at the cardinality. The escalated aggregate fans the
	// resolve (drawfan.go).
	want := int(count)
	if want > s.card() {
		want = s.card()
	}
	out := resp.AppendArrayHeader(cx.Aux[:0], want)
	emit := func(m []byte) { out = resp.AppendBulk(out, m) }
	if want < s.card() && s.fanDraws(cx, want) {
		drawFanDistinct(cx, g, s, want, emit)
	} else {
		s.sampleDistinct(g, want, emit)
	}
	cx.Aux = out
	r.Raw(out)
}
