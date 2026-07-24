package stream

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// The stream command surface over the inline and native bands (spec 2064/f3/14
// section 6). Every handler runs on its shard's owner goroutine, so the registry
// and every stream in it are plain single-owner state, the same discipline the
// other collection types keep. This slice lands the write path: XADD, XLEN, XDEL,
// XSETID. XRANGE and the read path arrive with the counted directory (slice 3).

// Xadd answers XADD key [NOMKSTREAM] <id> field value [field value ...]: allocate
// the entry ID (auto, partial, or explicit, section 3.6), append the entry, and
// reply the assigned ID as a bulk string. NOMKSTREAM replies nil without creating
// the key when it is absent. An optional trim clause (MAXLEN/MINID [=|~] threshold
// [LIMIT n]) runs after the append, so the new entry counts toward the threshold,
// matching Redis. A malformed or non-increasing ID is a client error and creates
// nothing.
func Xadd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	i := 1
	nomkstream := false
	if i < len(args) && eqFold(args[i], "NOMKSTREAM") {
		nomkstream = true
		i++
	}
	var trimSet bool
	var trim trimSpec
	if i < len(args) && (eqFold(args[i], "MAXLEN") || eqFold(args[i], "MINID")) {
		sp, next, msg := parseTrim(args[i:])
		if msg != "" {
			r.Err(msg)
			return
		}
		trim, trimSet = sp, true
		i += next
	}
	if i >= len(args) {
		r.Err("ERR wrong number of arguments for 'xadd' command")
		return
	}
	idArg := args[i]
	i++
	// The remaining args are field-value pairs and must be a non-empty even run.
	if i >= len(args) || (len(args)-i)%2 != 0 {
		r.Err("ERR wrong number of arguments for 'xadd' command")
		return
	}

	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	newKey := s == nil
	if newKey {
		if nomkstream {
			r.Null()
			return
		}
		s = newStream()
	}

	id, ok, msg := s.allocID(idArg, uint64(cx.NowMs))
	if !ok {
		// A freshly built stream that fails ID validation is never inserted, so a
		// rejected XADD leaves no empty key behind.
		r.Err(msg)
		return
	}

	fields := parseFields(args[i:])
	s.appendEntry(id, fields)
	if newKey {
		g.m[string(args[0])] = s
	}
	var removed int
	if trimSet {
		removed = s.trim(args[0], trim)
		// Exact trim tombstones the boundary block's overshoot in place, leaving a
		// partially-dead sealed block for the gc pass; approximate trim only drops
		// whole front blocks, already reclaimed. Mark the stream dirty only in the
		// exact case, so the maintainer visits it.
		if removed > 0 && !trim.approx && s.kind == bandNative {
			g.markDirty(s)
		}
	}
	// One command, one run: collnew when the key is new, the entry at its
	// assigned id, and the trim clause's removals when it dropped any. A
	// refused emission skips the serves below, the pushCmd rule: a liveness
	// cost, never a correctness one, since XREAD serves are pure reads and
	// frame nothing themselves.
	if err := cx.LogStreamAdd(args[0], newKey, id.ms, id.seq, args[i:], uint64(removed)); err != nil {
		g.note(s)
		r.Err(err.Error())
		return
	}
	// The appended entry is a read event for any client blocked on this key: its
	// ID exceeds every parked waiter's after-ID (they parked at or below the prior
	// last ID), so serving wakes each with at least this entry. A trim that runs
	// first never drops it, so the wake still delivers. Skipped when no XREAD has
	// ever blocked, the common path, so a plain XADD touches no waiter state.
	if len(g.waiters) != 0 {
		serveWaiters(cx, g, args[0])
	}

	// Reconcile the stream's footprint into the registry running sum: the append
	// grew a block (and a trim may have dropped some), so the resident total moves
	// here at the command boundary the shard reads it. A no-op when accounting is off.
	g.note(s)
	cx.Val = formatID(cx.Val[:0], id)
	r.Bulk(cx.Val)
}

// Xtrim answers XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT n]: drop entries
// from the front to the threshold and reply the count removed (section 6.6). A
// missing key removes nothing and replies 0. The whole-block drops touch the
// directory once per block, not once per entry, and the surviving tail is never
// rewritten.
func Xtrim(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	sp, next, msg := parseTrim(args[1:])
	if msg != "" {
		r.Err(msg)
		return
	}
	if 1+next != len(args) {
		r.Err("ERR syntax error")
		return
	}
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
	removed := s.trim(args[0], sp)
	// Exact trim leaves a partially-dead boundary block for the gc pass (see Xadd);
	// approximate trim reclaims whole blocks and leaves nothing to collect.
	if removed > 0 && !sp.approx && s.kind == bandNative {
		g.markDirty(s)
	}
	g.note(s)
	// A trim that removed nothing frames nothing; one that did frames its
	// count, and never a colldrop, since a trim never drops the stream.
	if removed > 0 {
		if err := cx.LogStreamTrim(args[0], uint64(removed)); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Int(int64(removed))
}

// parseFields views the field-value tail as []field. The bytes are caller-owned
// for the call; appendEntry copies them into the block blob, so they need not
// outlive it.
func parseFields(kv [][]byte) []field {
	fields := make([]field, 0, len(kv)/2)
	for j := 0; j+1 < len(kv); j += 2 {
		fields = append(fields, field{name: kv[j], value: kv[j+1]})
	}
	return fields
}

// Xlen answers XLEN key: the live entry count, 0 for a missing key (section 6.6).
func Xlen(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	s, wrong := registry(cx).lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Int(0)
		return
	}
	r.Int(int64(s.length))
}

// Xdel answers XDEL key id [id ...]: tombstone each named entry and reply the
// count actually removed (section 6.5). Every ID is parsed before any deletion,
// so a malformed ID fails the whole command without a partial effect, matching
// Redis. A missing key deletes nothing. The stream is kept even when its last
// entry is tombstoned; lastID does not move back.
func Xdel(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	ids := make([]streamID, 0, len(args)-1)
	for _, a := range args[1:] {
		id, ok := parseStreamID(a)
		if !ok {
			r.Err(errInvalidID)
			return
		}
		ids = append(ids, id)
	}
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
	// The frame carries only the ids that actually removed, in argument
	// order; collecting them is gated on a wired log so the unlogged path
	// stays allocation-free.
	logging := cx.Log != nil
	var delMs, delSeq []uint64
	var n int64
	for _, id := range ids {
		if s.delete(id) {
			n++
			if logging {
				delMs = append(delMs, id.ms)
				delSeq = append(delSeq, id.seq)
			}
		}
	}
	// A tombstone in a native sealed block accrues dead bytes the gc pass reclaims;
	// mark the stream so the owner's maintainer visits it. The inline band compacts
	// on trim, so it needs no gc.
	if n > 0 && s.kind == bandNative {
		g.markDirty(s)
	}
	// A tombstone leaves the blob length unchanged, so the footprint holds until the
	// gc pass reclaims the block; note keeps the running total exact at the boundary
	// regardless, and picks up any auxiliary change the command made.
	g.note(s)
	// An XDEL that removed nothing frames nothing, and one that emptied the
	// stream frames no colldrop: the stream persists and lastID never moves
	// back.
	if n > 0 {
		if err := cx.LogStreamDel(args[0], delMs, delSeq); err != nil {
			r.Err(err.Error())
			return
		}
	}
	r.Int(n)
}

// Xsetid answers XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]: set the
// stream's lastID (and optionally the lifetime add count and max-deleted ID),
// used to graft a stream onto a replica or a restored dump (section 6.7). The new
// ID may not be smaller than the greatest entry currently in the stream, or a
// later auto ID would collide. The key must already exist.
func Xsetid(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	id, ok := parseStreamID(args[1])
	if !ok {
		r.Err(errInvalidID)
		return
	}
	entriesAdded, maxDeleted, ok := parseSetidOpts(args[2:])
	if !ok {
		r.Err("ERR syntax error")
		return
	}

	s, wrong := registry(cx).lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Err("ERR The XSETID command requires the key to exist.")
		return
	}
	// The new last ID may not drop below the greatest entry the stream holds, so a
	// subsequent auto ID still lands above every stored entry. XDEL never lowers
	// lastID, so comparing against it is the tightest valid check this slice needs.
	if s.length > 0 && id.cmp(s.lastID) < 0 {
		r.Err("ERR The ID specified in XSETID is smaller than the target stream top item")
		return
	}
	s.lastID = id
	if entriesAdded.set {
		s.entriesAdded = entriesAdded.val
	}
	if maxDeleted.set {
		s.maxDeletedID = maxDeleted.id
	}
	// The frame carries all three resulting values unconditionally, the
	// optional-argument merge already done, so replay assigns without flags.
	// The key must exist, so a collnew never leads.
	if err := cx.LogStreamSetID(args[0], s.lastID.ms, s.lastID.seq, s.entriesAdded, s.maxDeletedID.ms, s.maxDeletedID.seq); err != nil {
		r.Err(err.Error())
		return
	}
	r.Status("OK")
}

type uintOpt struct {
	val uint64
	set bool
}

type idOpt struct {
	id  streamID
	set bool
}

// parseSetidOpts reads the optional ENTRIESADDED and MAXDELETEDID clauses of
// XSETID. ok is false on an unknown keyword, a missing value, or a malformed one.
func parseSetidOpts(rest [][]byte) (entriesAdded uintOpt, maxDeleted idOpt, ok bool) {
	for i := 0; i < len(rest); {
		switch {
		case eqFold(rest[i], "ENTRIESADDED") && i+1 < len(rest):
			v, vok := parseUint(rest[i+1])
			if !vok {
				return entriesAdded, maxDeleted, false
			}
			entriesAdded = uintOpt{val: v, set: true}
			i += 2
		case eqFold(rest[i], "MAXDELETEDID") && i+1 < len(rest):
			id, idok := parseStreamID(rest[i+1])
			if !idok {
				return entriesAdded, maxDeleted, false
			}
			maxDeleted = idOpt{id: id, set: true}
			i += 2
		default:
			return entriesAdded, maxDeleted, false
		}
	}
	return entriesAdded, maxDeleted, true
}
