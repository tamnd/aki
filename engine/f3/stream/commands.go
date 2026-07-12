package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream command surface over the inline and native bands (spec 2064/f3/14
// section 6). Every handler runs on its shard's owner goroutine, so the registry
// and every stream in it are plain single-owner state, the same discipline the
// other collection types keep. This slice lands the write path: XADD, XLEN, XDEL,
// XSETID. XRANGE and the read path arrive with the counted directory (slice 3).

// Xadd answers XADD key [NOMKSTREAM] <id> field value [field value ...]: allocate
// the entry ID (auto, partial, or explicit, section 3.6), append the entry, and
// reply the assigned ID as a bulk string. NOMKSTREAM replies nil without creating
// the key when it is absent. The trim clause (MAXLEN/MINID) and LIMIT arrive with
// XTRIM in slice 4; a request carrying one is refused here rather than silently
// ignored. A malformed or non-increasing ID is a client error and creates
// nothing.
func Xadd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	i := 1
	nomkstream := false
	if i < len(args) && eqFold(args[i], "NOMKSTREAM") {
		nomkstream = true
		i++
	}
	// The trim keywords are not parsed yet; reject rather than misread the ID.
	if i < len(args) && (eqFold(args[i], "MAXLEN") || eqFold(args[i], "MINID") || eqFold(args[i], "LIMIT")) {
		r.Err("ERR stream trimming is not supported yet")
		return
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

	cx.Val = formatID(cx.Val[:0], id)
	r.Bulk(cx.Val)
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
	s, wrong := registry(cx).lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Int(0)
		return
	}
	var n int64
	for _, id := range ids {
		if s.delete(id) {
			n++
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
