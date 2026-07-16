package stream

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// XACK, the acknowledgement (spec 2064/f3/14 section 7.5). Each acked ID is one
// hash probe to find the pending entry, one tree delete, and two counter
// decrements, the slab freed; the owning consumer is read straight from the slab,
// never a second lookup, which is the whole point of one group PEL with an owner
// field. An ID that is not pending (already acked, or never delivered) counts
// zero. A missing key or group is not an error, just nothing to ack.

// Xack answers XACK key group id [id ...]: retire the named pending entries and
// reply how many it actually removed. IDs that were not pending do not count. A
// missing key or an unknown group replies 0; only a wrong-typed key or a malformed
// ID is an error.
func Xack(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, ids := args[0], args[1], args[2:]

	g := registry(cx)
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	grp := groupOf(s, name)

	// Validate every ID before mutating, so a bad ID late in the list leaves the
	// PEL untouched, the all-or-nothing parse Redis does.
	parsed := make([]streamID, len(ids))
	for i := range ids {
		id, ok := parseStreamID(ids[i])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		parsed[i] = id
	}
	if grp == nil || grp.pel == nil {
		r.Int(0)
		return
	}

	var acked int64
	for _, id := range parsed {
		if grp.ackOne(id) {
			acked++
		}
	}
	// Retiring pending entries shrinks the group's ledger tree; reconcile the
	// footprint into the running sum at the boundary.
	g.note(s)
	r.Int(acked)
}

// ackOne removes id from the group PEL, dropping the group and owning consumer
// counts, and reports whether it removed a pending entry. A miss changes nothing.
func (grp *streamGroup) ackOne(id streamID) bool {
	ord, ok := grp.pel.ack(id)
	if !ok {
		return false
	}
	grp.pelCount--
	if int(ord) < len(grp.consumerByOrd) {
		if con := grp.consumerByOrd[ord]; con != nil {
			con.pelCount--
		}
	}
	return true
}
