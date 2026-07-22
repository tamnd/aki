package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XACKDEL, the acknowledge-and-delete (Redis 8.2, spec 2064/f3/14 section 7.5):
// XACK and XDELEX fused into one step. It retires each id from the named group's
// pending list, then deletes the entry from the stream under the same
// reference policy XDELEX uses, so a consumer can acknowledge and reclaim an
// entry's storage in a single command instead of an XACK followed by an XDELEX.
//
//	XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
//
// Only entries the group actually has pending are touched: an id not in the
// group PEL is reported -1 and neither acked nor deleted. For a pending id the
// ack always happens (it leaves the group and its consumer's PEL); what happens
// to the entry then follows the strategy. KEEPREF deletes it and leaves other
// groups' references dangling. DELREF deletes it and scrubs it from every other
// group PEL. ACKED deletes it only if no other group still references it, and
// reports it still-referenced otherwise. An entry already gone from the stream
// (an XDEL since delivery) is still reported deleted once acked.
//
// It replies an array of one status code per id, in argument order: 1 acked and
// deleted, 2 acked but not deleted because a group still references it (ACKED
// only), -1 the id was not pending in the group. A missing key or an unknown
// group reports -1 for every id. Every id is parsed before any mutation, the
// all-or-nothing parse XACK does, because the reply cannot be an error once some
// ids have been acked.

// Xackdel answers XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id [id ...]:
// ack each named entry out of the group and delete it under the chosen reference
// strategy, replying the per-id status array. A missing key or unknown group
// replies an array of -1, one per id.
func Xackdel(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name := args[0], args[1]
	strategy, idTokens, msg := parseAckDelArgs(args[2:])
	if msg != "" {
		r.Err(msg)
		return
	}

	g := registry(cx)
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}

	// All-or-nothing parse: validate every id before mutating, so a bad id late in
	// the list leaves the group and stream untouched.
	ids := make([]streamID, len(idTokens))
	for i := range idTokens {
		id, ok := parseStreamID(idTokens[i])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		ids[i] = id
	}

	grp := groupOf(s, name)
	if s == nil || grp == nil || grp.pel == nil {
		r.Raw(appendCodeArray(cx.Aux[:0], len(ids), xdelexNoID))
		return
	}

	out := resp.AppendArrayHeader(cx.Aux[:0], len(ids))
	var deleted int64
	for _, id := range ids {
		code := int64(xdelexNoID)
		// Only a pending id is acted on. The ack retires it from the group and its
		// consumer's PEL; a non-pending id is reported -1 and left alone.
		if grp.ackOne(id) {
			logPelDel(cx, key, name, id)
			canDelete := true
			switch strategy {
			case delStratAcked:
				// The id is already out of this group's PEL, so entryReferenced now
				// tests only the other groups: delete only if none still holds it.
				if s.entryReferenced(id) {
					canDelete = false
				}
			case delStratDelRef:
				s.cleanupEntryGroupRefs(cx, key, id)
			}
			if canDelete {
				if s.delete(id) {
					deleted++
					logDel(cx, key, id)
				}
				// An entry already gone from the stream is still reported deleted:
				// the ack is the delete from the caller's view.
				code = xdelexDeleted
			} else {
				code = xdelexStillRef
			}
		}
		out = resp.AppendInt(out, code)
	}
	cx.Aux = out

	if deleted > 0 {
		if s.kind == bandNative {
			g.markDirty(s)
		}
		cx.NotifyKeyspaceEvent(shard.NotifyStream, "xdel", key)
	}
	g.note(s)
	r.Raw(out)
}
