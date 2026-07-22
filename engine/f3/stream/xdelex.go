package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XDELEX, the reference-aware delete (Redis 8.2, spec 2064/f3/14 section 6.5):
// the same tombstone XDEL cuts, but with a policy for the entries a consumer
// group still holds pending. XDEL deletes an entry and leaves every group's PEL
// pointing at a now-gone id (a claim later finds it deleted); XDELEX lets the
// caller pick what happens to those references and reports, per id, whether the
// entry was deleted, still referenced, or never there.
//
//	XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
//
// The three strategies match Redis exactly. KEEPREF (the default) deletes the
// entry and leaves the pending references dangling, the historical XDEL
// behaviour. DELREF deletes the entry and scrubs it from every group PEL first,
// so no consumer is left holding a phantom. ACKED deletes only entries no group
// still references (fully acknowledged); an entry any group has pending is left
// in place and reported as still-referenced.
//
// It replies an array of one status code per id, in argument order: 1 the entry
// was deleted, 2 it was not deleted because a group still references it (ACKED
// only), -1 no such id in the stream. A missing key is an empty stream: every id
// reports -1. Every id is parsed before any mutation, so a malformed id fails the
// whole command with no partial effect, the all-or-nothing parse XDEL and XACK do.

// The consumer-group reference strategy an XDELEX/XACKDEL applies to the entries
// it deletes. delStratKeepRef is the default, matching Redis's DELETE_STRATEGY_*
// values (KEEPREF=1, DELREF=2, ACKED=3).
const (
	delStratKeepRef = iota + 1
	delStratDelRef
	delStratAcked
)

// Per-id XDELEX/XACKDEL status codes, the values the reply array carries.
const (
	xdelexNoID     = -1 // id not in the stream (XDELEX) or not in the group PEL (XACKDEL)
	xdelexDeleted  = 1  // entry deleted
	xdelexStillRef = 2  // entry not deleted, a group still references it (ACKED only)
)

// Xdelex answers XDELEX key [KEEPREF|DELREF|ACKED] IDS numids id [id ...]: delete
// each named entry under the chosen reference strategy and reply the per-id status
// array. A missing key replies an array of -1, one per id, the way an empty stream
// reports every id absent.
func Xdelex(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[0]
	strategy, idTokens, msg := parseAckDelArgs(args[1:])
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
	// the list leaves the stream untouched.
	ids := make([]streamID, len(idTokens))
	for i := range idTokens {
		id, ok := parseStreamID(idTokens[i])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		ids[i] = id
	}

	if s == nil {
		r.Raw(appendCodeArray(cx.Aux[:0], len(ids), xdelexNoID))
		return
	}

	out := resp.AppendArrayHeader(cx.Aux[:0], len(ids))
	var deleted int64
	for _, id := range ids {
		code := int64(xdelexNoID)
		canDelete := true
		switch strategy {
		case delStratAcked:
			// Only delete an entry no group still holds pending; otherwise report it
			// still-referenced and leave it in the stream.
			if s.entryReferenced(id) {
				canDelete = false
			}
		case delStratDelRef:
			// Scrub the entry from every group PEL before deleting it, so no consumer
			// is left holding a reference to a gone entry.
			s.cleanupEntryGroupRefs(cx, key, id)
		}
		if canDelete {
			if s.delete(id) {
				deleted++
				logDel(cx, key, id)
				code = xdelexDeleted
			}
		} else {
			code = xdelexStillRef
		}
		out = resp.AppendInt(out, code)
	}
	cx.Aux = out

	if deleted > 0 {
		// A tombstone in a native sealed block accrues dead bytes the gc pass reclaims;
		// mark the stream so the owner's maintainer visits it, exactly as XDEL does.
		if s.kind == bandNative {
			g.markDirty(s)
		}
		cx.NotifyKeyspaceEvent(shard.NotifyStream, "xdel", key)
	}
	g.note(s)
	r.Raw(out)
}

// parseAckDelArgs parses the strategy-and-IDS tail shared by XDELEX (tail after
// the key) and XACKDEL (tail after the key and group): an optional
// KEEPREF|DELREF|ACKED, then the required IDS numids id... clause, in either order.
// It returns the resolved strategy (KEEPREF when none was given), the id tokens,
// and a Redis error text (empty on success): a non-integer or non-positive numids,
// an id count that does not match numids, an unknown option, or a missing IDS
// keyword each map to their exact Redis message. It matches Redis's
// streamParseAckDelArgsOrReply.
func parseAckDelArgs(tail [][]byte) (strategy int, ids [][]byte, msg string) {
	sawIDS := false
	for i := 0; i < len(tail); {
		opt := tail[i]
		switch {
		case eqFold(opt, "KEEPREF") && strategy == 0:
			strategy = delStratKeepRef
			i++
		case eqFold(opt, "DELREF") && strategy == 0:
			strategy = delStratDelRef
			i++
		case eqFold(opt, "ACKED") && strategy == 0:
			strategy = delStratAcked
			i++
		case eqFold(opt, "IDS") && i+1 < len(tail):
			n, ok := parseInt(tail[i+1])
			if !ok {
				return 0, nil, errNotInt
			}
			if n < 1 {
				return 0, nil, "ERR Number of IDs must be a positive integer"
			}
			start := i + 2
			if n > int64(len(tail)-start) {
				return 0, nil, "ERR The `numids` parameter must match the number of arguments"
			}
			ids = tail[start : start+int(n)]
			sawIDS = true
			i = start + int(n)
		default:
			return 0, nil, "ERR syntax error"
		}
	}
	if !sawIDS {
		return 0, nil, "ERR IDS option is required"
	}
	if strategy == 0 {
		strategy = delStratKeepRef
	}
	return strategy, ids, ""
}

// appendCodeArray writes an n-element array of the same integer code, the reply an
// XDELEX or XACKDEL over a missing key returns (every id absent, code -1).
func appendCodeArray(dst []byte, n int, code int64) []byte {
	dst = resp.AppendArrayHeader(dst, n)
	for range n {
		dst = resp.AppendInt(dst, code)
	}
	return dst
}

// entryReferenced reports whether any consumer group still holds id in its PEL,
// the predicate XDELEX ACKED and XACKDEL ACKED gate deletion on (Redis's
// streamEntryIsReferenced). O(groups), a point PEL lookup per group; a stream with
// no groups is never referenced.
func (s *stream) entryReferenced(id streamID) bool {
	for _, grp := range s.groups {
		if grp.pel == nil {
			continue
		}
		if _, ok := grp.pel.find(id); ok {
			return true
		}
	}
	return false
}

// cleanupEntryGroupRefs removes id from every consumer group PEL that holds it,
// dropping each group's and owning consumer's pending count, the scrub XDELEX
// DELREF and XACKDEL DELREF do before deleting the entry (Redis's
// streamCleanupEntryCGroupRefs). Each removed reference cuts a pel-del effect so a
// replay retires the same pending entries. O(groups).
func (s *stream) cleanupEntryGroupRefs(cx *shard.Ctx, key []byte, id streamID) {
	for name, grp := range s.groups {
		if grp.pel == nil {
			continue
		}
		if grp.ackOne(id) {
			logPelDel(cx, key, []byte(name), id)
		}
	}
}
