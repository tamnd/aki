package stream

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// XNACK, the negative acknowledgement (spec 2064/f3/14 section 7.6, new in Redis
// 8.8): a consumer hands entries it cannot process back to the group so another
// consumer picks them up, without acking and without waiting out an idle timeout.
// Each named id is one PEL point op, the same in-place slab rewrite XCLAIM does: the
// entry is disowned (its consumer cleared, its idle clock reset to the epoch so any
// min-idle predicate matches it at once) and its delivery count is adjusted by the
// mode or an explicit RETRYCOUNT so retry accounting survives the nack. Nothing
// scans the stream and nothing moves between structures, so a nack is PEL-size
// independent per id.

// idsMax caps the numids argument at Redis's INT_MAX, the range
// getRangeLongFromObjectOrReply enforces before the count is compared to the
// remaining arguments.
const idsMax = int64(1<<31 - 1)

// Xnack answers XNACK key group <SILENT|FAIL|FATAL> IDS numids id [id ...]
// [RETRYCOUNT count] [FORCE]. It releases each pending id back to the group PEL as
// an unowned, immediately-claimable entry and replies the integer count actually
// nacked. An id not pending is skipped unless FORCE and its log entry still exists,
// in which case it is created unowned; an id whose log entry is gone is always
// skipped. A missing key or unknown group is NOGROUP; a wrong-typed key is
// WRONGTYPE.
func Xnack(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name := args[0], args[1]

	g := registry(cx)
	s, wrong := g.lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	grp := groupOf(s, name)
	if grp == nil {
		r.Err(nogroupGeneric(key, name))
		return
	}

	mode, ok := parseNackMode(args[2])
	if !ok {
		r.Err("ERR mode must be SILENT, FAIL, or FATAL")
		return
	}

	idTokens, retry, hasRetry, force, msg := parseNackOpts(args[3:])
	if msg != "" {
		r.Err(msg)
		return
	}

	// All-or-nothing parse: a bad id anywhere leaves the PEL untouched, so validate
	// every id before mutating, the way XACK and XCLAIM do.
	ids := make([]streamID, len(idTokens))
	for i := range idTokens {
		id, ok := parseStreamID(idTokens[i])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		ids[i] = id
	}

	var nacked int64
	for _, id := range ids {
		if grp.nack(s, id, mode, retry, hasRetry, force) {
			nacked++
		}
	}
	// A FORCE nack can add a pending slab for a not-yet-pending entry; reconcile the
	// footprint into the running sum.
	g.note(s)
	r.Int(nacked)
}

// parseNackMode reads the required SILENT/FAIL/FATAL positional. ok is false for any
// other token, which the caller renders as the mode error.
func parseNackMode(arg []byte) (nackMode, bool) {
	switch {
	case eqFold(arg, "SILENT"):
		return nackSilent, true
	case eqFold(arg, "FAIL"):
		return nackFail, true
	case eqFold(arg, "FATAL"):
		return nackFatal, true
	default:
		return 0, false
	}
}

// parseNackOpts reads the XNACK option tail after the mode: the required IDS clause
// (a positive numids and exactly that many following id tokens), an optional
// RETRYCOUNT, and an optional FORCE, in any order. It returns the id tokens and the
// parsed options, or a Redis error text (empty on success): a non-positive numids,
// an id count that does not match numids, a negative RETRYCOUNT, an unknown option,
// or a missing IDS keyword each map to their exact Redis message.
func parseNackOpts(rest [][]byte) (ids [][]byte, retry int64, hasRetry, force bool, msg string) {
	retry = -1
	sawIDS := false
	for i := 0; i < len(rest); {
		switch {
		case eqFold(rest[i], "IDS") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok || n < 1 || n > idsMax {
				return nil, 0, false, false, "ERR numids must be a positive integer"
			}
			start := i + 2
			if int64(len(rest)-start) < n {
				return nil, 0, false, false, "ERR number of IDs doesn't match numids"
			}
			ids = rest[start : start+int(n)]
			sawIDS = true
			i = start + int(n)
		case eqFold(rest[i], "RETRYCOUNT") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok {
				return nil, 0, false, false, errNotInt
			}
			if n < 0 {
				return nil, 0, false, false, "ERR Invalid RETRYCOUNT value, must be >= 0"
			}
			retry, hasRetry = n, true
			i += 2
		case eqFold(rest[i], "FORCE"):
			force = true
			i++
		default:
			return nil, 0, false, false, "ERR Unrecognized XNACK option '" + string(rest[i]) + "'"
		}
	}
	if !sawIDS {
		return nil, 0, false, false, "ERR syntax error, expected IDS keyword"
	}
	return ids, retry, hasRetry, force, ""
}
