package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XCLAIM, the explicit ownership transfer (spec 2064/f3/14 section 7.7): a
// recovery client reassigns pending entries a dead or stuck consumer left behind
// to a live one. Each named ID is one point op on the group PEL: a tree probe by
// ID, an idle check against the entry's delivery clock, then an in-place slab
// rewrite that swaps the owner, stamps the delivery time, and bumps the retry
// count. Nothing scans the stream and nothing moves between structures, so a claim
// is PEL-size-independent (prediction P4). FORCE creates a pending slab for an
// entry that exists in the log but was never delivered to the group; JUSTID
// suppresses the count bump and renders IDs only. A pending entry whose log entry
// an XDEL removed since is dropped from the PEL rather than claimed, and XCLAIM
// simply omits it from the reply (XAUTOCLAIM reports such IDs separately).

// Xclaim answers XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms]
// [TIME unix-ms] [RETRYCOUNT n] [FORCE] [JUSTID]. It replies an array of the
// claimed entries, each [id, [field value ...]] like XRANGE, or just the IDs under
// JUSTID. A missing key or unknown group is NOGROUP.
func Xclaim(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, conName := args[0], args[1], args[2]
	minIdle, ok := parseInt(args[3])
	if !ok {
		r.Err("ERR Invalid min-idle-time argument for XCLAIM")
		return
	}
	ids, opts, msg := parseClaim(args[4:])
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
	grp := groupOf(s, name)
	if grp == nil {
		r.Err(nogroupGeneric(key, name))
		return
	}
	con := grp.ensureConsumer(conName, cx.NowMs)
	con.seenTime = cx.NowMs

	claimed := make([]streamID, 0, len(ids))
	for _, id := range ids {
		if res := grp.claimOne(s, id, con, cx.NowMs, minIdle, opts); res.claimed {
			claimed = append(claimed, res.id)
		}
	}
	// Taking at least one entry is an active operation for the target consumer, so it
	// advances the active clock; a claim that took nothing leaves it, as Redis stamps
	// active_time per claimed entry only.
	if len(claimed) > 0 {
		con.activeTime = cx.NowMs
	}

	// A claim can create the target consumer and, under FORCE, add a pending slab for
	// a not-yet-pending entry; reconcile the footprint into the running sum.
	g.note(s)
	cx.Aux = frameClaim(cx.Aux[:0], s, claimed, opts.justid)
	r.Raw(cx.Aux)
}

// parseClaim reads the id list and the option tail of an XCLAIM (or the option
// tail of an XAUTOCLAIM, which passes no ids). IDs lead and are taken greedily
// until a token fails to parse as a stream ID, at which point the options begin,
// the same boundary Redis draws. It returns a Redis error text (empty on success):
// no id at all, or a bad option value, is a client error.
func parseClaim(rest [][]byte) (ids []streamID, opts xclaimOpts, msg string) {
	i := 0
	for i < len(rest) {
		id, ok := parseStreamID(rest[i])
		if !ok {
			break
		}
		ids = append(ids, id)
		i++
	}
	if len(ids) == 0 {
		return nil, opts, errInvalidID
	}
	if msg := parseClaimOpts(rest[i:], &opts); msg != "" {
		return nil, opts, msg
	}
	return ids, opts, ""
}

// parseClaimOpts reads the shared option tail of XCLAIM and XAUTOCLAIM into opts.
// It returns a Redis error text (empty on success).
func parseClaimOpts(rest [][]byte, opts *xclaimOpts) string {
	for i := 0; i < len(rest); {
		switch {
		case eqFold(rest[i], "IDLE") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok {
				return errNotInt
			}
			opts.hasIdle, opts.idleMs = true, n
			i += 2
		case eqFold(rest[i], "TIME") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok {
				return errNotInt
			}
			opts.hasTime, opts.timeMs = true, n
			i += 2
		case eqFold(rest[i], "RETRYCOUNT") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok {
				return errNotInt
			}
			opts.hasRetry, opts.retry = true, n
			i += 2
		case eqFold(rest[i], "FORCE"):
			opts.force = true
			i++
		case eqFold(rest[i], "JUSTID"):
			opts.justid = true
			i++
		default:
			return "ERR syntax error"
		}
	}
	return ""
}

// frameClaim appends the XCLAIM reply: the claimed IDs alone under JUSTID, else
// each as its current [id, [field value ...]] log entry, the same entry shape
// XRANGE renders. Every claimed ID is live (claimOne marks claimed only when the
// log entry exists), so the field fetch always finds its entry.
func frameClaim(dst []byte, s *stream, claimed []streamID, justid bool) []byte {
	dst = resp.AppendArrayHeader(dst, len(claimed))
	for _, id := range claimed {
		if justid {
			dst = appendIDBulk(dst, id)
			continue
		}
		fields, _ := s.entryAt(id)
		dst = appendEntryReply(dst, id, fields)
	}
	return dst
}

// errNotInt is Redis's reply for a non-integer option value on the claim path.
const errNotInt = "ERR value is not an integer or out of range"
