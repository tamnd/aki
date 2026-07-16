package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XAUTOCLAIM, the scanning ownership transfer (spec 2064/f3/14 section 7.7): a
// recovery loop drains a stuck consumer's backlog in bounded slices without naming
// each id. It seeks the group PEL from a cursor, walks in id order claiming every
// entry idle at least min-idle-time for the target consumer, drops pending entries
// whose log entry an XDEL removed since (reported separately), and returns a cursor
// to resume from. The scan is budgeted at ten times COUNT so a PEL where idle
// entries are sparse still returns promptly, and each claim is the same in-place
// slab rewrite XCLAIM does, PEL-size-independent per id.

// Xautoclaim answers XAUTOCLAIM key group consumer min-idle-time start [COUNT n]
// [JUSTID]. It replies a three-element array: the resume cursor (0-0 at the end),
// the claimed entries as [id, [field value ...]] like XRANGE (or ids alone under
// JUSTID), and the ids dropped because their log entry no longer exists. A missing
// key or unknown group is NOGROUP.
func Xautoclaim(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name, conName := args[0], args[1], args[2]
	minIdle, ok := parseInt(args[3])
	if !ok {
		r.Err("ERR Invalid min-idle-time argument for XAUTOCLAIM")
		return
	}
	if minIdle < 0 {
		minIdle = 0
	}
	bnd, ok := parseBound(args[4], true)
	if !ok {
		r.Err(errInvalidID)
		return
	}
	start, ok := startAfter(bnd)
	if !ok {
		r.Err("ERR invalid start ID for the interval")
		return
	}
	count, justid, msg := parseAutoClaimOpts(args[5:])
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

	res := grp.autoClaim(s, start, con, cx.NowMs, minIdle, count, justid)
	// A pass that transferred at least one entry is an active fetch for the target
	// consumer; a pass that only dropped deleted entries or found nothing is not.
	if len(res.claimed) > 0 {
		con.activeTime = cx.NowMs
	}
	// The pass reassigns pending slabs to the target consumer and prunes deleted
	// entries; reconcile the footprint into the running sum.
	g.note(s)
	cx.Aux = frameAutoClaim(cx.Aux[:0], s, res, justid)
	r.Raw(cx.Aux)
}

// startAfter resolves an XAUTOCLAIM start bound to the first id the scan includes:
// an inclusive bound is itself, an exclusive "(id" bound is the next id, seq first
// then ms. ok is false only for an exclusive bound at the maximum id, which has no
// successor, matching Redis's "invalid start ID for the interval".
func startAfter(b bound) (streamID, bool) {
	if !b.excl {
		return b.id, true
	}
	return idAfter(b.id)
}

// idAfter returns the id immediately above id in (ms, seq) order, carrying a seq at
// its ceiling into the next ms. ok is false when id is the maximum id.
func idAfter(id streamID) (streamID, bool) {
	if id.seq != ^uint64(0) {
		return streamID{ms: id.ms, seq: id.seq + 1}, true
	}
	if id.ms != ^uint64(0) {
		return streamID{ms: id.ms + 1, seq: 0}, true
	}
	return streamID{}, false
}

// autoClaimMaxCount caps COUNT so the count*10 scan budget cannot overflow, the
// same guard Redis applies (LONG_MAX / attempts-factor).
const autoClaimMaxCount = int(^uint(0)>>1) / 10

// parseAutoClaimOpts reads the [COUNT n] [JUSTID] tail of XAUTOCLAIM. count defaults
// to 100 and must be a positive integer within the scan-budget cap; JUSTID renders
// ids only. It returns a Redis error text (empty on success): a non-integer or
// out-of-range COUNT is the "COUNT must be > 0" error Redis reports for both, any
// other token the syntax error.
func parseAutoClaimOpts(rest [][]byte) (count int, justid bool, msg string) {
	count = 100
	for i := 0; i < len(rest); {
		switch {
		case eqFold(rest[i], "COUNT") && i+1 < len(rest):
			n, ok := parseInt(rest[i+1])
			if !ok || n < 1 || n > int64(autoClaimMaxCount) {
				return 0, false, "ERR COUNT must be > 0"
			}
			count = int(n)
			i += 2
		case eqFold(rest[i], "JUSTID"):
			justid = true
			i++
		default:
			return 0, false, "ERR syntax error"
		}
	}
	return count, justid, ""
}

// frameAutoClaim appends the three-element XAUTOCLAIM reply: the resume cursor as an
// id bulk, the claimed entries (ids alone under JUSTID, else each as its current
// [id, [field value ...]] log entry like XRANGE), and the ids dropped because their
// log entry no longer exists. Every claimed id is live, so the field fetch finds it.
func frameAutoClaim(dst []byte, s *stream, res autoClaimResult, justid bool) []byte {
	dst = resp.AppendArrayHeader(dst, 3)
	dst = appendIDBulk(dst, res.cursor)
	dst = resp.AppendArrayHeader(dst, len(res.claimed))
	for _, id := range res.claimed {
		if justid {
			dst = appendIDBulk(dst, id)
			continue
		}
		fields, _ := s.entryAt(id)
		dst = appendEntryReply(dst, id, fields)
	}
	dst = resp.AppendArrayHeader(dst, len(res.deleted))
	for _, id := range res.deleted {
		dst = appendIDBulk(dst, id)
	}
	return dst
}
