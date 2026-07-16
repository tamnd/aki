package stream

import (
	"sort"
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// XPENDING, the pending-entries introspection (spec 2064/f3/14 section 7.7). The
// summary form reads the maintained counts and two tree end-peeks, O(consumers);
// the extended form seeks the PEL tree to start and walks to count, filtering by
// idle time and optionally by a single consumer, O(log p + n). Both are pure reads
// of the owner-local ledger; neither touches the stream log.

// Xpending answers the two XPENDING forms. Summary: XPENDING key group replies
// [total, min-id, max-id, [[consumer, count] ...]]. Extended: XPENDING key group
// [IDLE ms] start end count [consumer] replies one [id, consumer, idle-ms,
// delivery-count] row per pending entry in range. A missing key or unknown group
// is NOGROUP.
func Xpending(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key, name := args[0], args[1]
	s, wrong := registry(cx).lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	grp := groupOf(s, name)
	if grp == nil {
		r.Err(nogroupGeneric(key, name))
		return
	}

	if len(args) == 2 {
		xpendingSummary(cx, grp, r)
		return
	}
	xpendingExtended(cx, s, grp, args[2:], r)
}

// xpendingSummary emits the four-element summary: the total pending count, the
// least and greatest pending IDs (nil when none), and the per-consumer breakdown
// (nil when none), consumers in name order to match Redis's sorted iteration.
func xpendingSummary(cx *shard.Ctx, grp *streamGroup, r shard.Reply) {
	out := resp.AppendArrayHeader(cx.Aux[:0], 4)
	out = resp.AppendInt(out, int64(grp.pelCount))

	if grp.pelCount == 0 {
		out = resp.AppendNull(out)
		out = resp.AppendNull(out)
		out = resp.AppendNullArray(out)
		cx.Aux = out
		r.Raw(out)
		return
	}

	out = appendIDBulk(out, grp.pel.minEntry().id)
	out = appendIDBulk(out, grp.pel.maxEntry().id)

	owners := make([]*streamConsumer, 0, len(grp.consumers))
	for _, con := range grp.consumers {
		if con.pelCount > 0 {
			owners = append(owners, con)
		}
	}
	sort.Slice(owners, func(i, j int) bool { return string(owners[i].name) < string(owners[j].name) })

	out = resp.AppendArrayHeader(out, len(owners))
	for _, con := range owners {
		out = resp.AppendArrayHeader(out, 2)
		out = resp.AppendBulk(out, con.name)
		// Redis renders the per-consumer count as a bulk string, not an integer.
		out = resp.AppendBulk(out, strconv.AppendInt(nil, int64(con.pelCount), 10))
	}
	cx.Aux = out
	r.Raw(out)
}

// xpendingExtended emits the [id, consumer, idle-ms, delivery-count] rows for the
// pending entries in [start, end], up to count, at least min-idle old, optionally
// only a named consumer's. A malformed clause is a syntax or ID error; a negative
// or zero count yields the empty array.
func xpendingExtended(cx *shard.Ctx, s *stream, grp *streamGroup, rest [][]byte, r shard.Reply) {
	f, msg := parsePendingFilter(rest)
	if msg != "" {
		r.Err(msg)
		return
	}

	// Filter to a single consumer's ordinal when named; an unknown consumer owns
	// nothing, so the walk still runs and yields the empty array.
	var onlyOrd uint32
	filterOrd := false
	if f.consumer != nil {
		filterOrd = true
		if con := grp.consumer(f.consumer); con != nil {
			onlyOrd = con.ord
		} else {
			onlyOrd = ^uint32(0) // an ordinal no slab holds
		}
	}

	rows := make([]*pelEntry, 0, minInt(f.count, 64))
	if f.count > 0 && grp.pel != nil {
		grp.pel.walkFrom(f.lo.id, func(pe *pelEntry) bool {
			if !belowHi(pe.id, f.hi) {
				return false
			}
			if !aboveLo(pe.id, f.lo) {
				return true
			}
			if filterOrd && pe.consumerOrd != onlyOrd {
				return true
			}
			// The min-idle floor gates owned entries only; an unowned NACK (XNACK,
			// section 7.6) is always reported, matching Redis's `nack->consumer &&
			// minidle` guard.
			if pe.consumerOrd != noOwner && cx.NowMs-pe.deliveryTime < f.minIdle {
				return true
			}
			rows = append(rows, pe)
			return len(rows) < f.count
		})
	}

	out := resp.AppendArrayHeader(cx.Aux[:0], len(rows))
	for _, pe := range rows {
		out = resp.AppendArrayHeader(out, 4)
		out = appendIDBulk(out, pe.id)
		// An unowned NACK renders an empty consumer name and a -1 idle, the sentinel
		// Redis reports for an entry no consumer holds.
		if pe.consumerOrd == noOwner {
			out = resp.AppendBulk(out, nil)
			out = resp.AppendInt(out, -1)
		} else {
			out = resp.AppendBulk(out, grp.consumerByOrd[pe.consumerOrd].name)
			out = resp.AppendInt(out, cx.NowMs-pe.deliveryTime)
		}
		out = resp.AppendInt(out, int64(pe.deliveryCount))
	}
	cx.Aux = out
	r.Raw(out)
}

// pendingFilter is the parsed extended-XPENDING clause: the min-idle floor in ms,
// the [lo, hi] ID window, the row cap, and an optional single consumer.
type pendingFilter struct {
	minIdle  int64
	lo, hi   bound
	count    int
	consumer []byte
}

// parsePendingFilter reads the [IDLE ms] start end count [consumer] tail. It
// returns the filter and a Redis error text (empty on success): a bad IDLE or
// count is the integer error, a bad start or end the ID error, a wrong token count
// the syntax error. A negative count clamps to zero (the empty result).
func parsePendingFilter(rest [][]byte) (f pendingFilter, msg string) {
	i := 0
	if len(rest) > 0 && eqFold(rest[0], "IDLE") {
		if len(rest) < 2 {
			return f, "ERR syntax error"
		}
		n, ok := parseInt(rest[1])
		if !ok {
			return f, "ERR value is not an integer or out of range"
		}
		f.minIdle = n
		i = 2
	}
	body := rest[i:]
	if len(body) < 3 || len(body) > 4 {
		return f, "ERR syntax error"
	}
	lo, ok := parseBound(body[0], true)
	if !ok {
		return f, errInvalidID
	}
	hi, ok := parseBound(body[1], false)
	if !ok {
		return f, errInvalidID
	}
	n, ok := parseInt(body[2])
	if !ok {
		return f, "ERR value is not an integer or out of range"
	}
	f.lo, f.hi = lo, hi
	if n < 0 {
		n = 0
	}
	f.count = int(n)
	if len(body) == 4 {
		f.consumer = body[3]
	}
	return f, ""
}

// minInt returns the smaller of two ints, used to size the row buffer without
// over-allocating on a huge COUNT.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseInt parses a signed base-10 integer, the grammar XPENDING's IDLE and count
// arguments take. ok is false on anything strconv rejects.
func parseInt(b []byte) (int64, bool) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
