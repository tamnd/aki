package sqlo1

// XPENDING, XCLAIM, and XAUTOCLAIM, the pending surface. The parse
// orders are pinned live against Redis 8.8 and differ per command:
// XPENDING and XAUTOCLAIM parse their arguments before any key
// lookup, so a malformed ID outranks WRONGTYPE, while XCLAIM resolves
// the key and group first, so WRONGTYPE and NOGROUP outrank its
// option errors. All three share the bare NOGROUP text without
// XREADGROUP's trailing clause.

import (
	"context"
	"math"
	"strconv"
	"strings"
)

const (
	errXclaimMinIdle     = "ERR Invalid min-idle-time argument for XCLAIM"
	errXclaimIdle        = "ERR Invalid IDLE option argument for XCLAIM"
	errXclaimTime        = "ERR Invalid TIME option argument for XCLAIM"
	errXclaimRetry       = "ERR Invalid RETRYCOUNT option argument for XCLAIM"
	errXautoclaimMinIdle = "ERR Invalid min-idle-time argument for XAUTOCLAIM"
	errXautoclaimCount   = "ERR COUNT must be > 0"
)

// pendingNoGroupErr renders the bare NOGROUP text XPENDING, XCLAIM,
// and XAUTOCLAIM share for a missing key or group.
func pendingNoGroupErr(reply []byte, key, group []byte) []byte {
	return AppendError(reply, "NOGROUP No such key '"+string(key)+"' or consumer group '"+string(group)+"'")
}

// xpendingCmd is XPENDING key group [[IDLE ms] start end count
// [consumer]]. The summary form takes exactly the key and group; four
// or five arguments read as a syntax error, and anything after the
// consumer is ignored, both pinned. The extended form's IDs and
// integers parse before the key resolves.
func (s *Server) xpendingCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 3 {
		return arityErr(reply, "XPENDING")
	}
	key, group := args[1], args[2]
	if len(args) == 3 {
		total, minID, maxID, cons, err := s.x.PendingSummary(ctx, key, group)
		if err == errStreamNoGroup {
			return pendingNoGroupErr(reply, key, group)
		}
		if err != nil {
			return storeErr(reply, err)
		}
		reply = AppendArray(reply, 4)
		reply = AppendInt(reply, int64(total))
		if total == 0 {
			reply = AppendNullBulk(reply)
			reply = AppendNullBulk(reply)
			return AppendNullArray(reply)
		}
		reply = appendStreamIDBulk(reply, minID)
		reply = appendStreamIDBulk(reply, maxID)
		reply = AppendArray(reply, len(cons))
		for i := range cons {
			reply = AppendArray(reply, 2)
			reply = AppendBulk(reply, cons[i].name)
			reply = AppendBulk(reply, strconv.AppendUint(nil, cons[i].n, 10))
		}
		return reply
	}
	if len(args) < 6 {
		return syntaxErr(reply)
	}
	i := 3
	minIdle := int64(0)
	if strings.EqualFold(string(args[3]), "IDLE") {
		n, ok := parseCanonicalInt(args[4])
		if !ok {
			return AppendError(reply, errNotInteger)
		}
		minIdle = n
		i = 5
	}
	if len(args) < i+3 {
		return syntaxErr(reply)
	}
	start, sx, ok := parseStreamRangeID(args[i], false)
	if !ok {
		return AppendError(reply, errInvalidStreamID)
	}
	if sx {
		next, ok := streamIDNext(start)
		if !ok {
			return AppendError(reply, errInvalidStreamID)
		}
		start = next
	}
	end, ex, ok := parseStreamRangeID(args[i+1], true)
	if !ok {
		return AppendError(reply, errInvalidStreamID)
	}
	if ex {
		prev, ok := streamIDPrev(end)
		if !ok {
			return AppendError(reply, errInvalidStreamID)
		}
		end = prev
	}
	count, ok := parseCanonicalInt(args[i+2])
	if !ok {
		return AppendError(reply, errNotInteger)
	}
	var consumer []byte
	if len(args) >= i+4 {
		consumer = args[i+3]
	}
	var rows []byte
	n := 0
	err := s.x.PendingExt(ctx, key, group, start, end, count, consumer, minIdle, now, func(id streamID, cons []byte, idle int64, dcount uint32) {
		rows = AppendArray(rows, 4)
		rows = appendStreamIDBulk(rows, id)
		rows = AppendBulk(rows, cons)
		rows = AppendInt(rows, idle)
		rows = AppendInt(rows, int64(dcount))
		n++
	})
	if err == errStreamNoGroup {
		return pendingNoGroupErr(reply, key, group)
	}
	if err != nil {
		return storeErr(reply, err)
	}
	reply = AppendArray(reply, n)
	return append(reply, rows...)
}

// xclaimCmd is XCLAIM key group consumer min-idle-time id... with the
// IDLE, TIME, RETRYCOUNT, FORCE, JUSTID, and LASTID options. IDs
// parse greedily after min-idle-time; the first token that is not an
// ID starts the options, and an option missing its argument falls to
// the unrecognized-option text, Redis's scan.
func (s *Server) xclaimCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 6 {
		return arityErr(reply, "XCLAIM")
	}
	key, group, consumer := args[1], args[2], args[3]
	minIdle, ok := parseCanonicalInt(args[4])
	if !ok {
		return AppendError(reply, errXclaimMinIdle)
	}
	if minIdle < 0 {
		minIdle = 0
	}
	// The key and group resolve before the IDs and options scan, the
	// pinned XCLAIM order.
	if err := s.x.ReadGroupCheck(ctx, key, group); err != nil {
		if err == errStreamNoGroup {
			return pendingNoGroupErr(reply, key, group)
		}
		return storeErr(reply, err)
	}
	ids := make([]streamID, 0, len(args)-5)
	i := 5
	for ; i < len(args); i++ {
		mode, id, ok := parseStreamXaddID(args[i])
		if !ok || mode != xidExplicit {
			break
		}
		ids = append(ids, id)
	}
	o := streamClaimOpts{retry: -1}
	for ; i < len(args); i++ {
		tok := string(args[i])
		more := i+1 < len(args)
		switch {
		case strings.EqualFold(tok, "IDLE") && more:
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errXclaimIdle)
			}
			o.setIdle, o.idle = true, n
			i++
		case strings.EqualFold(tok, "TIME") && more:
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errXclaimTime)
			}
			o.setTime, o.time = true, n
			i++
		case strings.EqualFold(tok, "RETRYCOUNT") && more:
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errXclaimRetry)
			}
			// A negative RETRYCOUNT reads as unset, the pinned
			// fall-through to the plain delivery bump.
			o.retry = n
			i++
		case strings.EqualFold(tok, "LASTID") && more:
			mode, id, ok := parseStreamXaddID(args[i+1])
			if !ok || mode != xidExplicit {
				return AppendError(reply, errInvalidStreamID)
			}
			o.setLast, o.last = true, id
			i++
		case strings.EqualFold(tok, "FORCE"):
			o.force = true
		case strings.EqualFold(tok, "JUSTID"):
			o.justid = true
		default:
			return AppendError(reply, "ERR Unrecognized XCLAIM option '"+tok+"'")
		}
	}
	claimed, err := s.x.Claim(ctx, key, group, consumer, minIdle, ids, &o, now)
	if err == errStreamNoGroup {
		return pendingNoGroupErr(reply, key, group)
	}
	if err != nil {
		return storeErr(reply, err)
	}
	if o.justid {
		reply = AppendArray(reply, len(claimed))
		for _, id := range claimed {
			reply = appendStreamIDBulk(reply, id)
		}
		return reply
	}
	reply = AppendArray(reply, len(claimed))
	for _, id := range claimed {
		err := s.x.Range(ctx, key, id, id, 1, false, func(int) {}, func(rid streamID, fv [][]byte) {
			reply = appendStreamEntry(reply, rid, fv)
		})
		if err != nil {
			return storeErr(reply, err)
		}
	}
	return reply
}

// xautoclaimCmd is XAUTOCLAIM key group consumer min-idle-time start
// [COUNT count] [JUSTID]. Everything parses before the key resolves,
// so a malformed start outranks WRONGTYPE, and COUNT rejects zero,
// negatives, and non-integers with the same text, all pinned.
func (s *Server) xautoclaimCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 6 {
		return arityErr(reply, "XAUTOCLAIM")
	}
	key, group, consumer := args[1], args[2], args[3]
	minIdle, ok := parseCanonicalInt(args[4])
	if !ok {
		return AppendError(reply, errXautoclaimMinIdle)
	}
	if minIdle < 0 {
		minIdle = 0
	}
	start, sx, ok := parseStreamRangeID(args[5], false)
	if !ok {
		return AppendError(reply, errInvalidStreamID)
	}
	if sx {
		next, ok := streamIDNext(start)
		if !ok {
			return AppendError(reply, errInvalidStreamID)
		}
		start = next
	}
	count := int64(100)
	justid := false
	for i := 6; i < len(args); i++ {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok || n <= 0 || n > math.MaxInt64/10 {
				return AppendError(reply, errXautoclaimCount)
			}
			count = n
			i++
		case strings.EqualFold(string(args[i]), "JUSTID"):
			justid = true
		default:
			return syntaxErr(reply)
		}
	}
	cursor, claimed, deleted, err := s.x.AutoClaim(ctx, key, group, consumer, minIdle, start, count, justid, now)
	if err == errStreamNoGroup {
		return pendingNoGroupErr(reply, key, group)
	}
	if err != nil {
		return storeErr(reply, err)
	}
	reply = AppendArray(reply, 3)
	reply = appendStreamIDBulk(reply, cursor)
	reply = AppendArray(reply, len(claimed))
	for _, id := range claimed {
		if justid {
			reply = appendStreamIDBulk(reply, id)
			continue
		}
		err := s.x.Range(ctx, key, id, id, 1, false, func(int) {}, func(rid streamID, fv [][]byte) {
			reply = appendStreamEntry(reply, rid, fv)
		})
		if err != nil {
			return storeErr(reply, err)
		}
	}
	reply = AppendArray(reply, len(deleted))
	for _, id := range deleted {
		reply = appendStreamIDBulk(reply, id)
	}
	return reply
}
