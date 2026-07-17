package sqlo1

// The stream command surface, doc 10 wired to SRV: XADD, XLEN, and the
// range pair. The ID grammar parses here and the layer sees only the
// resolved modes, the shape every type surface follows.

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
)

// errInvalidStreamID is Redis's reply to any malformed ID argument,
// XADD and range bounds alike.
const errInvalidStreamID = "ERR Invalid stream ID specified as stream command argument"

// parseStreamUint parses a decimal uint64 ID part, digits only with an
// overflow check.
func parseStreamUint(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (math.MaxUint64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// parseStreamXaddID parses XADD's ID argument into the resolve mode:
// "*" fully auto, "ms-*" auto seq, "ms-seq" explicit, and a bare ms is
// ms-0, Redis's rule, not the auto-seq form.
func parseStreamXaddID(a []byte) (mode int, id streamID, ok bool) {
	if len(a) == 1 && a[0] == '*' {
		return xidAuto, streamID{}, true
	}
	msPart := a
	var seqPart []byte
	if i := bytes.IndexByte(a, '-'); i >= 0 {
		msPart, seqPart = a[:i], a[i+1:]
		if len(seqPart) == 1 && seqPart[0] == '*' {
			ms, ok := parseStreamUint(msPart)
			return xidAutoSeq, streamID{ms: ms}, ok
		}
	}
	ms, ok1 := parseStreamUint(msPart)
	seq, ok2 := uint64(0), true
	if seqPart != nil {
		seq, ok2 = parseStreamUint(seqPart)
	}
	return xidExplicit, streamID{ms: ms, seq: seq}, ok1 && ok2
}

// parseStreamRangeID parses one XRANGE bound: "-" the minimum, "+" the
// maximum, "(" marking an exclusive bound, and a bare ms defaulting its
// seq to 0 as a start and to the maximum as an end, so XRANGE 1 1 spans
// the whole millisecond. The infinities do not take "(", Redis's rule.
func parseStreamRangeID(a []byte, end bool) (id streamID, excl, ok bool) {
	if len(a) > 0 && a[0] == '(' {
		excl = true
		a = a[1:]
	}
	if len(a) == 1 && !excl {
		switch a[0] {
		case '-':
			return streamID{}, false, true
		case '+':
			return streamID{ms: math.MaxUint64, seq: math.MaxUint64}, false, true
		}
	}
	msPart := a
	var seqPart []byte
	if i := bytes.IndexByte(a, '-'); i >= 0 {
		msPart, seqPart = a[:i], a[i+1:]
	}
	ms, ok1 := parseStreamUint(msPart)
	seq, ok2 := uint64(0), true
	if seqPart != nil {
		seq, ok2 = parseStreamUint(seqPart)
	} else if end {
		seq = math.MaxUint64
	}
	return streamID{ms: ms, seq: seq}, excl, ok1 && ok2
}

// appendStreamIDBulk replies one entry ID in its ms-seq text form.
func appendStreamIDBulk(reply []byte, id streamID) []byte {
	var b [41]byte
	p := strconv.AppendUint(b[:0], id.ms, 10)
	p = append(p, '-')
	p = strconv.AppendUint(p, id.seq, 10)
	return AppendBulk(reply, p)
}

// xaddCmd is XADD: options, the ID grammar, then the pair list. An
// unknown option token falls through to the ID parse and answers the
// invalid ID error, Redis's observed shape.
func (s *Server) xaddCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 5 {
		return arityErr(reply, "XADD")
	}
	i := 2
	noMk := false
	for i < len(args) {
		tok := string(args[i])
		if strings.EqualFold(tok, "NOMKSTREAM") {
			noMk = true
			i++
			continue
		}
		if strings.EqualFold(tok, "MAXLEN") || strings.EqualFold(tok, "MINID") {
			// The trim slice lands next; refusing beats silently
			// keeping data the caller asked to drop.
			return AppendError(reply, "ERR XADD trim options are not implemented yet")
		}
		break
	}
	if i >= len(args) {
		return arityErr(reply, "XADD")
	}
	mode, req, ok := parseStreamXaddID(args[i])
	if !ok {
		return AppendError(reply, errInvalidStreamID)
	}
	pairs := args[i+1:]
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return arityErr(reply, "XADD")
	}
	id, added, err := s.x.Add(ctx, args[1], mode, req, now, noMk, pairs)
	if err != nil {
		return storeErr(reply, err)
	}
	if !added {
		return AppendNullBulk(reply)
	}
	return appendStreamIDBulk(reply, id)
}

// xrangeCmd is XRANGE and XREVRANGE, which takes its bounds reversed
// (end first) but answers the same window backward. An exclusive bound
// that cannot step inward is Redis's interval error, checked before any
// key access like the parse it is part of.
func (s *Server) xrangeCmd(ctx context.Context, reply []byte, args [][]byte, rev bool) []byte {
	cmd := "XRANGE"
	if rev {
		cmd = "XREVRANGE"
	}
	if len(args) != 4 && len(args) != 6 {
		if len(args) == 5 {
			return syntaxErr(reply)
		}
		return arityErr(reply, cmd)
	}
	startArg, endArg := args[2], args[3]
	if rev {
		startArg, endArg = endArg, startArg
	}
	start, startEx, ok1 := parseStreamRangeID(startArg, false)
	end, endEx, ok2 := parseStreamRangeID(endArg, true)
	if !ok1 || !ok2 {
		return AppendError(reply, errInvalidStreamID)
	}
	if startEx {
		if start.seq < math.MaxUint64 {
			start.seq++
		} else if start.ms < math.MaxUint64 {
			start = streamID{ms: start.ms + 1}
		} else {
			return AppendError(reply, "ERR invalid start ID for the interval")
		}
	}
	if endEx {
		if end.seq > 0 {
			end.seq--
		} else if end.ms > 0 {
			end = streamID{ms: end.ms - 1, seq: math.MaxUint64}
		} else {
			return AppendError(reply, "ERR invalid end ID for the interval")
		}
	}
	count := int64(-1)
	if len(args) == 6 {
		if !strings.EqualFold(string(args[4]), "COUNT") {
			return syntaxErr(reply)
		}
		c, ok := parseCanonicalInt(args[5])
		if !ok {
			return AppendError(reply, "ERR value is not an integer or out of range")
		}
		// COUNT 0 and negative counts answer the empty array, after
		// the type check the layer runs first so WRONGTYPE still wins.
		count = max(c, 0)
	}
	mark := len(reply)
	err := s.x.Range(ctx, args[1], start, end, count, rev, func(n int) {
		reply = AppendArray(reply, n)
	}, func(id streamID, fv [][]byte) {
		reply = AppendArray(reply, 2)
		reply = appendStreamIDBulk(reply, id)
		reply = AppendArray(reply, len(fv))
		for _, b := range fv {
			reply = AppendBulk(reply, b)
		}
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}
