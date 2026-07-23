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

// The trim clause's wire texts, Redis 8.8's exactly, trailing periods
// included.
const (
	errNotInteger      = "ERR value is not an integer or out of range"
	errMaxlenNegative  = "ERR The MAXLEN argument must be >= 0."
	errLimitNegative   = "ERR The LIMIT argument must be >= 0."
	errLimitNeedsTilde = "ERR syntax error, LIMIT cannot be used without the special ~ option"
	errTrimBothModes   = "ERR syntax error, MAXLEN and MINID options at the same time are not compatible"
)

// streamTrimSpec is one parsed trim clause, XADD's and XTRIM's shared
// grammar: MAXLEN|MINID [=|~] threshold [LIMIT n].
type streamTrimSpec struct {
	present bool
	byID    bool
	approx  bool
	maxlen  int64
	minid   streamID
	limit   int64 // resolved: 0 unlimited
}

// parseStreamTrimClause parses the clause starting at the strategy
// token args[i], which the caller already matched. It reports the index
// past the clause; short is true when the threshold ran off the end of
// args (the caller's arity error) and msg carries any other failure's
// wire text. Redis's parse quirks hold: a trailing ~ or = with nothing
// after it reads as the threshold, and the LIMIT checks run in the
// order missing-value, tilde, integer, sign.
func parseStreamTrimClause(args [][]byte, i int, spec *streamTrimSpec) (next int, short bool, msg string) {
	spec.present = true
	spec.byID = strings.EqualFold(string(args[i]), "MINID")
	spec.approx = false
	i++
	if i+1 < len(args) && len(args[i]) == 1 && (args[i][0] == '~' || args[i][0] == '=') {
		spec.approx = args[i][0] == '~'
		i++
	}
	if i >= len(args) {
		return i, true, ""
	}
	if spec.byID {
		mode, id, ok := parseStreamXaddID(args[i])
		if !ok || mode != xidExplicit {
			return i, false, errInvalidStreamID
		}
		spec.minid = id
	} else {
		n, ok := parseCanonicalInt(args[i])
		if !ok {
			return i, false, errNotInteger
		}
		if n < 0 {
			return i, false, errMaxlenNegative
		}
		spec.maxlen = n
	}
	i++
	spec.limit = 0
	if spec.approx {
		spec.limit = 100 * streamRunMaxEntries
	}
	if i < len(args) && strings.EqualFold(string(args[i]), "LIMIT") {
		if i+1 >= len(args) {
			return i, false, "ERR syntax error"
		}
		if !spec.approx {
			return i, false, errLimitNeedsTilde
		}
		n, ok := parseCanonicalInt(args[i+1])
		if !ok {
			return i, false, errNotInteger
		}
		if n < 0 {
			return i, false, errLimitNegative
		}
		spec.limit = n
		i += 2
	}
	return i, false, ""
}

// xaddCmd is XADD: options, the ID grammar, then the pair list. An
// unknown option token falls through to the ID parse and answers the
// invalid ID error, Redis's observed shape; NOMKSTREAM and the trim
// clause come in either order.
func (s *Server) xaddCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 5 {
		return arityErr(reply, "XADD")
	}
	i := 2
	noMk := false
	var trim streamTrimSpec
	for i < len(args) {
		tok := string(args[i])
		if strings.EqualFold(tok, "NOMKSTREAM") {
			noMk = true
			i++
			continue
		}
		if strings.EqualFold(tok, "MAXLEN") || strings.EqualFold(tok, "MINID") {
			if trim.present {
				return AppendError(reply, errTrimBothModes)
			}
			var short bool
			var msg string
			i, short, msg = parseStreamTrimClause(args, i, &trim)
			if short {
				return arityErr(reply, "XADD")
			}
			if msg != "" {
				return AppendError(reply, msg)
			}
			continue
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
	if trim.present {
		// The trim is the append's second op, Redis's order: MAXLEN 0
		// lands the entry and then empties the stream. R1 serializes
		// the pair.
		if _, err := s.x.Trim(ctx, args[1], trim.byID, trim.maxlen, trim.minid, trim.approx, trim.limit); err != nil {
			return storeErr(reply, err)
		}
	}
	return appendStreamIDBulk(reply, id)
}

// xtrimCmd is XTRIM, the trim clause against one key, replying the
// number of live entries removed. Parse failures beat both the missing
// key and WRONGTYPE, Redis's order, which the parse-then-call shape
// gives for free.
func (s *Server) xtrimCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "XTRIM")
	}
	if !strings.EqualFold(string(args[2]), "MAXLEN") && !strings.EqualFold(string(args[2]), "MINID") {
		return syntaxErr(reply)
	}
	var spec streamTrimSpec
	i, short, msg := parseStreamTrimClause(args, 2, &spec)
	if short {
		return arityErr(reply, "XTRIM")
	}
	if msg != "" {
		return AppendError(reply, msg)
	}
	if i != len(args) {
		if strings.EqualFold(string(args[i]), "MAXLEN") || strings.EqualFold(string(args[i]), "MINID") {
			return AppendError(reply, errTrimBothModes)
		}
		return syntaxErr(reply)
	}
	removed, err := s.x.Trim(ctx, args[1], spec.byID, spec.maxlen, spec.minid, spec.approx, spec.limit)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, removed)
}

// The XSETID option texts and the shared XINFO error shapes, Redis
// 8.8's exactly. The max-deleted check is part of the argument parse:
// it outranks the missing key and WRONGTYPE alike.
const (
	errXsetidMaxDel    = "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id"
	errEntriesAddedNeg = "ERR entries_added must be positive"
)

// xsetidCmd is XSETID key id [ENTRIESADDED n] [MAXDELETEDID id]: the
// root field rewrite. Duplicate options are legal and the last wins,
// Redis's loop shape.
func (s *Server) xsetidCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "XSETID")
	}
	mode, id, ok := parseStreamXaddID(args[2])
	if !ok || mode != xidExplicit {
		return AppendError(reply, errInvalidStreamID)
	}
	var setAdded, setMaxDel bool
	var added uint64
	var maxDel streamID
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return syntaxErr(reply)
		}
		switch {
		case strings.EqualFold(string(args[i]), "ENTRIESADDED"):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errNotInteger)
			}
			if n < 0 {
				return AppendError(reply, errEntriesAddedNeg)
			}
			setAdded, added = true, uint64(n)
		case strings.EqualFold(string(args[i]), "MAXDELETEDID"):
			m, mid, ok := parseStreamXaddID(args[i+1])
			if !ok || m != xidExplicit {
				return AppendError(reply, errInvalidStreamID)
			}
			setMaxDel, maxDel = true, mid
		default:
			return syntaxErr(reply)
		}
	}
	if setMaxDel && id.less(maxDel) {
		return AppendError(reply, errXsetidMaxDel)
	}
	if err := s.x.SetID(ctx, args[1], id, setAdded, added, setMaxDel, maxDel); err != nil {
		return storeErr(reply, err)
	}
	return AppendSimple(reply, "OK")
}

// unknownXinfo is the shared XINFO error for a bad subcommand or a
// malformed STREAM tail, echoing the offending token as typed.
func unknownXinfo(reply []byte, tok []byte) []byte {
	return AppendError(reply, "ERR unknown subcommand or wrong number of arguments for '"+string(tok)+"'. Try XINFO HELP.")
}

// xinfoCmd dispatches XINFO: STREAM key [FULL [COUNT n]], GROUPS key,
// CONSUMERS key group, and HELP. Too few arguments for a known
// subcommand is the container arity error; a malformed STREAM tail is
// the shared unknown text, Redis's split.
func (s *Server) xinfoCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	sub := string(args[1])
	switch {
	case strings.EqualFold(sub, "STREAM"):
		if len(args) < 3 {
			return arityErr(reply, "XINFO|STREAM")
		}
		return s.xinfoStreamCmd(ctx, reply, args)
	case strings.EqualFold(sub, "GROUPS"):
		if len(args) != 3 {
			return arityErr(reply, "XINFO|GROUPS")
		}
		if _, err := s.x.Info(ctx, args[2]); err != nil {
			return storeErr(reply, err)
		}
		// No group records exist before the group slice; a stream
		// without groups answers the empty array either way.
		return AppendArray(reply, 0)
	case strings.EqualFold(sub, "CONSUMERS"):
		if len(args) != 4 {
			return arityErr(reply, "XINFO|CONSUMERS")
		}
		if _, err := s.x.Info(ctx, args[2]); err != nil {
			return storeErr(reply, err)
		}
		return AppendError(reply, "NOGROUP No such consumer group '"+string(args[3])+"' for key name '"+string(args[2])+"'")
	case strings.EqualFold(sub, "HELP") && len(args) == 2:
		lines := []string{
			"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
			"CONSUMERS <key> <groupname>",
			"    Show consumers of <groupname>.",
			"GROUPS <key>",
			"    Show the stream consumer groups.",
			"STREAM <key> [FULL [COUNT <count>]",
			"    Show information about the stream.",
			"HELP",
			"    Print this help.",
		}
		reply = AppendArray(reply, len(lines))
		for _, l := range lines {
			reply = AppendSimple(reply, l)
		}
		return reply
	}
	return AppendError(reply, "ERR unknown subcommand '"+sub+"'. Try XINFO HELP.")
}

// appendStreamEntry replies one [id, [field value ...]] pair, the
// range rows and the XINFO entry fields.
func appendStreamEntry(reply []byte, id streamID, fv [][]byte) []byte {
	reply = AppendArray(reply, 2)
	reply = appendStreamIDBulk(reply, id)
	reply = AppendArray(reply, len(fv))
	for _, b := range fv {
		reply = AppendBulk(reply, b)
	}
	return reply
}

// appendStreamHeader replies the thirteen header pairs the summary and
// FULL forms share. The radix-tree pair is synthesized from the fence
// geometry, monotone and plausible; the idempotent-producer block is
// Redis 8.8's defaults since the feature has no state here.
func appendStreamHeader(reply []byte, info streamInfo, recorded streamID) []byte {
	reply = AppendBulk(reply, []byte("length"))
	reply = AppendInt(reply, int64(info.count))
	reply = AppendBulk(reply, []byte("radix-tree-keys"))
	reply = AppendInt(reply, info.geom)
	reply = AppendBulk(reply, []byte("radix-tree-nodes"))
	reply = AppendInt(reply, info.geom+1)
	reply = AppendBulk(reply, []byte("last-generated-id"))
	reply = appendStreamIDBulk(reply, info.last)
	reply = AppendBulk(reply, []byte("max-deleted-entry-id"))
	reply = appendStreamIDBulk(reply, info.maxDel)
	reply = AppendBulk(reply, []byte("entries-added"))
	reply = AppendInt(reply, int64(info.added))
	reply = AppendBulk(reply, []byte("recorded-first-entry-id"))
	reply = appendStreamIDBulk(reply, recorded)
	reply = AppendBulk(reply, []byte("idmp-duration"))
	reply = AppendInt(reply, 100)
	reply = AppendBulk(reply, []byte("idmp-maxsize"))
	reply = AppendInt(reply, 100)
	reply = AppendBulk(reply, []byte("pids-tracked"))
	reply = AppendInt(reply, 0)
	reply = AppendBulk(reply, []byte("iids-tracked"))
	reply = AppendInt(reply, 0)
	reply = AppendBulk(reply, []byte("iids-added"))
	reply = AppendInt(reply, 0)
	reply = AppendBulk(reply, []byte("iids-duplicates"))
	reply = AppendInt(reply, 0)
	return reply
}

// xinfoStreamCmd answers XINFO STREAM key [FULL [COUNT n]]: sixteen
// pairs for the summary, fifteen for FULL with its COUNT-bounded entry
// window (default 10, 0 unbounded, negatives folded to the default)
// and the still-empty groups array.
func (s *Server) xinfoStreamCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	full := false
	count := int64(10)
	if len(args) > 3 {
		if !strings.EqualFold(string(args[3]), "FULL") {
			return unknownXinfo(reply, args[1])
		}
		full = true
		switch {
		case len(args) == 4:
		case len(args) == 6 && strings.EqualFold(string(args[4]), "COUNT"):
			n, ok := parseCanonicalInt(args[5])
			if !ok {
				return AppendError(reply, errNotInteger)
			}
			switch {
			case n < 0:
				count = 10
			case n == 0:
				count = -1
			default:
				count = n
			}
		default:
			return unknownXinfo(reply, args[1])
		}
	}
	info, err := s.x.Info(ctx, args[2])
	if err != nil {
		return storeErr(reply, err)
	}

	// The recorded-first-entry-id and the summary's end peeks render
	// into scratch first: fv is only valid inside emit, and the pair
	// order puts the recorded ID before the entries that reveal it.
	var recorded streamID
	var firstEnt, lastEnt []byte
	found, err := s.x.EntryPeek(ctx, args[2], false, func(id streamID, fv [][]byte) {
		recorded = id
		firstEnt = appendStreamEntry(firstEnt, id, fv)
	})
	if err != nil {
		return storeErr(reply, err)
	}
	if found && !full {
		_, err = s.x.EntryPeek(ctx, args[2], true, func(id streamID, fv [][]byte) {
			lastEnt = appendStreamEntry(lastEnt, id, fv)
		})
		if err != nil {
			return storeErr(reply, err)
		}
	}

	if full {
		reply = AppendArray(reply, 30)
		reply = appendStreamHeader(reply, info, recorded)
		reply = AppendBulk(reply, []byte("entries"))
		mark := len(reply)
		err := s.x.Range(ctx, args[2], streamID{}, streamID{ms: math.MaxUint64, seq: math.MaxUint64}, count, false, func(n int) {
			reply = AppendArray(reply, n)
		}, func(id streamID, fv [][]byte) {
			reply = appendStreamEntry(reply, id, fv)
		})
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		reply = AppendBulk(reply, []byte("groups"))
		return AppendArray(reply, 0)
	}

	reply = AppendArray(reply, 32)
	reply = appendStreamHeader(reply, info, recorded)
	reply = AppendBulk(reply, []byte("groups"))
	reply = AppendInt(reply, int64(info.groups))
	reply = AppendBulk(reply, []byte("first-entry"))
	if found {
		reply = append(reply, firstEnt...)
	} else {
		reply = AppendNullBulk(reply)
	}
	reply = AppendBulk(reply, []byte("last-entry"))
	if len(lastEnt) > 0 {
		reply = append(reply, lastEnt...)
	} else {
		reply = AppendNullBulk(reply)
	}
	return reply
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
		reply = appendStreamEntry(reply, id, fv)
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}
