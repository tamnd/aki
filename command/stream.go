package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/resp"
)

// Stream error strings, kept verbatim to match Redis.
const (
	errStreamIDSmaller   = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	errStreamIDNotGT0    = "ERR The ID specified in XADD must be greater than 0-0"
	errStreamInvalidID   = "ERR Invalid stream ID specified as stream command argument"
	errStreamUnbalanced  = "ERR Unbalanced XREAD list of streams: for each stream key there should be an id"
	errStreamCountPos    = "ERR value is not an integer or out of range"
	errStreamTimeoutNeg  = "ERR timeout is negative"
	errStreamReadCountER = "ERR COUNT must be a positive integer"
)

// streamCommands returns the core stream entry-log commands. Trimming, XSETID,
// XINFO, consumer groups, and blocking reads land in later slices.
func streamCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "xadd", Group: GroupStream, Since: "5.0.0",
			Arity: -5, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXAdd},
		{Name: "xlen", Group: GroupStream, Since: "5.0.0",
			Arity: 2, Flags: FlagReadOnly | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXLen},
		{Name: "xrange", Group: GroupStream, Since: "5.0.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleXRange(ctx, false) }},
		{Name: "xrevrange", Group: GroupStream, Since: "5.0.0",
			Arity: -4, Flags: FlagReadOnly, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: func(ctx *Ctx) { handleXRange(ctx, true) }},
		{Name: "xdel", Group: GroupStream, Since: "5.0.0",
			Arity: -3, Flags: FlagWrite | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handleXDel},
		{Name: "xread", Group: GroupStream, Since: "5.0.0",
			Arity: -4, Flags: FlagReadOnly | FlagBlocking, FirstKey: 0, LastKey: 0, Step: 0,
			Handler: handleXRead},
	}
}

// autoID generates the next ID for the * form from the clock and the stream's
// last ID, keeping the sequence monotone when the clock does not advance.
func autoID(s *stream, now uint64) streamID {
	if now > s.lastID.ms {
		return streamID{ms: now, seq: 0}
	}
	return streamID{ms: s.lastID.ms, seq: s.lastID.seq + 1}
}

// autoSeqID generates the sequence for the ms-* form: zero for a fresh
// millisecond, or one past the last sequence within the same millisecond.
func autoSeqID(s *stream, ms uint64) (streamID, bool) {
	if ms < s.lastID.ms {
		return streamID{}, false
	}
	if ms == s.lastID.ms {
		if s.lastID.seq == ^uint64(0) {
			return streamID{}, false
		}
		return streamID{ms: ms, seq: s.lastID.seq + 1}, true
	}
	return streamID{ms: ms, seq: 0}, true
}

// resolveStreamID turns the XADD id argument into a concrete ID, handling *,
// ms-*, a bare ms, and a full ms-seq, and validating monotonicity.
func resolveStreamID(s *stream, raw string, now uint64) (streamID, string) {
	if raw == "*" {
		return autoID(s, now), ""
	}
	if strings.HasSuffix(raw, "-*") {
		ms, err := strconv.ParseUint(raw[:len(raw)-2], 10, 64)
		if err != nil {
			return streamID{}, errStreamInvalidID
		}
		id, ok := autoSeqID(s, ms)
		if !ok {
			return streamID{}, errStreamIDSmaller
		}
		return id, ""
	}
	id, ok := parseStreamID(raw, 0)
	if !ok {
		return streamID{}, errStreamInvalidID
	}
	if id.ms == 0 && id.seq == 0 {
		return streamID{}, errStreamIDNotGT0
	}
	if !s.lastID.less(id) {
		return streamID{}, errStreamIDSmaller
	}
	return id, ""
}

// handleXAdd implements XADD key [NOMKSTREAM] id field value [field value ...].
// Trimming options are parsed and rejected for now; they arrive in the trim
// slice.
func handleXAdd(ctx *Ctx) {
	argv := ctx.Argv
	key := argv[1]
	i := 2
	noMkStream := false
	if i < len(argv) && strings.EqualFold(string(argv[i]), "NOMKSTREAM") {
		noMkStream = true
		i++
	}
	// Trimming clauses are not handled in this slice.
	if i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "MAXLEN", "MINID":
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	if i >= len(argv) {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xadd' command")
		return
	}
	rawID := string(argv[i])
	i++
	rest := argv[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		ctx.enc().WriteError("ERR wrong number of arguments for 'xadd' command")
		return
	}

	now := uint64(keyspace.NowMillis())
	var (
		newID    streamID
		mkMissed bool
		wrongTyp bool
		replyErr string
	)
	if !ctx.update(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if !found {
			if noMkStream {
				mkMissed = true
				return nil
			}
			s = &stream{}
		}
		id, errStr := resolveStreamID(s, rawID, now)
		if errStr != "" {
			replyErr = errStr
			return nil
		}
		fields := make([][]byte, len(rest))
		for j, b := range rest {
			cp := make([]byte, len(b))
			copy(cp, b)
			fields[j] = cp
		}
		s.entries = append(s.entries, streamEntry{id: id, fields: fields})
		s.lastID = id
		s.entriesAdded++
		newID = id
		ttl := int64(-1)
		if found {
			ttl = keepTTL(hdr, found)
		}
		return storeStream(db, key, s, ttl)
	}) {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case replyErr != "":
		ctx.enc().WriteError(replyErr)
	case mkMissed:
		ctx.enc().WriteNull()
	default:
		ctx.enc().WriteBulkStringStr(newID.String())
	}
}

// handleXLen implements XLEN key.
func handleXLen(ctx *Ctx) {
	var (
		n        int64
		wrongTyp bool
	)
	if !ctx.view(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, ctx.Argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if found {
			n = int64(len(s.entries))
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(n)
}

// handleXRange implements XRANGE and XREVRANGE. For XREVRANGE the argument order
// is end then start, and the result is emitted high-to-low.
func handleXRange(ctx *Ctx, rev bool) {
	argv := ctx.Argv
	startArg, endArg := string(argv[2]), string(argv[3])
	if rev {
		startArg, endArg = endArg, startArg
	}
	start, ok := parseRangeStart(startArg)
	if !ok {
		ctx.enc().WriteError(errStreamInvalidID)
		return
	}
	end, ok := parseRangeEnd(endArg)
	if !ok {
		ctx.enc().WriteError(errStreamInvalidID)
		return
	}

	count := int64(-1)
	if len(argv) > 4 {
		if len(argv) != 6 || !strings.EqualFold(string(argv[4]), "COUNT") {
			ctx.enc().WriteError("ERR syntax error")
			return
		}
		c, okc := parseInteger(argv[5])
		if !okc {
			ctx.enc().WriteError(errStreamCountPos)
			return
		}
		count = c
	}

	var (
		out      []streamEntry
		wrongTyp bool
	)
	if !ctx.view(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if found {
			// For reverse output the count caps the highest entries, so collect
			// the full range and trim after reversing.
			gather := count
			if rev {
				gather = -1
			}
			out = collectRange(s, start, end, gather)
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if rev {
		reverseEntries(out)
		if count >= 0 && int64(len(out)) > count {
			out = out[:count]
		}
	}
	writeEntries(ctx.enc(), out)
}

// collectRange returns the live entries within the inclusive or exclusive
// bounds, capped by count when count is non-negative, in ascending order.
func collectRange(s *stream, start, end rangeBound, count int64) []streamEntry {
	lo := s.lowerBound(start.id)
	if start.excl {
		for lo < len(s.entries) && s.entries[lo].id.equal(start.id) {
			lo++
		}
	}
	var out []streamEntry
	for i := lo; i < len(s.entries); i++ {
		id := s.entries[i].id
		if end.id.less(id) {
			break
		}
		if end.excl && id.equal(end.id) {
			break
		}
		out = append(out, s.entries[i])
		if count >= 0 && int64(len(out)) >= count {
			break
		}
	}
	return out
}

// reverseEntries flips an entry slice in place.
func reverseEntries(es []streamEntry) {
	for i, j := 0, len(es)-1; i < j; i, j = i+1, j-1 {
		es[i], es[j] = es[j], es[i]
	}
}

// writeEntries emits a list of stream entries in the [id, [f, v, ...]] shape
// XRANGE and XREAD share.
func writeEntries(enc *resp.Encoder, entries []streamEntry) {
	enc.WriteArrayLen(len(entries))
	for _, e := range entries {
		writeEntry(enc, e)
	}
}

// writeEntry emits one [id, [field, value, ...]] pair.
func writeEntry(enc *resp.Encoder, e streamEntry) {
	enc.WriteArrayLen(2)
	enc.WriteBulkStringStr(e.id.String())
	enc.WriteArrayLen(len(e.fields))
	for _, f := range e.fields {
		enc.WriteBulkString(f)
	}
}

// handleXDel implements XDEL key id [id ...]. It marks entries gone, decrements
// the live count, advances max-deleted-id, and deletes an emptied key.
func handleXDel(ctx *Ctx) {
	argv := ctx.Argv
	ids := make([]streamID, 0, len(argv)-2)
	for _, raw := range argv[2:] {
		id, ok := parseStreamID(string(raw), 0)
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		ids = append(ids, id)
	}

	var (
		deleted  int64
		wrongTyp bool
	)
	if !ctx.update(func(db *keyspace.DB) error {
		s, hdr, found, err := getStream(db, argv[1])
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeStream {
			wrongTyp = true
			return nil
		}
		if !found {
			return nil
		}
		for _, id := range ids {
			idx := s.findEntry(id)
			if idx < 0 {
				continue
			}
			s.entries = append(s.entries[:idx], s.entries[idx+1:]...)
			deleted++
			if s.maxDeletedID.less(id) {
				s.maxDeletedID = id
			}
		}
		if deleted == 0 {
			return nil
		}
		return storeStream(db, argv[1], s, keepTTL(hdr, found))
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	ctx.enc().WriteInteger(deleted)
}

// handleXRead implements XREAD [COUNT n] STREAMS key [key ...] id [id ...]. The
// BLOCK option is parsed but always reads non-blocking for now, returning null
// when nothing is available, which is the correct non-blocking result.
func handleXRead(ctx *Ctx) {
	argv := ctx.Argv
	i := 1
	count := int64(-1)
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "COUNT":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			c, ok := parseInteger(argv[i+1])
			if !ok || c <= 0 {
				ctx.enc().WriteError(errStreamReadCountER)
				return
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= len(argv) {
				ctx.enc().WriteError("ERR syntax error")
				return
			}
			ms, ok := parseInteger(argv[i+1])
			if !ok {
				ctx.enc().WriteError("ERR timeout is not an integer or out of range")
				return
			}
			if ms < 0 {
				ctx.enc().WriteError(errStreamTimeoutNeg)
				return
			}
			i += 2
		case "STREAMS":
			i++
			handleXReadStreams(ctx, argv[i:], count)
			return
		default:
			ctx.enc().WriteError("ERR syntax error")
			return
		}
	}
	ctx.enc().WriteError("ERR syntax error")
}

// handleXReadStreams reads the key-then-id half of the STREAMS clause and
// replies the per-stream entries that follow each given ID.
func handleXReadStreams(ctx *Ctx, rest [][]byte, count int64) {
	if len(rest) == 0 || len(rest)%2 != 0 {
		ctx.enc().WriteError(errStreamUnbalanced)
		return
	}
	n := len(rest) / 2
	keys := rest[:n]
	idArgs := rest[n:]

	starts := make([]streamID, n)
	for j := range n {
		raw := string(idArgs[j])
		if raw == "$" {
			// $ means deliver entries after the current last ID. Without a
			// stored stream the last ID is 0-0; it is resolved per key below.
			starts[j] = maxStreamID
			continue
		}
		if raw == "+" {
			starts[j] = maxStreamID
			continue
		}
		id, ok := parseStreamID(raw, 0)
		if !ok {
			ctx.enc().WriteError(errStreamInvalidID)
			return
		}
		starts[j] = id
	}

	type result struct {
		key     []byte
		entries []streamEntry
	}
	var results []result
	var wrongTyp bool
	if !ctx.view(func(db *keyspace.DB) error {
		for j := range n {
			s, hdr, found, err := getStream(db, keys[j])
			if err != nil {
				return err
			}
			if found && hdr.Type != keyspace.TypeStream {
				wrongTyp = true
				return nil
			}
			if !found {
				continue
			}
			start := starts[j]
			if string(idArgs[j]) == "$" {
				start = s.lastID
			}
			es := collectRange(s, rangeBound{id: start, excl: true}, rangeBound{id: maxStreamID}, count)
			if len(es) > 0 {
				results = append(results, result{key: keys[j], entries: es})
			}
		}
		return nil
	}) {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}

	enc := ctx.enc()
	if len(results) == 0 {
		enc.WriteNullArray()
		return
	}
	if enc.Proto() >= 3 {
		enc.WriteMapLen(len(results))
		for _, r := range results {
			enc.WriteBulkString(r.key)
			writeEntries(enc, r.entries)
		}
		return
	}
	enc.WriteArrayLen(len(results))
	for _, r := range results {
		enc.WriteArrayLen(2)
		enc.WriteBulkString(r.key)
		writeEntries(enc, r.entries)
	}
}
