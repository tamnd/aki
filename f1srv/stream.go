package f1srv

import (
	"encoding/binary"
	"strconv"
	"strings"
	"time"
)

// Stream is the fifth collection type on f1raw and the richest: a stream is an append-only log
// of ID-keyed entries plus (later slices) consumer-group machinery (spec 2064/f1_rewrite_ltm/09).
// This slice lands the log layer: every entry is its own record under the stream's entry family,
// sub-keyed by the entry's 16-byte big-endian ID, so the entry rows sort in ID order under plain
// byte comparison and a forward cursor over them is an XRANGE. A stream that takes a billion
// entries appends one row per XADD and answers a hundred-entry XRANGE window in a bounded handful
// of row reads, never a whole-log decode.
//
// The entry family follows the same length-prefixed composite-key shape the other collections use,
// plus a one-byte family tag ('e') so the shared ordered element index keeps a stream's entry rows
// apart from any sibling family (the group, consumer, and PEL families land in later slices, tagged
// 'g'/'c'/'p'). The entry key is uvarint(len(skey)) | skey | 'e' | 16-byte big-endian ID: the
// millisecond part big-endian then the sequence part big-endian, so byte order equals ID order,
// which is what makes XRANGE a seek-to-start then walk-forward with no per-entry comparison beyond
// the bound check, exactly as the zset score family makes ZRANGEBYSCORE a bounded walk.
//
// A per-stream header row (kindStreamMeta under the bare key) holds the live entry count, the last
// generated ID, the max deleted ID, and the entries-added counter, so XLEN is one header read and
// never a scan. Unlike list/set/hash/zset, a stream with zero entries still exists: the header
// persists at length 0 and only DEL/UNLINK/expiry removes the stream key (spec section 1.3). So the
// header writer here never deletes the header at count 0, which is the one place the stream breaks
// from the empty-is-no-key rule the other collections follow.
//
// Write serialization: XADD takes the per-key stripe lock (shared with the INCR family and the
// other collections) so an entry-row append and the header update stay consistent under concurrent
// writers. Reads (XLEN/XRANGE/XREVRANGE/XREAD) are lock-free.
const (
	kindStreamEntry byte = 0x06 // one entry row, keyed by the 16-byte big-endian ID, value is the field map
	kindStreamMeta  byte = 0x0C // the per-stream header row (coll_header)
)

// streamEntryTag is the family discriminator placed after the length-prefixed stream key, so a
// stream's entry rows sort together under one prefix in the shared ordered index. Later slices add
// sibling family tags for the group, consumer, and PEL rows.
const streamEntryTag byte = 'e'

// streamHeaderBytes is the fixed header value width: six little-endian u64 fields, in order
// length, last-id ms, last-id seq, max-deleted-id ms, max-deleted-id seq, entries-added. A
// fixed-width value lets the header rewrite land in place on every XADD.
const streamHeaderBytes = 48

// streamTrimBatch bounds how many entry rows one trim scan-and-delete round touches, so a huge
// trim never holds the index across the whole drop.
const streamTrimBatch = 512

// Stream error strings, kept verbatim to match Redis 8.8 and Valkey 9.1.
const (
	errStreamIDSmaller  = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	errStreamIDNotGT0   = "ERR The ID specified in XADD must be greater than 0-0"
	errStreamInvalidID  = "ERR Invalid stream ID specified as stream command argument"
	errStreamUnbalanced = "ERR Unbalanced XREAD list of streams: for each stream key there should be an id"
	errStreamCountER    = "ERR value is not an integer or out of range"
	errStreamReadCount  = "ERR COUNT must be a positive integer"
	errStreamTimeoutNeg = "ERR timeout is negative"
	errStreamTimeoutInt = "ERR timeout is not an integer or out of range"
	errStreamMaxLenArg  = "ERR invalid MAXLEN argument"
	errStreamMinIDArg   = "ERR invalid MINID argument"
	errStreamLimitZero  = "ERR The ~ prefix is not valid for MINID or MAXLEN when LIMIT is specified with value 0"
)

// streamID is a 128-bit entry ID: a millisecond timestamp and a sequence that breaks ties within
// the millisecond.
type streamID struct {
	ms  uint64
	seq uint64
}

// less reports whether a sorts before b, ms then seq.
func (a streamID) less(b streamID) bool {
	if a.ms != b.ms {
		return a.ms < b.ms
	}
	return a.seq < b.seq
}

// String renders the ID in the textual ms-seq form the wire uses.
func (a streamID) String() string {
	return strconv.FormatUint(a.ms, 10) + "-" + strconv.FormatUint(a.seq, 10)
}

// maxStreamID is the largest possible ID, the open upper bound '+' resolves to.
var maxStreamID = streamID{ms: ^uint64(0), seq: ^uint64(0)}

// parseStreamID parses a full ms-seq or partial ms ID; a partial ID takes defaultSeq for the
// missing sequence, which lets a range bound expand a bare ms to ms-0 (start) or ms-max (end).
func parseStreamID(s string, defaultSeq uint64) (streamID, bool) {
	msStr, seqStr, hasSeq := strings.Cut(s, "-")
	ms, err := strconv.ParseUint(msStr, 10, 64)
	if err != nil {
		return streamID{}, false
	}
	if !hasSeq {
		return streamID{ms: ms, seq: defaultSeq}, true
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return streamID{}, false
	}
	return streamID{ms: ms, seq: seq}, true
}

// putStreamID writes id's 16 big-endian bytes into dst[:16].
func putStreamID(dst []byte, id streamID) {
	binary.BigEndian.PutUint64(dst[0:8], id.ms)
	binary.BigEndian.PutUint64(dst[8:16], id.seq)
}

// decodeEntryID reads the ID off the tail of an entry-row composite key, whose last 16 bytes are
// the big-endian ID.
func decodeEntryID(entryKey []byte) streamID {
	t := entryKey[len(entryKey)-16:]
	return streamID{ms: binary.BigEndian.Uint64(t[0:8]), seq: binary.BigEndian.Uint64(t[8:16])}
}

// streamEntryKey builds the entry-family composite key for (skey, id) into the reused kbuf:
// uvarint(len(skey)) | skey | 'e' | 16-byte big-endian id.
func (c *connState) streamEntryKey(skey []byte, id streamID) []byte {
	b := c.kbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	b = append(b, streamEntryTag)
	var idb [16]byte
	putStreamID(idb[:], id)
	b = append(b, idb[:]...)
	c.kbuf = b
	return b
}

// streamEntryPrefix builds the entry-family enumeration prefix for skey into pbuf:
// uvarint(len(skey)) | skey | 'e'. Every entry row of the stream carries this prefix and no other
// family does, so a rank or scan bounded by it sees exactly the entry rows in ID order. It uses
// pbuf, not kbuf, so a caller can hold the prefix across a kbuf rebuild.
func (c *connState) streamEntryPrefix(skey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(skey)))
	b = append(b, tmp[:n]...)
	b = append(b, skey...)
	b = append(b, streamEntryTag)
	c.pbuf = b
	return b
}

// streamHeader reads a stream's header row: the live entry count, the last generated ID, the max
// deleted ID, and the entries-added counter. ok is false when the stream has no header (the key
// does not exist).
func (c *connState) streamHeader(skey []byte) (length uint64, lastID, maxDel streamID, entriesAdded uint64, ok bool) {
	var hb [streamHeaderBytes]byte
	v, got := c.srv.store.GetKind(skey, hb[:0], kindStreamMeta)
	if !got || len(v) < streamHeaderBytes {
		return 0, streamID{}, streamID{}, 0, false
	}
	length = binary.LittleEndian.Uint64(v[0:8])
	lastID = streamID{ms: binary.LittleEndian.Uint64(v[8:16]), seq: binary.LittleEndian.Uint64(v[16:24])}
	maxDel = streamID{ms: binary.LittleEndian.Uint64(v[24:32]), seq: binary.LittleEndian.Uint64(v[32:40])}
	entriesAdded = binary.LittleEndian.Uint64(v[40:48])
	return length, lastID, maxDel, entriesAdded, true
}

// streamPutHeader writes a stream's header row. Unlike the other collections it never deletes the
// header at count 0, because a zero-entry stream still exists (spec section 1.3); only DEL removes
// the header.
func (c *connState) streamPutHeader(skey []byte, length uint64, lastID, maxDel streamID, entriesAdded uint64) error {
	var hb [streamHeaderBytes]byte
	binary.LittleEndian.PutUint64(hb[0:8], length)
	binary.LittleEndian.PutUint64(hb[8:16], lastID.ms)
	binary.LittleEndian.PutUint64(hb[16:24], lastID.seq)
	binary.LittleEndian.PutUint64(hb[24:32], maxDel.ms)
	binary.LittleEndian.PutUint64(hb[32:40], maxDel.seq)
	binary.LittleEndian.PutUint64(hb[40:48], entriesAdded)
	_, err := c.srv.store.PutKind(skey, hb[:], kindStreamMeta)
	return err
}

// encodeStreamFields encodes a flat field/value list (even indices fields, odd values) into the
// naive per-entry form: nfields uvarint, then each pair as len-prefixed field and len-prefixed
// value, in insertion order. The field-dictionary form (spec section 3.2) is a later lab decision;
// the naive form is correct for every stream and is the slice-1 default.
func encodeStreamFields(dst []byte, fields [][]byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(fields)/2))
	dst = append(dst, tmp[:n]...)
	for _, f := range fields {
		m := binary.PutUvarint(tmp[:], uint64(len(f)))
		dst = append(dst, tmp[:m]...)
		dst = append(dst, f...)
	}
	return dst
}

// --- ID assignment and validation (spec section 3.6) ---

// autoID generates the next ID for the '*' form from the clock and the last ID, keeping the
// sequence monotone when the clock does not advance past the last ID's millisecond.
func autoID(lastID streamID, now uint64) streamID {
	if now > lastID.ms {
		return streamID{ms: now, seq: 0}
	}
	return streamID{ms: lastID.ms, seq: lastID.seq + 1}
}

// autoSeqID generates the sequence for the 'ms-*' form: zero for a fresh millisecond greater than
// the last ID's, or one past the last sequence within the same millisecond. ok is false when ms is
// below the last ID's millisecond (the assigned ID would not be strictly greater).
func autoSeqID(lastID streamID, ms uint64) (streamID, bool) {
	if ms < lastID.ms {
		return streamID{}, false
	}
	if ms == lastID.ms {
		if lastID.seq == ^uint64(0) {
			return streamID{}, false
		}
		return streamID{ms: ms, seq: lastID.seq + 1}, true
	}
	return streamID{ms: ms, seq: 0}, true
}

// resolveStreamID turns the XADD id argument into a concrete ID, handling '*', 'ms-*', a bare 'ms',
// and a full 'ms-seq', and validating monotonicity against the stream's last ID. The returned error
// string is empty on success.
func resolveStreamID(lastID streamID, raw string, now uint64) (streamID, string) {
	if raw == "*" {
		return autoID(lastID, now), ""
	}
	if strings.HasSuffix(raw, "-*") {
		ms, err := strconv.ParseUint(raw[:len(raw)-2], 10, 64)
		if err != nil {
			return streamID{}, errStreamInvalidID
		}
		id, ok := autoSeqID(lastID, ms)
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
	if !lastID.less(id) {
		return streamID{}, errStreamIDSmaller
	}
	return id, ""
}

// --- trim clause (shared by XADD's inline trim and, later, XTRIM) ---

type streamTrimKind int

const (
	streamTrimNone streamTrimKind = iota
	streamTrimMaxLen
	streamTrimMinID
)

// streamTrimSpec is a parsed MAXLEN or MINID clause.
type streamTrimSpec struct {
	kind     streamTrimKind
	maxLen   uint64
	minID    streamID
	approx   bool
	limit    uint64
	hasLimit bool
}

// parseStreamTrim parses MAXLEN|MINID [=|~] threshold [LIMIT count] starting at the strategy
// keyword. It returns the spec, the number of args consumed, and an error string. The element-per-
// row store trims exactly, so the '~' form still keeps no more than the threshold, which satisfies
// the approximate contract.
func parseStreamTrim(args [][]byte) (streamTrimSpec, int, string) {
	var ts streamTrimSpec
	i := 0
	switch strings.ToUpper(string(args[i])) {
	case "MAXLEN":
		ts.kind = streamTrimMaxLen
	case "MINID":
		ts.kind = streamTrimMinID
	default:
		return ts, 0, "ERR syntax error"
	}
	i++
	if i >= len(args) {
		return ts, 0, "ERR syntax error"
	}
	switch string(args[i]) {
	case "~":
		ts.approx = true
		i++
	case "=":
		i++
	}
	if i >= len(args) {
		return ts, 0, "ERR syntax error"
	}
	if ts.kind == streamTrimMaxLen {
		n, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil || n < 0 {
			return ts, 0, errStreamMaxLenArg
		}
		ts.maxLen = uint64(n)
	} else {
		id, ok := parseStreamID(string(args[i]), 0)
		if !ok {
			return ts, 0, errStreamMinIDArg
		}
		ts.minID = id
	}
	i++
	if i < len(args) && strings.EqualFold(string(args[i]), "LIMIT") {
		if !ts.approx {
			return ts, 0, "ERR syntax error"
		}
		if i+1 >= len(args) {
			return ts, 0, "ERR syntax error"
		}
		n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil || n < 0 {
			return ts, 0, "ERR syntax error"
		}
		if n == 0 {
			return ts, 0, errStreamLimitZero
		}
		ts.limit = uint64(n)
		ts.hasLimit = true
		i += 2
	}
	return ts, i, ""
}

// streamTrim removes entry rows from the low (oldest) end of the stream per ts and returns the
// count removed. It scans the entry family from the front in bounded batches and point-deletes each
// dropped row, so a trim touches only the rows it removes, never the whole log. The caller holds
// the stripe lock and updates the header length by the returned count. max-deleted-id is not
// advanced by a trim, matching Redis, since last-id already records the high-water mark.
func (c *connState) streamTrim(skey []byte, prefix []byte, length uint64, ts streamTrimSpec) uint64 {
	var target uint64 // how many to drop
	switch ts.kind {
	case streamTrimMaxLen:
		if length > ts.maxLen {
			target = length - ts.maxLen
		}
	case streamTrimMinID:
		target = length // MINID drops until the first surviving id; bound applied per row below
	default:
		return 0
	}
	if ts.hasLimit && target > ts.limit {
		target = ts.limit
	}
	if target == 0 {
		return 0
	}
	var dropped uint64
	for dropped < target {
		want := int(target - dropped)
		if want > streamTrimBatch {
			want = streamTrimBatch
		}
		keys, _ := c.srv.store.CollScan(prefix, nil, want, nil)
		if len(keys) == 0 {
			break
		}
		progressed := false
		for _, k := range keys {
			if ts.kind == streamTrimMinID && !decodeEntryID(k).less(ts.minID) {
				return dropped // reached the first entry at or above minID
			}
			c.srv.store.DeleteKind(k, kindStreamEntry)
			c.srv.store.CollRemove(k)
			dropped++
			progressed = true
			if dropped >= target {
				break
			}
		}
		if !progressed {
			break
		}
	}
	return dropped
}

// dropStream removes a stream's entry rows and its header, the DEL/UNLINK cascade for a stream key.
// A stream is the one collection whose header outlives an empty entry range, so dropStream is the
// only path (besides expiry) that removes the header. Later slices extend it to drop the group,
// consumer, and PEL sibling families.
func (c *connState) dropStream(skey []byte) {
	c.dropCollIndex(c.streamEntryPrefix(skey), kindStreamEntry)
	c.srv.store.DeleteKind(skey, kindStreamMeta)
}

// --- commands ---

func (c *connState) cmdXAdd(argv [][]byte) {
	// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
	if len(argv) < 5 {
		c.writeErr("ERR wrong number of arguments for 'xadd' command")
		return
	}
	skey := argv[1]
	i := 2
	noMkStream := false
	if i < len(argv) && eqFold(argv[i], "NOMKSTREAM") {
		noMkStream = true
		i++
	}
	var trim streamTrimSpec
	if i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "MAXLEN", "MINID":
			ts, n, errStr := parseStreamTrim(argv[i:])
			if errStr != "" {
				c.writeErr(errStr)
				return
			}
			trim = ts
			i += n
		}
	}
	if i >= len(argv) {
		c.writeErr("ERR wrong number of arguments for 'xadd' command")
		return
	}
	rawID := string(argv[i])
	i++
	rest := argv[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		c.writeErr("ERR wrong number of arguments for 'xadd' command")
		return
	}

	now := uint64(time.Now().UnixMilli())
	mu := &c.srv.incrMu[c.srv.stripe(skey)]
	mu.Lock()
	if c.stringConflict(skey) {
		mu.Unlock()
		c.writeErr(wrongType)
		return
	}

	length, lastID, maxDel, entriesAdded, exists := c.streamHeader(skey)
	if !exists && noMkStream {
		mu.Unlock()
		c.writeNil()
		return
	}

	id, errStr := resolveStreamID(lastID, rawID, now)
	if errStr != "" {
		mu.Unlock()
		c.writeErr(errStr)
		return
	}

	ek := c.streamEntryKey(skey, id)
	val := encodeStreamFields(c.vbuf[:0], rest)
	c.vbuf = val
	if _, err := c.srv.store.PutKind(ek, val, kindStreamEntry); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	c.srv.store.CollInsert(ek, kindStreamEntry)
	length++
	entriesAdded++

	if trim.kind != streamTrimNone {
		prefix := c.streamEntryPrefix(skey)
		dropped := c.streamTrim(skey, prefix, length, trim)
		length -= dropped
	}

	if err := c.streamPutHeader(skey, length, id, maxDel, entriesAdded); err != nil {
		mu.Unlock()
		c.writeErr("ERR " + err.Error())
		return
	}
	mu.Unlock()
	c.writeBulk([]byte(id.String()))
}

func (c *connState) cmdXLen(argv [][]byte) {
	// XLEN key
	if len(argv) != 2 {
		c.writeErr("ERR wrong number of arguments for 'xlen' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	length, _, _, _, _ := c.streamHeader(argv[1])
	c.writeInt(int64(length))
}

// streamRankBoundary counts the entry rows sorting below an ID boundary: below the ID when
// includeEqual is false, at or below it when includeEqual is true (achieved by appending a 0x00
// that sorts after the 16-byte ID and before any next ID, exactly as the zset lex boundary does).
// It builds the boundary in kbuf from the prefix held in pbuf.
func (c *connState) streamRankBoundary(prefix []byte, id streamID, includeEqual bool, card int) int {
	b := c.kbuf[:0]
	b = append(b, prefix...)
	var idb [16]byte
	putStreamID(idb[:], id)
	b = append(b, idb[:]...)
	if includeEqual {
		b = append(b, 0x00)
	}
	c.kbuf = b
	r := c.srv.store.CollRankOf(prefix, b)
	if r > card {
		r = card
	}
	return r
}

// streamWindow computes the entry-family window for an ID range and returns its keys in ascending
// ID order. start/end are the inclusive-or-exclusive bounds; count caps the window (from the front
// for a forward read, from the back for a reverse read); a negative count means the whole window.
// It ranks both bounds on the entry family and reads exactly the window off the ordered index: one
// positional seek plus a bounded forward scan, so the cost tracks the window, not the log length.
func (c *connState) streamWindow(skey []byte, start streamID, startExcl bool, end streamID, endExcl bool, count int, rev bool) [][]byte {
	length, _, _, _, ok := c.streamHeader(skey)
	if !ok || length == 0 {
		return c.zkeys[:0]
	}
	card := int(length)
	prefix := c.streamEntryPrefix(skey)
	// Inclusive start counts entries below start; exclusive start counts entries at or below start.
	startIdx := c.streamRankBoundary(prefix, start, startExcl, card)
	// Inclusive end counts entries at or below end; exclusive end counts entries below end.
	endIdx := c.streamRankBoundary(prefix, end, !endExcl, card)
	if startIdx >= endIdx {
		return c.zkeys[:0]
	}
	lo, hi := startIdx, endIdx
	if count >= 0 {
		if rev {
			if hi-count > lo {
				lo = hi - count
			}
		} else {
			if lo+count < hi {
				hi = lo + count
			}
		}
	}
	return c.collectWindow(prefix, lo, hi)
}

// emitStreamEntry writes one entry row as an [id, [field, value, ...]] reply pair, reading and
// decoding the field map straight from the entry's value bytes. The field list is emitted in
// insertion order, the order XADD received it.
func (c *connState) emitStreamEntry(entryKey []byte) {
	id := decodeEntryID(entryKey)
	c.writeArrayHeader(2)
	c.writeBulk([]byte(id.String()))
	val, ok := c.srv.store.GetKind(entryKey, c.vbuf[:0], kindStreamEntry)
	c.vbuf = val
	if !ok {
		c.writeArrayHeader(0)
		return
	}
	nf, off := binary.Uvarint(val)
	c.writeArrayHeader(int(nf) * 2)
	for j := uint64(0); j < nf*2; j++ {
		l, m := binary.Uvarint(val[off:])
		off += m
		c.writeBulk(val[off : off+int(l)])
		off += int(l)
	}
}

// cmdXRange answers XRANGE (rev=false) and XREVRANGE (rev=true). For the reverse form the wire
// argument order is end then start, and the result is emitted high ID to low.
func (c *connState) cmdXRange(argv [][]byte, rev bool) {
	name := "xrange"
	if rev {
		name = "xrevrange"
	}
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	startArg, endArg := string(argv[2]), string(argv[3])
	if rev {
		startArg, endArg = endArg, startArg
	}
	start, startExcl, ok1 := parseRangeBound(startArg, 0)
	end, endExcl, ok2 := parseRangeBound(endArg, ^uint64(0))
	if !ok1 || !ok2 {
		c.writeErr(errStreamInvalidID)
		return
	}

	count := -1
	if len(argv) > 4 {
		if len(argv) != 6 || !eqFold(argv[4], "COUNT") {
			c.writeErr("ERR syntax error")
			return
		}
		n, err := strconv.Atoi(string(argv[5]))
		if err != nil {
			c.writeErr(errStreamCountER)
			return
		}
		if n < 0 {
			n = 0
		}
		count = n
	}

	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	keys := c.streamWindow(argv[1], start, startExcl, end, endExcl, count, rev)
	c.writeArrayHeader(len(keys))
	if rev {
		for i := len(keys) - 1; i >= 0; i-- {
			c.emitStreamEntry(keys[i])
		}
		return
	}
	for _, k := range keys {
		c.emitStreamEntry(k)
	}
}

// parseRangeBound parses an XRANGE endpoint: an optional leading '(' marks it exclusive, '-' and
// '+' are the ID-space extremes, and a full or partial ID takes defaultSeq for a missing sequence
// (0 for a start bound, max for an end bound).
func parseRangeBound(arg string, defaultSeq uint64) (id streamID, exclusive, ok bool) {
	if strings.HasPrefix(arg, "(") {
		exclusive = true
		arg = arg[1:]
	}
	switch arg {
	case "-":
		return streamID{}, exclusive, true
	case "+":
		return maxStreamID, exclusive, true
	}
	id, ok = parseStreamID(arg, defaultSeq)
	return id, exclusive, ok
}

// cmdXRead implements the non-blocking XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...].
// It reads the entries strictly after each given id per stream and replies the per-stream entries
// that follow. The BLOCK option is parsed (and its timeout validated) but a block does not park in
// this slice: when no entry is available it returns the null array. Blocking XREAD wakeup lands with
// the stream blocking slice, the same way the list blocking commands landed after the non-blocking
// list path.
func (c *connState) cmdXRead(argv [][]byte) {
	i := 1
	count := -1
	for i < len(argv) {
		switch strings.ToUpper(string(argv[i])) {
		case "COUNT":
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, err := strconv.Atoi(string(argv[i+1]))
			if err != nil || n <= 0 {
				c.writeErr(errStreamReadCount)
				return
			}
			count = n
			i += 2
		case "BLOCK":
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			ms, err := strconv.ParseInt(string(argv[i+1]), 10, 64)
			if err != nil {
				c.writeErr(errStreamTimeoutInt)
				return
			}
			if ms < 0 {
				c.writeErr(errStreamTimeoutNeg)
				return
			}
			i += 2
		case "STREAMS":
			i++
			c.xreadStreams(argv[i:], count)
			return
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}
	c.writeErr("ERR syntax error")
}

// xreadStreams reads the key-then-id half of the STREAMS clause and replies the entries after each
// given id per stream. A '$' or '+' after-id resolves to the stream's current last id, so the read
// returns nothing for a stream with no newer entries.
func (c *connState) xreadStreams(rest [][]byte, count int) {
	if len(rest) == 0 || len(rest)%2 != 0 {
		c.writeErr(errStreamUnbalanced)
		return
	}
	n := len(rest) / 2
	keys := rest[:n]
	idArgs := rest[n:]

	starts := make([]streamID, n)
	for j := 0; j < n; j++ {
		raw := string(idArgs[j])
		if raw == "$" || raw == "+" {
			// $ and + both mean "entries after the current last id".
			_, lastID, _, _, ok := c.streamHeader(keys[j])
			if ok {
				starts[j] = lastID
			}
			continue
		}
		id, ok := parseStreamID(raw, 0)
		if !ok {
			c.writeErr(errStreamInvalidID)
			return
		}
		starts[j] = id
	}

	// Wrong-type guard for every named key before producing any reply.
	for _, k := range keys {
		if c.stringConflict(k) {
			c.writeErr(wrongType)
			return
		}
	}

	// Gather each stream's window; a stream that produced no entries is omitted from the reply.
	type result struct {
		key     []byte
		entries [][]byte
	}
	var results []result
	for j := 0; j < n; j++ {
		w := c.streamWindow(keys[j], starts[j], true, maxStreamID, false, count, false)
		if len(w) == 0 {
			continue
		}
		// Copy the window keys out of the shared zkeys scratch, which the next stream's
		// streamWindow call reuses.
		cp := make([][]byte, len(w))
		copy(cp, w)
		results = append(results, result{key: keys[j], entries: cp})
	}

	if len(results) == 0 {
		c.writeNilArray()
		return
	}
	c.writeArrayHeader(len(results))
	for _, r := range results {
		c.writeArrayHeader(2)
		c.writeBulk(r.key)
		c.writeArrayHeader(len(r.entries))
		for _, ek := range r.entries {
			c.emitStreamEntry(ek)
		}
	}
}
