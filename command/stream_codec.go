package command

import (
	"strconv"
	"strings"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// streamID is a 128-bit stream entry ID: a millisecond timestamp and a
// sequence number that breaks ties within the same millisecond.
type streamID struct {
	ms  uint64
	seq uint64
}

// less reports whether a sorts before b, comparing ms then seq.
func (a streamID) less(b streamID) bool {
	if a.ms != b.ms {
		return a.ms < b.ms
	}
	return a.seq < b.seq
}

// equal reports whether two IDs are the same.
func (a streamID) equal(b streamID) bool { return a.ms == b.ms && a.seq == b.seq }

// String renders the ID in the textual ms-seq form used on the wire.
func (a streamID) String() string {
	return strconv.FormatUint(a.ms, 10) + "-" + strconv.FormatUint(a.seq, 10)
}

// maxStreamID is the largest possible ID, used as the open upper bound.
var maxStreamID = streamID{ms: ^uint64(0), seq: ^uint64(0)}

// streamEntry is one log entry: an ID and a flat field/value list where even
// indices are field names and odd indices are their values.
type streamEntry struct {
	id     streamID
	fields [][]byte
}

// stream is the in-memory form of a stream value. Entries are kept sorted by ID
// in ascending order.
type stream struct {
	lastID       streamID
	maxDeletedID streamID
	entriesAdded uint64
	entries      []streamEntry
}

// streamDecode unpacks a stored stream body. The body is the header (last ID,
// max-deleted ID, entries-added counter), the entry count, the entries, and a
// trailing group count that is zero until consumer groups land.
func streamDecode(body []byte) (*stream, error) {
	s := &stream{}
	if len(body) == 0 {
		return s, nil
	}
	off := 0
	read := func() (uint64, error) {
		v, n, err := encoding.Uvarint(body[off:])
		if err != nil {
			return 0, err
		}
		off += n
		return v, nil
	}
	var err error
	if s.lastID.ms, err = read(); err != nil {
		return nil, err
	}
	if s.lastID.seq, err = read(); err != nil {
		return nil, err
	}
	if s.maxDeletedID.ms, err = read(); err != nil {
		return nil, err
	}
	if s.maxDeletedID.seq, err = read(); err != nil {
		return nil, err
	}
	if s.entriesAdded, err = read(); err != nil {
		return nil, err
	}
	count, err := read()
	if err != nil {
		return nil, err
	}
	s.entries = make([]streamEntry, 0, count)
	for range count {
		var e streamEntry
		if e.id.ms, err = read(); err != nil {
			return nil, err
		}
		if e.id.seq, err = read(); err != nil {
			return nil, err
		}
		nf, err := read()
		if err != nil {
			return nil, err
		}
		e.fields = make([][]byte, 0, nf*2)
		for range nf * 2 {
			chunk, m, err := readChunk(body, off)
			if err != nil {
				return nil, err
			}
			off = m
			e.fields = append(e.fields, chunk)
		}
		s.entries = append(s.entries, e)
	}
	// Group count, currently always zero; reserved for consumer groups.
	if _, err = read(); err != nil {
		return nil, err
	}
	return s, nil
}

// streamEncode packs a stream back into its stored body form.
func streamEncode(s *stream) []byte {
	body := encoding.AppendUvarint(nil, s.lastID.ms)
	body = encoding.AppendUvarint(body, s.lastID.seq)
	body = encoding.AppendUvarint(body, s.maxDeletedID.ms)
	body = encoding.AppendUvarint(body, s.maxDeletedID.seq)
	body = encoding.AppendUvarint(body, s.entriesAdded)
	body = encoding.AppendUvarint(body, uint64(len(s.entries)))
	for _, e := range s.entries {
		body = encoding.AppendUvarint(body, e.id.ms)
		body = encoding.AppendUvarint(body, e.id.seq)
		body = encoding.AppendUvarint(body, uint64(len(e.fields)/2))
		for _, chunk := range e.fields {
			body = encoding.AppendUvarint(body, uint64(len(chunk)))
			body = append(body, chunk...)
		}
	}
	// Group count placeholder.
	body = encoding.AppendUvarint(body, 0)
	return body
}

// getStream loads the stream at key. The found flag and a non-stream header type
// let the caller raise WRONGTYPE without a second lookup.
func getStream(db *keyspace.DB, key []byte) (*stream, keyspace.ValueHeader, bool, error) {
	body, hdr, found, err := db.Get(key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeStream {
		return nil, hdr, true, nil
	}
	s, err := streamDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return s, hdr, true, nil
}

// storeStream writes the stream back at key, keeping the existing TTL.
func storeStream(db *keyspace.DB, key []byte, s *stream, ttlMs int64) error {
	return db.Set(key, streamEncode(s), keyspace.TypeStream, keyspace.EncStream, ttlMs)
}

// findEntry returns the index of the entry with the given ID, or -1 when it is
// absent. Entries are sorted, so this is a binary search.
func (s *stream) findEntry(id streamID) int {
	lo, hi := 0, len(s.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if s.entries[mid].id.less(id) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(s.entries) && s.entries[lo].id.equal(id) {
		return lo
	}
	return -1
}

// lowerBound returns the index of the first entry whose ID is not less than id.
func (s *stream) lowerBound(id streamID) int {
	lo, hi := 0, len(s.entries)
	for lo < hi {
		mid := (lo + hi) / 2
		if s.entries[mid].id.less(id) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// parseStreamID parses a full or partial ID. A partial ID (no -seq part) takes
// defaultSeq for the missing sequence, which lets range bounds expand ms to
// ms-0 or ms-max. It does not handle the * or special range tokens.
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

// rangeBound is a parsed XRANGE endpoint: an ID plus whether it is exclusive.
type rangeBound struct {
	id   streamID
	excl bool
}

// parseRangeStart parses an XRANGE start endpoint. It accepts -, +, a leading (
// for an exclusive bound, and full or partial IDs with the seq defaulting to 0.
func parseRangeStart(arg string) (rangeBound, bool) {
	return parseRangeBound(arg, 0)
}

// parseRangeEnd parses an XRANGE end endpoint, defaulting a partial ID's seq to
// the maximum so a bare ms covers the whole millisecond.
func parseRangeEnd(arg string) (rangeBound, bool) {
	return parseRangeBound(arg, ^uint64(0))
}

// parseRangeBound is the shared endpoint parser. defaultSeq fills the sequence
// of a partial ID.
func parseRangeBound(arg string, defaultSeq uint64) (rangeBound, bool) {
	excl := false
	if strings.HasPrefix(arg, "(") {
		excl = true
		arg = arg[1:]
	}
	switch arg {
	case "-":
		return rangeBound{id: streamID{0, 0}, excl: excl}, true
	case "+":
		return rangeBound{id: maxStreamID, excl: excl}, true
	}
	id, ok := parseStreamID(arg, defaultSeq)
	if !ok {
		return rangeBound{}, false
	}
	return rangeBound{id: id, excl: excl}, true
}
