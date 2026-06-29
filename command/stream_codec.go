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

// pelEntry is one Pending Entries List record: an entry delivered to a consumer
// in a group but not yet acknowledged.
type pelEntry struct {
	id            streamID
	consumer      string
	deliveryTime  int64
	deliveryCount uint64
}

// consumer is one named reader inside a group.
type consumer struct {
	name       string
	seenTime   int64
	activeTime int64
}

// group is a consumer group on a stream. The global PEL is kept sorted by entry
// ID; the per-consumer PEL is the subset whose consumer field matches.
type group struct {
	name        string
	lastID      streamID
	entriesRead uint64
	pel         []pelEntry
	consumers   []*consumer
}

// stream is the in-memory form of a stream value. Entries are kept sorted by ID
// in ascending order, and groups by name.
type stream struct {
	lastID       streamID
	maxDeletedID streamID
	entriesAdded uint64
	entries      []streamEntry
	groups       []*group
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
	groups, _, err := decodeGroups(body, off)
	if err != nil {
		return nil, err
	}
	s.groups = groups
	return s, nil
}

// decodeGroups unpacks the trailing group section of a stream body starting at
// off: a group count followed by that many groups, each with its consumers and
// pending-entries list. It returns the groups, the offset past the section, and
// any error, so both the blob codec and the coll-form header row share one
// reader.
func decodeGroups(body []byte, off int) ([]*group, int, error) {
	read := func() (uint64, error) {
		v, n, err := encoding.Uvarint(body[off:])
		if err != nil {
			return 0, err
		}
		off += n
		return v, nil
	}
	groupCount, err := read()
	if err != nil {
		return nil, off, err
	}
	groups := make([]*group, 0, groupCount)
	for range groupCount {
		g := &group{}
		nameChunk, m, err := readChunk(body, off)
		if err != nil {
			return nil, off, err
		}
		off = m
		g.name = string(nameChunk)
		if g.lastID.ms, err = read(); err != nil {
			return nil, off, err
		}
		if g.lastID.seq, err = read(); err != nil {
			return nil, off, err
		}
		if g.entriesRead, err = read(); err != nil {
			return nil, off, err
		}
		consumerCount, err := read()
		if err != nil {
			return nil, off, err
		}
		g.consumers = make([]*consumer, 0, consumerCount)
		for range consumerCount {
			cn, m, err := readChunk(body, off)
			if err != nil {
				return nil, off, err
			}
			off = m
			seen, err := read()
			if err != nil {
				return nil, off, err
			}
			active, err := read()
			if err != nil {
				return nil, off, err
			}
			g.consumers = append(g.consumers, &consumer{
				name:       string(cn),
				seenTime:   int64(seen),
				activeTime: int64(active),
			})
		}
		pelCount, err := read()
		if err != nil {
			return nil, off, err
		}
		g.pel = make([]pelEntry, 0, pelCount)
		for range pelCount {
			var pe pelEntry
			if pe.id.ms, err = read(); err != nil {
				return nil, off, err
			}
			if pe.id.seq, err = read(); err != nil {
				return nil, off, err
			}
			cn, m, err := readChunk(body, off)
			if err != nil {
				return nil, off, err
			}
			off = m
			pe.consumer = string(cn)
			dt, err := read()
			if err != nil {
				return nil, off, err
			}
			pe.deliveryTime = int64(dt)
			if pe.deliveryCount, err = read(); err != nil {
				return nil, off, err
			}
			g.pel = append(g.pel, pe)
		}
		groups = append(groups, g)
	}
	return groups, off, nil
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
	return encodeGroups(body, s.groups)
}

// encodeGroups appends the group section (a count followed by each group's
// consumers and pending-entries list) to body, shared by the blob codec and the
// coll-form header row.
func encodeGroups(body []byte, groups []*group) []byte {
	body = encoding.AppendUvarint(body, uint64(len(groups)))
	for _, g := range groups {
		body = encoding.AppendUvarint(body, uint64(len(g.name)))
		body = append(body, g.name...)
		body = encoding.AppendUvarint(body, g.lastID.ms)
		body = encoding.AppendUvarint(body, g.lastID.seq)
		body = encoding.AppendUvarint(body, g.entriesRead)
		body = encoding.AppendUvarint(body, uint64(len(g.consumers)))
		for _, c := range g.consumers {
			body = encoding.AppendUvarint(body, uint64(len(c.name)))
			body = append(body, c.name...)
			body = encoding.AppendUvarint(body, uint64(c.seenTime))
			body = encoding.AppendUvarint(body, uint64(c.activeTime))
		}
		body = encoding.AppendUvarint(body, uint64(len(g.pel)))
		for _, pe := range g.pel {
			body = encoding.AppendUvarint(body, pe.id.ms)
			body = encoding.AppendUvarint(body, pe.id.seq)
			body = encoding.AppendUvarint(body, uint64(len(pe.consumer)))
			body = append(body, pe.consumer...)
			body = encoding.AppendUvarint(body, uint64(pe.deliveryTime))
			body = encoding.AppendUvarint(body, pe.deliveryCount)
		}
	}
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
	// A large stream lives in the btree-backed sub-tree form (stream_tree.go); for
	// it the value db.Get returned is only the coll metadata, so materialize the
	// entries and groups from the sub-tree instead of decoding the body as a blob.
	if hdr.IsColl() {
		s, e := streamCollMaterialize(db, key)
		return s, hdr, true, e
	}
	s, err := streamDecode(body)
	if err != nil {
		return nil, hdr, true, err
	}
	return s, hdr, true, nil
}

// storeStream writes the stream back at key, keeping the existing TTL. A stream
// past the promote threshold is written in the btree-backed sub-tree form so a
// large stream is never held or rewritten as one blob; a small one stays inline.
// The coll write rebuilds the sub-tree in place under one shard write lock, so a
// key already in coll form is replaced atomically and never demotes to a blob
// while it is still large.
func storeStream(db *keyspace.DB, key []byte, s *stream, ttlMs int64) error {
	if streamWantsTree(s) {
		return streamStoreColl(db, key, s)
	}
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
