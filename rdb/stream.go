package rdb

import (
	"strconv"

	"github.com/tamnd/aki/encoding"
)

// This file serializes a stream value into the RDB STREAM_LISTPACKS_3 form (type
// byte 19, spec 2064 doc 17 section 3.11) and reads it back. A stream is the one
// value type that does not fit the flat encodeBody/decodeValue shape, so it gets
// its own neutral StreamData carrier and its own pair of functions.
//
// The on-disk layout is: a count of macro nodes, then per node a master ID
// (ms and seq as length-encoded ints) and a listpack blob holding a run of
// entries; then the stream metadata (length, last ID, first ID, max deleted ID,
// entries added); then the consumer groups, each with its PEL and consumers.
//
// Inside a macro node the listpack uses the Redis stream master-entry format: a
// master entry of [count, deleted, num-master-fields, master fields, 0] followed
// by one record per entry of [flags, ms-diff, seq-diff, num-fields, field/value
// pairs, lp-count]. aki always writes zero master fields, so every entry carries
// its own field names. The diffs are taken against the node master ID, which is
// the ID of the node's first entry.

// streamNodeMaxEntries caps how many entries go in one macro node, matching the
// Redis stream-node-max-entries default of 100. A real stream packs entries into
// nodes of this size; aki does the same so a reload keeps the node shape close.
const streamNodeMaxEntries = 100

// stream listpack entry flags, matching Redis. NONE is the common case aki writes;
// the others are read so a dump from a real Redis still decodes.
const (
	streamItemDeleted    = 1 // entry was deleted, skip it on read
	streamItemSameFields = 2 // entry reuses the node master field names
)

// StreamID is a 128-bit stream entry ID inside an RDB stream value: a millisecond
// timestamp and a sequence number.
type StreamID struct {
	MS  uint64
	Seq uint64
}

// StreamEntry is one log entry: its ID and a flat field/value list where even
// indices are field names and odd indices are the matching values.
type StreamEntry struct {
	MS     uint64
	Seq    uint64
	Fields [][]byte
}

// StreamPEL is one Pending Entries List record of a consumer group: an entry that
// was delivered to a consumer but not yet acknowledged.
type StreamPEL struct {
	MS            uint64
	Seq           uint64
	DeliveryTime  uint64
	DeliveryCount uint64
}

// StreamConsumer is one named reader in a group, with its seen and active times
// and the IDs it currently holds pending.
type StreamConsumer struct {
	Name       []byte
	SeenTime   uint64
	ActiveTime uint64
	PendingIDs []StreamID
}

// StreamGroup is a consumer group: its name, last delivered ID, entries-read
// counter, the group PEL, and the consumers.
type StreamGroup struct {
	Name        []byte
	LastMS      uint64
	LastSeq     uint64
	EntriesRead uint64
	PEL         []StreamPEL
	Consumers   []StreamConsumer
}

// StreamData is the neutral carrier for a stream value between aki's keyspace and
// the RDB byte form, the stream counterpart of the List, Set, Hash and ZSet fields
// on Value.
type StreamData struct {
	Entries      []StreamEntry
	LastMS       uint64
	LastSeq      uint64
	FirstMS      uint64
	FirstSeq     uint64
	MaxDelMS     uint64
	MaxDelSeq    uint64
	EntriesAdded uint64
	Groups       []StreamGroup
}

// encodeStream writes a stream value body in the STREAM_LISTPACKS_3 form.
func encodeStream(v Value) (byte, []byte, error) {
	s := v.Stream
	if s == nil {
		s = &StreamData{}
	}
	var body []byte

	nodes := chunkStreamEntries(s.Entries, streamNodeMaxEntries)
	body = appendLength(body, uint64(len(nodes)))
	for _, node := range nodes {
		master := StreamID{MS: node[0].MS, Seq: node[0].Seq}
		body = appendLength(body, master.MS)
		body = appendLength(body, master.Seq)
		body = appendString(body, encodeStreamNode(node, master))
	}

	body = appendLength(body, uint64(len(s.Entries)))
	body = appendLength(body, s.LastMS)
	body = appendLength(body, s.LastSeq)
	body = appendLength(body, s.FirstMS)
	body = appendLength(body, s.FirstSeq)
	body = appendLength(body, s.MaxDelMS)
	body = appendLength(body, s.MaxDelSeq)
	body = appendLength(body, s.EntriesAdded)

	body = appendLength(body, uint64(len(s.Groups)))
	for _, g := range s.Groups {
		body = appendString(body, g.Name)
		body = appendLength(body, g.LastMS)
		body = appendLength(body, g.LastSeq)
		body = appendLength(body, g.EntriesRead)
		body = appendLength(body, uint64(len(g.PEL)))
		for _, p := range g.PEL {
			body = encoding.AppendU64(body, p.MS)
			body = encoding.AppendU64(body, p.Seq)
			body = encoding.AppendU64(body, p.DeliveryTime)
			body = appendLength(body, p.DeliveryCount)
		}
		body = appendLength(body, uint64(len(g.Consumers)))
		for _, c := range g.Consumers {
			body = appendString(body, c.Name)
			body = encoding.AppendU64(body, c.SeenTime)
			body = encoding.AppendU64(body, c.ActiveTime)
			body = appendLength(body, uint64(len(c.PendingIDs)))
			for _, id := range c.PendingIDs {
				body = encoding.AppendU64(body, id.MS)
				body = encoding.AppendU64(body, id.Seq)
			}
		}
	}
	return typeStream, body, nil
}

// chunkStreamEntries splits the entries into macro nodes of at most size entries
// each, the unit a stream listpack node holds. An empty stream yields no nodes.
func chunkStreamEntries(entries []StreamEntry, size int) [][]StreamEntry {
	var nodes [][]StreamEntry
	for i := 0; i < len(entries); i += size {
		end := i + size
		if end > len(entries) {
			end = len(entries)
		}
		nodes = append(nodes, entries[i:end])
	}
	return nodes
}

// encodeStreamNode packs one macro node's entries into a listpack using the Redis
// stream master-entry format. The master ID is the node's first entry ID, and each
// entry stores its ms and seq as a diff from that master.
func encodeStreamNode(entries []StreamEntry, master StreamID) []byte {
	var elems [][]byte
	// Master entry: total count, deleted count, master field count, then a zero
	// terminator. aki keeps no deleted tombstones and writes no master fields.
	elems = append(elems, lpInt(int64(len(entries))))
	elems = append(elems, lpInt(0))
	elems = append(elems, lpInt(0))
	elems = append(elems, lpInt(0))
	for _, e := range entries {
		numFields := len(e.Fields) / 2
		elems = append(elems, lpInt(0)) // flags: none, fields are inline
		elems = append(elems, lpInt(int64(e.MS)-int64(master.MS)))
		elems = append(elems, lpInt(int64(e.Seq)-int64(master.Seq)))
		elems = append(elems, lpInt(int64(numFields)))
		elems = append(elems, e.Fields...)
		// lp_count is the number of listpack elements this record spans, the value
		// the reverse walk uses to step back over a whole entry: flags, ms, seq,
		// numfields, the field/value pairs, and this trailing count itself.
		elems = append(elems, lpInt(int64(2*numFields+4)))
	}
	return listpackEncode(elems)
}

// decodeStream reads a stream value body written by encodeStream or by a real
// Redis STREAM_LISTPACKS_3 record.
func decodeStream(r *reader) (Value, error) {
	s := &StreamData{}

	nodeCount, _, _ := r.readLength()
	for i := uint64(0); i < nodeCount; i++ {
		masterMS, _, _ := r.readLength()
		masterSeq, _, _ := r.readLength()
		blob := r.readString()
		if r.err != nil {
			return Value{}, r.err
		}
		ents, err := decodeStreamNode(blob, StreamID{MS: masterMS, Seq: masterSeq})
		if err != nil {
			return Value{}, err
		}
		s.Entries = append(s.Entries, ents...)
	}

	r.readLength() // stream length, recomputable from the entries
	s.LastMS, _, _ = r.readLength()
	s.LastSeq, _, _ = r.readLength()
	s.FirstMS, _, _ = r.readLength()
	s.FirstSeq, _, _ = r.readLength()
	s.MaxDelMS, _, _ = r.readLength()
	s.MaxDelSeq, _, _ = r.readLength()
	s.EntriesAdded, _, _ = r.readLength()

	groupCount, _, _ := r.readLength()
	for i := uint64(0); i < groupCount; i++ {
		var g StreamGroup
		g.Name = r.readString()
		g.LastMS, _, _ = r.readLength()
		g.LastSeq, _, _ = r.readLength()
		g.EntriesRead, _, _ = r.readLength()
		pelCount, _, _ := r.readLength()
		for j := uint64(0); j < pelCount; j++ {
			var p StreamPEL
			p.MS = readU64LE(r)
			p.Seq = readU64LE(r)
			p.DeliveryTime = readU64LE(r)
			p.DeliveryCount, _, _ = r.readLength()
			if r.err != nil {
				return Value{}, r.err
			}
			g.PEL = append(g.PEL, p)
		}
		consumerCount, _, _ := r.readLength()
		for j := uint64(0); j < consumerCount; j++ {
			var c StreamConsumer
			c.Name = r.readString()
			c.SeenTime = readU64LE(r)
			c.ActiveTime = readU64LE(r)
			cpCount, _, _ := r.readLength()
			for k := uint64(0); k < cpCount; k++ {
				var id StreamID
				id.MS = readU64LE(r)
				id.Seq = readU64LE(r)
				if r.err != nil {
					return Value{}, r.err
				}
				c.PendingIDs = append(c.PendingIDs, id)
			}
			if r.err != nil {
				return Value{}, r.err
			}
			g.Consumers = append(g.Consumers, c)
		}
		if r.err != nil {
			return Value{}, r.err
		}
		s.Groups = append(s.Groups, g)
	}
	if r.err != nil {
		return Value{}, r.err
	}
	return Value{Kind: KindStream, Stream: s}, nil
}

// decodeStreamNode walks one macro node's listpack and rebuilds its entries,
// adding each entry's diff back to the node master ID. A deleted record is read
// past but dropped.
func decodeStreamNode(blob []byte, master StreamID) ([]StreamEntry, error) {
	elems, err := listpackDecode(blob)
	if err != nil {
		return nil, err
	}
	c := &lpCursor{elems: elems}

	c.int() // total count
	c.int() // deleted count
	numMaster := c.int()
	masterFields := make([][]byte, 0, sliceHint(uint64(maxNonNeg(numMaster))))
	for i := int64(0); i < numMaster; i++ {
		masterFields = append(masterFields, c.bytes())
	}
	c.int() // master zero terminator
	if c.err != nil {
		return nil, c.err
	}

	var out []StreamEntry
	for c.more() {
		flags := c.int()
		msDiff := c.int()
		seqDiff := c.int()
		var fields [][]byte
		if flags&streamItemSameFields != 0 {
			for i := int64(0); i < numMaster; i++ {
				fields = append(fields, masterFields[i], c.bytes())
			}
		} else {
			numFields := c.int()
			for i := int64(0); i < numFields; i++ {
				f := c.bytes()
				v := c.bytes()
				fields = append(fields, f, v)
			}
		}
		c.int() // lp_count
		if c.err != nil {
			return nil, c.err
		}
		if flags&streamItemDeleted != 0 {
			continue
		}
		out = append(out, StreamEntry{
			MS:     uint64(int64(master.MS) + msDiff),
			Seq:    uint64(int64(master.Seq) + seqDiff),
			Fields: fields,
		})
	}
	return out, nil
}

// lpCursor walks the decoded elements of a listpack with a sticky error, the same
// one-shot error stance the byte reader uses. Once a read runs off the end every
// later read is a no-op.
type lpCursor struct {
	elems [][]byte
	i     int
	err   error
}

// bytes returns the next element, or records errTruncated past the end.
func (c *lpCursor) bytes() []byte {
	if c.err != nil {
		return nil
	}
	if c.i >= len(c.elems) {
		c.err = errTruncated
		return nil
	}
	v := c.elems[c.i]
	c.i++
	return v
}

// int returns the next element parsed as a base-10 integer. listpackDecode renders
// integer entries as their decimal text, so this reverses that.
func (c *lpCursor) int() int64 {
	s := c.bytes()
	if c.err != nil {
		return 0
	}
	v, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		c.err = err
		return 0
	}
	return v
}

// more reports whether another element is available and no error has been hit.
func (c *lpCursor) more() bool { return c.err == nil && c.i < len(c.elems) }

// lpInt renders an integer as the decimal text listpackEncode packs back into a
// compact integer entry.
func lpInt(v int64) []byte { return []byte(strconv.FormatInt(v, 10)) }

// readU64LE reads 8 little-endian bytes as a uint64, returning 0 once the reader
// has an error so encoding.U64 is never handed a short slice.
func readU64LE(r *reader) uint64 {
	b := r.readBytes(8)
	if r.err != nil {
		return 0
	}
	return encoding.U64(b)
}

// maxNonNeg clamps a possibly-negative count to zero before it sizes a slice.
func maxNonNeg(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
