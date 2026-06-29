package command

import (
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// A large stream is stored element-per-row in the key's btree-backed collection
// sub-tree (keyspace.CollUpdate / CollRead), the same machinery the hash, set,
// zset and list types use. A stream keeps two row families in the one sub-tree:
//
//	0x00                         -> header   (last ID, max-deleted ID, entries-added, groups)
//	0x01 + ms(8 BE) + seq(8 BE)  -> entry    (the packed field/value list)
//
// The entry row key is the 16-byte stream ID in big-endian, so the rows sort in
// exactly the ascending ID order XADD appends in and XRANGE/XREAD walk. The
// single header row sorts before every entry row (0x00 < 0x01), so a forward walk
// of the 0x01 rows reproduces the entry log. The collection metadata count is the
// live entry count, which makes XLEN O(1).
//
// A small stream keeps the single-blob form (stream_codec.go). It promotes to the
// sub-tree when its entry count crosses streamCollThreshold, which is the point
// past which a whole-blob decode-and-rewrite on every XADD, and a whole-blob
// materialize on every read, would dominate. The promote threshold is internal:
// a stream has no listpack/quicklist OBJECT ENCODING toggle the way the other
// collections do (OBJECT ENCODING on a stream reports "stream").
//
// XADD on a coll stream appends one entry row in place, XLEN reads the metadata
// count, and XRANGE/XREVRANGE/XREAD walk a bounded cursor window, so none of them
// materializes the whole stream. The remaining stream commands (XDEL, XTRIM,
// XSETID, and the consumer-group family) still go through the coll-aware getStream
// and storeStream, which materialize and rebuild the sub-tree; they keep the key
// in coll form (no demote) but are not yet bounded, and land in a later slice.

const (
	streamRowHeader = 0x00 // the single stream-level metadata row
	streamRowEntry  = 0x01 // an entry row, followed by the 16-byte ID

	// streamCollThreshold is the entry count at which a stream promotes from the
	// inline blob to the btree-backed sub-tree form.
	streamCollThreshold = 128
)

// streamWantsTree reports whether a stream with this many entries should live in
// the btree-backed form.
func streamWantsTree(s *stream) bool {
	return len(s.entries) >= streamCollThreshold
}

// streamEntryRow builds the 17-byte entry row key for id: the entry prefix then
// the ID as two big-endian 64-bit halves, so the rows sort in ascending ID order.
func streamEntryRow(id streamID) []byte {
	k := make([]byte, 0, 1+16)
	k = append(k, streamRowEntry)
	k = encoding.AppendU64BE(k, id.ms)
	k = encoding.AppendU64BE(k, id.seq)
	return k
}

// streamIDFromRow reads the stream ID back out of an entry row key.
func streamIDFromRow(k []byte) streamID {
	return streamID{ms: encoding.U64BE(k[1:9]), seq: encoding.U64BE(k[9:17])}
}

// streamEntryValue packs an entry's field/value list into its row value. The ID
// lives in the row key, so only the fields are stored here, in the same
// pair-count-then-chunks shape the blob codec uses per entry.
func streamEntryValue(fields [][]byte) []byte {
	val := encoding.AppendUvarint(nil, uint64(len(fields)/2))
	for _, chunk := range fields {
		val = encoding.AppendUvarint(val, uint64(len(chunk)))
		val = append(val, chunk...)
	}
	return val
}

// streamEntryFields unpacks a row value into the field/value list.
func streamEntryFields(val []byte) ([][]byte, error) {
	nf, n, err := encoding.Uvarint(val)
	if err != nil {
		return nil, err
	}
	off := n
	fields := make([][]byte, 0, nf*2)
	for range nf * 2 {
		chunk, m, err := readChunk(val, off)
		if err != nil {
			return nil, err
		}
		off = m
		fields = append(fields, chunk)
	}
	return fields, nil
}

// streamHeaderValue packs the stream-level metadata (last ID, max-deleted ID,
// entries-added counter, and the groups) into the header row value.
func streamHeaderValue(s *stream) []byte {
	b := encoding.AppendUvarint(nil, s.lastID.ms)
	b = encoding.AppendUvarint(b, s.lastID.seq)
	b = encoding.AppendUvarint(b, s.maxDeletedID.ms)
	b = encoding.AppendUvarint(b, s.maxDeletedID.seq)
	b = encoding.AppendUvarint(b, s.entriesAdded)
	return encodeGroups(b, s.groups)
}

// streamReadHeader fills the stream-level metadata of s from a header row value.
func streamReadHeader(s *stream, val []byte) error {
	off := 0
	read := func() (uint64, error) {
		v, n, err := encoding.Uvarint(val[off:])
		if err != nil {
			return 0, err
		}
		off += n
		return v, nil
	}
	var err error
	if s.lastID.ms, err = read(); err != nil {
		return err
	}
	if s.lastID.seq, err = read(); err != nil {
		return err
	}
	if s.maxDeletedID.ms, err = read(); err != nil {
		return err
	}
	if s.maxDeletedID.seq, err = read(); err != nil {
		return err
	}
	if s.entriesAdded, err = read(); err != nil {
		return err
	}
	groups, _, err := decodeGroups(val, off)
	if err != nil {
		return err
	}
	s.groups = groups
	return nil
}

// streamHeader probes the value header at key without decoding the body, so a
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func streamHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// streamCollMaterialize reads a btree-backed stream fully into the in-memory
// form: the header row gives the stream-level metadata and groups, and a forward
// walk of the entry rows gives the entries in ID order. The caller has confirmed
// the key is a coll stream. This is the fallback the not-yet-bounded commands
// share; the append and read hot paths use the dedicated helpers below and never
// call it.
func streamCollMaterialize(db *keyspace.DB, key []byte) (*stream, error) {
	s := &stream{}
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		hv, ok, e := r.Get([]byte{streamRowHeader})
		if e != nil {
			return e
		}
		if ok {
			if e := streamReadHeader(s, hv); e != nil {
				return e
			}
		}
		s.entries = make([]streamEntry, 0, r.Count())
		c := r.Cursor()
		if e := c.Seek([]byte{streamRowEntry}); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != streamRowEntry {
				break
			}
			fields, e := streamEntryFields(c.Value())
			if e != nil {
				return e
			}
			s.entries = append(s.entries, streamEntry{id: streamIDFromRow(k), fields: fields})
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return s, err
}

// streamStoreColl writes the whole stream into the btree-backed sub-tree form,
// rebuilding it under one shard write lock. It clears any rows already present
// (so an entry removed by XDEL or XTRIM does not survive a rebuild) and writes
// the header row plus one row per entry, setting the metadata count to the entry
// count. CollUpdate creates a fresh sub-tree when the key was a blob (promotion)
// and reuses the existing one when the key was already coll, so either way the
// result is the exact entry set, and the key's TTL is preserved.
func streamStoreColl(db *keyspace.DB, key []byte, s *stream) error {
	return db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		if err := streamClearRows(w); err != nil {
			return err
		}
		if _, err := w.Put([]byte{streamRowHeader}, streamHeaderValue(s)); err != nil {
			return err
		}
		for _, e := range s.entries {
			if _, err := w.Put(streamEntryRow(e.id), streamEntryValue(e.fields)); err != nil {
				return err
			}
		}
		w.SetCount(uint64(len(s.entries)))
		return nil
	})
}

// streamClearRows deletes every row currently in the writer's sub-tree. It
// gathers the keys in one forward pass before deleting so the cursor is not
// mutated mid-walk. A freshly created sub-tree (a blob being promoted) has no
// rows, so this is a no-op there.
func streamClearRows(w *keyspace.CollWriter) error {
	var keys [][]byte
	c := w.Cursor()
	if err := c.First(); err != nil {
		return err
	}
	for c.Valid() {
		keys = append(keys, append([]byte(nil), c.Key()...))
		if err := c.Next(); err != nil {
			return err
		}
	}
	for _, k := range keys {
		if _, err := w.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// streamCollLen returns the live entry count of a btree-backed stream from the
// collection metadata, without walking the entry rows. The caller has confirmed
// the key is a coll stream.
func streamCollLen(db *keyspace.DB, key []byte) (int64, error) {
	var n int64
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		n = int64(r.Count())
		return nil
	})
	return n, err
}

// streamCollLastID reads just the last ID of a btree-backed stream from its
// header row, for XADD to resolve the next ID and for XREAD to fix its $ cursor
// without materializing the entries.
func streamCollLastID(db *keyspace.DB, key []byte) (streamID, error) {
	var last streamID
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		hv, ok, e := r.Get([]byte{streamRowHeader})
		if e != nil || !ok {
			return e
		}
		var s stream
		if e := streamReadHeader(&s, hv); e != nil {
			return e
		}
		last = s.lastID
		return nil
	})
	return last, err
}

// streamCollAdd appends one entry to a btree-backed stream in place: it writes the
// entry row at id, bumps the entry count and the entries-added counter, advances
// the last ID, applies a MAXLEN or MINID trim from the low end, and rewrites only
// the small header row. It touches the appended entry, the trimmed window, and the
// header, never the whole stream. The caller has resolved id against the current
// last ID and confirmed the key is a coll stream.
func streamCollAdd(db *keyspace.DB, key []byte, id streamID, fields [][]byte, trim trimSpec) (trimmed int64, err error) {
	err = db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		var s stream
		hv, ok, e := w.Get([]byte{streamRowHeader})
		if e != nil {
			return e
		}
		if ok {
			if e := streamReadHeader(&s, hv); e != nil {
				return e
			}
		}
		if _, e := w.Put(streamEntryRow(id), streamEntryValue(fields)); e != nil {
			return e
		}
		s.lastID = id
		s.entriesAdded++
		count := w.Count() + 1
		if trim.kind != trimNone {
			dropped, e := streamCollTrim(w, trim, int64(count))
			if e != nil {
				return e
			}
			trimmed = dropped
			count -= uint64(dropped)
		}
		w.SetCount(count)
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(&s)); e != nil {
			return e
		}
		return nil
	})
	return trimmed, err
}

// streamCollTrim removes entries from the low end of a coll stream per ts and
// returns the count removed, deleting only the rows that fall outside the kept
// window. count is the current entry count. MAXLEN drops the lowest entries until
// the count fits; MINID drops the leading entries whose ID is below the bound. An
// approximate trim with a LIMIT caps how many it removes. Trimming does not
// advance max-deleted-id, matching Redis.
func streamCollTrim(w *keyspace.CollWriter, ts trimSpec, count int64) (int64, error) {
	var drop int64
	switch ts.kind {
	case trimMaxLen:
		if count > ts.maxLen {
			drop = count - ts.maxLen
		}
	case trimMinID:
		// Walk the low entry rows until the first ID at or above the bound.
		c := w.Cursor()
		if err := c.Seek([]byte{streamRowEntry}); err != nil {
			return 0, err
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != streamRowEntry {
				break
			}
			if !streamIDFromRow(k).less(ts.minID) {
				break
			}
			drop++
			if err := c.Next(); err != nil {
				return 0, err
			}
		}
	default:
		return 0, nil
	}
	if ts.hasLimit && drop > ts.limit {
		drop = ts.limit
	}
	if drop <= 0 {
		return 0, nil
	}
	// Collect the lowest drop entry-row keys, then delete them. A forward walk from
	// the first entry row gives them in ascending ID order.
	keys := make([][]byte, 0, drop)
	c := w.Cursor()
	if err := c.Seek([]byte{streamRowEntry}); err != nil {
		return 0, err
	}
	for c.Valid() && int64(len(keys)) < drop {
		k := c.Key()
		if len(k) == 0 || k[0] != streamRowEntry {
			break
		}
		keys = append(keys, append([]byte(nil), k...))
		if err := c.Next(); err != nil {
			return 0, err
		}
	}
	for _, k := range keys {
		if _, err := w.Delete(k); err != nil {
			return 0, err
		}
	}
	return int64(len(keys)), nil
}

// streamCollRange returns the entries of a btree-backed stream within the start
// and end bounds, capped by count when count is non-negative, walking a bounded
// cursor window rather than materializing the whole stream. rev returns the
// highest entries first and, with a count, walks down from the end so the cost is
// the returned window, not the full range. The caller has confirmed the key is a
// coll stream.
func streamCollRange(db *keyspace.DB, key []byte, start, end rangeBound, count int64, rev bool) ([]streamEntry, error) {
	var out []streamEntry
	clone := func(c *keyspace.CollCursor) streamEntry {
		fields, _ := streamEntryFields(c.Value())
		return streamEntry{id: streamIDFromRow(c.Key()), fields: fields}
	}
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		c.UseArena()
		if rev {
			// Walk down from the end bound, taking the highest matching entries first.
			if err := c.SeekForPrev(streamEntryRow(end.id)); err != nil {
				return err
			}
			for c.Valid() {
				k := c.Key()
				if len(k) == 0 || k[0] != streamRowEntry {
					break
				}
				id := streamIDFromRow(k)
				if end.excl && id.equal(end.id) {
					if err := c.Prev(); err != nil {
						return err
					}
					continue
				}
				if id.less(start.id) {
					break
				}
				if start.excl && id.equal(start.id) {
					break
				}
				e := clone(c)
				// streamEntryFields aliases the arena value, so copy the chunks the
				// entry keeps before the cursor moves.
				out = append(out, streamCopyEntry(e))
				if count >= 0 && int64(len(out)) >= count {
					break
				}
				if err := c.Prev(); err != nil {
					return err
				}
			}
			return nil
		}
		// Forward walk from the start bound.
		if err := c.Seek(streamEntryRow(start.id)); err != nil {
			return err
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != streamRowEntry {
				break
			}
			id := streamIDFromRow(k)
			if start.excl && id.equal(start.id) {
				if err := c.Next(); err != nil {
					return err
				}
				continue
			}
			if end.id.less(id) {
				break
			}
			if end.excl && id.equal(end.id) {
				break
			}
			out = append(out, streamCopyEntry(clone(c)))
			if count >= 0 && int64(len(out)) >= count {
				break
			}
			if err := c.Next(); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// streamCopyEntry deep-copies an entry's field chunks, which alias the cursor's
// arena buffer, so the returned entry stays valid after the cursor advances.
func streamCopyEntry(e streamEntry) streamEntry {
	fields := make([][]byte, len(e.fields))
	for i, f := range e.fields {
		fields[i] = append([]byte(nil), f...)
	}
	return streamEntry{id: e.id, fields: fields}
}
