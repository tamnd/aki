package command

import (
	"bytes"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// A large stream is stored element-per-row in the key's btree-backed collection
// sub-tree (keyspace.CollUpdate / CollRead), the same machinery the hash, set,
// zset and list types use. A stream keeps three row families in the one sub-tree:
//
//	0x00                                            -> header (last ID, max-deleted ID, entries-added, groups without PELs)
//	0x01 + ms(8 BE) + seq(8 BE)                     -> entry  (the packed field/value list)
//	0x02 + uvarint(len(group)) + group + ms + seq   -> PEL    (one pending-entry record: consumer, delivery-time, delivery-count)
//
// The entry row key is the 16-byte stream ID in big-endian, so the rows sort in
// exactly the ascending ID order XADD appends in and XRANGE/XREAD walk. The
// single header row sorts before every entry row (0x00 < 0x01), so a forward walk
// of the 0x01 rows reproduces the entry log. The collection metadata count is the
// live entry count (the header and PEL rows are not counted), which makes XLEN
// O(1).
//
// The consumer-group pending entries lists live in the 0x02 rows, not inline in
// the header, so a consumer that never acknowledges does not grow the header row
// without bound, and the XADD hot path (which rewrites only the header) does not
// pay the PEL size per append. A PEL row key is the group's length-prefixed name
// then the 16-byte entry ID, so a group's pending records are contiguous and
// sorted by ID, and one group's prefix can never collide with another's.
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
	streamRowPEL    = 0x02 // a pending-entry row, keyed by group name then 16-byte ID

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
	return encodeGroupsNoPEL(b, s.groups)
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
	groups, _, err := decodeGroupsNoPEL(val, off)
	if err != nil {
		return err
	}
	s.groups = groups
	return nil
}

// encodeGroupsNoPEL appends the coll header's group section: a group count then
// each group's name, last-delivered ID, entries-read counter, and consumer set.
// Unlike the blob codec's encodeGroups it omits the pending entries list, which
// the coll form keeps in the 0x02 sibling rows, so the header row stays bounded by
// the group and consumer count rather than by the pending count.
func encodeGroupsNoPEL(body []byte, groups []*group) []byte {
	body = encoding.AppendUvarint(body, uint64(len(groups)))
	for _, g := range groups {
		body = encoding.AppendUvarint(body, uint64(len(g.name)))
		body = append(body, g.name...)
		body = encoding.AppendUvarint(body, g.lastID.ms)
		body = encoding.AppendUvarint(body, g.lastID.seq)
		body = encoding.AppendUvarint(body, g.entriesRead)
		body = encoding.AppendUvarint(body, g.pending)
		body = encoding.AppendUvarint(body, uint64(len(g.consumers)))
		for _, c := range g.consumers {
			body = encoding.AppendUvarint(body, uint64(len(c.name)))
			body = append(body, c.name...)
			body = encoding.AppendUvarint(body, uint64(c.seenTime))
			body = encoding.AppendUvarint(body, uint64(c.activeTime))
			body = encoding.AppendUvarint(body, c.pending)
		}
	}
	return body
}

// decodeGroupsNoPEL unpacks the coll header's group section written by
// encodeGroupsNoPEL: groups with their consumers but no pending entries list. The
// returned groups have a nil pel; a caller that needs the PELs reads them from the
// 0x02 rows through loadGroupPELs.
func decodeGroupsNoPEL(body []byte, off int) ([]*group, int, error) {
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
		if g.pending, err = read(); err != nil {
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
			pending, err := read()
			if err != nil {
				return nil, off, err
			}
			g.consumers = append(g.consumers, &consumer{
				name:       string(cn),
				seenTime:   int64(seen),
				activeTime: int64(active),
				pending:    pending,
			})
		}
		groups = append(groups, g)
	}
	return groups, off, nil
}

// streamPELPrefix builds the row-key prefix shared by every PEL row of one group:
// the PEL prefix byte, the group name length, and the group name. A group's PEL
// rows are exactly the rows under this prefix, sorted by entry ID. The length
// prefix keeps one group's rows from being mistaken for another's even when one
// name is a prefix of the other.
func streamPELPrefix(group string) []byte {
	p := make([]byte, 0, 1+10+len(group))
	p = append(p, streamRowPEL)
	p = encoding.AppendUvarint(p, uint64(len(group)))
	p = append(p, group...)
	return p
}

// streamPELRow builds the full PEL row key: the group prefix then the 16-byte ID
// in big-endian, so a group's records sort by entry ID.
func streamPELRow(group string, id streamID) []byte {
	k := streamPELPrefix(group)
	k = encoding.AppendU64BE(k, id.ms)
	k = encoding.AppendU64BE(k, id.seq)
	return k
}

// streamPELRowID reads the entry ID back out of a PEL row key, given the length of
// the group prefix the key carries.
func streamPELRowID(k []byte, prefixLen int) streamID {
	return streamID{ms: encoding.U64BE(k[prefixLen : prefixLen+8]), seq: encoding.U64BE(k[prefixLen+8 : prefixLen+16])}
}

// streamPELValue packs a pending-entry record's row value: the owning consumer,
// the delivery time, and the delivery count. The entry ID lives in the row key.
func streamPELValue(pe pelEntry) []byte {
	v := encoding.AppendUvarint(nil, uint64(len(pe.consumer)))
	v = append(v, pe.consumer...)
	v = encoding.AppendUvarint(v, uint64(pe.deliveryTime))
	v = encoding.AppendUvarint(v, pe.deliveryCount)
	return v
}

// streamPELFromValue unpacks a PEL row value into a pelEntry. The id field is left
// zero; the caller fills it from the row key.
func streamPELFromValue(val []byte) (pelEntry, error) {
	consumer, off, err := readChunk(val, 0)
	if err != nil {
		return pelEntry{}, err
	}
	dt, n, err := encoding.Uvarint(val[off:])
	if err != nil {
		return pelEntry{}, err
	}
	off += n
	dc, _, err := encoding.Uvarint(val[off:])
	if err != nil {
		return pelEntry{}, err
	}
	return pelEntry{consumer: string(consumer), deliveryTime: int64(dt), deliveryCount: dc}, nil
}

// pelCursorSource is the common cursor factory of a CollReader and a CollWriter, so
// loadGroupPELs can scan the PEL rows from either a read closure or a write closure.
type pelCursorSource interface {
	Cursor() *keyspace.CollCursor
}

// loadGroupPELs fills each group's pel slice from its 0x02 sibling rows, walking
// one group's contiguous prefix at a time in entry-ID order. The caller uses this
// only when it needs the PELs (the consumer-group commands); the hot append and
// trim paths read the header alone and leave the PEL rows untouched.
func loadGroupPELs(src pelCursorSource, groups []*group) error {
	for _, g := range groups {
		prefix := streamPELPrefix(g.name)
		c := src.Cursor()
		if err := c.Seek(prefix); err != nil {
			return err
		}
		for c.Valid() {
			k := c.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			pe, err := streamPELFromValue(c.Value())
			if err != nil {
				return err
			}
			pe.id = streamPELRowID(k, len(prefix))
			g.pel = append(g.pel, pe)
			if err := c.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

// streamCollLoadGroupPELWindow fills each group's pel slice with at most window rows
// from its 0x02 sibling prefix, in entry-ID order. A window of -1 loads the whole
// PEL (the caller asked for everything with COUNT 0). XINFO STREAM FULL uses it so
// the introspection reply scans a bounded PEL window per group rather than cloning
// the whole pending list; the true per-group and per-consumer totals come from the
// header counters, not the loaded window length.
func streamCollLoadGroupPELWindow(db *keyspace.DB, key []byte, groups []*group, window int64) error {
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		for _, g := range groups {
			prefix := streamPELPrefix(g.name)
			c := r.Cursor()
			if err := c.Seek(prefix); err != nil {
				return err
			}
			for c.Valid() {
				if window >= 0 && int64(len(g.pel)) >= window {
					break
				}
				k := c.Key()
				if !bytes.HasPrefix(k, prefix) {
					break
				}
				pe, err := streamPELFromValue(c.Value())
				if err != nil {
					return err
				}
				pe.id = streamPELRowID(k, len(prefix))
				g.pel = append(g.pel, pe)
				if err := c.Next(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

// streamClearPELRows deletes every 0x02 PEL row in the writer's sub-tree, gathering
// the keys in one forward pass before deleting so the cursor is not mutated
// mid-walk. storeStreamGroupsFull uses it to replace a stream's PELs wholesale.
func streamClearPELRows(w *keyspace.CollWriter) error {
	var keys [][]byte
	c := w.Cursor()
	if err := c.Seek([]byte{streamRowPEL}); err != nil {
		return err
	}
	for c.Valid() {
		k := c.Key()
		if len(k) == 0 || k[0] != streamRowPEL {
			break
		}
		keys = append(keys, append([]byte(nil), k...))
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

// collGetter is the point-read method shared by a CollReader and a CollWriter, so
// the same entry-row and PEL-row lookups serve a read closure or a write closure.
type collGetter interface {
	Get(sub []byte) ([]byte, bool, error)
}

// streamPELGet point-reads one group's PEL row for id, filling the id from the
// caller (the row value omits it). It is the bounded alternative to scanning a
// whole group's PEL: a consumer-group command that names specific ids reads only
// those rows.
func streamPELGet(g collGetter, group string, id streamID) (pelEntry, bool, error) {
	v, ok, err := g.Get(streamPELRow(group, id))
	if err != nil || !ok {
		return pelEntry{}, false, err
	}
	pe, err := streamPELFromValue(v)
	if err != nil {
		return pelEntry{}, false, err
	}
	pe.id = id
	return pe, true, nil
}

// streamPELPut writes one PEL row, an insert or an in-place update of the named
// pending entry, touching no other row.
func streamPELPut(w *keyspace.CollWriter, group string, pe pelEntry) error {
	_, err := w.Put(streamPELRow(group, pe.id), streamPELValue(pe))
	return err
}

// streamPELDelete removes one PEL row, reporting whether it existed.
func streamPELDelete(w *keyspace.CollWriter, group string, id streamID) (bool, error) {
	return w.Delete(streamPELRow(group, id))
}

// streamCollPELBounds returns the smallest and largest entry IDs held in one
// group's PEL rows by reading just the two end rows of the group's 0x02 prefix:
// a forward seek to the prefix for the min and a backward seek past the prefix
// for the max. It is the bounded source of the XPENDING summary min/max IDs, so
// the summary never loads the whole pending list. found is false when the group
// has no PEL rows.
func streamCollPELBounds(db *keyspace.DB, key []byte, group string) (minID, maxID streamID, found bool, err error) {
	prefix := streamPELPrefix(group)
	ok, e := db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		if e := c.Seek(prefix); e != nil {
			return e
		}
		if !c.Valid() || !bytes.HasPrefix(c.Key(), prefix) {
			return nil
		}
		minID = streamPELRowID(c.Key(), len(prefix))
		c2 := r.Cursor()
		if e := c2.SeekForPrev(streamPELRow(group, maxStreamID)); e != nil {
			return e
		}
		if c2.Valid() && bytes.HasPrefix(c2.Key(), prefix) {
			maxID = streamPELRowID(c2.Key(), len(prefix))
		} else {
			maxID = minID
		}
		found = true
		return nil
	})
	if e != nil {
		return minID, maxID, false, e
	}
	if !ok {
		return minID, maxID, false, nil
	}
	return minID, maxID, found, nil
}

// streamCollPersistDelivery writes back a > delivery: the header row (the advanced
// group last id, entries-read counter, and consumer times the caller already
// updated in s) plus one PEL row per just-delivered entry. It touches only the
// delivered rows, so the cost is the delivered count, never the whole pending
// list. A NOACK delivery keeps no PEL, so it writes the header alone.
func streamCollPersistDelivery(db *keyspace.DB, key []byte, s *stream, group string, es []streamEntry, consumer string, now int64, noAck bool) error {
	return db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s)); e != nil {
			return e
		}
		if noAck {
			return nil
		}
		for _, e := range es {
			pe := pelEntry{id: e.id, consumer: consumer, deliveryTime: now, deliveryCount: 1}
			if err := streamPELPut(w, group, pe); err != nil {
				return err
			}
		}
		return nil
	})
}

// streamCollDelConsumer drops a consumer from the header group and deletes its PEL
// rows, returning the count removed. It scans only the group's PEL prefix (the rows
// keyed by group then id), so the cost is the group's pending size, never the entry
// log; a consumer with nothing pending costs one seek. The caller has confirmed the
// key is a coll stream and the group exists.
func streamCollDelConsumer(db *keyspace.DB, key []byte, group, consumer string) (int64, error) {
	var removed int64
	err := db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		hv, ok, e := w.Get([]byte{streamRowHeader})
		if e != nil || !ok {
			return e
		}
		s := &stream{}
		if e := streamReadHeader(s, hv); e != nil {
			return e
		}
		g := s.findGroup(group)
		if g == nil {
			return nil
		}
		for i, c := range g.consumers {
			if c.name == consumer {
				g.consumers = append(g.consumers[:i], g.consumers[i+1:]...)
				break
			}
		}
		prefix := streamPELPrefix(group)
		var keys [][]byte
		cur := w.Cursor()
		if e := cur.Seek(prefix); e != nil {
			return e
		}
		for cur.Valid() {
			k := cur.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			pe, e := streamPELFromValue(cur.Value())
			if e != nil {
				return e
			}
			if pe.consumer == consumer {
				keys = append(keys, append([]byte(nil), k...))
			}
			if e := cur.Next(); e != nil {
				return e
			}
		}
		for _, k := range keys {
			if _, e := w.Delete(k); e != nil {
				return e
			}
			removed++
		}
		// Drop the removed rows from the group's pending counter before the header is
		// written, so the bounded readers see the right count.
		if uint64(removed) <= g.pending {
			g.pending -= uint64(removed)
		} else {
			g.pending = 0
		}
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s)); e != nil {
			return e
		}
		return nil
	})
	return removed, err
}

// streamCollDestroyGroup removes one group from the header and deletes that group's
// PEL rows in a single prefix scan of its 0x02 rows, reporting whether the group
// existed. The cost is the destroyed group's pending size, never the entry log; a
// group with nothing pending costs one seek. The caller has confirmed the key is a
// coll stream. The other groups' rows are untouched (their prefixes differ).
func streamCollDestroyGroup(db *keyspace.DB, key []byte, group string) (bool, error) {
	var existed bool
	err := db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		hv, ok, e := w.Get([]byte{streamRowHeader})
		if e != nil || !ok {
			return e
		}
		s := &stream{}
		if e := streamReadHeader(s, hv); e != nil {
			return e
		}
		if !s.removeGroup(group) {
			return nil
		}
		existed = true
		prefix := streamPELPrefix(group)
		var keys [][]byte
		cur := w.Cursor()
		if e := cur.Seek(prefix); e != nil {
			return e
		}
		for cur.Valid() {
			k := cur.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			keys = append(keys, append([]byte(nil), k...))
			if e := cur.Next(); e != nil {
				return e
			}
		}
		for _, k := range keys {
			if _, e := w.Delete(k); e != nil {
				return e
			}
		}
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s)); e != nil {
			return e
		}
		return nil
	})
	return existed, err
}

// streamHeader probes the value header at key without decoding the body, so a
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func streamHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// streamHeaderIsColl reports whether key is stored in coll (sub-tree) form, so a
// command can pick the header-only loader for a coll stream and the full-blob
// loader otherwise. A missing key reads as not coll.
func streamHeaderIsColl(db *keyspace.DB, key []byte) (bool, error) {
	hdr, found, err := streamHeader(db, key)
	if err != nil || !found {
		return false, err
	}
	return hdr.IsColl(), nil
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
			if e := loadGroupPELs(r, s.groups); e != nil {
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
	// The promotion carries the full PEL in s; derive the header counters from it so
	// the bounded header-only readers see correct pending counts.
	recountPending(s)
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
		// The groups' PELs ride in the 0x02 sibling rows, not the header, so write
		// one row per pending record. A promotion from the blob form carries its
		// PELs here; streamClearRows above cleared any stale rows first.
		for _, g := range s.groups {
			for _, pe := range g.pel {
				if _, err := w.Put(streamPELRow(g.name, pe.id), streamPELValue(pe)); err != nil {
					return err
				}
			}
		}
		w.SetCount(uint64(len(s.entries)))
		return nil
	})
}

// getStreamGroups reads a stream's group metadata without materializing the entry
// log. In coll form it decodes only the small header row (last ID, max-deleted ID,
// entries-added, and the groups); the entry rows are never touched, so a consumer-
// group command on a million-entry stream stays bounded by the group set rather
// than the log. In blob form it falls back to getStream, where the whole small
// blob decodes in one shot anyway. The returned stream has no entries, so callers
// must only read or write group and header state, never the entry list.
func getStreamGroups(db *keyspace.DB, key []byte) (*stream, keyspace.ValueHeader, bool, error) {
	hdr, found, err := streamHeader(db, key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeStream {
		return nil, hdr, true, nil
	}
	if !hdr.IsColl() {
		return getStream(db, key)
	}
	s := &stream{}
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		hv, ok, e := r.Get([]byte{streamRowHeader})
		if e != nil {
			return e
		}
		if ok {
			return streamReadHeader(s, hv)
		}
		return nil
	})
	if err != nil {
		return nil, hdr, true, err
	}
	return s, hdr, true, nil
}

// storeStreamGroups writes a stream's group metadata back. In coll form it
// rewrites only the header row, leaving the entry rows and the metadata count
// untouched, so the write is bounded by the group set and the key stays coll (no
// demote, no entry rebuild). In blob form it falls back to storeStream. The caller
// must have loaded s through getStreamGroups (coll: entries empty by design; blob:
// entries intact) so a blob rewrite never drops the entry log.
func storeStreamGroups(db *keyspace.DB, key []byte, s *stream, ttlMs int64) error {
	hdr, found, err := streamHeader(db, key)
	if err != nil {
		return err
	}
	if !found || !hdr.IsColl() {
		return storeStream(db, key, s, ttlMs)
	}
	return db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		// Put on the existing 0x00 row is an update, not an insert, and Count is
		// caller-maintained, so the entry count the metadata carries is unchanged.
		_, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s))
		return e
	})
}

// getStreamGroupsFull reads a stream's group metadata together with the pending
// entries lists. In coll form it decodes the header row for the groups and then
// scans each group's 0x02 PEL rows into its pel slice; the entry log (the 0x01
// rows) is still not touched. In blob form it falls back to getStream, where the
// whole small blob (PELs included) decodes in one shot. The consumer-group commands
// that read or rewrite a PEL use this rather than getStreamGroups, which leaves the
// PELs unloaded for the hot append and trim paths.
func getStreamGroupsFull(db *keyspace.DB, key []byte) (*stream, keyspace.ValueHeader, bool, error) {
	hdr, found, err := streamHeader(db, key)
	if err != nil || !found {
		return nil, hdr, found, err
	}
	if hdr.Type != keyspace.TypeStream {
		return nil, hdr, true, nil
	}
	if !hdr.IsColl() {
		return getStream(db, key)
	}
	s := &stream{}
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		hv, ok, e := r.Get([]byte{streamRowHeader})
		if e != nil {
			return e
		}
		if ok {
			if e := streamReadHeader(s, hv); e != nil {
				return e
			}
		}
		return loadGroupPELs(r, s.groups)
	})
	if err != nil {
		return nil, hdr, true, err
	}
	return s, hdr, true, nil
}

// storeStreamGroupsFull writes a stream's group metadata and PELs back. In coll
// form it rewrites the header row and replaces every 0x02 PEL row from the in-memory
// groups, leaving the entry rows and the metadata count untouched, so the key stays
// coll. In blob form it falls back to storeStream. The caller must have loaded s
// through getStreamGroupsFull (coll: PELs scanned in; blob: PELs inline). It costs
// the total pending size, the same as the old inline-in-header encoding did; the
// per-command bounding lands in the later 3e sub-slices.
func storeStreamGroupsFull(db *keyspace.DB, key []byte, s *stream, ttlMs int64) error {
	hdr, found, err := streamHeader(db, key)
	if err != nil {
		return err
	}
	if !found || !hdr.IsColl() {
		return storeStream(db, key, s, ttlMs)
	}
	// This path holds the full PEL in s, so derive the header counters from it; this
	// both writes correct counts and heals any drift left by a bounded incremental
	// path before the next full-PEL rewrite.
	recountPending(s)
	return db.CollUpdate(key, keyspace.TypeStream, keyspace.EncStream, func(w *keyspace.CollWriter) error {
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(s)); e != nil {
			return e
		}
		if e := streamClearPELRows(w); e != nil {
			return e
		}
		for _, g := range s.groups {
			for _, pe := range g.pel {
				if _, e := w.Put(streamPELRow(g.name, pe.id), streamPELValue(pe)); e != nil {
					return e
				}
			}
		}
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

// streamCollDel deletes the listed entries from a coll stream in place, point
// deleting each entry row that is present, advancing the max-deleted ID, and
// decrementing the count. It rewrites only the small header row and the deleted
// rows, never the whole stream. When the last entry is removed it reports
// emptied=true and the surviving stream-level metadata: a stream with no entries
// still exists in Redis (unlike the other collection types), but CollUpdate tears
// the sub-tree down when the count reaches zero, so the caller recreates the empty
// stream from the returned metadata.
func streamCollDel(db *keyspace.DB, key []byte, ids []streamID) (deleted int64, emptied bool, meta stream, err error) {
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
		count := w.Count()
		for _, id := range ids {
			existed, e := w.Delete(streamEntryRow(id))
			if e != nil {
				return e
			}
			if !existed {
				continue
			}
			deleted++
			count--
			if s.maxDeletedID.less(id) {
				s.maxDeletedID = id
			}
		}
		meta = s
		if deleted == 0 {
			return nil
		}
		if count == 0 {
			// Let CollUpdate tear the now entry-less sub-tree down; the caller
			// recreates the empty stream as a blob from meta. Load the PEL rows into
			// meta first, since the sub-tree (and its 0x02 rows) is about to go and the
			// recreated blob must keep the groups' pending entries.
			if e := loadGroupPELs(w, s.groups); e != nil {
				return e
			}
			emptied = true
			w.SetCount(0)
			return nil
		}
		w.SetCount(count)
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(&s)); e != nil {
			return e
		}
		return nil
	})
	return deleted, emptied, meta, err
}

// streamCollTrimCmd trims a coll stream from the low end per ts and returns the
// number removed. It reuses streamCollTrim (the same low-end delete XADD uses) and
// rewrites only the metadata count; the trim does not touch the header fields
// (Redis does not advance max-deleted ID on a trim). As with streamCollDel, when
// the trim empties the stream it reports emptied=true and the surviving metadata
// so the caller recreates the empty stream rather than letting it tear down.
func streamCollTrimCmd(db *keyspace.DB, key []byte, ts trimSpec) (removed int64, emptied bool, meta stream, err error) {
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
		meta = s
		count := int64(w.Count())
		dropped, e := streamCollTrim(w, ts, count)
		if e != nil {
			return e
		}
		removed = dropped
		newCount := count - dropped
		if newCount <= 0 && count > 0 {
			// As in streamCollDel, load the PEL rows into meta before the sub-tree
			// tears down so the recreated empty blob keeps the groups' pending entries.
			if e := loadGroupPELs(w, s.groups); e != nil {
				return e
			}
			emptied = true
			w.SetCount(0)
			return nil
		}
		w.SetCount(uint64(newCount))
		return nil
	})
	return removed, emptied, meta, err
}

// streamCollSetID rewrites a coll stream's header fields in place for XSETID:
// the last ID, and optionally the entries-added counter and max-deleted ID. It
// reads the highest present entry through the writer's cursor to reject a last ID
// below the log, touching only the header row. tooSmall reports that rejection.
func streamCollSetID(db *keyspace.DB, key []byte, newLast streamID, setEntriesAdded bool, entriesAdded uint64, setMaxDeleted bool, maxDeleted streamID) (tooSmall bool, err error) {
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
		// The new last ID cannot drop below the highest entry still in the log. The
		// last entry row is the highest present ID; the header sorts below it.
		c := w.Cursor()
		if e := c.Last(); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if len(k) != 0 && k[0] == streamRowEntry {
				if newLast.less(streamIDFromRow(k)) {
					tooSmall = true
					return nil
				}
				break
			}
			if len(k) != 0 && k[0] != streamRowEntry {
				break
			}
			if e := c.Prev(); e != nil {
				return e
			}
		}
		s.lastID = newLast
		if setEntriesAdded {
			s.entriesAdded = entriesAdded
		}
		if setMaxDeleted {
			s.maxDeletedID = maxDeleted
		}
		if _, e := w.Put([]byte{streamRowHeader}, streamHeaderValue(&s)); e != nil {
			return e
		}
		return nil
	})
	return tooSmall, err
}

// streamCollEntry point-fetches one entry's fields from a coll stream's entry row
// through an open reader. ok is false when the row is absent (the entry was deleted
// or trimmed). The fields are copied, so they stay valid after the reader closes.
// XCLAIM and XAUTOCLAIM use this to check existence and build their replies without
// materializing the whole stream.
func streamCollEntry(r collGetter, id streamID) (fields [][]byte, ok bool, err error) {
	v, ok, err := r.Get(streamEntryRow(id))
	if err != nil || !ok {
		return nil, ok, err
	}
	f, err := streamEntryFields(v)
	if err != nil {
		return nil, false, err
	}
	return streamCopyEntry(streamEntry{fields: f}).fields, true, nil
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
