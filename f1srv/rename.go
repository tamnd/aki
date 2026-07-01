package f1srv

import (
	"bytes"
	"encoding/binary"
)

// RENAME and RENAMENX move a key, of any type, to a new name. On f1raw a physical key equals
// the user key bytes stored in the record, and the store never migrates a key between primary
// buckets, so a rename cannot relabel a record in place: it re-publishes every row the key owns
// under the new name and deletes the old rows. For a string that is one record; for a collection
// it is the header row plus every element (and every sibling family) row.
//
// Every element and sibling row is keyed uvarint(len(key)) | key | suffix, where the suffix
// carries no reference to the key (a hash field's bytes, a zset member after its family tag, a
// stream entry ID after its family tag, and so on). That uniform shape is what makes the move
// generic: the new composite key is the destination's key-header followed by the same suffix,
// so one routine rewrites hash fields, set members, both zset indexes, and all four stream
// families without knowing what any of them means. List elements are the one family not carried
// in the ordered index (positional access is window arithmetic, not a prefix scan), so they are
// walked straight off the header window instead.
//
// The header rows and the TTL sidecar hold no key bytes in their value, so they move verbatim:
// a collection's header keeps its count and encoding, a list's header keeps its head/tail window
// (the destination inherits the same positions), and the destination inherits the source's TTL
// while any TTL the destination carried is discarded. Semantics match Redis 8.8 and Valkey 9.1:
// a missing source is "ERR no such key"; a rename onto an existing destination overwrites it;
// RENAMENX returns 0 when the destination exists (including the source-equals-destination case)
// and 1 otherwise; renaming a key to itself is a no-op that still requires the key to exist.

// renameNoSuchKey is the exact error Redis and Valkey return when the source key is absent.
const renameNoSuchKey = "ERR no such key"

// renameBatch bounds one CollScanKV pull while collecting a family's element rows, so a huge
// collection is gathered in bounded rounds rather than one unbounded scan.
const renameBatch = 4096

// cmdRename moves src to dst, overwriting dst if it exists. It errors when src is absent and is a
// no-op OK when src equals dst (and exists).
func (c *connState) cmdRename(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'rename' command")
		return
	}
	src, dst := argv[1], argv[2]
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(src)
		c.expireIfNeeded(dst)
	}
	unlock := c.lockStripes([][]byte{src, dst})
	defer unlock()
	if c.resolveType(src) == keyMissing {
		c.writeErr(renameNoSuchKey)
		return
	}
	if bytes.Equal(src, dst) {
		// Renaming a key to itself keeps it as it is; Redis and Valkey reply OK.
		c.writeSimple("OK")
		return
	}
	c.renameInto(src, dst)
	c.writeSimple("OK")
}

// cmdRenameNx moves src to dst only when dst does not already exist, reporting 1 on the move and
// 0 when dst is present. An absent source is still an error, and src equal to dst counts as dst
// existing, so it reports 0.
func (c *connState) cmdRenameNx(argv [][]byte) {
	if len(argv) != 3 {
		c.writeErr("ERR wrong number of arguments for 'renamenx' command")
		return
	}
	src, dst := argv[1], argv[2]
	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(src)
		c.expireIfNeeded(dst)
	}
	unlock := c.lockStripes([][]byte{src, dst})
	defer unlock()
	if c.resolveType(src) == keyMissing {
		c.writeErr(renameNoSuchKey)
		return
	}
	if bytes.Equal(src, dst) || c.resolveType(dst) != keyMissing {
		c.writeInt(0)
		return
	}
	c.renameInto(src, dst)
	c.writeInt(1)
}

// renameInto performs the move once the caller holds both keys' stripe locks and has confirmed
// src exists and differs from dst. It drops any existing dst in full, moves every row of src to
// dst, and carries the TTL. The source's expiry is read before any mutation so the counter stays
// exact across the drop of dst (which may clear a dst expiry) and the move.
func (c *connState) renameInto(src, dst []byte) {
	atMs, hasTTL := c.getExpiry(src)
	// Clear the destination first so its own rows and TTL do not survive under the new name.
	c.dropKeyLocked(dst)
	c.moveRows(src, dst)
	// The source's expire row and the destination's (already dropped) are handled explicitly so
	// the volatile counter tracks exactly one add and one remove; moveRows never touches expiry.
	if c.srv.volatile.Load() != 0 {
		c.clearExpiryLocked(src)
	}
	if hasTTL {
		c.setExpiryLocked(dst, atMs)
	}
}

// moveRows re-publishes every value-bearing row of src under dst and deletes the src rows,
// dispatched on the source type the same way dropKeyLocked's cascade is. It parallels that
// cascade exactly, moving where the drop deletes.
func (c *connState) moveRows(src, dst []byte) {
	switch c.resolveType(src) {
	case keyString:
		v, _ := c.srv.store.Get(src, nil)
		_ = c.srv.store.Set(dst, v)
		c.srv.store.Delete(src)
	case keyHash:
		c.moveIndexedFamily(src, dst, kindHashField)
		c.moveHeader(src, dst, kindHashMeta)
	case keySet:
		c.moveIndexedFamily(src, dst, kindSetMember)
		c.moveHeader(src, dst, kindSetMeta)
	case keyZset:
		c.moveIndexedFamily(src, dst, kindZsetMember)
		c.moveIndexedFamily(src, dst, kindZsetScore)
		c.moveHeader(src, dst, kindZsetMeta)
	case keyList:
		c.moveListElems(src, dst)
		c.moveHeader(src, dst, kindListMeta)
	case keyStream:
		c.moveIndexedFamily(src, dst, kindStreamEntry)
		c.moveIndexedFamily(src, dst, kindStreamGroup)
		c.moveIndexedFamily(src, dst, kindStreamConsumer)
		c.moveIndexedFamily(src, dst, kindStreamPEL)
		c.moveHeader(src, dst, kindStreamMeta)
	}
}

// moveIndexedFamily moves every ordered-index-backed element row of one kind from src to dst.
// It gathers the source rows first (a pure read that leaves the ordered index stable), then
// re-keys each: the new composite key is dst's key-header followed by the source row's suffix
// (everything past src's key-header), so a hash field, a zset member or score, or a stream
// entry/group/consumer/PEL all move with the same rewrite. Each moved row is inserted into the
// ordered index under its new key and the old row is deleted and unlinked, so the index and the
// hash agree on the live set exactly as a normal write would leave them.
func (c *connState) moveIndexedFamily(src, dst []byte, kind byte) {
	prefix := familyScanPrefix(src, kind)
	hdrLen := keyHeaderLen(src)
	dstHeader := appendKeyHeader(nil, dst)

	// Phase 1: collect the source rows. Each round scans a fresh batch and appends it to the
	// accumulator, advancing after by the last key, until a batch comes back empty or the cursor
	// runs out. Because this phase only reads, the ordered index does not shift under the cursor,
	// and the collected keys and offsets stay valid (the arena is grow-only).
	var oldKeys [][]byte
	var offs []uint64
	scanK := make([][]byte, 0, renameBatch)
	scanO := make([]uint64, 0, renameBatch)
	var after []byte
	for {
		keys, os, last := c.srv.store.CollScanKV(prefix, after, renameBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		oldKeys = append(oldKeys, keys...)
		offs = append(offs, os...)
		if last == nil {
			break
		}
		after = last
	}

	// Phase 2: re-publish under dst and drop the old row. No scan is live now, so mutating the
	// ordered index here is safe. The old key subslice stays readable after its slot is cleared
	// because the arena is grow-only, so delete-after-republish never loses the bytes.
	var vbuf, nkbuf []byte
	for i, oldk := range oldKeys {
		val := c.srv.store.ReadValueAt(offs[i], vbuf[:0])
		nkbuf = append(nkbuf[:0], dstHeader...)
		nkbuf = append(nkbuf, oldk[hdrLen:]...)
		if _, err := c.srv.store.PutKind(nkbuf, val, kind); err != nil {
			continue
		}
		c.srv.store.CollInsert(nkbuf, kind)
		c.srv.store.DeleteKind(oldk, kind)
		c.srv.store.CollRemove(oldk)
	}
}

// moveListElems moves a list's element rows, which are not carried in the ordered index, by
// walking the header window [head, tail) and re-keying each position under dst. The destination
// keeps the same window (moveHeader copies the header verbatim), so position p maps to position p.
func (c *connState) moveListElems(src, dst []byte) {
	head, tail, _, _, ok := c.listHeader(src)
	if !ok {
		return
	}
	var vbuf []byte
	for p := head; p < tail; p++ {
		ek := c.listElemKey(src, p)
		v, got := c.srv.store.TakeKind(ek, vbuf[:0], kindListElem)
		if !got {
			continue
		}
		nek := c.listElemKey(dst, p)
		if _, err := c.srv.store.PutKind(nek, v, kindListElem); err != nil {
			continue
		}
	}
}

// moveHeader moves a collection's header row, which lives under the bare key and holds no key
// bytes in its value, so it re-publishes verbatim. Header rows are not carried in the ordered
// index (they are top-level keys enumerated by the bucket walk), so no index fixup is needed.
func (c *connState) moveHeader(src, dst []byte, metaKind byte) {
	v, ok := c.srv.store.GetKind(src, nil, metaKind)
	if !ok {
		return
	}
	if _, err := c.srv.store.PutKind(dst, v, metaKind); err != nil {
		return
	}
	c.srv.store.DeleteKind(src, metaKind)
}

// familyScanPrefix builds the ordered-index scan bound for one element kind of key into a fresh
// buffer: uvarint(len(key)) | key for the tagless families (hash fields, set members) and that
// header plus the family tag for the tagged families (the two zset indexes and the four stream
// families). It is the rename path's own builder so it never borrows the reusable pbuf a caller
// may still hold.
func familyScanPrefix(key []byte, kind byte) []byte {
	b := appendKeyHeader(nil, key)
	switch kind {
	case kindZsetMember:
		b = append(b, zsetMemberTag)
	case kindZsetScore:
		b = append(b, zsetScoreTag)
	case kindStreamEntry:
		b = append(b, streamEntryTag)
	case kindStreamGroup:
		b = append(b, streamGroupTag)
	case kindStreamConsumer:
		b = append(b, streamConsumerTag)
	case kindStreamPEL:
		b = append(b, streamPELTag)
	}
	return b
}

// appendKeyHeader appends uvarint(len(key)) | key to dst, the shared prefix of every composite
// key, and returns the grown slice.
func appendKeyHeader(dst, key []byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(key)))
	dst = append(dst, tmp[:n]...)
	return append(dst, key...)
}

// keyHeaderLen is the byte length of appendKeyHeader(nil, key), the offset at which a composite
// key's suffix begins.
func keyHeaderLen(key []byte) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutUvarint(tmp[:], uint64(len(key))) + len(key)
}
