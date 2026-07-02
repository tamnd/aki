package f1srv

import "bytes"

// COPY duplicates a key of any type to a new name, keeping the source. It is the one keyspace
// generic command that has to touch elements rather than repoint a header: RENAME and MOVE move
// a binding, but COPY produces a second, independent object, so every row the source owns is
// re-published under the destination while the source rows stay in place.
//
// The copy reuses the same uniform composite-key shape RENAME relies on (see rename.go): every
// element and sibling row is keyed uvarint(len(key)) | key | suffix, so one routine streams a
// hash's fields, a set's members, both zset indexes, and all four stream families into the
// destination by swapping the key-header, one row at a time. It never buffers the whole
// collection: a large source costs its element count in row writes, not a blob clone. List
// elements are copied off the header window (they are not in the ordered index) and header and
// TTL rows copy verbatim because their values carry no key bytes. The destination inherits the
// source TTL; any TTL a replaced destination held is dropped with its old rows.
//
// f1srv exposes a single logical database, so the only valid destination DB is 0 (the current
// db). A DB option naming any other index is refused as out of range, which matches Redis and
// Valkey for a negative or too-large index; a same-db copy is the supported path and is verified
// byte-identical against both. Cross-db COPY into a populated second database is a multi-database
// concern shared with MOVE and SWAPDB and is out of scope for the single-db engine.

// copySameObject is the exact error Redis and Valkey return when source and destination name the
// same key in the same database, checked before the source is even looked up.
const copySameObject = "ERR source and destination objects are the same"

// cmdCopy implements COPY src dst [DB n] [REPLACE]. It replies 1 when the copy happens and 0 when
// it does not (source missing, or destination present without REPLACE), and errors on a same-key
// copy, a bad option, or a DB index the single-db engine cannot satisfy.
func (c *connState) cmdCopy(argv [][]byte) {
	if len(argv) < 3 {
		c.writeErr("ERR wrong number of arguments for 'copy' command")
		return
	}
	src, dst := argv[1], argv[2]

	// Parse the options first so a syntax or DB-range error is reported before any lookup, the
	// same order Redis uses. The current database is 0 and the only one this engine has, so a DB
	// option is accepted only when it names 0; any other index is out of range.
	sameDb := true // whether the destination db equals the source db (always db 0 here)
	replace := false
	for i := 3; i < len(argv); {
		switch {
		case eqFold(argv[i], "REPLACE"):
			replace = true
			i++
		case eqFold(argv[i], "DB"):
			if i+1 >= len(argv) {
				c.writeErr("ERR syntax error")
				return
			}
			n, ok := parseInt64Strict(argv[i+1])
			if !ok {
				c.writeErr("ERR value is not an integer or out of range")
				return
			}
			if n != 0 {
				c.writeErr("ERR DB index is out of range")
				return
			}
			sameDb = n == 0
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return
		}
	}

	// Source and destination that name the same key in the same database are the same object, an
	// error Redis raises before it checks whether the key even exists.
	if sameDb && bytes.Equal(src, dst) {
		c.writeErr(copySameObject)
		return
	}

	if c.srv.volatile.Load() != 0 {
		c.expireIfNeeded(src)
		c.expireIfNeeded(dst)
	}
	unlock := c.lockStripes([][]byte{src, dst})
	defer unlock()

	if c.resolveType(src) == keyMissing {
		c.writeInt(0)
		return
	}
	if c.resolveType(dst) != keyMissing {
		if !replace {
			c.writeInt(0)
			return
		}
		c.dropKeyLocked(dst)
	}
	c.copyInto(src, dst)
	c.writeInt(1)
}

// copyInto duplicates every row of src under dst and carries the TTL, once the caller holds both
// stripe locks and has confirmed src exists, dst is clear (dropped if it existed), and the two
// names differ. The source is left untouched.
func (c *connState) copyInto(src, dst []byte) {
	atMs, hasTTL := c.getExpiry(src)
	c.copyRows(src, dst)
	if hasTTL {
		c.setExpiryLocked(dst, atMs)
	}
}

// copyRows re-publishes every value-bearing row of src under dst, dispatched on the source type
// exactly as moveRows (rename.go) and dropKeyLocked do, but it copies where the move deletes: the
// source rows stay in place. Because a copied row lands under dst's distinct key-header, a family
// scan of src never sees the new rows, so each family is a bounded read of the source followed by
// a bounded write of the destination.
func (c *connState) copyRows(src, dst []byte) {
	switch c.resolveType(src) {
	case keyString:
		v, _ := c.srv.store.Get(src, nil)
		_ = c.srv.store.Set(dst, v)
	case keyHash:
		c.copyIndexedFamily(src, dst, kindHashField)
		c.copyHeader(src, dst, kindHashMeta)
		c.propagateHashFieldTTLs(src, dst, false)
	case keySet:
		c.copyIndexedFamily(src, dst, kindSetMember)
		c.copyHeader(src, dst, kindSetMeta)
	case keyZset:
		c.copyIndexedFamily(src, dst, kindZsetMember)
		c.copyIndexedFamily(src, dst, kindZsetScore)
		c.copyHeader(src, dst, kindZsetMeta)
	case keyList:
		c.copyListElems(src, dst)
		c.copyHeader(src, dst, kindListMeta)
	case keyStream:
		c.copyIndexedFamily(src, dst, kindStreamEntry)
		c.copyIndexedFamily(src, dst, kindStreamGroup)
		c.copyIndexedFamily(src, dst, kindStreamConsumer)
		c.copyIndexedFamily(src, dst, kindStreamPEL)
		c.copyHeader(src, dst, kindStreamMeta)
	}
}

// copyIndexedFamily copies every ordered-index-backed element row of one kind from src to dst. It
// gathers the source rows first (a pure read that leaves the ordered index stable), then for each
// re-keys it under dst's key-header and its own suffix and inserts the new row into the ordered
// index, leaving the source row untouched. It is copyRows' counterpart to rename's
// moveIndexedFamily, minus the delete of the old row.
func (c *connState) copyIndexedFamily(src, dst []byte, kind byte) {
	prefix := familyScanPrefix(src, kind)
	hdrLen := keyHeaderLen(src)
	dstHeader := appendKeyHeader(nil, dst)

	// Phase 1: collect the source rows in bounded batches, advancing the cursor by the last key,
	// the same idiom the enumerating reads use. This phase only reads, so the ordered index does
	// not shift under the cursor and the collected keys and offsets stay valid.
	var srcKeys [][]byte
	var offs []uint64
	scanK := make([][]byte, 0, renameBatch)
	scanO := make([]uint64, 0, renameBatch)
	var after []byte
	for {
		keys, os, last := c.srv.store.CollScanKV(prefix, after, renameBatch, scanK[:0], scanO[:0])
		if len(keys) == 0 {
			break
		}
		srcKeys = append(srcKeys, keys...)
		offs = append(offs, os...)
		if last == nil {
			break
		}
		after = last
	}

	// Phase 2: publish each row under dst. No scan is live now, so mutating the ordered index is
	// safe, and the source rows remain in place.
	var vbuf, nkbuf []byte
	for i, sk := range srcKeys {
		val := c.srv.store.ReadValueAt(offs[i], vbuf[:0])
		nkbuf = append(nkbuf[:0], dstHeader...)
		nkbuf = append(nkbuf, sk[hdrLen:]...)
		if _, err := c.srv.store.PutKind(nkbuf, val, kind); err != nil {
			continue
		}
		c.srv.store.CollInsert(nkbuf, kind)
	}
}

// copyListElems copies a list's element rows, which are not carried in the ordered index, by
// walking the header window [head, tail) and re-keying each position under dst. The destination
// header is copied verbatim, so it keeps the same window and position p maps to position p.
func (c *connState) copyListElems(src, dst []byte) {
	// Retire any resident hot-list window on src first, so its ring-only positions are flushed to
	// f1raw rows before this positional copy reads them (slice 3, impl/34). COPY holds src's exclusive
	// stripe lock, which drainEvict requires.
	c.listWinDrainEvict(src)
	head, tail, _, _, ok := c.listHeader(src)
	if !ok {
		return
	}
	var vbuf []byte
	for p := head; p < tail; p++ {
		v, got := c.srv.store.GetKind(c.listElemKey(src, p), vbuf[:0], kindListElem)
		if !got {
			continue
		}
		if _, err := c.srv.store.PutKind(c.listElemKey(dst, p), v, kindListElem); err != nil {
			continue
		}
	}
}

// copyHeader copies a collection's header row, which lives under the bare key and holds no key
// bytes in its value, so it re-publishes verbatim. Header rows are top-level keys enumerated by
// the bucket walk, not carried in the ordered index, so no index fixup is needed.
func (c *connState) copyHeader(src, dst []byte, metaKind byte) {
	v, ok := c.srv.store.GetKind(src, nil, metaKind)
	if !ok {
		return
	}
	_, _ = c.srv.store.PutKind(dst, v, metaKind)
}
