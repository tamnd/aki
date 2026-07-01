package f1raw

import "errors"

// The collection primitives are the string point path over again, but parameterized
// on a record kind so an element row lives in its own namespace. The engine stays a
// pure byte-keyed point store: it does not know "hash" or "field", only that a record
// carries a kind byte (the spec's type_tag) that a probe matches alongside the key
// bytes. That one byte is what lets a hash field row, a hash header row, and a string
// share the same index and arena without ever colliding, so the element-per-row model
// the larger-than-memory design depends on rides straight on the lock-free hot path
// with no second structure.
//
// Concurrency contract: GetKind is lock-free and safe against any concurrent readers
// and writers, exactly like Get. PutKind and DeleteKind are lock-free against writers
// to other keys, but the caller must serialize writes to one collection key (the
// server does this with a per-key stripe lock), so that PutKind's created flag and a
// header row's maintained count stay exact. Redis serializes all writes to one key
// anyway, so this costs nothing the wire semantics did not already require.

// GetKind copies the value for key in the given kind namespace into dst and reports
// whether it is present. It is the lock-free element read: HGET is one call of this.
func (s *Store) GetKind(key, dst []byte, kind byte) ([]byte, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return dst[:0], false
	}
	return s.readValue(off, dst), true
}

// ExistsKind reports whether key is present in the given kind namespace without
// copying the value, for HEXISTS and the header presence check.
func (s *Store) ExistsKind(key []byte, kind byte) bool {
	h := hash(key)
	_, _, _, _, found := s.find(key, h, kind)
	return found
}

// PutKind upserts val under key in the given kind namespace and reports whether the
// record was newly created (true) versus an update of an existing one (false). HSET
// reads created to count new fields and to know when to bump the header count. The
// created flag is exact only when writes to this key are serialized by the caller.
func (s *Store) PutKind(key, val []byte, kind byte) (created bool, err error) {
	if len(key) == 0 {
		return false, errors.New("f1raw: empty key")
	}
	if len(key) > maxKey || len(val) > maxVal {
		return false, ErrTooBig
	}
	h := hash(key)
	if off, _, _, _, found := s.find(key, h, kind); found {
		if uint64(len(val)) <= s.vcapBytes(off) {
			s.inPlace(off, val)
			return false, nil
		}
		// Outgrew the record: republish a wider one. publish rescans and replaces the
		// entry in place, count unchanged, so this is still an update, not a create.
		if err := s.publish(key, val, h, kind, 0); err != nil {
			return false, err
		}
		// The record moved to a new offset. If it is indexed for ordered enumeration,
		// point its node at the new offset so a value-carrying scan (CollScanKV) reads the
		// current value straight from the node instead of the abandoned old record. refresh
		// is a no-op for an unindexed record (a header row), so this is safe for any kind.
		if noff, _, _, _, ok := s.find(key, h, kind); ok {
			s.oidx.refresh(noff)
		}
		return false, nil
	}
	// Absent under the caller's serialization, so publish will fill an empty slot and
	// bump the count: a genuine create.
	if err := s.publish(key, val, h, kind, 0); err != nil {
		return false, err
	}
	return true, nil
}

// DeleteKind removes key in the given kind namespace and reports whether it was
// present, mirroring Delete for element and header rows.
func (s *Store) DeleteKind(key []byte, kind byte) bool {
	h := hash(key)
	for {
		_, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			s.count.Add(-1)
			return true
		}
	}
}

// CollInsert records key in the ordered element index so a bounded cursor can
// enumerate it in key order. Call it right after PutKind reports a new element row so
// the ordered run and the hash index agree on the live set. It is a no-op if the
// record is not found (a concurrent delete), keeping the index from pointing at a
// vanished record. Serialize it with the collection's other writers, the same stripe
// lock PutKind's created flag relies on.
func (s *Store) CollInsert(key []byte, kind byte) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return
	}
	s.oidx.insert(off)
}

// CollRemove drops key from the ordered element index. Call it right after DeleteKind
// removes an element row, under the same stripe lock, so the ordered run does not
// enumerate a deleted element.
func (s *Store) CollRemove(key []byte) {
	s.oidx.remove(key)
}

// CollScan appends, to dst, the full composite keys of up to limit element rows whose
// key has the given prefix and sorts strictly after `after` (nil after starts at the
// prefix), in key order. It returns the grown dst and the last key returned, to pass
// back as `after` for the next batch; last is nil when the batch is empty. Each
// returned key is a subslice of the immutable arena, valid for the store's life, so
// the caller reads it without copying and re-resolves the value through GetKind. This
// is the bounded cursor the whole-collection reads (HGETALL, HKEYS, HVALS, HSCAN)
// stream through, so they never materialize the collection.
func (s *Store) CollScan(prefix, after []byte, limit int, dst [][]byte) (keys [][]byte, last []byte) {
	offs, last := s.oidx.scanBatch(prefix, after, limit, make([]uint64, 0, limit))
	for _, off := range offs {
		dst = append(dst, s.keyAt(off))
	}
	return dst, last
}

// CollScanKV is CollScan for the value-carrying enumerations (HGETALL, HVALS): it appends
// each element's composite key to dstKeys and its record offset to dstOffs, in key order,
// so the caller reads the value straight from the offset with ReadValueAt instead of
// re-resolving it with a per-element GetKind (a hash plus a bucket probe per field). It
// returns the grown slices and the last key, to resume like CollScan. The offset is
// authoritative for the live value: a create pairs CollInsert with the offset, an in-place
// update keeps it, and an outgrow-republish refreshes it (PutKind), so ReadValueAt never
// reads a stale record. Both slices grow in lockstep, so dstOffs[i] is the offset of
// dstKeys[i]. This is the primitive that closes the HVALS/HGETALL gap to HKEYS.
func (s *Store) CollScanKV(prefix, after []byte, limit int, dstKeys [][]byte, dstOffs []uint64) (keys [][]byte, offs []uint64, last []byte) {
	dstOffs, last = s.oidx.scanBatch(prefix, after, limit, dstOffs)
	for _, off := range dstOffs {
		dstKeys = append(dstKeys, s.keyAt(off))
	}
	return dstKeys, dstOffs, last
}

// ReadValueAt copies the value of the record at off into dst (reusing its capacity) and
// returns it, the seqlock read GetKind runs after it resolves the offset. It is exported
// for the value-carrying scan (CollScanKV), which already holds the offset from the ordered
// index and so skips the hash-and-probe find GetKind would repeat. off must come from a
// current CollScanKV batch, where it is guaranteed to be the element's live record.
func (s *Store) ReadValueAt(off uint64, dst []byte) []byte {
	return s.readValue(off, dst)
}

// CollSelectAt returns the composite key of the element at 0-based localIndex within the
// collection bounded by prefix, in key order, and whether it exists. It rides the ordered
// index's order-statistic spans, so a random member is one O(log n) rank-then-select
// descent, never an O(n) count. This is the random-access primitive SPOP and
// SRANDMEMBER seek through (spec 2064/f1_rewrite_ltm/06 section 10.1): the server draws a
// uniform localIndex in [0, cardinality) and this returns the corresponding member row's
// key, a subslice of the immutable arena valid for the store's life. localIndex at or
// past the collection's cardinality reports absent rather than crossing into a sibling
// collection.
func (s *Store) CollSelectAt(prefix []byte, localIndex int) (key []byte, ok bool) {
	return s.oidx.selectInPrefix(prefix, localIndex)
}

// CollSelectRemoveAt selects the element at localIndex within prefix and drops it from the
// ordered index in one descent, returning its composite key and whether it existed. It is
// the fused select-then-CollRemove that SPOP-without-count runs: the caller still deletes
// the element's row through DeleteKind (the returned key is exactly that row's key), but
// the ordered-index select and unlink share a single positional descent under one write
// lock instead of a select descent, a rank descent, and a separate remove descent. Use it
// only when the very member just selected is the one being removed; for a select that does
// not remove, use CollSelectAt.
func (s *Store) CollSelectRemoveAt(prefix []byte, localIndex int) (key []byte, ok bool) {
	return s.oidx.selectAndRemoveInPrefix(prefix, localIndex)
}

// CollRankOf returns the 0-based position of key within the collection bounded by
// prefix, in key order: the inverse of CollSelectAt. It rides the same order-statistic
// spans, so ZRANK/ZREVRANK seek a member's position in O(log n) rather than counting
// the members below it. The caller must confirm key is a live element (through GetKind
// on the element row) before trusting the result, because the rank of an absent key is
// where it would fall, not an error. localIndex and rank are inverse over a prefix:
// CollSelectAt(prefix, CollRankOf(prefix, k)) returns k for any live k under prefix.
func (s *Store) CollRankOf(prefix, key []byte) int {
	return s.oidx.rankInPrefix(prefix, key)
}
