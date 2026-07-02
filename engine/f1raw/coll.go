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
// When the store runs the cold tier and this element's value was separated (a large hash
// field value), the record's cell holds a 12-byte cold pointer, so the read resolves
// through one pread of the immutable cold log, the collection twin of the string Get cold
// branch. A separated record never mutates, so the cold read needs no seqlock.
func (s *Store) GetKind(key, dst []byte, kind byte) ([]byte, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return dst[:0], false
	}
	if s.cold != nil && s.isSep(off) {
		return s.readSeparated(off, dst)
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

// GetKindAt is GetKind that also returns the record's arena offset, so a caller that
// will rewrite the same record in place can skip the second index probe PutKind would
// repeat. It is the read half of the fused read-then-in-place-update a fixed-width
// header row wants: a list pop reads the header window, edits it, and writes it back,
// and the offset lets the write-back land straight on the record with InPlaceAt instead
// of a fresh hash-and-probe. The offset stays valid for the record's life as long as the
// record is not outgrown-and-republished, which a fixed-width header (constant value
// size) never is. Serialize the read-edit-write with the key's other writers, the same
// stripe lock PutKind already requires; a concurrent unrelated write never moves this
// record (the arena is grow-only and other keys append elsewhere), so the offset holds
// across the caller's edit.
func (s *Store) GetKindAt(key, dst []byte, kind byte) (val []byte, off uint64, ok bool) {
	h := hash(key)
	o, _, _, _, found := s.find(key, h, kind)
	if !found {
		return dst[:0], 0, false
	}
	return s.readValue(o, dst), o, true
}

// InPlaceAt rewrites the value of the record at off under its seqlock, for a caller that
// already holds the offset from GetKindAt and knows val fits the record's reserved room.
// It is the write half of the fused header update: a fixed-width header row (list, and any
// other constant-size coll_header) is always rewritten with a same-length value, which by
// construction fits the room the first PutKind reserved, so this never needs the outgrow
// republish path PutKind carries. The caller must guarantee off came from a current
// GetKindAt under the same stripe lock and that len(val) does not exceed the record's
// reserved capacity (true for any fixed-width header); using it on a record that could
// outgrow its cell would silently truncate, so it is deliberately not a general upsert.
func (s *Store) InPlaceAt(off uint64, val []byte) {
	s.inPlace(off, val)
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
	off, _, _, _, found := s.find(key, h, kind)
	// A large element value goes out of line to the cold log as a fresh separated record,
	// exactly like the string setSeparated path: the index entry swaps to it, so any prior
	// record for this key/kind is replaced, and a separated record is immutable (never an
	// in-place update). This is what lets a collection of large values (a hash of big
	// fields) exceed memory while its index and field names stay resident. created is true
	// only when no prior record existed under the caller's serialization.
	if s.cold != nil && len(val) > s.sepThreshold {
		if err := s.putKindSeparated(key, val, h, kind); err != nil {
			return false, err
		}
		// The record moved; point any ordered-index node at the new offset so a
		// value-carrying scan reads the current record. A no-op for an unindexed record.
		if noff, _, _, _, ok := s.find(key, h, kind); ok {
			s.oidx.refresh(noff)
		}
		return !found, nil
	}
	if found {
		// An inline in-place update needs the existing record inline too: a separated
		// record's cell is a 12-byte cold pointer, not value bytes, so a small value must
		// not be memcpy'd over it. When the target is separated (a shrinking overwrite of a
		// once-large field), fall through to publish, which swaps to a fresh inline record.
		if !s.isSep(off) && uint64(len(val)) <= s.vcapBytes(off) {
			s.inPlace(off, val)
			return false, nil
		}
		// Outgrew the record (or it was separated): republish a wider one. publish rescans
		// and replaces the entry in place, count unchanged, so this is still an update.
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
		off, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			// A separated element's cold value is now unreferenced; account it as dead.
			s.markSepDead(off)
			s.count.Add(-1)
			s.addTop(kind, -1)
			return true
		}
	}
}

// TakeKind reads the value for key in the given kind namespace into dst and removes the
// record in a single index probe, reporting whether it was present. It is the fused read
// then point-delete a list pop wants: LPOP and RPOP read the element they return and then
// delete its row, and doing both off one find halves the probes a separate GetKind plus
// DeleteKind would cost. The value is copied into dst before the slot is cleared, so the
// returned bytes stay valid after the record is gone (the arena is grow-only, so the bytes
// at the record's offset are never reclaimed underneath the copy either). Like DeleteKind it
// must be serialized with the key's other writers by the caller's stripe lock; the CAS loop
// only guards against a lost race, which that lock already prevents.
func (s *Store) TakeKind(key, dst []byte, kind byte) ([]byte, bool) {
	h := hash(key)
	for {
		off, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return dst[:0], false
		}
		// A separated element (a large list element on the cold log) resolves through the
		// pread path; the value is copied into dst before the slot is cleared, so the
		// returned bytes stay valid after the index entry is gone. The cold bytes are left
		// as dead space, reclaimed by a later compaction milestone, the same as any other
		// separated overwrite.
		var v []byte
		if s.cold != nil && s.isSep(off) {
			var ok bool
			if v, ok = s.readSeparated(off, dst); !ok {
				return dst[:0], false
			}
		} else {
			v = s.readValue(off, dst)
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			// The popped element's cold value (a separated large list element) is now
			// unreferenced; account it as dead space for a later compaction pass.
			s.markSepDead(off)
			s.count.Add(-1)
			s.addTop(kind, -1)
			return v, true
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

// ScanKeys walks the fixed primary bucket array for top-level keys, starting at primary
// bucket index `cursor`, and appends each key whose record kind satisfies isTop to dst. It
// is the keyspace enumerator KEYS, SCAN, and RANDOMKEY ride: the engine stays type-agnostic
// and the caller decides through isTop which kinds are top-level (a plain string record and a
// collection header row are, an element row and a sidecar expire row are not), so the kind
// policy stays in the server.
//
// The primary bucket array is fixed at construction and never rehashes, and a key never
// migrates between primary buckets, so a bucket index is a stable resumable cursor: a key
// present for a whole iteration is visited in exactly one bucket and so returned at least
// once, which is the SCAN guarantee. It visits whole primary buckets (each with its overflow
// chain) so a batch never splits a bucket, and returns the next cursor to resume from, which
// is 0 once the array is exhausted.
//
// count is a target key count, not a bucket count: the walk keeps taking whole buckets until
// it has appended at least count keys, then stops on the bucket boundary. This is what keeps
// a full iteration proportional to the number of keys rather than the number of buckets, since
// the array is fixed large (millions of buckets) regardless of how few keys live in it, and a
// per-bucket batch would force an empty keyspace through millions of round trips. Sweeping the
// empty stretches inside one call instead of across many is the same amount of bucket work but
// a handful of round trips. count at or below zero targets a single key, which is how RANDOMKEY
// asks for the first live key at or after a random bucket. Each appended key is a subslice of
// the immutable arena, stable for the store's life, so the caller reads it without copying. A
// record logically expired but not yet reaped is still returned here (the engine has no TTL
// concept); the caller filters those.
func (s *Store) ScanKeys(cursor uint64, count int, dst [][]byte, isTop func(kind byte) bool) (out [][]byte, next uint64) {
	n := uint64(len(s.buckets))
	if cursor >= n {
		return dst, 0
	}
	target := count
	if target < 1 {
		target = 1
	}
	start := len(dst)
	for bi := cursor; bi < n; bi++ {
		b := &s.buckets[bi]
		for b != nil {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					continue
				}
				off := w & addrMask
				if !isTop(s.arena[off+offKind]) {
					continue
				}
				klen := s.klen(off)
				dst = append(dst, s.arena[off+hdrSize:off+hdrSize+klen])
			}
			b = s.nextBucket(b, false)
		}
		if len(dst)-start >= target {
			// Stop on this bucket boundary; resume at the next bucket. If that is the end of
			// the array the caller learns the iteration is done from the zero cursor below.
			if bi+1 >= n {
				return dst, 0
			}
			return dst, bi + 1
		}
	}
	return dst, 0
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
// current CollScanKV batch, where it is guaranteed to be the element's live record. When the
// record's value was separated to the cold log, this resolves it with one pread, which is the
// fallback ValueAtLocked documents for a separated record (its zero-copy path serves inline
// values only), so a value-carrying enumeration over a hash of large fields reads each
// field's value straight from the cold log.
func (s *Store) ReadValueAt(off uint64, dst []byte) []byte {
	if s.cold != nil && s.isSep(off) {
		v, _ := s.readSeparated(off, dst)
		return v
	}
	return s.readValue(off, dst)
}

// ValueAtLocked returns the inline value of the record at off as a zero-copy subslice of the
// grow-only arena, the value twin of keyAt, so the value-carrying scan (HGETALL/HVALS) hands
// both the field and its value straight to writeBulk without the ReadValueAt copy into a
// scratch buffer. It is safe only under two conditions the caller must guarantee. First, the
// caller holds the key's stripe lock, so no concurrent writer can rewrite the value in place
// or outgrow-republish the record while the returned slice is read; without that lock a writer
// under the seqlock could tear the read, which is exactly why the general Get path copies under
// the latch instead. Second, the record is inline: a cold-log separated value is a 12-byte
// pointer, not value bytes, so this reports inline=false for a separated record and the caller
// must fall back to ReadValueAt, which resolves the cold log. The arena never moves a published
// record, so under the held lock the returned slice stays valid for the caller's use of it.
func (s *Store) ValueAtLocked(off uint64) (val []byte, inline bool) {
	if s.cold != nil && s.isSep(off) {
		return nil, false
	}
	vbase := off + hdrSize + align8(s.klen(off))
	n := uint64(s.vlenAt(off).Load())
	return s.arena[vbase : vbase+n], true
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
