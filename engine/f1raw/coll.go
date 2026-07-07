package f1raw

import (
	"errors"
)

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
	if s.segmented {
		// Pin the operation to the live epoch so a migrator cannot free the segment holding
		// this element's bytes while the read resolves and copies them (arena.go M2, doc 21
		// section 7 D18), the collection twin of the string Get guard. Only on the opt-in
		// segmented path; the default in-memory element read below is unchanged.
		g := s.pin()
		off, _, _, _, found := s.find(key, h, kind)
		if !found {
			g.unpin()
			return dst[:0], false
		}
		v, ok := s.readValueByAddr(off, dst)
		g.unpin()
		return v, ok
	}
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return dst[:0], false
	}
	return s.readValueByAddr(off, dst)
}

// ExistsKind reports whether key is present in the given kind namespace without
// copying the value, for HEXISTS and the header presence check.
func (s *Store) ExistsKind(key []byte, kind byte) bool {
	h := hash(key)
	_, _, _, _, found := s.find(key, h, kind)
	return found
}

// ExistsAnyKey reports whether any record carries key in any kind namespace, hashing the key once
// and walking its probe chain. It is the lock-free "does this key exist at all" probe a generic
// command wants when it must tell a truly missing key apart from a wrong-typed one without paying a
// separate ExistsKind per candidate type. All kinds of one key share a probe chain because the index
// hashes the key bytes alone (the kind is not in the hash), so one walk sees a string record and
// every collection header row for the key; the element rows live under composite keys and never
// collide, so a header hit is exactly key existence. It matches on key bytes and ignores the kind
// byte, and reads only immutable record fields, so it is safe against concurrent writers exactly
// like ExistsKind.
func (s *Store) ExistsAnyKey(key []byte) bool {
	h := hash(key)
	tag := tagOf(h)
	b := &s.buckets[h&s.mask]
	for b != nil {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i].Load()
			if w == 0 || w>>tagShift != tag {
				continue
			}
			if s.recordMatchesKey(w&addrMask, key) {
				return true
			}
		}
		b = s.nextBucket(b, false)
	}
	return false
}

// recordMatchesKey is recordMatches without the kind check, for the kind-agnostic ExistsAnyKey
// probe. It reads only the immutable key length and key bytes, so it is concurrency-safe like
// recordMatches.
func (s *Store) recordMatchesKey(off uint64, key []byte) bool {
	if s.klen(off) != uint64(len(key)) {
		return false
	}
	start := off + hdrSize
	return string(s.arena[start:start+uint64(len(key))]) == string(key)
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
			s.oidx.Load().refresh(noff)
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
			s.oidx.Load().refresh(noff)
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
			// A separated element's cold value is now unreferenced; account it as dead,
			// and return its resident bytes to its segment so the arena can drain it.
			s.markSepDead(off)
			s.unlinkResident(off)
			s.count.Add(-1)
			s.addTop(kind, -1)
			return true
		}
	}
}

// DeleteKindNoCount removes key in the given kind namespace like DeleteKind but leaves the
// store's global record count alone, so a coalesced run of deletes charges the shared counter
// once instead of once per element. It is the destructive counterpart to TakeKindNoCount: the
// count-batching a set or hash delete burst wants. s.count is a single line every connection's
// write hammers, so decrementing it once per removed element under a hot single-key SREM/HDEL
// burst puts a contended atomic back on the per-element path the coalesced applier is trying to
// keep off it, exactly the line the profile named on the delete gate. The caller counts the rows
// it actually removed and folds them into one AddCount, the same coalescing slice 1 applied to
// the header cardinality. Like DeleteKind it must be serialized with the key's other writers by
// the caller's stripe lock; the CAS loop only guards a lost race. It is only correct for a kind
// that is never top-level (a collection element or score row), since it does not adjust topCount;
// a top-level kind must go through DeleteKind so DBSIZE stays exact.
func (s *Store) DeleteKindNoCount(key []byte, kind byte) bool {
	h := hash(key)
	for {
		off, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			// A separated element's cold value is now unreferenced; account it as dead. This
			// stays per element because it addresses the specific record's cold bytes; only the
			// contended global count is what the caller batches. The resident bytes leave the
			// segment too, per element for the same reason.
			s.markSepDead(off)
			s.unlinkResident(off)
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
			// unreferenced; account it as dead space for a later compaction pass, and
			// return its resident bytes to its segment so the arena can drain it.
			s.markSepDead(off)
			s.unlinkResident(off)
			s.count.Add(-1)
			s.addTop(kind, -1)
			return v, true
		}
	}
}

// TakeKindNoCount reads and removes key like TakeKind but leaves the store's record count
// alone, so a coalesced run of takes charges the shared counter once instead of once per
// element. A pop burst on one hot list claims a contiguous run of element rows and takes
// them off the window's commit mutex; s.count is a single line every connection's push and
// pop hammer, so decrementing it once per popped element puts a contended atomic back on
// the per-element path the window claim just took off it. The caller takes the run through
// this variant, counts the rows it actually removed, and folds them into one AddCount, the
// same coalescing the window claim already applies to the commit mutex. It must be
// serialized with the key's other writers by the caller's stripe lock, exactly like
// TakeKind, and the caller must call AddCount with the negated number of rows removed so
// the counter stays consistent with the index. It is only correct for a kind that is never
// top-level (a list element row), since it does not adjust topCount; a top-level kind must
// go through TakeKind.
func (s *Store) TakeKindNoCount(key, dst []byte, kind byte) ([]byte, bool) {
	h := hash(key)
	for {
		off, b, slot, word, found := s.find(key, h, kind)
		if !found {
			return dst[:0], false
		}
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
			s.markSepDead(off)
			s.unlinkResident(off)
			return v, true
		}
	}
}

// AddCount folds a coalesced run's record-count change into the shared counter in one
// atomic, the batched companion to TakeKindNoCount. Serialize it with the key's other
// writers the same way TakeKind is serialized, and pass the negated number of rows the run
// actually removed so the counter tracks the live record set exactly.
func (s *Store) AddCount(delta int64) { s.count.Add(delta) }

// CountAddInt64 adds delta to the 8-byte little-endian signed counter held in the first
// eight value bytes of key's record in the given kind namespace, returning the post-add
// value and whether the record was present. This is the cheap cardinality update a
// collection header row uses for its element count: a delete decrements it with one latched
// read-modify-write of the count word instead of a GetKind plus a full-value PutKind that
// re-reads the encoding tag and rewrites the whole header value.
//
// The read-modify-write runs under the record's seqlock, the same latch inPlace and
// incrInPlace take, so it is consistent with a concurrent readValue: a reader spins while
// the latch is held and retries if the version moved during its copy, so it never observes a
// torn count. Only bytes 0..7 move here; a sibling field packed at value byte 8 (a set or
// hash encoding tag) is left untouched, so a header keeps its encoding across a count update.
// The caller must still serialize this with the key's other header writers (SADD's full
// header write) through its stripe lock, exactly as the pre-atomic count path was serialized;
// the seqlock only makes the update safe against lock-free readers, not against a racing
// writer. The record must already exist: the absent-to-present create that first writes the
// header stays under the caller's stripe lock.
func (s *Store) CountAddInt64(key []byte, kind byte, delta int64) (int64, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return 0, false
	}
	if uint64(s.vlenAt(off).Load()) < 8 {
		return 0, false
	}
	verp := s.verAt(off)
	spins := 0
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			spins = spinWait(spins)
			continue
		}
		if !verp.CompareAndSwap(v, v+1) { // acquire: make it odd
			spins = spinWait(spins)
			continue
		}
		cnt := s.countAt(off)
		res := cnt.Load() + delta
		cnt.Store(res)    // atomic: synchronizes concurrent shared-lock bumps and lock-free readers
		verp.Store(v + 2) // release: back to even, one tick newer
		return res, true
	}
}

// CountInt64 reads the 8-byte little-endian signed counter in the first eight value bytes of
// key's record in the given kind namespace, returning the value and whether the record was
// present. It is the lock-free read SCARD, HLEN, and ZCARD ride: it takes the seqlock read
// path (spin while latched, retry if the version moved), so a cardinality query never takes
// the stripe lock and never tears against a concurrent CountAddInt64.
func (s *Store) CountInt64(key []byte, kind byte) (int64, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, kind)
	if !found {
		return 0, false
	}
	verp := s.verAt(off)
	spins := 0
	for {
		v1 := verp.Load()
		if v1&verLockBit != 0 {
			spins = spinWait(spins)
			continue
		}
		if uint64(s.vlenAt(off).Load()) < 8 {
			return 0, false
		}
		x := s.countAt(off).Load()
		if verp.Load() == v1 {
			return x, true
		}
		spins = spinWait(spins)
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
	s.oidx.Load().insert(off)
}

// CollRemove drops key from the ordered element index. Call it right after DeleteKind
// removes an element row, under the same stripe lock, so the ordered run does not
// enumerate a deleted element.
func (s *Store) CollRemove(key []byte) {
	s.oidx.Load().remove(key)
}

// CollRemoveMany drops every key in keys from the ordered element index under a single
// index-lock acquisition. It is the batched counterpart to CollRemove for a coalesced
// delete run: the server has already removed each element row from the hash index and
// copied the removed keys out of its per-command key scratch, so this folds one lock
// cycle over the whole run instead of one per element. Call it under the collection's
// stripe lock, the same serialization CollRemove relies on.
func (s *Store) CollRemoveMany(keys [][]byte) {
	s.oidx.Load().removeMany(keys)
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
	if s.segmented {
		// Hold the epoch for the span of this one bounded batch so a migrator cannot free a
		// segment holding an element this batch resolves keyAt against (doc 21 section 7 D20).
		// The window is bounded by limit, and a fresh CollScan for the next batch republishes,
		// so a long enumeration never starves reclamation. Only on the opt-in segmented path.
		g := s.pin()
		defer g.unpin()
	}
	// Take the keys from the ordered index itself (the node's inline cache for an inline key),
	// not from keyAt(off), so enumeration returns the right key after the element's record has
	// migrated cold and its resident offset was reclaimed. The offsets are discarded here; the
	// value-carrying twin CollScanKV keeps them for the value read. A nil dst would tell
	// scanBatch to collect no keys (the offsets-only convention the set-vector rebuilds use),
	// so materialize a slice first: CollScan's contract is to return the keys.
	if dst == nil {
		dst = make([][]byte, 0, limit)
	}
	_, dst, last = s.oidx.Load().scanBatch(prefix, after, limit, make([]uint64, 0, limit), dst)
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
	// A nil dstKeys means "collect no keys" to scanBatch (the offsets-only convention the set
	// member-vector rebuilds use), but CollScanKV must return the keys alongside their offsets,
	// so materialize a slice first when the caller passed none.
	if dstKeys == nil {
		dstKeys = make([][]byte, 0, limit)
	}
	dstOffs, dstKeys, last = s.oidx.Load().scanBatch(prefix, after, limit, dstOffs, dstKeys)
	return dstKeys, dstOffs, last
}

// ReadValueAt copies the value of the record at off into dst (reusing its capacity) and
// returns it, the seqlock read GetKind runs after it resolves the offset. It is exported
// for the value-carrying scan (CollScanKV), which already holds the offset from the ordered
// index and so skips the hash-and-probe find GetKind would repeat. off must come from a
// current CollScanKV batch, where it is guaranteed to be the element's live record. off is a
// logical address, so it carries the tier bit: when the cold record region is engaged, the
// scan re-resolves each node through the tier-aware index (D22 Option B) and hands back a
// tier-tagged address for an element that migrated cold, so this funnels through
// readValueByAddr to follow the record across the tier boundary with one pread. The resident
// branch is exactly today's read: a value separated to the cold value log resolves with one
// pread, an inline value reads under the seqlock. A value-carrying enumeration over a hash of
// large fields thus reads each field straight from whichever tier holds it.
func (s *Store) ReadValueAt(off uint64, dst []byte) []byte {
	v, _ := s.readValueByAddr(off, dst)
	return v
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
// must fall back to ReadValueAt, which resolves the cold log. A record that migrated to the cold
// record region is likewise not resident, so off carries the tier bit and this reports inline=false
// for it too, sending the caller to ReadValueAt (which preads the cold frame); the zero-copy arena
// slice is only ever returned for a genuinely resident inline record. The arena never moves a
// published record, so under the held lock the returned slice stays valid for the caller's use of it.
func (s *Store) ValueAtLocked(off uint64) (val []byte, inline bool) {
	if off&tierBit != 0 {
		return nil, false
	}
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
	return s.oidx.Load().selectInPrefix(prefix, localIndex)
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
	return s.oidx.Load().selectAndRemoveInPrefix(prefix, localIndex)
}

// CollRankOf returns the 0-based position of key within the collection bounded by
// prefix, in key order: the inverse of CollSelectAt. It rides the same order-statistic
// spans, so ZRANK/ZREVRANK seek a member's position in O(log n) rather than counting
// the members below it. The caller must confirm key is a live element (through GetKind
// on the element row) before trusting the result, because the rank of an absent key is
// where it would fall, not an error. localIndex and rank are inverse over a prefix:
// CollSelectAt(prefix, CollRankOf(prefix, k)) returns k for any live k under prefix.
func (s *Store) CollRankOf(prefix, key []byte) int {
	return s.oidx.Load().rankInPrefix(prefix, key)
}
