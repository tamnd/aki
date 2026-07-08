package f1raw

import "encoding/binary"

// This file carries Option A, the set member row's tier-migration mechanism (spec
// 2064/f1_rewrite_ltm/22). The other collection element kinds ride Option B: their ordered-index
// node re-resolves its tier-tagged address through the primary index on every access (nodeAddr,
// oindex.go), so the migrator moves such a record with a plain index flip and the secondary
// structure follows for free. The set cannot: its dense member vector (randvec.go) caches raw arena
// offsets with no keys and doc 20 left no ordered index to rebuild from, so the vector's cached
// offset would dangle the moment the migrator reclaimed the resident bytes. Option A repairs the
// cached offset in place as the record moves, which only the migrator can do because it is the one
// actor that knows both the old resident offset and the new cold address. migrateVecMember is the
// shared mover the hand path (MigrateToCold) and the background drain (flipVecMember, called from
// drainSegment) both flow through, so the flip-and-retier is written once and the vector is never
// left holding a stale resident offset a concurrent draw could read past a reclaimed segment.

// resolveMemberVec locates the dense member vector and its owning randVec shard for the set member
// row whose composite key is memberKey (spec 2064/f1_rewrite_ltm/22 section 4). The member key
// layout is fixed: an unpartitioned member is uvarint(len(skey)) | skey | member and its vector
// prefix is uvarint(len(skey)) | skey; a partitioned member is that run plus a partition byte before
// the member, and its vector prefix runs through the partition byte. The leading uvarint gives
// len(skey), so the set-key prefix splits off in O(1). The two layouts are told apart by the
// partition descriptor: a lock-free descriptor probe at the whole-set prefix returns a descriptor
// for a partitioned set and nothing for the common unpartitioned one, the same probe dropPartVecs
// makes. It returns a nil shard when the key is too short to parse, which a torn or non-member key
// would produce and the caller treats as nothing to repair.
func (s *Store) resolveMemberVec(memberKey []byte) (*randVecShard, *memberVec) {
	skLen, n := binary.Uvarint(memberKey)
	if n <= 0 || int(skLen) > len(memberKey)-n {
		return nil, nil
	}
	prefixLen := n + int(skLen) // len(uvarint) + len(skey), the whole-set vector prefix length
	setPrefix := memberKey[:prefixLen]

	// Probe for a partition descriptor without building one. An unpartitioned set (the common case)
	// has none, so this is a single lock-free descriptor-shard map load and returns at once.
	if d := s.pdescs.shardFor(setPrefix).get(setPrefix); d != nil {
		if prefixLen >= len(memberKey) {
			return nil, nil // partitioned layout requires a partition byte after the set prefix
		}
		part := int(memberKey[prefixLen])
		if part >= d.p {
			return nil, nil // partition byte past the engaged count: a torn key, skip it
		}
		// descPartVec may rewrite base's final byte to resolve a not-yet-cached partition pointer, so
		// hand it a copy rather than the caller's key, which for the drain path aliases the arena and
		// must not be mutated. The eager vector (doc 20) is already cached, so the copy is touched
		// only on the first-ever draw against this partition, never on the steady drain path.
		base := append([]byte(nil), memberKey[:prefixLen+1]...)
		sh := s.rvec.shardFor(base)
		return sh, s.descPartVec(d, base, part)
	}

	// Unpartitioned: the vector is keyed straight by the whole-set prefix.
	sh := s.rvec.shardFor(setPrefix)
	return sh, sh.get(setPrefix)
}

// migrateVecMember sinks the resident set member row at off to the cold record region and repairs
// the dense member vector's cached offset in place (spec 2064/f1_rewrite_ltm/22 sections 3 and 5,
// Option A). It is the shared mover the hand path (MigrateToCold) and the background path
// (drainRecord) both call for a kindSetMember record, so the flip-and-retier discipline lives in one
// place.
//
// The sequence: append the cold frame first (migrateRecordAt, lock-free, reads the still-resident
// bytes at off), then take the vector shard mutex and, under it, re-probe the primary index and flip
// the entry from off to the cold address with a conditional CAS and retier the vector slot. Holding
// the shard mutex across the CAS and the retierSlot is the placement rule section 5 proves safe: a
// set writer (SADD/SREM), whose vector mutation takes the same mutex, can never observe a flipped
// primary entry the vector has not yet been repaired for. The CAS stays conditional on the entry
// still pointing at the resident off, so a raced overwrite, delete, or partition engage that moved
// the key off off makes the CAS lose and the freshly-appended frame becomes dead space, touching no
// vector. It returns false without retrying: the caller re-probes if it wants to, and a lost CAS
// means the record is no longer at off, which is a terminal state for this off.
func (s *Store) migrateVecMember(key []byte, kind byte, off uint64) bool {
	frameOff, err := s.migrateRecordAt(off)
	if err != nil {
		return false
	}
	return s.flipVecMember(key, kind, off, frameOff|tierBit)
}

// flipVecMember is the retier half migrateVecMember and the batched migrator drain share: it
// takes the vector shard mutex and, under it, flips the primary index entry for the set member
// row at off to newAddr and repairs the dense member vector's cached offset (spec
// 2064/f1_rewrite_ltm/22 sections 3 and 5, Option A). The frame newAddr points at is already
// written: migrateVecMember appends it per record, the drain writes a whole segment's frames in
// one pwrite before any flip, so by the time a flipped entry is visible its cold frame is durable
// on disk. Holding the shard mutex across the find, the CAS, and retierSlot is the placement rule
// section 5 proves safe: a set writer (SADD/SREM) taking the same mutex never observes a flipped
// primary entry the vector has not yet been repaired for. The CAS stays conditional on the entry
// still pointing at the resident off, so a raced overwrite, delete, or partition engage that moved
// the key off off makes the CAS lose and the already-written frame becomes dead space, touching no
// vector. It returns false without retrying: a lost CAS means the row is no longer at off, a
// terminal state for this off.
func (s *Store) flipVecMember(key []byte, kind byte, off, newAddr uint64) bool {
	sh, v := s.resolveMemberVec(key)
	if sh == nil {
		return false
	}
	sh.mu.Lock()
	curOff, b, slot, word, found := s.find(key, hash(key), kind)
	if !found || curOff != off {
		// The row moved off `off` (a raced overwrite, delete, or partition engage) or vanished
		// before the retier: leave the written frame as dead space and touch no vector.
		sh.mu.Unlock()
		return false
	}
	newWord := (word &^ addrMask) | newAddr
	if !b.slots[slot].CompareAndSwap(word, newWord) {
		// Lost the entry to a concurrent writer between the probe and the flip; drop the frame.
		sh.mu.Unlock()
		return false
	}
	// The flip won. Repair the vector slot that cached the old resident off so it holds the cold
	// address, before releasing the mutex. retierSlot reports false when off is already absent, which
	// a racing SREM that dropped the member under this mutex before us produces; that is correct, the
	// member is gone from the vector and the tier-aware delete path accounts the frame.
	if v != nil {
		v.retierSlot(off, newAddr)
	}
	// Charge the resident bytes out of their segment so it drains toward retirement, the same
	// decrement the string flip does. off is a resident offset here (curOff == off, no tier bit).
	s.unlinkResident(off)
	sh.mu.Unlock()
	return true
}
