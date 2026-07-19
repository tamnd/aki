package store

// SetExpiredSink installs the keyspace-notification hook the store calls whenever
// it reaps a key whose deadline has passed, on a lazy touch (findLive) or in the
// active cycle (reapBucket). The shard wires this to its worker's expired-event
// emitter; a store built without a shard (a unit test) leaves it nil and reaps
// silently. Set once at worker construction before Start, so the owner reads it with
// no synchronization.
func (s *Store) SetExpiredSink(fn func(key []byte)) { s.expiredSink = fn }

// ReapExpired is the active-expiry cycle's string arm (spec 2064/f3/16 section 3):
// it drops resident records whose deadline has passed, the same records a keyed
// read reaps lazily on touch, so the background cycle only bounds how long an
// untouched expired key lingers and never changes what any command observes. It
// walks the index examining at most budget resident slots, so one cycle stays
// O(budget) and never monopolizes the owner's idle slice; whatever it does not
// reach this pass a later pass or a first access reaps. A reaped record is dropped
// exactly the way findLive drops one (deleteAt + dropRecord + count), with no
// tombstone logged: the durable TTL re-derives the same expiry on replay, which is
// why the lazy drop logs none either. A cold record carries no deadline (the
// migrator demotes only TTL-free records), so the walk skips it the way the
// read-only walks do. now is the cycle clock; a zero now or non-positive budget
// reaps nothing. Returns the number of records dropped. Owner goroutine only.
func (s *Store) ReapExpired(now int64, budget int) int {
	if now == 0 || budget <= 0 {
		return 0
	}
	examined := 0
	reaped := 0
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		for i := range seg.buckets {
			if s.reapBucket(&seg.buckets[i], false, now, budget, &examined, &reaped) {
				return reaped
			}
		}
		for i := range seg.overflow {
			if s.reapBucket(&seg.overflow[i], true, now, budget, &examined, &reaped) {
				return reaped
			}
		}
	}
	return reaped
}

// reapBucket examines the resident records in one bucket, dropping each whose
// deadline has passed, and reports true once the per-cycle examine budget is spent
// so the caller halts the walk. inOverflow tells deleteAt which chain counter to
// decrement, the same bit rangeSegment threads by walking the home buckets and the
// overflow slab separately. Deleting a slot only zeroes it in place (deleteAt shifts
// nothing), so the rest of the walk sees the emptied slot as an ordinary hole and
// the drop is safe mid-iteration.
func (s *Store) reapBucket(b *bucket, inOverflow bool, now int64, budget int, examined, reaped *int) bool {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		if *examined >= budget {
			return true
		}
		*examined++
		addr := w & addrMask
		if at := s.expireAt(addr); at != 0 && at <= now {
			key := s.keyAt(addr)
			// Publish the expired event while the record's key still reads, before the
			// drop below reclaims it. The sink gates on the notify mask, so a store with
			// no subscriber pays only one atomic load per actually-expired key.
			if s.expiredSink != nil {
				s.expiredSink(key)
			}
			h := Hash(key)
			s.deleteAt(h, &b.slots[i], inOverflow)
			s.dropRecord(addr)
			s.count--
			*reaped++
		}
	}
	return false
}
