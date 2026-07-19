package store

// The maxmemory evictor's string arm (spec 2064/f3/16 sections 6.4 and 7.3):
// the shard's residency figure, the sampled victim pick, and the durable drop
// the dispatch evictor drives once LiveResident overshoots the budget. It is the
// string half of the same three-call contract every collection package exposes
// (EvictVictim, EvictKey, and the keyspace's own accounting gate), scoring
// candidates through the one store.EvictScore comparator so a victim from the
// string store weighs against a set or a stream on one scale.

// LiveResident is the shard's live RAM figure for the maxmemory budget: index
// footprint plus the arena's live charge, a synchronously-maintained figure that
// drops the instant a record is dropped (unlike UsedMemory's fill basis which
// only falls when a later boundary compacts), so an eviction loop that targets it
// stops the moment enough victims are chosen rather than over-evicting against a
// lagging counter (the pendingUncertain discipline, spec 2064/f3/16 section 6.4).
// O(segments), boundary-rate only.
func (s *Store) LiveResident() uint64 {
	return s.idx.bytes() + s.arena.live()
}

// EvictVictim samples up to sample RESIDENT records from the index (skip cold
// slots, the same slotCold skip ReapExpired does) and returns the best victim for
// the policy: the record with the highest store.EvictScore, key copied out.
// volatileOnly skips a record with no deadline (expireAt==0). ok is false when the
// index holds no eligible resident record. Go's index walk order plus the sample
// bound is the random draw, the way redis samples maxmemory-samples keys.
// Owner-only. now is the eviction clock (cx.NowMs).
func (s *Store) EvictVictim(policy uint8, now int64, sample int, volatileOnly bool) (key []byte, score int64, ok bool) {
	if sample <= 0 {
		return nil, 0, false
	}
	st := evictScan{sample: sample}
	// Walk the index the way ReapExpired does, home buckets then the overflow
	// slab, per non-nil segment; the sample bound stops the walk mid-segment once
	// enough eligible slots are seen, so a large keyspace pays only the sample.
walk:
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		for i := range seg.buckets {
			if s.evictBucket(&seg.buckets[i], policy, now, volatileOnly, &st) {
				break walk
			}
		}
		for i := range seg.overflow {
			if s.evictBucket(&seg.overflow[i], policy, now, volatileOnly, &st) {
				break walk
			}
		}
	}
	if !st.found {
		return nil, 0, false
	}
	// Copy the winning key out before returning: keyAt aliases the arena, which a
	// later write can move or reuse, so the caller gets its own bytes.
	return append([]byte(nil), s.keyAt(st.bestAddr)...), st.bestScore, true
}

// evictScan is the running state of one EvictVictim walk: the eligible-slot budget
// and count, and the best-scoring resident slot seen so far. It rides through the
// per-bucket helper the way ReapExpired threads its examined and reaped counters.
type evictScan struct {
	sample    int
	examined  int
	found     bool
	bestAddr  uint64
	bestScore int64
}

// evictBucket scores the resident records in one bucket into the running best,
// skipping cold slots and, under volatileOnly, deadline-free records, and reports
// true once the eligible-slot sample budget is spent so the caller halts the walk.
// A cold or filtered slot is not eligible and does not count against the budget,
// so the sample is exactly maxmemory-samples records the policy could actually
// evict, matching redis's own eviction pool fill.
func (s *Store) evictBucket(b *bucket, policy uint8, now int64, volatileOnly bool, st *evictScan) bool {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 || slotCold(w) {
			continue
		}
		addr := w & addrMask
		expireAt := s.expireAt(addr)
		if volatileOnly && expireAt == 0 {
			continue
		}
		if st.examined >= st.sample {
			return true
		}
		st.examined++
		idle := IdleSecondsFrom(s.recClock(addr), now)
		score := EvictScore(policy, uint32(idle), expireAt)
		if !st.found || score > st.bestScore {
			st.bestScore = score
			st.bestAddr = addr
			st.found = true
		}
	}
	return false
}

// EvictKey drops the resident record at key and reports whether one was there. It
// drops it the way Del does, logging the delete/tombstone (eviction is a real
// removal the durable log must carry so a replay does not resurrect the evicted
// key; contrast ReapExpired which logs nothing because the durable TTL re-derives
// expiry). Owner-only.
func (s *Store) EvictKey(key []byte) bool {
	h := Hash(key)
	slot, addr, inOverflow := s.findEntry(h, key)
	if addr == 0 || slotCold(*slot) {
		// Absent, or cold: the evictor samples and drops resident records only, so a
		// cold key (which already left the arena the budget measures) is not its to
		// take here.
		return false
	}
	s.deleteAt(h, slot, inOverflow)
	s.dropRecord(addr)
	s.count--
	s.logTombstone(key)
	return true
}
