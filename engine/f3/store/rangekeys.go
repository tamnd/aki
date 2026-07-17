package store

// RangeKeys calls fn with every live key this shard's string store holds,
// resident or cold, the read-only iteration primitive KEYS, SCAN, and
// RANDOMKEY read (spec 2064/f3/04 section 2). It walks the index directly:
// every segment's home buckets and their overflow slab, decoding each
// non-zero slot by tier. A resident record whose deadline has passed is
// skipped, matching the lazy-expiry rule every read follows; a cold record
// carries no deadline (the migrator demotes only TTL-free records), so its key
// always shows. fn returns false to stop the walk early, so a bounded scan
// halts without draining the whole index. now is the batch clock for the
// expiry skip; zero skips the check.
//
// The walk mutates nothing: it sets no visited bit and reaps no expired record,
// unlike a keyed read, so it is perf-neutral by construction and leaves the
// residency clock untouched. It runs on the owner goroutine at a command
// boundary. The key slice fn receives aliases the arena, or for a cold entry
// the shared cold scratch, and is valid only for that call: fn copies what it
// keeps before returning.
func (s *Store) RangeKeys(now int64, fn func(key []byte) bool) {
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		if !s.rangeSegment(seg, now, fn) {
			return
		}
	}
}

// rangeSegment walks one segment's home buckets and its overflow slab. The
// slab is walked directly rather than by chain links: every live slot lives in
// some slab bucket, and an emptied bucket a delete left behind holds only zero
// slots the scan skips, so the direct walk visits every key with no chain
// bookkeeping. It reports false to stop the outer walk when fn asked to halt.
func (s *Store) rangeSegment(seg *indexSegment, now int64, fn func(key []byte) bool) bool {
	for i := range seg.buckets {
		if !s.rangeBucket(&seg.buckets[i], now, fn) {
			return false
		}
	}
	for i := range seg.overflow {
		if !s.rangeBucket(&seg.overflow[i], now, fn) {
			return false
		}
	}
	return true
}

// VolatileKeys counts the string keys carrying an expiry deadline, the
// string-store contribution to INFO's Keyspace expires field. It walks the
// index the same read-only way RangeKeys does, mutating nothing and reaping
// nothing, so it is perf-neutral and leaves the residency clock untouched; INFO
// is a cold path, so the O(keys) walk is off every command's critical path.
// Only resident records are examined: a cold record carries no deadline (the
// migrator demotes only TTL-free records), so it never counts. A deadline is
// counted whether or not it has passed, matching the map-size basis of the key
// count (a lazily-expired-but-unreaped key still shows in both totals until a
// keyed read drops it). It runs on the owner goroutine at a command boundary.
func (s *Store) VolatileKeys() uint64 {
	var n uint64
	for _, seg := range s.idx.segs {
		if seg == nil {
			continue
		}
		count := func(b *bucket) {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i]
				if w == 0 || slotCold(w) {
					continue
				}
				if s.expireAt(w&addrMask) != 0 {
					n++
				}
			}
		}
		for i := range seg.buckets {
			count(&seg.buckets[i])
		}
		for i := range seg.overflow {
			count(&seg.overflow[i])
		}
	}
	return n
}

// rangeBucket decodes each non-zero slot in one bucket and hands its key to fn.
// A zero word is an empty slot. A resident word's address is an arena offset
// whose key bytes read directly; an expired resident record is skipped without
// being reaped. A cold word's address is a cold-frame offset whose key reads
// with one pooled pread; a read error drops the entry from the walk, the safe
// answer for a frame the store cannot reach. It reports false when fn asked to
// stop.
func (s *Store) rangeBucket(b *bucket, now int64, fn func(key []byte) bool) bool {
	for i := 0; i < slotsPerBucket; i++ {
		w := b.slots[i]
		if w == 0 {
			continue
		}
		var key []byte
		if slotCold(w) {
			k, ok := s.coldKeyAt(w & addrMask)
			if !ok {
				continue
			}
			key = k
		} else {
			addr := w & addrMask
			if now != 0 {
				if at := s.expireAt(addr); at != 0 && at <= now {
					continue
				}
			}
			key = s.keyAt(addr)
		}
		if !fn(key) {
			return false
		}
	}
	return true
}
