package f1raw

// Weighted-partition draw (spec 2064/f1_rewrite_ltm/19 sections 2.1 and 6.5/6.6). A partitioned
// set stores its members as P independent dense vectors, one per partition, keyed in the store's
// random-access structure (randvec.go) by the partition-scan prefix uvarint(len(skey))|skey|part.
// A uniform draw over the whole set composes the P per-partition vectors into one exactly-uniform
// global draw: read the P partition counts, sum to total, pick a global index r in [0, total),
// and prefix-sum-walk to the partition p whose half-open range [offset_p, offset_p+count_p)
// contains r, then draw slot r-offset_p within p's vector. Section 2.2 proves every live member
// comes out with probability exactly 1/total, skew and all, because a fat partition's higher pick
// probability is exactly cancelled by its members' lower per-slot probability.
//
// The count read is lock-free and, after warmup, map-lookup-free. Each partition vector publishes
// its slots snapshot through an atomic pointer, so summing P snapshot lengths touches no lock, and
// the P partition-vector pointers are cached in a per-set descriptor (partdesc.go), so summing the
// counts is P atomic loads with no shard-map probe. P concurrent draws that route to P different
// partitions take P different locks (or none, on the non-destructive read). This is the fix for the
// single-hot-key wall: doc 18's one vector under one lock became P vectors under P locks, so a hot
// set's draws scale across cores instead of serializing on one (section 1). The descriptor is what
// makes that split show as throughput: slice 4 read the counts with O(P) map lookups per draw, a tax
// that grew with P faster than the lock split saved, so slice 4b (section 5) caches the pointers.
//
// A partition's vector is built lazily on the first draw against it (deriveOnFirstDraw scans the
// partition's prefix range), exactly as the unpartitioned vector is, so a partition never drawn
// from allocates nothing. The routed writes (cmdSAddPart/cmdSRemPart) keep a built vector current
// through CollRandInsert/CollRandRemove against the same partition prefix.

// mixWord advances a splitmix64 word, used to derive a fresh uniform word from the caller's draw
// word for a redraw or clamp without touching any shared counter. The caller passes one word from
// its own PRNG; when a draw needs a second independent word (a stale-index clamp, a retry after a
// partition emptied under a concurrent pop, or successive distinct-sample draws), it mixes forward
// from that word rather than reaching back to the shared connection PRNG.
func mixWord(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// collPartVec returns the partition vector for prefix, building it lazily under the shard write
// mutex if it does not exist yet. It is the per-partition analogue of the lazy build the single
// draw paths run: the fast path is a lock-free map load, and only the first draw against a
// partition takes the write mutex to scan the partition's prefix range and install its vector.
// prefix is the partition-scan prefix uvarint(len(skey))|skey|part, which bounds exactly one
// partition's member rows, so the built vector holds precisely that partition's live members.
func (s *Store) collPartVec(prefix []byte) *memberVec {
	sh := s.rvec.shardFor(prefix)
	if v := sh.get(prefix); v != nil {
		return v
	}
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		v = s.deriveOnFirstDraw(prefix)
		sh.put(prefix, v)
	}
	sh.mu.Unlock()
	return v
}

// CollPartRandInsert records a newly-added member's offset in partition part's dense vector of the
// P-partition set whose partition-scan base is base, building the partition vector eagerly if it
// does not exist yet. It is the partitioned companion of CollRandInsert: doc 20 makes the vector the
// authoritative membership structure, so a partitioned set that is only enumerated still needs its
// partition vectors built on write rather than deferred to the first draw. The build goes through the
// descriptor (partDescFor then descPartVec), not straight through the randVec shard, because a
// partition vector is torn down only through the descriptor CollRandDrop consults: a vector built off
// the shard alone would leak past DEL and a grow's re-home and read stale (doc 20 section 6.1).
// descPartVec builds it whole through collPartVec's deriveOnFirstDraw scan, which CollInsert has
// already fed this member into, so the resolve-and-add below is idempotent for this member and only
// appends when the vector already existed. Call it under the partition's stripe lock, right after
// PutKind reports a new member and CollInsert adds its ordered node; base's final byte is rewritten
// to part internally. It re-reads the vector under the shard mutex before adding so a concurrent
// CollRandDrop between the descriptor build and the add makes it a no-op rather than reviving a
// dropped set's vector, matching CollRandInsert's drop safety.
func (s *Store) CollPartRandInsert(base []byte, p, part int, key []byte, kind byte) {
	d := s.partDescFor(base[:len(base)-1], p)
	s.descPartVec(d, base, part) // rewrites base's final byte to part and registers the vector
	sh := s.rvec.shardFor(base)
	sh.mu.Lock()
	if v := sh.get(base); v != nil {
		if off, _, _, _, found := s.find(key, hash(key), kind); found {
			v.add(off)
		}
	}
	sh.mu.Unlock()
}

// walkPart maps a global index g in [0, total) to its partition by the prefix-sum walk: it
// subtracts each partition's count until g falls within the current partition's range, and
// returns that partition and the within-partition index. An empty partition contributes a
// zero-width slice, so the walk steps straight over it and g never lands in it. total must equal
// sum(counts[:p]) and g must be in [0, total), which the caller guarantees by drawing g against
// that same total.
func walkPart(counts []int, p, g int) (part, local int) {
	for part = 0; part < p; part++ {
		if g < counts[part] {
			return part, g
		}
		g -= counts[part]
	}
	// Unreachable when g < total; return the last partition defensively rather than panic.
	return p - 1, 0
}

// CollPartRandOne draws one uniform random member from the P-partition set by the weighted scheme,
// non-destructively, and returns its composite key (a stable arena subslice). base is the
// partition-scan prefix uvarint(len(skey))|skey|<byte> whose final byte this rewrites per
// partition; p is the partition count; r is one random word from the caller's per-connection PRNG.
// It is the hot no-count SRANDMEMBER path and the per-draw primitive the negative-count form
// loops, and it takes no lock at all on the common path: the count sum and the slot read both go
// through atomic snapshots.
//
// The one race it guards is doc 18's stale-count race, now per partition: between the lock-free
// count sum and the chosen partition's slot read, a concurrent SPOP may have shrunk that partition,
// so the drawn index can land past its current length. The fix matches the single-vector path:
// clamp within the partition's current live count (still uniform for that partition), or if the
// partition emptied outright, remix the word and redraw across all partitions. The retry budget is
// bounded so a pathological concurrent-drain cannot spin.
func (s *Store) CollPartRandOne(base []byte, p int, r uint64) (key []byte, ok bool) {
	last := len(base) - 1
	d := s.partDescFor(base[:last], p)
	for attempt := 0; attempt < 8; attempt++ {
		var counts [maxPartitions]int
		total := s.weightedCountsDesc(d, base, counts[:])
		if total == 0 {
			return nil, false
		}
		part, local := walkPart(counts[:], p, drawIndexWord(r, total))
		base[last] = byte(part)
		vs := s.rvec.shardFor(base).get(base).view.Load()
		n := len(vs.s)
		if local < n {
			return s.keyAt(vs.s[local]), true
		}
		if n > 0 {
			return s.keyAt(vs.s[drawIndexWord(mixWord(r), n)]), true
		}
		r = mixWord(r)
	}
	return nil, false
}

// CollPartTotal returns the total live member count of the P-partition set, summing the P lock-free
// partition lengths. It is the emptiness-and-cap probe the count-form reads take: SRANDMEMBER with a
// count returns an empty array on a missing set and caps a positive count at the cardinality. base
// is the partition-scan prefix whose final byte this rewrites per partition, and each partition's
// vector is built lazily as it is counted.
func (s *Store) CollPartTotal(base []byte, p int) int {
	d := s.partDescFor(base[:len(base)-1], p)
	var counts [maxPartitions]int
	return s.weightedCountsDesc(d, base, counts[:])
}

// CollPartPick chooses a partition weighted by the P lock-free partition counts and returns it, or
// -1 when the whole set is empty. It sums the P published partition lengths to total, draws a global
// index in [0, total) from the caller's word r, and prefix-sum-walks to the partition whose range
// contains it, so partition q is chosen with probability count_q/total. base's final byte is left
// set to the chosen partition, so the caller takes that partition's stripe lock and hands base
// straight to CollPartPopLocked. Composed with CollPartPopLocked's uniform within-partition draw
// (1/count_q), a member comes out with probability exactly 1/total (section 2.2). It is the
// destructive SPOP's partition selection: lock-free, so P pops routing to P partitions never
// contend on the selection, only on their distinct partition locks.
func (s *Store) CollPartPick(base []byte, p int, r uint64) int {
	d := s.partDescFor(base[:len(base)-1], p)
	var counts [maxPartitions]int
	total := s.weightedCountsDesc(d, base, counts[:])
	if total == 0 {
		return -1
	}
	part, _ := walkPart(counts[:], p, drawIndexWord(r, total))
	base[len(base)-1] = byte(part)
	return part
}

// CollPartPopLocked draws one uniform random member from the single partition vector bounded by base
// (its final byte already the chosen partition, from CollPartPick) and swap-removes it, returning
// the member's arena key. The caller must hold that partition's stripe lock so the vector removal,
// the hash-record delete it does next, and the ordered-index splice serialize against that
// partition's writers as one unit; without it a concurrent SREM of the same member could delete the
// hash record between this pick and the caller's delete and both would decrement the cardinality.
// The within-partition slot is drawn from the shard's own PRNG, independent of the partition pick,
// so the two draws compose to an exactly-uniform member and the pop is sampling without replacement
// when looped. It reports false when the partition is empty (a concurrent pop drained it after
// CollPartPick counted it), on which the caller re-picks a partition with fresh counts.
func (s *Store) CollPartPopLocked(base []byte) (key []byte, ok bool) {
	sh := s.rvec.shardFor(base)
	sh.mu.Lock()
	v := sh.get(base)
	if v == nil {
		v = s.deriveOnFirstDraw(base)
		sh.put(base, v)
	}
	n := len(v.slots)
	if n == 0 {
		sh.mu.Unlock()
		return nil, false
	}
	i := sh.drawIndex(n)
	off := v.slots[i]
	v.remove(off)
	sh.mu.Unlock()
	return s.keyAt(off), true
}

// CollPartSampleDistinct draws up to want distinct members by the weighted scheme without
// removing any, appending each composite key to dst and returning the grown slice. It is the
// positive-count SRANDMEMBER form: sampling without replacement, so no member repeats. Distinctness
// is tracked by arena offset, a member's stable identity, so a member drawn twice is skipped and
// redrawn; the counts are read once up front, and because nothing is removed the partition vectors
// stay put for the whole sample. want is clamped to the cardinality by the caller's contract, and
// this also clamps it to total defensively. It runs off the hot path (the count form, not the
// single-draw gate path), so the seen-set retry it shares with the unpartitioned sampler is fine.
func (s *Store) CollPartSampleDistinct(base []byte, p, want int, r uint64, dst [][]byte) [][]byte {
	if want <= 0 {
		return dst
	}
	last := len(base) - 1
	d := s.partDescFor(base[:last], p)
	var counts [maxPartitions]int
	total := s.weightedCountsDesc(d, base, counts[:])
	if total == 0 {
		return dst
	}
	if want > total {
		want = total
	}
	seen := make(map[uint64]struct{}, want)
	for len(seen) < want {
		r = mixWord(r)
		part, local := walkPart(counts[:], p, drawIndexWord(r, total))
		base[last] = byte(part)
		vs := s.rvec.shardFor(base).get(base).view.Load()
		if local >= len(vs.s) {
			continue
		}
		off := vs.s[local]
		if _, dup := seen[off]; dup {
			continue
		}
		seen[off] = struct{}{}
		dst = append(dst, s.keyAt(off))
	}
	return dst
}
