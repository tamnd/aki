package f1raw

// The mixed-P merge surface (spec 2064/f1_rewrite_ltm/24 section 5.1). The same-P merge in
// sorthashmerge.go pairs partition t of A with partition t of B, an identity that holds only when both
// operands share a partition count: a member routes to hash & (P-1), so the same hash lands in the same
// partition index in two sets only when they use the same P. Doc 19 grows P with cardinality, so two
// sets at comparable sizes can still straddle a power-of-two boundary and sit at different P, e.g.
// SINTER(A@P=64, B@P=8). For that case the driver re-partitions the smaller operand's already-sorted
// arrays into the larger operand's P in one O(|small|) pass, then runs the same-P merge against the
// larger operand's real partitions. Because P is a power of two and the routing mask is the low bits,
// growing pSmall to pLarge is a stable bucket-split, not a re-sort: a target partition t in [0, pLarge)
// is fed only by source partition t & (pSmall-1) (its lower bits), and each source array is already in
// ascending (hash, off) order, so distributing it in order keeps every target array sorted.
//
// The far-smaller case does not come here at all: the driver routes it to the doc-20 probe, which is at
// the random-probe floor and needs no sorted array. Re-partition earns its O(|small|) pass only when the
// two operands are comparable enough that the merge's sequential streaming still beats the probe, which
// is exactly the size band the crossover k gates (section 7). The caller holds every source's stripe
// lock across the build and the merge and has run SyncSortedHashes first, so the snapshots the view
// captures are current and cannot change under the merge.

// RepartView is the smaller operand's sorted arrays re-bucketed to the larger operand's partition count,
// the immutable input the mixed-P merge reads. snaps[t] holds the members routing to target partition t
// in ascending (hash, off) order; prefixLen is the smaller operand's member-key prefix length, so a
// merge resolves a view-side arena offset to its member bytes exactly as the same-P path resolves a
// partition prefix's members. It is built once per mixed-P command and read by the pLarge partition
// merges with no further synchronization, since the caller's held stripe locks freeze the sources.
type RepartView struct {
	snaps     []*sortedSnap
	prefixLen int
}

// SortedRepartition builds a RepartView of the operand whose partitions are named by srcPrefixes (its
// partition count is len(srcPrefixes), one prefix per source partition in index order) re-bucketed to
// pLarge target partitions, in one pass over the source arrays. It returns false if the fold facility is
// off or any source partition is not current, so the caller falls back to the probe rather than merge a
// stale view. pLarge must be a power-of-two multiple of the source partition count, which the caller
// guarantees because both P values come from the same power-of-two engage ladder (doc 19); the routing
// mask is pLarge-1 and a member in source partition sp lands in a target t with t & (len-1) == sp, so no
// two sources feed one target and each target inherits its single source's ascending order.
func (s *Store) SortedRepartition(srcPrefixes [][]byte, pLarge int) (*RepartView, bool) {
	if s.shReg == nil || pLarge < 1 {
		return nil, false
	}
	hs := make([][]uint64, pLarge)
	offs := make([][]uint64, pLarge)
	mask := uint64(pLarge - 1)
	for _, prefix := range srcPrefixes {
		if !s.SortedHashCurrent(prefix) {
			return nil, false
		}
		snap := s.SortedHashSnapshot(prefix)
		if snap == nil {
			return nil, false
		}
		// Walk the source array in its existing ascending (hash, off) order and append each member to
		// its target bucket. Appending in order keeps every target sorted, so the split is O(n) with no
		// re-sort. Only targets sharing this source's low bits receive any member, so a target is fed by
		// one source and stays a clean ascending run.
		for i := range snap.h {
			t := snap.h[i] & mask
			hs[t] = append(hs[t], snap.h[i])
			offs[t] = append(offs[t], snap.off[i])
		}
	}
	snaps := make([]*sortedSnap, pLarge)
	for t := range pLarge {
		snaps[t] = &sortedSnap{h: hs[t], off: offs[t]}
	}
	prefixLen := 0
	if len(srcPrefixes) > 0 {
		prefixLen = len(srcPrefixes[0])
	}
	return &RepartView{snaps: snaps, prefixLen: prefixLen}, true
}

// SetSortedIntersectMixed runs the SINTER two-pointer merge over one target partition when the operands
// sit at different P: realPrefix is the larger operand's real partition-target prefix and view is the
// smaller operand re-partitioned by SortedRepartition. It mirrors SetSortedIntersectPart but takes the
// smaller side from the view's target bucket rather than a second registry prefix. Because intersection
// membership is symmetric, it emits the larger operand's member bytes (resolved against realPrefix's
// length); the byte-confirm still resolves both sides. It reports false when the larger operand's target
// partition is not current, so the caller falls back to the probe.
func (s *Store) SetSortedIntersectMixed(realPrefix []byte, view *RepartView, target int, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(realPrefix) {
		return false
	}
	a := s.SortedHashSnapshot(realPrefix)
	if a == nil {
		return false
	}
	b := view.snaps[target]
	la := len(realPrefix)
	confirm := s.sortedMergeConfirm(la, view.prefixLen)
	g := s.pinTiered()
	intersectEmit(a, b, confirm, func(offA uint64) {
		k := s.keyAtTiered(offA, nil)
		if len(k) >= la {
			emit(k[la:])
		}
	})
	g.unpin()
	return true
}

// SetSortedIntersectCountMixed is SetSortedIntersectMixed without materializing the members, for
// SINTERCARD's mixed-P case. It sums the confirmed shared members in the target partition, stopping at a
// positive limit, and reports false when the larger operand's target partition is not current. Its
// per-partition counts are disjoint for the same reason the same-P path's are, so the caller sums them
// and caps the total at LIMIT.
func (s *Store) SetSortedIntersectCountMixed(realPrefix []byte, view *RepartView, target, limit int) (int, bool) {
	if !s.SortedHashCurrent(realPrefix) {
		return 0, false
	}
	a := s.SortedHashSnapshot(realPrefix)
	if a == nil {
		return 0, false
	}
	b := view.snaps[target]
	confirm := s.sortedMergeConfirm(len(realPrefix), view.prefixLen)
	g := s.pinTiered()
	n := intersectCount(a, b, confirm, limit)
	g.unpin()
	return n, true
}

// SetSortedDiffMixed runs the SDIFF two-pointer merge over one target partition when the operands sit at
// different P. SDIFF is not commutative, so realIsA says whether the larger operand (the real prefix) is
// the A operand keys[0], the minuend whose surviving members are emitted: when it is, the real side is A
// and the view is B; when it is not, keys[0] is the smaller operand so the view is A and the real side is
// B. Either way diffEmit keeps A's members that no B member byte-confirms, and the emitted offsets
// resolve against A's prefix length. It reports false when the larger operand's target partition is not
// current.
func (s *Store) SetSortedDiffMixed(realPrefix []byte, realIsA bool, view *RepartView, target int, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(realPrefix) {
		return false
	}
	real := s.SortedHashSnapshot(realPrefix)
	if real == nil {
		return false
	}
	vs := view.snaps[target]
	lReal := len(realPrefix)
	lView := view.prefixLen
	g := s.pinTiered()
	if realIsA {
		confirm := s.sortedMergeConfirm(lReal, lView)
		diffEmit(real, vs, confirm, func(off uint64) {
			k := s.keyAtTiered(off, nil)
			if len(k) >= lReal {
				emit(k[lReal:])
			}
		})
	} else {
		confirm := s.sortedMergeConfirm(lView, lReal)
		diffEmit(vs, real, confirm, func(off uint64) {
			k := s.keyAtTiered(off, nil)
			if len(k) >= lView {
				emit(k[lView:])
			}
		})
	}
	g.unpin()
	return true
}

// SetSortedUnionMixed runs the SUNION two-pointer merge over one target partition when the operands sit
// at different P. Union is commutative, so it always streams the real side as A and the view as B; a
// member both hold is emitted once from the real side and a bare hash collision keeps both distinct. The
// real-side offsets resolve against realPrefix's length and the view-only offsets against the view's
// prefix length. It reports false when the larger operand's target partition is not current, so the
// caller falls back to the seen-set probe.
func (s *Store) SetSortedUnionMixed(realPrefix []byte, view *RepartView, target int, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(realPrefix) {
		return false
	}
	real := s.SortedHashSnapshot(realPrefix)
	if real == nil {
		return false
	}
	vs := view.snaps[target]
	lReal := len(realPrefix)
	lView := view.prefixLen
	confirm := s.sortedMergeConfirm(lReal, lView)
	g := s.pinTiered()
	unionEmit(real, vs, confirm,
		func(off uint64) {
			k := s.keyAtTiered(off, nil)
			if len(k) >= lReal {
				emit(k[lReal:])
			}
		},
		func(off uint64) {
			k := s.keyAtTiered(off, nil)
			if len(k) >= lView {
				emit(k[lView:])
			}
		})
	g.unpin()
	return true
}
