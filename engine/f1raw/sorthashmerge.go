package f1raw

// Exported set-algebra merge surface (spec 2064/f1_rewrite_ltm/24 slice 2). f1srv drives the
// SINTER/SINTERCARD merge but cannot name the engine-internal sortedSnap, confirmFunc, or the
// two-pointer primitives in sorthash.go, so these methods are the merge's public entry: one per
// partition pair, taking the two partition prefixes the sorted-hash registry keys on (sorthashfold.go),
// resolving member bytes through keyAtTiered so both the in-memory and the larger-than-memory regime
// work, and byte-confirming a hash match so a stale offset or the astronomically rare 64-bit collision
// never emits. The driver gates on SortedHashEnabled, then SyncSortedHashes and SortedHashCurrent,
// before it calls these under the sources' held stripe locks, so a partition read here is current and
// cannot change under the merge.

// SortedHashEnabled reports whether the sorted-hash fold facility is running, the gate f1srv checks
// before it considers the merge path at all. A store that never called EnableSortedHashFold reads
// false and the driver stays on the doc-20 probe, so the merge machinery is entirely opt-in.
func (s *Store) SortedHashEnabled() bool {
	return s.shOn.Load()
}

// SortedHashMergeStable reports whether the sorted arrays' cached arena offsets stay resolvable for
// the life of the entry, which holds exactly when the arena is not segmented. The merge resolves each
// member through the raw offset the fold recorded, and only the segmented arena ever reclaims a
// resident segment out from under such an offset: the migrator's Option A retier (retier.go) repairs
// the dense member vector as a record moves cold but not this separate sorted array, so a migrated
// member would leave a stale resident offset here. On the grow-only arena a member's bytes are never
// reclaimed, so an offset the fold holds always resolves and the merge is safe. The driver gates on
// this, keeping the larger-than-memory regime with an active migrator on the always-correct probe
// until the sorted array grows its own retier hook.
func (s *Store) SortedHashMergeStable() bool {
	return !s.segmented
}

// sortedMergeConfirm builds the byte-confirm the merge calls on a 64-bit hash match: it resolves both
// arena offsets to their composite keys and compares the member bytes past each operand's prefix, so
// two offsets confirm equal only when they name the same member. la and lb are the prefix lengths of
// the A and B operands (they differ only if the two sets have different key lengths, which is the
// normal case). It resolves each key with keyAtTiered against a nil destination, which returns a
// zero-copy arena subslice on the resident non-segmented arena and a fresh caller-owned copy on the
// segmented or cold tiers, so it is safe to call concurrently across the partition merges.
func (s *Store) sortedMergeConfirm(la, lb int) confirmFunc {
	return func(offA, offB uint64) bool {
		ka := s.keyAtTiered(offA, nil)
		kb := s.keyAtTiered(offB, nil)
		if len(ka) < la || len(kb) < lb {
			return false
		}
		return string(ka[la:]) == string(kb[lb:])
	}
}

// SetSortedIntersectPart runs the two-pointer intersection over one partition pair and calls emit with
// each shared member's bytes, in A's ascending hash order. prefixA and prefixB are the partition
// prefixes the sorted arrays are registered under: uvarint(len(skey))|skey for an unpartitioned set,
// that plus one partition byte for one partition of a partitioned set. It loads each partition's
// published sorted snapshot, walks the two hash-ordered arrays as sequential streams the prefetcher
// serves, and on a hash match byte-confirms the candidates before emitting, so a stale offset or a
// bare hash collision is filtered. The emitted member is an arena-stable subslice (or a cold copy
// under the larger-than-memory regime), valid after this returns. It reports false when either
// partition's array is not current, so the caller falls back to the probe rather than emit a stale
// result; under the caller's held stripe locks after a sync, current is the expected case.
func (s *Store) SetSortedIntersectPart(prefixA, prefixB []byte, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(prefixA) || !s.SortedHashCurrent(prefixB) {
		return false
	}
	a := s.SortedHashSnapshot(prefixA)
	b := s.SortedHashSnapshot(prefixB)
	if a == nil || b == nil {
		return false
	}
	la := len(prefixA)
	confirm := s.sortedMergeConfirm(la, len(prefixB))
	// Pin the reader's epoch across the whole merge so a migrator cannot reclaim a segment an offset
	// still names while keyAtTiered copies its key out; a no-op on the pure in-memory arena.
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

// SetSortedIntersectCountPart is SetSortedIntersectPart without materializing the members, for
// SINTERCARD. It returns the number of confirmed shared members in the partition pair, stopping once
// the count reaches a positive limit (pass limit <= 0 to count them all), and false when either array
// is not current. The caller sums the per-partition counts and caps the total at the command's LIMIT:
// because a member routes to exactly one partition, the per-partition counts are disjoint and sum to
// the whole intersection size, so passing the command's LIMIT as each partition's early-stop cap and
// capping the sum again is correct.
func (s *Store) SetSortedIntersectCountPart(prefixA, prefixB []byte, limit int) (int, bool) {
	if !s.SortedHashCurrent(prefixA) || !s.SortedHashCurrent(prefixB) {
		return 0, false
	}
	a := s.SortedHashSnapshot(prefixA)
	b := s.SortedHashSnapshot(prefixB)
	if a == nil || b == nil {
		return 0, false
	}
	confirm := s.sortedMergeConfirm(len(prefixA), len(prefixB))
	g := s.pinTiered()
	n := intersectCount(a, b, confirm, limit)
	g.unpin()
	return n, true
}

// SetSortedDiffPart runs the two-pointer difference over one partition pair and calls emit with each
// member of A not present in B (byte-confirmed), in A's ascending hash order. It mirrors
// SetSortedIntersectPart: same currency gate, same snapshot load, same epoch pin, but the inner loop is
// diffEmit. Every emitted member comes from the A operand, so it resolves against prefixA's length. It
// reports false when either partition's array is not current, so the caller falls back to the probe.
func (s *Store) SetSortedDiffPart(prefixA, prefixB []byte, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(prefixA) || !s.SortedHashCurrent(prefixB) {
		return false
	}
	a := s.SortedHashSnapshot(prefixA)
	b := s.SortedHashSnapshot(prefixB)
	if a == nil || b == nil {
		return false
	}
	la := len(prefixA)
	confirm := s.sortedMergeConfirm(la, len(prefixB))
	g := s.pinTiered()
	diffEmit(a, b, confirm, func(offA uint64) {
		k := s.keyAtTiered(offA, nil)
		if len(k) >= la {
			emit(k[la:])
		}
	})
	g.unpin()
	return true
}

// SetSortedUnionPart runs the two-pointer union over one partition pair and calls emit with each
// distinct member across both, in merged ascending hash order. It mirrors SetSortedIntersectPart but
// the inner loop is unionEmit, which yields members from both operands: an A member resolves against
// prefixA's length and a B-only member against prefixB's, since the two prefixes differ whenever the
// sets have different key lengths. It reports false when either partition's array is not current, so
// the caller falls back to the seen-set probe.
func (s *Store) SetSortedUnionPart(prefixA, prefixB []byte, emit func(member []byte)) bool {
	if !s.SortedHashCurrent(prefixA) || !s.SortedHashCurrent(prefixB) {
		return false
	}
	a := s.SortedHashSnapshot(prefixA)
	b := s.SortedHashSnapshot(prefixB)
	if a == nil || b == nil {
		return false
	}
	la, lb := len(prefixA), len(prefixB)
	confirm := s.sortedMergeConfirm(la, lb)
	g := s.pinTiered()
	unionEmit(a, b, confirm,
		func(offA uint64) {
			k := s.keyAtTiered(offA, nil)
			if len(k) >= la {
				emit(k[la:])
			}
		},
		func(offB uint64) {
			k := s.keyAtTiered(offB, nil)
			if len(k) >= lb {
				emit(k[lb:])
			}
		})
	g.unpin()
	return true
}
