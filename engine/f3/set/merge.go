package set

// The P11 set-algebra merge kernels (spec 2064/f3/11 sections 6.1, 6.4, 6.6),
// ported from f1's sorted-hash merge (engine/f1raw/sorthash.go) with f1's
// concurrent-reader machinery deleted: no published snapshot, no generation
// check, no epoch pin, no probe fallback. Under single ownership (F1) the owner
// is the only reader and writer of a set's sorted arrays, so the kernel walks
// them in place while its multi-key command holds the intent barrier.
//
// Each operand presents as a run-merge-tail stream (section 6.3): a sorted run
// of (hash, ordinal) entries plus a small unsorted tail sorted once at command
// entry (algebra.go builds the stream). The kernel sees one logical ascending
// stream per operand, with tombstoned run entries skipped, and byte-confirms a
// hash tie before it emits, folding the confirm and the emit into one callback
// so a matched member's bytes are resolved once, not twice (the mergeresolve
// discipline, K16). advance gallops when one run is locally sparse (section
// 6.6), which is what lets an asymmetric-but-above-floor merge degrade toward
// the probe cost instead of scanning a long run element by element.

// hentry is one member's sorted-array cell: its 64-bit hash and its record
// ordinal, the locator that stays valid across an LTM retier because only the
// record's loc moves, never its ordinal (doc 11 sections 6.1 and 10.3). 16
// bytes after alignment, the section-11.2 algebra tax. The ordinal's top bit is
// the tombstone flag (a removed member's run entry keeps its hash for the sort
// order but carries the tomb bit until the next flush compacts it); ordinals are
// bounded by the native band's 256K ceiling, far below 1<<31, so the bit is free.
type hentry struct {
	h   uint64
	ord uint32
}

// tomb marks a removed run entry: its hash still orders it in the run, but the
// stream skips it and the next tail flush drops it (doc 11 section 6.3).
const tomb = uint32(1) << 31

func isTomb(ord uint32) bool { return ord&tomb != 0 }

// stream is the run-merge-tail view one operand presents to the kernels. run is
// the operand's sorted array (borrowed, never copied); tail is a private sorted
// copy of the unsorted tail the owner made at command entry (so the source set
// is untouched, matching f1's snapshot copy). ri and ti are the two cursors.
type stream struct {
	run  []hentry
	tail []hentry // sorted ascending by hash at command entry, tombstone-free
	ri   int
	ti   int
}

// skipTomb advances the run cursor past tombstoned entries so the head is always
// a live member.
func (s *stream) skipTomb() {
	for s.ri < len(s.run) && isTomb(s.run[s.ri].ord) {
		s.ri++
	}
}

// empty reports whether the stream is drained.
func (s *stream) empty() bool {
	s.skipTomb()
	return s.ri >= len(s.run) && s.ti >= len(s.tail)
}

// peek returns the next head hash. The caller has checked !empty.
func (s *stream) peek() uint64 {
	s.skipTomb()
	if s.ri >= len(s.run) {
		return s.tail[s.ti].h
	}
	if s.ti >= len(s.tail) || s.run[s.ri].h <= s.tail[s.ti].h {
		return s.run[s.ri].h
	}
	return s.tail[s.ti].h
}

// next returns and consumes the head entry, its ordinal cleared of the tomb bit.
// The caller has checked !empty.
func (s *stream) next() hentry {
	s.skipTomb()
	if s.ri >= len(s.run) {
		e := s.tail[s.ti]
		s.ti++
		return e
	}
	if s.ti >= len(s.tail) || s.run[s.ri].h <= s.tail[s.ti].h {
		e := s.run[s.ri]
		s.ri++
		return hentry{h: e.h, ord: e.ord} // already live: skipTomb ran
	}
	e := s.tail[s.ti]
	s.ti++
	return e
}

// takeRun consumes every entry whose hash equals hv from both cursors, appending
// them to dst. Equal hashes are the collision case (runs of length one are the
// common case, doc 11 line 275), so dst usually gains one entry.
func (s *stream) takeRun(hv uint64, dst []hentry) []hentry {
	for {
		s.skipTomb()
		took := false
		if s.ri < len(s.run) && s.run[s.ri].h == hv {
			dst = append(dst, hentry{h: hv, ord: s.run[s.ri].ord})
			s.ri++
			took = true
		}
		if s.ti < len(s.tail) && s.tail[s.ti].h == hv {
			dst = append(dst, s.tail[s.ti])
			s.ti++
			took = true
		}
		if !took {
			return dst
		}
	}
}

// gallopTo advances both cursors to the first entry with hash >= target. The run
// cursor gallops (double the step, then binary search) so a locally sparse run
// is skipped in log steps instead of a linear scan; the tail is at most T
// entries so it is stepped linearly (doc 11 section 6.6). Tombstoned run entries
// keep their hash, so the gallop's comparisons stay correct and skipTomb cleans
// up on the next head read.
func (s *stream) gallopTo(target uint64) {
	if s.ri < len(s.run) && s.run[s.ri].h < target {
		i := s.ri
		step := 1
		for i+step < len(s.run) && s.run[i+step].h < target {
			i += step
			step <<= 1
		}
		lo := i
		hi := i + step
		if hi > len(s.run) {
			hi = len(s.run)
		}
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if s.run[mid].h < target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		s.ri = lo
	}
	for s.ti < len(s.tail) && s.tail[s.ti].h < target {
		s.ti++
	}
}

// mergeIntersect is the section-6.6 two-pointer intersection over one operand
// pair. For each hash tie it collects the equal-hash runs on both sides and
// calls onPair with every candidate ordinal pair in A's ascending order; onPair
// byte-confirms the pair and, when it names the same member, emits it and returns
// true so the walk stops scanning that A member's B run. Non-matching hashes are
// galloped past, so a skewed pair degrades toward the probe cost. This is the
// SINTER and SINTERSTORE inner loop over one partition pair.
func mergeIntersect(a, b *stream, onPair func(ordA, ordB uint32) bool) {
	var ra, rb []hentry
	for !a.empty() && !b.empty() {
		ha, hb := a.peek(), b.peek()
		switch {
		case ha < hb:
			a.gallopTo(hb)
		case ha > hb:
			b.gallopTo(ha)
		default:
			ra = a.takeRun(ha, ra[:0])
			rb = b.takeRun(hb, rb[:0])
			for _, ea := range ra {
				for _, eb := range rb {
					if onPair(ea.ord, eb.ord) {
						break
					}
				}
			}
		}
	}
}

// mergeIntersectCount is mergeIntersect without materializing the members, for
// SINTERCARD. It byte-confirms each tie through confirm and stops early once the
// count reaches a positive limit (pass limit <= 0 to count them all), which is
// SINTERCARD's LIMIT early exit inside the merge loop (doc 11 section 6.4).
func mergeIntersectCount(a, b *stream, confirm func(ordA, ordB uint32) bool, limit int) int {
	var ra, rb []hentry
	count := 0
	for !a.empty() && !b.empty() {
		ha, hb := a.peek(), b.peek()
		switch {
		case ha < hb:
			a.gallopTo(hb)
		case ha > hb:
			b.gallopTo(ha)
		default:
			ra = a.takeRun(ha, ra[:0])
			rb = b.takeRun(hb, rb[:0])
			for _, ea := range ra {
				for _, eb := range rb {
					if confirm(ea.ord, eb.ord) {
						count++
						if limit > 0 && count >= limit {
							return count
						}
						break
					}
				}
			}
		}
	}
	return count
}

// mergeDiff walks A minus B over one operand pair and calls emitA with the
// A-side ordinal of every member of A not present in B (byte-confirmed), in A's
// ascending order (doc 11 section 6.4, the SDIFF inner loop). An A member
// survives when its hash is below B's cursor with no match, or when its hash ties
// a B run but no B member confirms equal (a bare collision, so the members
// differ). The excluder side is galloped forward on a miss, since B is never
// emitted; A is stepped one at a time because every A member must be examined.
func mergeDiff(a, b *stream, confirm func(ordA, ordB uint32) bool, emitA func(ord uint32)) {
	var ra, rb []hentry
	for !a.empty() {
		if b.empty() {
			emitA(a.next().ord)
			continue
		}
		ha, hb := a.peek(), b.peek()
		switch {
		case ha < hb:
			emitA(a.next().ord)
		case ha > hb:
			b.gallopTo(ha)
		default:
			ra = a.takeRun(ha, ra[:0])
			rb = b.takeRun(hb, rb[:0])
			for _, ea := range ra {
				matched := false
				for _, eb := range rb {
					if confirm(ea.ord, eb.ord) {
						matched = true
						break
					}
				}
				if !matched {
					emitA(ea.ord)
				}
			}
		}
	}
}

// mergeUnion walks A union B over one operand pair and calls emitA/emitB with the
// distinct members of both in merged ascending order (doc 11 section 6.4, the
// SUNION inner loop). Every A member is in the union, so each goes through emitA;
// a B member goes through emitB only when no A member in its equal-hash run
// confirms equal, so a member both sets hold is emitted once (from A) and a bare
// collision keeps both. Two callbacks let the caller resolve each ordinal against
// its own operand. Neither side gallops: the union must see every element.
func mergeUnion(a, b *stream, confirm func(ordA, ordB uint32) bool, emitA, emitB func(ord uint32)) {
	var ra, rb []hentry
	for !a.empty() && !b.empty() {
		ha, hb := a.peek(), b.peek()
		switch {
		case ha < hb:
			emitA(a.next().ord)
		case ha > hb:
			emitB(b.next().ord)
		default:
			ra = a.takeRun(ha, ra[:0])
			rb = b.takeRun(hb, rb[:0])
			for _, ea := range ra {
				emitA(ea.ord)
			}
			for _, eb := range rb {
				matched := false
				for _, ea := range ra {
					if confirm(ea.ord, eb.ord) {
						matched = true
						break
					}
				}
				if !matched {
					emitB(eb.ord)
				}
			}
		}
	}
	for !a.empty() {
		emitA(a.next().ord)
	}
	for !b.empty() {
		emitB(b.next().ord)
	}
}
