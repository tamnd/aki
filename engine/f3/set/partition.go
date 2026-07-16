package set

import (
	"math/bits"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The partitioned band (spec 2064/f3/11 section 4, lab 04
// labs/f3/m1/04_partition_engagement). At the engagement threshold a native set
// splits by member hash into P independent native sub-tables under the same
// single owner (F1): no cross-shard anything, an intra-set layout change. Each
// sub-table is a full htable (member.go) with its own table, records, slab, and
// draw vector; the partition layer routes point ops, carries the exactly-uniform
// weighted draw, and walks the sub-tables in order for enumeration and SSCAN.
//
// The band buys bounded maintenance, not write concurrency (there is no lock to
// spread under F1): a rehash, a vector memmove, or a sorted-array flush now scale
// with n/P, not n, so the worst single rehash pause stays near a millisecond no
// matter how large the whole set grows (lab 04's grow-pause column is the whole
// argument). Transitions are one-way (F4): a set never drops back to the native
// band and P never shrinks, so an SPOP drain that takes the count below the
// threshold keeps the partitioned layout.

const (
	// partTarget is both the engagement threshold and the per-partition member
	// target, 1<<18 = 262144, the doc-19 defaults that survived the 6c sweep (K12)
	// and lab 04's confirmed constant: it is the largest single-table size whose
	// worst rehash pause is still sub-millisecond (0.88 ms at the threshold on the
	// lab box), and one doubling above it the pause passes a millisecond and climbs
	// without bound. The per-partition target is the real invariant: holding every
	// partition at or under it caps every rehash at the same ~1 ms pause, and P
	// follows from the target rather than being chosen directly.
	partTarget = 1 << 18

	// partFloorP floors the derived P at 4 so the walk skips P=2 (L5): doc 19
	// measured P=2 as the worst operating point, paying full routing cost for a
	// merely halved structure, so the first split jumps straight to P4 (lab 04
	// confirms the floor makes P=2 unreachable by construction).
	partFloorP = 4
)

// partitionThreshold is the member count at which a native set engages the
// partitioned band. It equals partTarget in production; the tests lower it to
// exercise the split and the P walk cheaply, matching algebraMaintain's
// test-seam pattern. deriveP reads it too, so a lowered threshold scales the
// whole walk down together.
var partitionThreshold = partTarget

// deriveP returns the target partition count for a set of the given cardinality:
// the next power of two at or above ceil(card/partitionThreshold), floored at
// partFloorP (doc 11 section 4.1, lab 04). At the threshold it is P4; it doubles
// as the count crosses target times the current P, so P4 up to 4x the target,
// then P8, P16, and on past 4M, exactly the growth walk lab 04 froze.
func deriveP(card int) int {
	q := (card + partitionThreshold - 1) / partitionThreshold // ceil(card/threshold)
	p := 1
	for p < q {
		p <<= 1
	}
	if p < partFloorP {
		p = partFloorP
	}
	return p
}

// partOf routes member hash to its partition using the top log2(P) hash bits,
// clear of the sub-table's own low-bit slot and mid-bit H2 tag (lab 04: top-bit
// routing keeps placement independent of the sub-table's hashing and spread 16M
// members within 5% of even). P is always a power of two at least partFloorP.
func partOf(hash uint64, p int) int {
	lg := bits.Len(uint(p)) - 1 // log2(p) for a power of two
	return int(hash >> (64 - lg))
}

// partitioned is one set's partitioned layout: P sub-tables selected by member
// hash, the per-partition live counts that weight the draw, the running total,
// and the partition generation the SSCAN cursor tags so a split invalidates a
// stale cursor into a safe rescan. It is owner-local, so nothing locks.
type partitioned struct {
	parts  []*htable // P native sub-tables, indexed by partOf
	counts []uint64  // per-partition live count, parallel to parts; the draw weights
	total  uint64    // sum of counts, the set cardinality
	pgen   uint32    // partition generation, bumped on every split (SSCAN cursor tag)

	// esc is the F13 draw-escalation layer (escalate.go), nil until the execution
	// model engages it on a saturated draw-heavy hot key (doc 11 section 5.4). When
	// set it groups the partitions into k draw-groups and carries the two-level
	// weighted draw; a nil esc is the default, so add, rem, and at pay one
	// never-taken branch and the flat draw path stays the pre-escalation path.
	esc *escalation
}

// buildPartitioned redistributes the members drained by drain into p sub-tables.
// hint sizes each sub-table for its expected share so the redistribution fills
// without a mid-build resize. The members flow through the sub-table's ordinary
// add, so under the algebra flag each partition engages and maintains its own
// sorted arrays as it crosses the floor (doc 11 section 6.3, the per-partition
// runs that keep maintenance cache-resident); with the flag off it is the plain
// insert path.
func buildPartitioned(p, hint int, drain func(emit func(m []byte))) *partitioned {
	pt := &partitioned{parts: make([]*htable, p), counts: make([]uint64, p)}
	per := hint/p + 1
	for i := range pt.parts {
		pt.parts[i] = newHashtable(per)
	}
	drain(func(m []byte) {
		idx := partOf(store.Hash(m), p)
		if pt.parts[idx].add(m) {
			pt.counts[idx]++
			pt.total++
		}
	})
	return pt
}

// partitionedSet builds a partitioned set from a fully built native table, the
// shared path for the native-to-partitioned transition and the STORE
// pre-partition-at-build (doc 11 section 7). P is derived from the table's
// cardinality, and pgen starts at 1 so any continuation cursor carries a nonzero
// generation tag and only the done cursor is ever zero.
func partitionedSet(h *htable) *set {
	pt := buildPartitioned(deriveP(h.card()), h.card(), func(emit func(m []byte)) { h.each(emit) })
	pt.pgen = 1
	return &set{enc: encPartitioned, part: pt}
}

// grow repartitions to newP by redistributing every member through the new hash
// bits, the one-way split of doc 11 section 4.1. It bumps pgen so an SSCAN cursor
// issued under the old P degrades to a rescan rather than reading a moved member
// out of the wrong partition. newP is always larger than the current P (F4, P
// never shrinks).
func (pt *partitioned) grow(newP int) {
	k := 0
	if pt.esc != nil {
		k = len(pt.esc.totals) // preserve the F13 group count across the split
	}
	old := pt.parts
	np := buildPartitioned(newP, int(pt.total), func(emit func(m []byte)) {
		for _, h := range old {
			h.each(emit)
		}
	})
	np.pgen = pt.pgen + 1
	*pt = *np
	if k > 0 {
		// The split raised P; rebuild the escalation groups over the new partitions
		// keeping k, since k divides every P in the one-way doubling walk (both are
		// powers of two and k <= P). The draw stays escalated across the grow (F4).
		pt.esc = newEscalation(pt.counts, newP/k, k)
	}
}

// add routes m to its partition and inserts it, then splits when the growing
// cardinality raises the target P (doc 11 section 4.1: the walk doubles as the
// count crosses target times P). It reports whether the set gained a member.
func (pt *partitioned) add(m []byte) bool {
	idx := partOf(store.Hash(m), len(pt.parts))
	if !pt.parts[idx].add(m) {
		return false
	}
	pt.counts[idx]++
	pt.total++
	if pt.esc != nil {
		pt.esc.totals[idx/pt.esc.span]++ // keep the group scatter weight current (5.4)
	}
	if target := deriveP(int(pt.total)); target > len(pt.parts) {
		pt.grow(target)
	}
	return true
}

// rem routes m to its partition and removes it, reporting whether it was present.
// It never shrinks P and never rejoins partitions (F4, one-way): a drained
// partitioned set stays partitioned until the key is dropped.
func (pt *partitioned) rem(m []byte) bool {
	idx := partOf(store.Hash(m), len(pt.parts))
	if !pt.parts[idx].rem(m) {
		return false
	}
	pt.counts[idx]--
	pt.total--
	if pt.esc != nil {
		pt.esc.totals[idx/pt.esc.span]-- // a drain thins the group weight, never de-escalates (5.4, F4)
	}
	return true
}

func (pt *partitioned) has(m []byte) bool {
	return pt.parts[partOf(store.Hash(m), len(pt.parts))].has(m)
}

// residentBytes sums the sub-tables' footprints plus the routing slices (the
// parts pointers and the per-partition counts), the partitioned band's term of
// the collection resident-byte estimate (spec 2064/f3/06 section 6.3). The F13
// escalation groups, off by default, are not counted; they engage only under the
// execution model's draw-heavy hot-key path and never move the demotion decision.
func (pt *partitioned) residentBytes() uint64 {
	n := uint64(cap(pt.parts)) * 8
	n += uint64(cap(pt.counts)) * 8
	for _, h := range pt.parts {
		n += h.residentBytes()
	}
	return n
}

func (pt *partitioned) card() int { return int(pt.total) }

// each visits every member across the partitions in index order. Order is
// already arbitrary for a set, so the per-partition walk costs nothing (doc 11
// section 4.2). The []byte aliases a sub-table's slab and is valid only for the
// call.
func (pt *partitioned) each(fn func(m []byte)) {
	for _, h := range pt.parts {
		h.each(fn)
	}
}

// eachUntil visits members across the partitions until fn returns false, the
// early-stop enumeration SINTERCARD's LIMIT walk rides over a partitioned
// operand.
func (pt *partitioned) eachUntil(fn func(m []byte) bool) {
	for _, h := range pt.parts {
		stop := false
		h.eachUntil(func(m []byte) bool {
			if !fn(m) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}
}

// locate maps a flat draw position r in [0, total) to its partition and the
// in-partition draw index, the exactly-uniform weighted draw of doc 11 section
// 4.3: the flat position space is the partitions laid end to end by count, so a
// uniform r lands on partition p with probability count_p/total and then on a
// uniform slot within it, giving every member probability 1/total regardless of
// skew across partitions.
//
// The scan is branchless as lab 04 requires (line 302): it walks all P counts
// with no data-dependent branch, deriving the "r is past partition i" bit from
// the sign of r-end rather than a compare-and-jump, so the ~10ns bound holds at
// 4M+ members where lab 04's scalar early-exit walk drifted to 25-28ns. Counts
// total well under 2^63, so the signed-shift trick never overflows.
func (pt *partitioned) locate(r uint64) (part, local int) {
	var end, start uint64
	p := 0
	for i := 0; i < len(pt.counts); i++ {
		end += pt.counts[i]
		// mask is all-ones when r >= end (partition i lies entirely before r) and
		// zero otherwise: int64(r-end) is non-negative exactly when r >= end, so its
		// arithmetic sign shift is 0 there and all-ones when r < end; the outer
		// complement flips that to the "beyond" sense with no branch.
		mask := ^uint64(int64(r-end) >> 63)
		p += int(mask & 1)
		start += pt.counts[i] & mask
	}
	return p, int(r - start)
}

// at returns the member at flat draw index i, resolving the partition through the
// branchless weighted scan. This is the seam draw.go's drawIndex feeds: drawIndex
// picks a uniform i in [0, card) and at resolves it to the weighted partition and
// in-partition draw, so nothing in draw.go changes for the partitioned band.
func (pt *partitioned) at(i int) []byte {
	var part, local int
	if pt.esc != nil {
		part, local = pt.locateEscalated(uint64(i)) // F13 two-level draw (escalate.go)
	} else {
		part, local = pt.locate(uint64(i))
	}
	return pt.parts[part].at(local)
}

// SSCAN cursor packing for the partitioned band (doc 11 section 8.2): the top
// bits carry the partition generation, the middle bits the partition index, and
// the low bits the in-partition downward boundary plus one, so a live
// continuation cursor is never zero and only the done cursor is. A split bumps
// pgen, and a cursor tagged with a stale pgen (including a native-band cursor
// carried across the engagement, whose implicit pgen is 0) restarts the scan,
// which keeps the at-least-once guarantee at the cost of re-returning some
// already-seen members, exactly the degrade the doc names.
const (
	scanIdxBits  = 40
	scanPartBits = 12
	scanIdxMask  = (1 << scanIdxBits) - 1
	scanPartMask = (1 << scanPartBits) - 1
)

func packCursor(pgen uint32, part, b int) uint64 {
	return uint64(pgen)<<(scanIdxBits+scanPartBits) | uint64(part)<<scanIdxBits | uint64(b+1)
}

// scanPage walks the partitioned draw vectors partition by partition, each one
// downward like the native cursor (member.go scanPage), and packs the resume
// point into one opaque cursor. Within a partition the carried proof holds
// unchanged: swap-remove only slides the last live ordinal into a vacated slot,
// and members never cross partitions except on a split, which the pgen tag
// catches. COUNT bounds the total slots examined across the page; MATCH filters
// the emitted members.
func (pt *partitioned) scanPage(cursor uint64, count int, match []byte, emit func(m []byte)) uint64 {
	P := len(pt.parts)
	var part, b int
	switch {
	case cursor == 0:
		part, b = 0, pt.parts[0].vlen()
	case uint32(cursor>>(scanIdxBits+scanPartBits)) != pt.pgen:
		// Stale generation: a split moved members since this cursor was issued.
		// Restart from the top; every currently-present member is walked again, so
		// at-least-once holds. Splits are one-way and bounded (F4), so a scan pays
		// this restart a bounded number of times.
		part, b = 0, pt.parts[0].vlen()
	default:
		part = int((cursor >> scanIdxBits) & scanPartMask)
		b = int(cursor&scanIdxMask) - 1
	}
	remaining := count
	for part < P {
		h := pt.parts[part]
		if n := h.vlen(); b > n {
			b = n // a mid-scan shrink carried the old boundary past the new end
		}
		for b > 0 && remaining > 0 {
			m := h.memberByOrd(h.vec[b-1])
			if match == nil || globMatch(match, m) {
				emit(m)
			}
			b--
			remaining--
		}
		if b > 0 {
			return packCursor(pt.pgen, part, b) // page budget spent mid-partition
		}
		part++
		if part < P {
			b = pt.parts[part].vlen()
		}
		if remaining == 0 {
			if part < P {
				return packCursor(pt.pgen, part, b)
			}
			return 0
		}
	}
	return 0
}

// membersTotal is the exact byte width of the SMEMBERS reply over the whole
// partitioned set: the array header plus every member's bulk frame across every
// partition. The handler needs it to size the streaming commit before the first
// chunk goes out.
func (pt *partitioned) membersTotal() int64 {
	n := 0
	tot := int64(0)
	for _, h := range pt.parts {
		for i := 0; i < h.vlen(); i++ {
			tot += bulkFrameLen(h.mlenByOrd(h.ordAt(i)))
		}
		n += h.vlen()
	}
	return int64(arrayHeaderLen(n)) + tot
}

// partMembersStream streams SMEMBERS over a partitioned set as one multi-bulk
// reply: a single array header for the whole set, then every member of every
// partition in index order. It snapshots each partition's draw-vector ordinals
// and pins every sub-table, the same freeze the native stream takes (smembers.go),
// so record reuse and slab compaction stand down across all partitions until the
// last frame drains.
type partMembersStream struct {
	parts []*htable
	ords  [][]uint32
	total int

	pi      int    // current partition
	idx     int    // index within the current partition's snapshot
	buf     []byte // the element currently being emitted
	off     int    // bytes of buf already copied to the wire
	started bool   // array header framed yet
}

// Next fills dst with the next run of reply bytes, framing one element at a time
// so its working set is one member plus the chunk, never the whole set.
func (m *partMembersStream) Next(dst []byte) (int, error) {
	n := 0
	for n < len(dst) {
		if m.off >= len(m.buf) {
			switch {
			case !m.started:
				m.buf = resp.AppendArrayHeader(m.buf[:0], m.total)
				m.started = true
				m.off = 0
			case m.pi < len(m.parts):
				if m.idx >= len(m.ords[m.pi]) {
					m.pi++
					m.idx = 0
					continue
				}
				m.buf = resp.AppendBulk(m.buf[:0], m.parts[m.pi].memberByOrd(m.ords[m.pi][m.idx]))
				m.idx++
				m.off = 0
			default:
				return n, nil // every element framed and copied
			}
		}
		c := copy(dst[n:], m.buf[m.off:])
		m.off += c
		n += c
	}
	return n, nil
}

// Release unpins every partition when the stream drains, the match to the pins
// pinMembersStream took.
func (m *partMembersStream) Release() {
	for _, h := range m.parts {
		h.unpinStream()
	}
}

// pinMembersStream snapshots every partition's live ordinals, pins every
// sub-table, and returns the stream source. The snapshot is 4 bytes per member;
// the member bytes themselves are never duplicated.
func (pt *partitioned) pinMembersStream() *partMembersStream {
	ords := make([][]uint32, len(pt.parts))
	total := 0
	for i, h := range pt.parts {
		nn := h.vlen()
		o := make([]uint32, nn)
		for k := 0; k < nn; k++ {
			o[k] = h.ordAt(k)
		}
		ords[i] = o
		h.pinStream()
		total += nn
	}
	return &partMembersStream{parts: pt.parts, ords: ords, total: total}
}
