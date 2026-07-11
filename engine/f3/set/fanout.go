package set

import (
	"bytes"
	"math/bits"
	"sort"
)

// Per-partition merge fan-out (spec 2064/f3/11 section 6.5). Same-P operands
// intersect partition by partition: both sides were split by the same member
// hash bits, so partition i of A can only match partition i of B, and the P
// partition-pair merges are independent tasks whose results concatenate into
// the command reply. This file cuts a merge-ready operand pair into those
// matched group views and runs the section-6.6 kernels over each pair in
// group order, which is what routes partitioned operands onto the merge path
// the driver previously deferred to probe.
//
// Cross-P pairs (P_A != P_B) need no repartition copy: partitions are selected
// by the top log2(P) hash bits (partition.go partOf) and the sorted arrays are
// ordered by that same hash, so a coarser operand's partition is already the
// concatenation of the finer operand's group ranges. The doc's stable bucket
// split of the smaller operand (section 6.5) reduces to slicing each coarse
// run at the group boundary hashes, a binary search per cut instead of an
// O(|B|) pass.
//
// Everything here runs on the coordinating owner, sequentially, under single
// ownership (F1): the group loop is the fan-out structure, not the fan-out
// itself. Donating group tasks to idle workers needs the F17 intent barrier to
// freeze both operands for the command (doc 03 section 6), and the multi-key
// set commands do not ride that path yet (algebra_commands.go records the
// co-location assumption). When the barrier lands, each iteration of the group
// loop is one donated read-only task, capped at half the pool and only above
// the pre-registered 64k-merge-element work threshold (section 6.5); lab 08
// (labs/f3/m1/08_merge_fanout) measures that crossover and the scaling.

// groupView pairs one group's merge stream with the sub-table that resolves
// its ordinals to member bytes.
type groupView struct {
	h *htable
	s stream
}

// indexed reports whether every populated partition carries the maintained
// sorted arrays, so the whole set can present per-partition merge streams. An
// empty partition streams as empty whether or not it ever engaged.
func (pt *partitioned) indexed() bool {
	for i, h := range pt.parts {
		if pt.counts[i] > 0 && !h.indexed() {
			return false
		}
	}
	return true
}

// groups cuts the partitioned operand into g ascending hash-range group
// streams, g a power-of-two multiple of P (the pair's max P). At g == P each
// group is one partition's whole run-merge-tail stream; at g > P each
// partition is sliced into g/P contiguous sub-streams at the group boundary
// hashes, the binary-search form of the section-6.5 bucket split. Each
// partition's tail is sorted once (mergeStream) and the slices alias it, so
// the cut costs log(n/P) per boundary and copies nothing.
func (pt *partitioned) groups(g int) []groupView {
	out := make([]groupView, g)
	r := g / len(pt.parts)
	shift := uint(64 - (bits.Len(uint(g)) - 1))
	for p, h := range pt.parts {
		var st stream
		if h.indexed() {
			st, _, _ = h.mergeStream(nil)
		}
		if r == 1 {
			out[p] = groupView{h: h, s: st}
			continue
		}
		run, tail := st.run, st.tail
		for j := 0; j < r; j++ {
			i := p*r + j
			if j == r-1 {
				out[i] = groupView{h: h, s: stream{run: run, tail: tail}}
				break
			}
			// The boundary between group i and i+1 is the first hash of group i+1;
			// tombstoned run entries keep their hash (merge.go), so the search
			// stays correct over them.
			hb := uint64(i+1) << shift
			cr := sort.Search(len(run), func(k int) bool { return run[k].h >= hb })
			ct := sort.Search(len(tail), func(k int) bool { return tail[k].h >= hb })
			out[i] = groupView{h: h, s: stream{run: run[:cr:cr], tail: tail[:ct:ct]}}
			run, tail = run[cr:], tail[ct:]
		}
	}
	return out
}

// pairGroups builds the matched group views over a merge-ready pair (both flat
// or both partitioned, mergeablePair). A flat pair is one group; a partitioned
// pair cuts both sides at the max of the two partition counts, so group i of A
// covers exactly the hash range of group i of B.
func pairGroups(a, b *set) (ga, gb []groupView) {
	if a.enc == encHashtable {
		sa, _, _ := a.ht.mergeStream(nil)
		sb, _, _ := b.ht.mergeStream(nil)
		return []groupView{{h: a.ht, s: sa}}, []groupView{{h: b.ht, s: sb}}
	}
	g := max(len(a.part.parts), len(b.part.parts))
	return a.part.groups(g), b.part.groups(g)
}

// mergeIntersectPair intersects two merge-ready operands through the
// section-6.6 kernel, one group pair at a time, emitting each confirmed member
// once in group-then-hash-ascending order.
func mergeIntersectPair(a, b *set, emit func(m []byte)) {
	ga, gb := pairGroups(a, b)
	for i := range ga {
		ha, hb := ga[i].h, gb[i].h
		mergeIntersect(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
			ma := ha.memberByOrd(oa)
			if bytes.Equal(ma, hb.memberByOrd(ob)) {
				emit(ma)
				return true
			}
			return false
		})
	}
}

// mergeIntersectCountPair counts the intersection with SINTERCARD's LIMIT
// early-stop threaded across the groups: each group counts against the limit
// remaining after the groups before it, and the walk stops at the group where
// the limit lands, so LIMIT still short-circuits mid-merge (doc 11 section
// 6.4). limit <= 0 counts everything.
func mergeIntersectCountPair(a, b *set, limit int) int {
	ga, gb := pairGroups(a, b)
	count := 0
	for i := range ga {
		ha, hb := ga[i].h, gb[i].h
		rem := 0
		if limit > 0 {
			rem = limit - count
		}
		count += mergeIntersectCount(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
			return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob))
		}, rem)
		if limit > 0 && count >= limit {
			return count
		}
	}
	return count
}

// mergeDiffPair walks A minus B through the section-6.6 kernel, one group pair
// at a time, emitting each surviving A member.
func mergeDiffPair(a, b *set, emit func(m []byte)) {
	ga, gb := pairGroups(a, b)
	for i := range ga {
		ha, hb := ga[i].h, gb[i].h
		mergeDiff(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
			return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob))
		}, func(o uint32) { emit(ha.memberByOrd(o)) })
	}
}

// mergeUnionPair walks A union B through the section-6.6 kernel, one group
// pair at a time, emitting each distinct member once (A's copy on a tie) with
// no transient table. Groups cover disjoint hash ranges, so the per-group
// dedup is the whole dedup.
func mergeUnionPair(a, b *set, emit func(m []byte)) {
	ga, gb := pairGroups(a, b)
	for i := range ga {
		ha, hb := ga[i].h, gb[i].h
		mergeUnion(&ga[i].s, &gb[i].s,
			func(oa, ob uint32) bool { return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob)) },
			func(o uint32) { emit(ha.memberByOrd(o)) },
			func(o uint32) { emit(hb.memberByOrd(o)) })
	}
}
