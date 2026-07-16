package set

import (
	"bytes"
	"math/bits"
	"sort"

	"github.com/tamnd/aki/engine/obs1/shard"
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
// The group loop rides worker donation (shard.Ctx.FanOut) when the pair's
// merge work clears fanoutFloor: each group pair is one donated read-only
// task, legal because the coordinating owner is blocked in the command for the
// fan-out's duration, so both operands are frozen exactly as the intent
// barrier freezes them on the cross-shard path (doc 03 section 6). Tasks
// collect their group's members into per-group slots and the coordinator emits
// the slots in group order, so the reply bytes are identical to the sequential
// loop's, which is the serial oracle the tests hold. Below the floor, or with
// no runtime to donate through, the loop runs sequentially on the owner as
// before; lab 08 (labs/f3/m1/08_merge_fanout) priced the crossover and lab 09
// (labs/f3/m1/09_donation_live) reads the live scaling through this path.

// fanoutFloor is the merge-element count (both sides' entries) at or above
// which the group loop donates, the doc's pre-registered 64k threshold
// confirmed conservative by lab 08: on darwin the fan-out pays from ~32k
// elements even under the pessimistic spawn model, so 64k leaves margin for a
// slower barrier and protects the fairness bound (doc 11 section 6.5).
const fanoutFloor = 65536

// fanWorthy reports whether a merge pair should donate its group loop: a live
// runtime to donate through, at least two groups to split, and the pair's
// merge elements at or above the floor.
func fanWorthy(cx *shard.Ctx, a, b *set, groups int) bool {
	return cx != nil && groups >= 2 && a.card()+b.card() >= fanoutFloor
}

// pairGroupCount is the group count pairGroups will cut a merge-ready pair
// into, computed ahead of the streams so the fan decision can gate the stream
// build too: one group for a flat pair, the max partition count otherwise.
func pairGroupCount(a, b *set) int {
	if a.enc == encHashtable {
		return 1
	}
	return max(len(a.part.parts), len(b.part.parts))
}

// fanCx returns cx when the pair clears the fan-out gate and nil otherwise,
// the one decision the pair walkers thread through both donated phases (the
// stream build and the group loop). A nil Ctx runs FanOut inline.
func fanCx(cx *shard.Ctx, a, b *set) *shard.Ctx {
	if fanWorthy(cx, a, b, pairGroupCount(a, b)) {
		return cx
	}
	return nil
}

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

// streams builds every partition's run-merge-tail view, the per-command tail
// copy and sort of mergeStream, fanned across donated workers when cx is live:
// the copy leaves the source set untouched and each task writes only its own
// slot, so the build is exactly the donation contract. At production tail
// bounds this is a real serial term (the 1M gate pair carries eight 13k-entry
// tail sorts, lab 09), which is why it rides the same fan the group loop does.
func (pt *partitioned) streams(cx *shard.Ctx) []stream {
	sts := make([]stream, len(pt.parts))
	cx.FanOut(len(pt.parts), func(i int) {
		if h := pt.parts[i]; h.indexed() {
			sts[i], _, _ = h.mergeStream(nil)
		}
	})
	return sts
}

// groups cuts the partitioned operand into g ascending hash-range group
// streams, g a power-of-two multiple of P (the pair's max P), over the
// prebuilt per-partition streams. At g == P each group is one partition's
// whole run-merge-tail stream; at g > P each partition is sliced into g/P
// contiguous sub-streams at the group boundary hashes, the binary-search form
// of the section-6.5 bucket split. Each partition's tail was sorted once
// (streams) and the slices alias it, so the cut costs log(n/P) per boundary
// and copies nothing.
func (pt *partitioned) groups(g int, sts []stream) []groupView {
	out := make([]groupView, g)
	r := g / len(pt.parts)
	shift := uint(64 - (bits.Len(uint(g)) - 1))
	for p, h := range pt.parts {
		st := sts[p]
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
// covers exactly the hash range of group i of B. cx, nil unless the pair
// cleared fanCx, donates both sides' stream builds.
func pairGroups(cx *shard.Ctx, a, b *set) (ga, gb []groupView) {
	if a.enc == encHashtable {
		sa, _, _ := a.ht.mergeStream(nil)
		sb, _, _ := b.ht.mergeStream(nil)
		return []groupView{{h: a.ht, s: sa}}, []groupView{{h: b.ht, s: sb}}
	}
	g := pairGroupCount(a, b)
	return a.part.groups(g, a.part.streams(cx)), b.part.groups(g, b.part.streams(cx))
}

// mergeIntersectPair intersects two merge-ready operands through the
// section-6.6 kernel, one group pair at a time, emitting each confirmed member
// once in group-then-hash-ascending order. Above the fan-out floor the group
// loop donates: each task collects its group's member views into its own slot
// (the views alias the frozen slabs, so no copy), and the coordinator emits
// the slots in group order, byte-identical to the sequential walk.
func mergeIntersectPair(cx *shard.Ctx, a, b *set, emit func(m []byte)) {
	cx = fanCx(cx, a, b)
	ga, gb := pairGroups(cx, a, b)
	if cx != nil {
		cols := make([][][]byte, len(ga))
		cx.FanOut(len(ga), func(i int) {
			ha, hb := ga[i].h, gb[i].h
			var col [][]byte
			mergeIntersect(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
				ma := ha.memberByOrd(oa)
				if bytes.Equal(ma, hb.memberByOrd(ob)) {
					col = append(col, ma)
					return true
				}
				return false
			})
			cols[i] = col
		})
		emitCols(cols, emit)
		return
	}
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

// emitCols streams the per-group collected members to emit in group order, the
// join half of a donated group loop.
func emitCols(cols [][][]byte, emit func(m []byte)) {
	for _, col := range cols {
		for _, m := range col {
			emit(m)
		}
	}
}

// mergeIntersectCountPair counts the intersection with SINTERCARD's LIMIT
// early-stop threaded across the groups: each group counts against the limit
// remaining after the groups before it, and the walk stops at the group where
// the limit lands, so LIMIT still short-circuits mid-merge (doc 11 section
// 6.4). limit <= 0 counts everything, and only that unlimited form donates:
// a LIMIT walk's early stop is worth more than the parallelism, so it stays
// sequential on the owner.
func mergeIntersectCountPair(cx *shard.Ctx, a, b *set, limit int) int {
	if limit > 0 {
		cx = nil
	}
	cx = fanCx(cx, a, b)
	ga, gb := pairGroups(cx, a, b)
	if cx != nil {
		cnts := make([]int, len(ga))
		cx.FanOut(len(ga), func(i int) {
			ha, hb := ga[i].h, gb[i].h
			cnts[i] = mergeIntersectCount(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
				return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob))
			}, 0)
		})
		count := 0
		for _, c := range cnts {
			count += c
		}
		return count
	}
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
// at a time, emitting each surviving A member. Above the fan-out floor the
// group loop donates like the intersect's.
func mergeDiffPair(cx *shard.Ctx, a, b *set, emit func(m []byte)) {
	cx = fanCx(cx, a, b)
	ga, gb := pairGroups(cx, a, b)
	if cx != nil {
		cols := make([][][]byte, len(ga))
		cx.FanOut(len(ga), func(i int) {
			ha, hb := ga[i].h, gb[i].h
			var col [][]byte
			mergeDiff(&ga[i].s, &gb[i].s, func(oa, ob uint32) bool {
				return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob))
			}, func(o uint32) { col = append(col, ha.memberByOrd(o)) })
			cols[i] = col
		})
		emitCols(cols, emit)
		return
	}
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
// dedup is the whole dedup, which is also what makes the donated tasks
// independent above the fan-out floor.
func mergeUnionPair(cx *shard.Ctx, a, b *set, emit func(m []byte)) {
	cx = fanCx(cx, a, b)
	ga, gb := pairGroups(cx, a, b)
	if cx != nil {
		cols := make([][][]byte, len(ga))
		cx.FanOut(len(ga), func(i int) {
			ha, hb := ga[i].h, gb[i].h
			var col [][]byte
			mergeUnion(&ga[i].s, &gb[i].s,
				func(oa, ob uint32) bool { return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob)) },
				func(o uint32) { col = append(col, ha.memberByOrd(o)) },
				func(o uint32) { col = append(col, hb.memberByOrd(o)) })
			cols[i] = col
		})
		emitCols(cols, emit)
		return
	}
	for i := range ga {
		ha, hb := ga[i].h, gb[i].h
		mergeUnion(&ga[i].s, &gb[i].s,
			func(oa, ob uint32) bool { return bytes.Equal(ha.memberByOrd(oa), hb.memberByOrd(ob)) },
			func(o uint32) { emit(ha.memberByOrd(o)) },
			func(o uint32) { emit(hb.memberByOrd(o)) })
	}
}
