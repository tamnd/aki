package set

import (
	"slices"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// The set-algebra driver (spec 2064/f3/11 section 6.4): for each command it
// chooses between the probe path (iterate the smaller operand, probe the larger
// operand's member table) and the merge path (both operands indexed, stream the
// sorted-hash arrays through the merge.go kernels). The choice is per operand
// pair, by the pre-registered crossover and the merge floor.
//
// Probe is the always-correct baseline: it needs nothing but has() on the other
// operands, so it works over every band (intset, listpack, native, partitioned)
// and works with SetAlgebraMaintain off, when no set is ever indexed. The merge
// path is an optimization that only fires when both operands carry the
// maintained arrays (the flag is on and both cleared the floor), their band
// shapes match (flat with flat, partitioned with partitioned; fanout.go runs
// the partitioned pairs group by group), and their size ratio sits below the
// crossover. This keeps the dispatcher correct in both flag states: off, the
// kernels are simply unavailable and probe always runs; on, comparable large
// pairs take the merge lever the gate box proves (doc 11 section 6.5).

const (
	// mergeCrossoverK is the |big|/|small| cardinality ratio at which probing the
	// small operand into the big operand's table beats streaming both as a merge
	// (lab 03, labs/f3/m1/03_merge_probe, pre-registered from K16, confirmed for
	// the DRAM regime and bracketed 7-12). At or above it probe wins; below it,
	// and above the floor, merge wins. It is perProbe/perMergeElem-1 evaluated at
	// the 40ns DRAM probe (doc 11 line 393) and a 5ns merge element (K16).
	mergeCrossoverK = 7

	// largeMemberBytes is the average-member size at or above which the crossover
	// inverts under high overlap: a probe then pays scattered DRAM confirms that
	// the sequential merge amortizes, so lab 03 says bias toward merge for large
	// members. Overlap is unknown before the command runs, so member size is the
	// usable signal (lab 03, the large-member caveat).
	largeMemberBytes = 32

	// largeMemberCrossoverK is the raised crossover the driver uses when the
	// smaller operand's members are large: it keeps the pair on the merge (and its
	// galloping advance) well past k=7, since merge is never much worse there and
	// wins outright at high overlap (lab 03: measured crossover 32 at overlap 0.5
	// and 64 at overlap 0.9 for 64-byte members).
	largeMemberCrossoverK = 64
)

// mergeable reports whether a set carries the maintained sorted-hash arrays the
// merge kernels stream: the native band with an engaged index, or the
// partitioned band with every populated partition indexed (fanout.go). Either
// only happens under SetAlgebraMaintain once the members cleared the floor
// (algebra.go).
func (s *set) mergeable() bool {
	if s == nil {
		return false
	}
	switch s.enc {
	case encHashtable:
		return s.ht.indexed()
	case encPartitioned:
		return s.part.indexed()
	}
	return false
}

// mergeablePair reports whether two operands can take the merge path together:
// both mergeable and the same band shape, so the group views line up (flat
// with flat, partitioned with partitioned; fanout.go). A mixed flat and
// partitioned pair falls to probe, the recorded cross-shape deferral.
func mergeablePair(a, b *set) bool {
	return a.mergeable() && b.mergeable() && a.enc == b.enc
}

// avgMemberBytes is the mean live member size, the large-member signal the
// crossover bias reads (lab 03). It is defined only for the bands that reach
// the merge path; the inline bands never do.
func (s *set) avgMemberBytes() int {
	if s.card() == 0 {
		return 0
	}
	switch s.enc {
	case encHashtable:
		return (len(s.ht.slab) - s.ht.dead) / s.card()
	case encPartitioned:
		live := 0
		for _, h := range s.part.parts {
			live += len(h.slab) - h.dead
		}
		return live / s.card()
	}
	return 0
}

// chooseMergeIntersect decides probe versus merge for one intersection pair,
// small being the smaller operand. Merge needs both operands indexed and the
// smaller past the floor; then it wins while the ratio stays under the crossover
// (raised for large members). Everything else, including either operand inline
// or the flag off, falls to probe.
func chooseMergeIntersect(small, large *set) bool {
	if !mergeablePair(small, large) {
		return false
	}
	if small.card() < algebraFloor {
		return false
	}
	k := mergeCrossoverK
	if small.avgMemberBytes() >= largeMemberBytes {
		k = largeMemberCrossoverK
	}
	return large.card() < small.card()*k
}

// chooseMergeDiff decides probe versus merge for A minus B. Diff always walks A,
// so the merge pays only when B is comparable to A (a small excluder is cheaper
// to probe); the crossover gates the max/min ratio, raised for large members on
// the driving operand.
func chooseMergeDiff(a, b *set) bool {
	if !mergeablePair(a, b) {
		return false
	}
	if a.card() < algebraFloor || b.card() < algebraFloor {
		return false
	}
	hi, lo := a.card(), b.card()
	if lo > hi {
		hi, lo = lo, hi
	}
	k := mergeCrossoverK
	if a.avgMemberBytes() >= largeMemberBytes {
		k = largeMemberCrossoverK
	}
	return hi < lo*k
}

// chooseMergeUnion decides probe versus merge for a union pair. A union must emit
// every distinct member of both operands, so the size ratio buys nothing (there
// is no small side to probe cheaply); the merge just needs both operands indexed
// and past the floor, and it then dedups the pair in one sequential pass with no
// transient table.
func chooseMergeUnion(a, b *set) bool {
	if !mergeablePair(a, b) {
		return false
	}
	return min(a.card(), b.card()) >= algebraFloor
}

// byCardAsc orders operands by ascending cardinality, the SINTER/SINTERCARD
// drive order (Redis intersects starting from the smallest set).
func byCardAsc(a, b *set) int { return a.card() - b.card() }

// sinter emits the intersection of every operand. A missing or empty operand
// makes the whole result empty (Redis: a missing key is an empty set). The
// operands are ordered by ascending cardinality so the smallest drives; the
// two-operand indexed case takes the merge lever, everything else probes the
// smallest's members against the rest.
func sinter(cx *shard.Ctx, sets []*set, emit func(m []byte)) {
	for _, s := range sets {
		if s == nil || s.card() == 0 {
			return
		}
	}
	order := slices.Clone(sets)
	slices.SortFunc(order, byCardAsc)
	small := order[0]
	rest := order[1:]
	if len(order) == 2 && chooseMergeIntersect(small, order[1]) {
		mergeIntersectPair(cx, small, order[1], emit)
		return
	}
	small.each(func(m []byte) {
		for _, o := range rest {
			if !o.has(m) {
				return
			}
		}
		emit(m)
	})
}

// sintercard counts the intersection with SINTERCARD's LIMIT early-stop: a
// positive limit stops the count the moment it is reached, limit 0 means
// unlimited (Redis). The merge path threads the limit through
// mergeIntersectCount; the probe path stops its own smallest-drive walk the same
// way.
func sintercard(cx *shard.Ctx, sets []*set, limit int) int {
	for _, s := range sets {
		if s == nil || s.card() == 0 {
			return 0
		}
	}
	order := slices.Clone(sets)
	slices.SortFunc(order, byCardAsc)
	small := order[0]
	rest := order[1:]
	if len(order) == 2 && chooseMergeIntersect(small, order[1]) {
		return mergeIntersectCountPair(cx, small, order[1], limit)
	}
	count := 0
	small.eachUntil(func(m []byte) bool {
		for _, o := range rest {
			if !o.has(m) {
				return true // not in the intersection, keep scanning
			}
		}
		count++
		if limit > 0 && count >= limit {
			return false // LIMIT reached, stop early
		}
		return true
	})
	return count
}

// sdiff emits the members of the first operand not present in any later operand
// (Redis SDIFF). The first operand drives, so its members carry the reply; a
// missing first key is an empty result and a missing later key excludes nothing.
func sdiff(cx *shard.Ctx, sets []*set, emit func(m []byte)) {
	first := sets[0]
	if first == nil || first.card() == 0 {
		return
	}
	rest := sets[1:]
	if len(sets) == 2 && chooseMergeDiff(first, sets[1]) {
		mergeDiffPair(cx, first, sets[1], emit)
		return
	}
	first.each(func(m []byte) {
		for _, o := range rest {
			if o != nil && o.has(m) {
				return
			}
		}
		emit(m)
	})
}

// sunion emits the distinct union of every operand (Redis SUNION). Missing keys
// contribute nothing. Two indexed operands dedup in one merge pass; otherwise a
// transient member table is the dedup, exactly the doc's "the result table is the
// dedup" with no separate seen-set (doc 11 section 6.4, the setunionstore lab).
func sunion(cx *shard.Ctx, sets []*set, emit func(m []byte)) {
	live := make([]*set, 0, len(sets))
	total := 0
	for _, s := range sets {
		if s != nil && s.card() > 0 {
			live = append(live, s)
			total += s.card()
		}
	}
	switch len(live) {
	case 0:
		return
	case 1:
		live[0].each(emit)
		return
	case 2:
		if chooseMergeUnion(live[0], live[1]) {
			mergeUnionPair(cx, live[0], live[1], emit)
			return
		}
	}
	dst := newHashtable(total)
	for _, s := range live {
		s.each(func(m []byte) { dst.addRaw(m) })
	}
	dst.each(emit)
}
