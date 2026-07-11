package set

import "github.com/tamnd/aki/engine/f3/shard"

// The escalated draw aggregate (spec 2064/f3/11 section 5.4, the F13 fan-out
// half that #614 left to the execution model). An escalated set's count-form
// draws (SPOP key count, SRANDMEMBER key count) split into two phases: the
// owner draws every flat index first, serially, off its own PCG, then the
// index-to-member resolution, which is the DRAM-touching bulk of the work,
// fans across donated workers in contiguous chunks (shard.Ctx.FanOut). The
// resolve is read-only over the frozen set, so it is exactly the donation
// contract; removal (SPOP) and reply encoding stay on the owner, in draw
// order, over the copies the chunks collected.
//
// Uniformity is untouched (F15): the indices are drawn by the same owner PCG
// in the same serial order whether the resolve is fanned or inline, so the
// draw distribution is byte-for-byte the sequential one, which is also the
// oracle the tests hold. The two-level escalated locate (escalate.go) runs
// inside each donated task's at() calls, so the scatter-then-group scan the
// doc hands to workers is what the donees actually execute; the standing
// multi-owner split (each group's span owned by a different worker, section
// 5.4) remains the execution model's box-gated follow-up, and this path is
// the donation form of the same fan-out.
//
// The two-phase pop draws its distinct indices over the initial cardinality
// and then removes, instead of interleaving draw and remove per member. Both
// forms are exact uniform ordered samples without replacement (doc 11 section
// 5.2: each accepted index is uniform over the indices not yet chosen), so
// the chi-squared gate is unchanged; the phases exist so the resolve can fan.

const (
	// drawFanFloor is the count-form draw size at or above which an escalated
	// set's resolve fans out (lab 09, labs/f3/m1/09_donation_live): below it
	// the donation offer and join cost more than the resolve they split, and
	// the flat interleaved path is already microseconds. Frozen from the lab's
	// crossover sweep; the escalated hot key is multi-million members, so the
	// gate-shaped aggregates sit far above it.
	drawFanFloor = 2048

	// drawChunkLen is the draws per donated task: big enough that a task
	// amortizes its claim to a few tens of microseconds of DRAM touches, small
	// enough that a pool's donees all get work at the floor.
	drawChunkLen = 512
)

// fanDraws reports whether a count-form draw over s should ride donation: a
// live runtime to donate through, the set escalated (F13 engaged, so the
// execution model has already judged the owner saturated), and the draw count
// at or above the floor.
func (s *set) fanDraws(cx *shard.Ctx, want int) bool {
	return cx != nil && want >= drawFanFloor && s.enc == encPartitioned && s.part.escalated()
}

// drawChunk is one donated resolve task's output: the chunk's members copied
// back to back in draw order, ends marking each member's end offset. Copies,
// not views, because a pop removes members right after the resolve and a
// swap-remove may relocate the bytes a later view would have read.
type drawChunk struct {
	buf  []byte
	ends []int
}

// resolveFan resolves the drawn flat indices to member copies, fanned across
// donated workers in contiguous chunks, then streams the copies to use in
// draw order. Each task writes only its own chunk slot, and the set is frozen
// for the fan-out's duration by the owner blocking in FanOut, so the tasks
// are the pure reads donation requires.
func resolveFan(cx *shard.Ctx, s *set, idx []uint32, use func(m []byte)) {
	tasks := (len(idx) + drawChunkLen - 1) / drawChunkLen
	chunks := make([]drawChunk, tasks)
	cx.FanOut(tasks, func(t int) {
		lo := t * drawChunkLen
		hi := min(lo+drawChunkLen, len(idx))
		c := drawChunk{ends: make([]int, 0, hi-lo)}
		for _, i := range idx[lo:hi] {
			c.buf = append(c.buf, s.at(int(i), nil)...)
			c.ends = append(c.ends, len(c.buf))
		}
		chunks[t] = c
	})
	for _, c := range chunks {
		p := 0
		for _, e := range c.ends {
			use(c.buf[p:e])
			p = e
		}
	}
}

// drawDistinct draws want distinct flat indices over card, an exact uniform
// ordered sample without replacement (F15): each accepted index is uniform
// over the indices not yet chosen. Small wants relative to the cardinality
// reject repeats against a set (expected O(want) draws); wants past half the
// cardinality partial-shuffle the identity permutation instead, where
// rejection would start thrashing. The indices are drawn serially on the
// owner either way, so the fanned resolve replays the sequential draw
// exactly.
func (g *reg) drawDistinct(card, want int) []uint32 {
	if want*2 >= card {
		idx := g.identityIndex(card)
		for i := 0; i < want; i++ {
			k := i + g.next(card-i)
			idx[i], idx[k] = idx[k], idx[i]
		}
		return idx[:want]
	}
	if cap(g.idxScratch) < want {
		g.idxScratch = make([]uint32, want)
	}
	idx := g.idxScratch[:0]
	seen := make(map[uint32]struct{}, want)
	for len(idx) < want {
		r := uint32(g.next(card))
		if _, dup := seen[r]; dup {
			continue
		}
		seen[r] = struct{}{}
		idx = append(idx, r)
	}
	return idx
}

// popFan is SPOP's escalated aggregate: draw want distinct indices over the
// initial cardinality, fan the resolve, then emit and remove each member on
// the owner in draw order. The caller guarantees want is below the
// cardinality, so the set never empties here.
func popFan(cx *shard.Ctx, g *reg, s *set, want int, emit func(m []byte)) {
	idx := g.drawDistinct(s.card(), want)
	resolveFan(cx, s, idx, func(m []byte) {
		emit(m)
		s.rem(m)
	})
}

// drawFanDistinct is SRANDMEMBER positive count's escalated aggregate: the
// distinct sample's indices drawn on the owner, the resolve fanned. The
// caller guarantees want is below the cardinality.
func drawFanDistinct(cx *shard.Ctx, g *reg, s *set, want int, emit func(m []byte)) {
	resolveFan(cx, s, g.drawDistinct(s.card(), want), emit)
}

// drawFanReplacement is SRANDMEMBER negative count's escalated aggregate:
// want independent uniform indices, repeats allowed, drawn on the owner, the
// resolve fanned.
func drawFanReplacement(cx *shard.Ctx, g *reg, s *set, want int, emit func(m []byte)) {
	if cap(g.idxScratch) < want {
		g.idxScratch = make([]uint32, want)
	}
	idx := g.idxScratch[:want]
	card := s.card()
	for i := range idx {
		idx[i] = uint32(g.next(card))
	}
	resolveFan(cx, s, idx, emit)
}
