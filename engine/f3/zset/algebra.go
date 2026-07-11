package zset

import (
	"bytes"
	"math"
	"sort"

	"github.com/tamnd/aki/engine/f3/store"
)

// The zset algebra kernels (spec 2064/f3/12 section 6.12), settled by
// labs/f3/m2/05_algebra_accum. The lab priced this mechanism, so this slice
// bakes its verdict rather than re-deriving it and ships no new lab:
//
//   - Sort at the end, never maintain a live score-ordered structure during
//     accumulation. Maintain-sorted costs eight to ten times the final sort and
//     loses at every size with collisions; the finding is compute, not cache, so
//     it is platform-independent and needs no gate box. Every kernel here folds
//     by member and sorts the result by (sortable score, member) once, exactly
//     what the STORE bulk load consumes.
//
//   - Merge is the primary union plan, hash the fallback, and the switch is the
//     scratch budget, not timing. Merge's per-source runs scale with total input
//     (about 16 bytes per input pair), so above a byte budget hash is the
//     memory-bounded choice; hash holds a bounded ~35 bytes per result member.
//     union() picks by the budget below.
//
//   - AGGREGATE (SUM, MIN, MAX) and WEIGHTS are aggregation-kernel details, not
//     structural: the lab measured them within 10 percent and they never reorder
//     the structures, so the fold operator is applied inside whichever structure
//     the budget picks (weightScore and aggregate below).
//
// ZINTER is the small-side hash join: drive the smallest source and probe the
// others' member hashes, no ordered access at all. ZDIFF streams the first
// source and probes the rest as a reject filter. ZINTERCARD is ZINTER's probe
// loop counting matches and stopping at LIMIT, allocating nothing.

const (
	// runEntryBytes is the per-input-pair cost of a merge run, the lab's 16-byte
	// figure (a score plus a member reference). union() multiplies it by the total
	// input to size the merge scratch against the budget.
	runEntryBytes = 16

	// mergeRunBudget bounds the merge path's per-source run scratch. Lab 05 froze
	// the rule: merge's run scratch is about 16 bytes times total input pairs and
	// balloons with fan-in times overlap (up to 160 bytes per result member at
	// k=8 equal-overlap), so once total input crosses this budget the driver
	// degrades to the hash accumulator, whose scratch is a bounded ~35 bytes per
	// result member. This is the "degrade to hash when input exceeds the scratch
	// budget" of section 6.12, in bytes.
	mergeRunBudget = 16 << 20
)

// hashMergeTimeCrossover is the pre-registered equal-overlap result cardinality
// where hash overtakes merge on wall-clock time (about 100k on the darwin lab
// box). The driver does NOT switch on time; it switches on the scratch budget
// above. This constant records the gate-box question only: lab 05 leaves the
// exact crossover and how far merge's large-m lead widens to the M2 gate run on
// the Linux box, since both move with the cache hierarchy and core count. The
// gate box confirms it; nothing here reads it.
const hashMergeTimeCrossover = 100_000

// aggMode is the AGGREGATE fold operator over post-weight scores.
type aggMode uint8

const (
	aggSum aggMode = iota // the default: add the weighted scores
	aggMin                // keep the smallest weighted score
	aggMax                // keep the largest weighted score
)

// operand is one source in an algebra command: its zset (nil for a missing key,
// read as empty) and its WEIGHTS multiplier (1 by default, unused by ZDIFF).
type operand struct {
	z      *zset
	weight float64
}

// scoredMember is one aggregated result pair. member aliases source storage for
// the command's duration; a STORE copies it into the destination and a read
// copies it into the reply, so it is never retained past the command.
type scoredMember struct {
	member []byte
	score  float64
}

// weightScore multiplies a raw score by a weight with the Redis quirk pinned
// (section 6.12): a weight of 0 times an infinite score is NaN by IEEE rules,
// and Redis substitutes 0 for that product, so no NaN ever reaches a tree key.
func weightScore(w, s float64) float64 {
	p := w * s
	if math.IsNaN(p) {
		return 0
	}
	return p
}

// aggregate folds one post-weight value into an accumulator under the mode. SUM
// substitutes 0 for a NaN sum (+inf plus -inf), the other pinned Redis quirk;
// MIN and MAX are plain comparisons on the post-weight values, which never
// produce NaN after weightScore has run.
func aggregate(mode aggMode, acc, val float64) float64 {
	switch mode {
	case aggMin:
		if val < acc {
			return val
		}
		return acc
	case aggMax:
		if val > acc {
			return val
		}
		return acc
	default: // aggSum
		s := acc + val
		if math.IsNaN(s) {
			return 0
		}
		return s
	}
}

// sortByScore orders the aggregated result by the zset total order: score
// ascending, ties broken by raw member bytes. It sorts on the sortable score key
// so signed zero and the infinities order exactly as the destination tree keys
// them (codec.go), the once-at-the-end sort the lab froze.
func sortByScore(pairs []scoredMember) {
	sort.Slice(pairs, func(i, j int) bool {
		ki, kj := scoreKey(pairs[i].score), scoreKey(pairs[j].score)
		if ki != kj {
			return ki < kj
		}
		return bytes.Compare(pairs[i].member, pairs[j].member) < 0
	})
}

// totalInput sums the source cardinalities, the upper bound on a union result
// and the merge scratch sizing input.
func totalInput(ops []operand) int {
	n := 0
	for _, o := range ops {
		if o.z != nil {
			n += o.z.card()
		}
	}
	return n
}

// union folds every source member across all sources with weighted aggregation
// and returns the result sorted by score. It picks the accumulation structure by
// the scratch budget (lab 05): merge while the per-source runs fit, hash above
// it. Both structures produce the identical aggregated result, so the choice is
// invisible to the caller and proven interchangeable by test.
func union(ops []operand, mode aggMode) []scoredMember {
	total := totalInput(ops)
	var pairs []scoredMember
	if total*runEntryBytes <= mergeRunBudget {
		pairs = mergeUnion(ops, mode, total)
	} else {
		pairs = hashUnion(ops, mode, total)
	}
	sortByScore(pairs)
	return pairs
}

// hashUnion is the fallback accumulation path: fold every source member into an
// open-addressed member accumulator, then collect. It holds a bounded ~35 bytes
// per result member regardless of fan-in, which is why it is the memory-bounded
// choice above the merge scratch budget.
func hashUnion(ops []operand, mode aggMode, total int) []scoredMember {
	acc := newAccum(total)
	for _, o := range ops {
		if o.z == nil {
			continue
		}
		w := o.weight
		o.z.forEach(func(m []byte, s float64) bool {
			acc.fold(m, weightScore(w, s), mode)
			return true
		})
	}
	return acc.collect()
}

// mergeUnion is the primary accumulation path: one member-sorted run per source
// (the hash slot walk of section 6.12), a k-way merge over the runs folding
// equal members with aggregation. The merge yields members in nondecreasing byte
// order, so equal members arrive consecutively and fold into the last emitted
// pair. The result is member-ordered; union sorts it by score once at the end.
func mergeUnion(ops []operand, mode aggMode, total int) []scoredMember {
	runs := make([][]scoredMember, 0, len(ops))
	for _, o := range ops {
		if o.z == nil || o.z.card() == 0 {
			continue
		}
		run := make([]scoredMember, 0, o.z.card())
		w := o.weight
		o.z.forEach(func(m []byte, s float64) bool {
			run = append(run, scoredMember{member: m, score: weightScore(w, s)})
			return true
		})
		sort.Slice(run, func(i, j int) bool {
			return bytes.Compare(run[i].member, run[j].member) < 0
		})
		runs = append(runs, run)
	}
	if len(runs) == 0 {
		return nil
	}

	// A binary min-heap over the run heads (the lab's implicit tournament tree),
	// keyed by member bytes. idx tracks each run's cursor.
	h := make([]heapNode, len(runs))
	idx := make([]int, len(runs))
	for r := range runs {
		h[r] = heapNode{member: runs[r][0].member, score: runs[r][0].score, run: r}
	}
	for i := len(h)/2 - 1; i >= 0; i-- {
		siftDown(h, i)
	}

	out := make([]scoredMember, 0, total)
	for len(h) > 0 {
		top := h[0]
		if n := len(out); n > 0 && bytes.Equal(out[n-1].member, top.member) {
			out[n-1].score = aggregate(mode, out[n-1].score, top.score)
		} else {
			out = append(out, scoredMember{member: top.member, score: top.score})
		}
		idx[top.run]++
		if idx[top.run] < len(runs[top.run]) {
			e := runs[top.run][idx[top.run]]
			h[0] = heapNode{member: e.member, score: e.score, run: top.run}
		} else {
			h[0] = h[len(h)-1]
			h = h[:len(h)-1]
		}
		if len(h) > 0 {
			siftDown(h, 0)
		}
	}
	return out
}

// heapNode is one run head in the merge min-heap.
type heapNode struct {
	member []byte
	score  float64
	run    int
}

// siftDown restores the min-heap order below i, ordering by member bytes.
func siftDown(h []heapNode, i int) {
	n := len(h)
	for {
		l, r, small := 2*i+1, 2*i+2, i
		if l < n && bytes.Compare(h[l].member, h[small].member) < 0 {
			small = l
		}
		if r < n && bytes.Compare(h[r].member, h[small].member) < 0 {
			small = r
		}
		if small == i {
			return
		}
		h[i], h[small] = h[small], h[i]
		i = small
	}
}

// intersect keeps the members present in every source, aggregating their
// weighted scores in positional source order (so MIN and MAX are deterministic
// and duplicate source keys each contribute), and returns the result sorted by
// score. It drives the smallest source and probes the others' member hashes, the
// small-side hash join of section 6.12: a nil (missing) source empties the
// intersection.
func intersect(ops []operand, mode aggMode) []scoredMember {
	driver, minCard := -1, 0
	for i, o := range ops {
		if o.z == nil {
			return nil
		}
		if c := o.z.card(); driver < 0 || c < minCard {
			driver, minCard = i, c
		}
	}
	if driver < 0 {
		return nil
	}
	var out []scoredMember
	ops[driver].z.forEach(func(m []byte, _ float64) bool {
		acc := 0.0
		for i := range ops {
			s, present := ops[i].z.score(m)
			if !present {
				return true
			}
			val := weightScore(ops[i].weight, s)
			if i == 0 {
				acc = val
			} else {
				acc = aggregate(mode, acc, val)
			}
		}
		out = append(out, scoredMember{member: m, score: acc})
		return true
	})
	sortByScore(out)
	return out
}

// intercard counts the members present in every source, stopping once the count
// reaches limit (limit 0 means unlimited). It is intersect's probe loop with no
// result materialized, so it allocates nothing on the count path (proven by
// test): the LIMIT early-stop is the return-false that ends the walk.
func intercard(ops []operand, limit int) int {
	driver, minCard := -1, 0
	for i, o := range ops {
		if o.z == nil {
			return 0
		}
		if c := o.z.card(); driver < 0 || c < minCard {
			driver, minCard = i, c
		}
	}
	if driver < 0 {
		return 0
	}
	count := 0
	ops[driver].z.forEach(func(m []byte, _ float64) bool {
		for i := range ops {
			if i == driver {
				continue
			}
			if _, present := ops[i].z.score(m); !present {
				return true
			}
		}
		count++
		return limit == 0 || count < limit
	})
	return count
}

// diff keeps the members of the first source not present in any later source,
// carrying the first source's scores (ZDIFF takes no weights), and returns the
// result sorted by score. It streams the first source and probes the rest as a
// reject filter (section 6.12): a missing first source is an empty result.
func diff(ops []operand) []scoredMember {
	if len(ops) == 0 || ops[0].z == nil {
		return nil
	}
	var out []scoredMember
	ops[0].z.forEach(func(m []byte, s float64) bool {
		for i := 1; i < len(ops); i++ {
			if ops[i].z == nil {
				continue
			}
			if _, present := ops[i].z.score(m); present {
				return true
			}
		}
		out = append(out, scoredMember{member: m, score: s})
		return true
	})
	sortByScore(out)
	return out
}

// buildDest builds a fresh destination zset from the aggregated, score-sorted
// pairs, choosing the band from the result's own cardinality and max member
// length (section 4, line 348): a result within the inline caps lands as a
// packed blob, everything else bulk-loads the native tree at the right-edge 0.9
// fill. It returns nil for an empty result, which the caller turns into a
// destination delete. The pairs alias source storage; both build paths copy the
// member bytes into the destination, so the result is independent of its sources
// before place installs it (the no-aliased-clone rule made structural).
func buildDest(pairs []scoredMember) *zset {
	if len(pairs) == 0 {
		return nil
	}
	maxLen := 0
	for _, p := range pairs {
		if len(p.member) > maxLen {
			maxLen = len(p.member)
		}
	}
	if len(pairs) <= maxListpackEntries && maxLen <= maxListpackValue {
		z := newZset()
		for _, p := range pairs {
			z.appendInlineSorted(p.member, p.score)
		}
		return z
	}
	nat := newNativeStore(len(pairs))
	for _, p := range pairs {
		nat.appendSorted(p.member, p.score)
	}
	nat.seal()
	return &zset{enc: encSkiplist, nat: nat}
}

// accEntry is one open-addressed accumulator slot: the member's hash and bytes
// (aliasing source storage) and its running aggregated score.
type accEntry struct {
	hash   uint64
	member []byte
	score  float64
	used   bool
}

// accum is the hash-union accumulator: an open-addressed member-to-score table
// with linear probing, no deletes (a union only folds), doubling at a 3/4 load.
// It holds member references, not copies, so a fold is a hash, a probe, and a
// score update, and the peak scratch is the lab's bounded ~35 bytes per member.
type accum struct {
	slots []accEntry
	mask  uint64
	count int
}

// newAccum sizes the table to hold hint members below the 3/4 load without a
// resize, rounded up to a power of two so mask indexing works.
func newAccum(hint int) *accum {
	size := 8
	for size*3 < hint*4 {
		size <<= 1
	}
	return &accum{slots: make([]accEntry, size), mask: uint64(size - 1)}
}

// fold aggregates one post-weight value for member m: a fresh member seats the
// value, a repeat folds under the mode. The member bytes alias the source and
// stay valid for the command, so the slot keeps the reference.
func (a *accum) fold(m []byte, val float64, mode aggMode) {
	h := store.Hash(m)
	i := h & a.mask
	for a.slots[i].used {
		if a.slots[i].hash == h && bytes.Equal(a.slots[i].member, m) {
			a.slots[i].score = aggregate(mode, a.slots[i].score, val)
			return
		}
		i = (i + 1) & a.mask
	}
	a.slots[i] = accEntry{hash: h, member: m, score: val, used: true}
	a.count++
	if a.count*4 >= len(a.slots)*3 {
		a.grow()
	}
}

// grow doubles the table and reseats every live slot, since linear-probe
// positions move with the mask.
func (a *accum) grow() {
	old := a.slots
	a.slots = make([]accEntry, len(old)*2)
	a.mask = uint64(len(a.slots) - 1)
	for _, e := range old {
		if !e.used {
			continue
		}
		i := e.hash & a.mask
		for a.slots[i].used {
			i = (i + 1) & a.mask
		}
		a.slots[i] = e
	}
}

// collect extracts the accumulated pairs in slot order; union sorts them by
// score afterward.
func (a *accum) collect() []scoredMember {
	out := make([]scoredMember, 0, a.count)
	for i := range a.slots {
		if a.slots[i].used {
			out = append(out, scoredMember{member: a.slots[i].member, score: a.slots[i].score})
		}
	}
	return out
}
