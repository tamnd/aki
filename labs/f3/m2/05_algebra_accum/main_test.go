package main

import (
	"math"
	"sort"
	"testing"
)

// reference accumulates the union with a plain Go map, the oracle every kernel
// must match: same members, same aggregated score, same (sortable score, member)
// order.
func reference(srcs []*source, mode aggMode) []respair {
	agg := map[uint64]float64{}
	for s, src := range srcs {
		_ = s
		for i, id := range src.ids {
			w := src.weight * src.scores[i]
			if cur, ok := agg[id]; ok {
				agg[id] = combine(mode, cur, w)
			} else {
				agg[id] = w
			}
		}
	}
	out := make([]respair, 0, len(agg))
	for id, sc := range agg {
		out = append(out, respair{key: sortableScore(sc), id: id})
	}
	sort.Slice(out, func(i, j int) bool { return lessPair(out[i], out[j]) })
	return out
}

// linearMerge is an independent k-way merge by linear cursor scan, a second
// oracle for the heap merge so a heap bug cannot hide behind the map reference.
func linearMerge(srcs []*source, mode aggMode) []respair {
	runs := buildRuns(srcs)
	pos := make([]int, len(runs))
	out := []respair{}
	haveCur := false
	var curID uint64
	var curScr float64
	for {
		best := -1
		var bestID uint64
		for s := range runs {
			if pos[s] >= len(runs[s]) {
				continue
			}
			if best == -1 || runs[s][pos[s]].id < bestID {
				best = s
				bestID = runs[s][pos[s]].id
			}
		}
		if best == -1 {
			break
		}
		e := runs[best][pos[best]]
		pos[best]++
		if haveCur && e.id == curID {
			curScr = combine(mode, curScr, e.scr)
			continue
		}
		if haveCur {
			out = append(out, respair{key: sortableScore(curScr), id: curID})
		}
		haveCur, curID, curScr = true, e.id, e.scr
	}
	if haveCur {
		out = append(out, respair{key: sortableScore(curScr), id: curID})
	}
	sortPairs(out)
	return out
}

// TestKernelsAgree is the correctness bar: whatever accumulation structure the
// slice-7 driver picks, hash, merge, and tree must all produce the same
// aggregated, score-ordered result, across shapes, fan-in, aggregate forms, and
// weights.
func TestKernelsAgree(t *testing.T) {
	for _, equal := range []bool{true, false} {
		for _, weighted := range []bool{false, true} {
			for _, k := range []int{2, 3, 4, 8} {
				for _, card := range []int{1, 10, 500, 3000} {
					for _, mode := range []aggMode{aggSum, aggMin, aggMax} {
						srcs := buildSources(k, card, equal, weighted, 16)
						ref := reference(srcs, mode)
						if got := hashAccum(srcs, mode); !pairsEqual(got, ref) {
							t.Fatalf("hash mismatch equal=%v w=%v k=%d card=%d %s", equal, weighted, k, card, mode)
						}
						if got := mergeAccum(srcs, mode); !pairsEqual(got, ref) {
							t.Fatalf("merge mismatch equal=%v w=%v k=%d card=%d %s", equal, weighted, k, card, mode)
						}
						if got := linearMerge(srcs, mode); !pairsEqual(got, ref) {
							t.Fatalf("linearMerge mismatch equal=%v w=%v k=%d card=%d %s", equal, weighted, k, card, mode)
						}
						if got := treeAccum(srcs, mode); !pairsEqual(got, ref) {
							t.Fatalf("tree mismatch equal=%v w=%v k=%d card=%d %s", equal, weighted, k, card, mode)
						}
					}
				}
			}
		}
	}
}

// TestSortableScoreMonotonic checks the section 3.1 transform is order preserving
// across the special values, since the whole point is that a sort on the
// sortable key equals zset score order for the destination bulk load.
func TestSortableScoreMonotonic(t *testing.T) {
	vals := []float64{
		math.Inf(-1), -1e300, -1000, -1, -0.5, math.Copysign(0, -1), 0, 0.5, 1, 1000, 1e300, math.Inf(1),
	}
	for i := 1; i < len(vals); i++ {
		a, b := sortableScore(vals[i-1]), sortableScore(vals[i])
		if vals[i-1] == vals[i] {
			if a != b {
				t.Fatalf("equal scores %v %v gave different keys", vals[i-1], vals[i])
			}
			continue
		}
		if a >= b {
			t.Fatalf("score %v < %v but key %d >= %d", vals[i-1], vals[i], a, b)
		}
	}
}

// TestAVLInvariant checks the maintain-sorted tree stays balanced and its
// in-order walk is sorted after a churn of delete-reinserts, so the sort-tax
// timing measures a correct structure.
func TestAVLInvariant(t *testing.T) {
	tr := newAVL(0)
	present := map[uint64]uint64{} // id -> current key
	for step := 0; step < 4000; step++ {
		id := uint64(mix(uint64(step)) % 400)
		key := mix(uint64(step)*2654435761) % 1000
		if old, ok := present[id]; ok {
			tr.root = tr.delete(tr.root, old, id)
		}
		tr.root = tr.insert(tr.root, key, id)
		present[id] = key
		if bad := checkBalanced(tr, tr.root); bad {
			t.Fatalf("unbalanced after step %d", step)
		}
	}
	walk := tr.inorder(nil)
	if len(walk) != len(present) {
		t.Fatalf("walk %d entries, present %d", len(walk), len(present))
	}
	for i := 1; i < len(walk); i++ {
		if !lessPair(walk[i-1], walk[i]) {
			t.Fatalf("in-order walk not sorted at %d", i)
		}
	}
}

func checkBalanced(t *avl, n int32) bool {
	if n == nilNode {
		return false
	}
	b := t.balance(n)
	if b < -1 || b > 1 {
		return true
	}
	return checkBalanced(t, t.nodes[n].left) || checkBalanced(t, t.nodes[n].right)
}

// TestResultCard checks the union cardinality bookkeeping the sweep normalizes
// by, so a bad m cannot silently rescale every per-member number.
func TestResultCard(t *testing.T) {
	if got := resultCard(4, 1000, true); got != 1000 {
		t.Fatalf("equal-overlap union card = %d, want 1000", got)
	}
	if got := resultCard(4, 1000, false); got != 4000 {
		t.Fatalf("disjoint union card = %d, want 4000", got)
	}
	srcs := buildSources(4, 1000, false, false, 16)
	if got := len(hashAccum(srcs, aggSum)); got != 4000 {
		t.Fatalf("disjoint hash produced %d members, want 4000", got)
	}
}
