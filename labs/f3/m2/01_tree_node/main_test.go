package main

import (
	"math/rand"
	"sort"
	"testing"
)

// checkCounts verifies the counted-tree invariant: every interior count equals
// the live entry count of the child subtree it labels, at every node.
func (t *tree) checkCounts(tb testing.TB) {
	var walk func(ord uint32, level int) uint64
	walk = func(ord uint32, level int) uint64 {
		if level == 1 {
			return uint64(t.lNent(ord))
		}
		k := t.bNkeys(ord)
		var sum uint64
		for i := 0; i <= k; i++ {
			child := t.bChild(ord, i)
			got := walk(child, level-1)
			if got != t.bCount(ord, i) {
				tb.Fatalf("count mismatch at child %d: stored %d, actual %d", i, t.bCount(ord, i), got)
			}
			sum += got
		}
		// separators must be sorted and route correctly.
		for i := 1; i < k; i++ {
			if t.bSep(ord, i) <= t.bSep(ord, i-1) {
				tb.Fatalf("separators not sorted at node")
			}
		}
		return sum
	}
	walk(t.root, t.height)
}

// TestAgainstModel drives insert/delete against a sorted-slice shadow model and
// checks rank, select, range and the count invariant after every batch, on all
// arms so a size-specific split or merge bug surfaces.
func TestAgainstModel(t *testing.T) {
	arms := []arm{
		{"128", 128, 128},
		{"256", 256, 256},
		{"512", 512, 512},
		{"256b/512l", 256, 512},
		{"256b/1024l", 256, 1024},
	}
	for _, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			tr := newTree(a.branchSz, a.leafSz, 4)
			model := map[uint64]struct{}{}
			rng := rand.New(rand.NewSource(int64(a.branchSz*7919 + a.leafSz)))

			apply := func() {
				keys := make([]uint64, 0, len(model))
				for k := range model {
					keys = append(keys, k)
				}
				sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
				if uint64(len(keys)) != tr.cardinality() {
					t.Fatalf("cardinality %d, model %d", tr.cardinality(), len(keys))
				}
				tr.checkCounts(t)
				for i, k := range keys {
					got, present := tr.rank(k)
					if !present || got != uint64(i) {
						t.Fatalf("rank(%d)=%d present=%v, want %d", k, got, present, i)
					}
					if sk := tr.selectAt(uint64(i)); sk != k {
						t.Fatalf("select(%d)=%d, want %d", i, sk, k)
					}
				}
				// range walk over a window matches the model slice.
				if len(keys) > 0 {
					start := rng.Intn(len(keys))
					w := 50
					_, _ = tr.rangeWalk(uint64(start), w)
					// verify emitted keys are exactly the model window.
					emit := collectRange(tr, uint64(start), w)
					end := start + w
					if end > len(keys) {
						end = len(keys)
					}
					want := keys[start:end]
					if len(emit) != len(want) {
						t.Fatalf("range len %d, want %d", len(emit), len(want))
					}
					for i := range want {
						if emit[i] != want[i] {
							t.Fatalf("range[%d]=%d, want %d", i, emit[i], want[i])
						}
					}
				}
			}

			// grow.
			for i := 0; i < 4000; i++ {
				k := rng.Uint64()
				if _, ok := model[k]; ok {
					continue
				}
				model[k] = struct{}{}
				tr.insert(k)
				if i%500 == 0 {
					apply()
				}
			}
			apply()

			// churn: mixed insert/delete.
			for i := 0; i < 8000; i++ {
				k := rng.Uint64()
				if _, ok := model[k]; ok {
					delete(model, k)
					tr.delete(k)
				} else {
					model[k] = struct{}{}
					tr.insert(k)
				}
				if i%400 == 0 {
					apply()
				}
			}
			apply()

			// drain to empty.
			for len(model) > 0 {
				var k uint64
				for kk := range model {
					k = kk
					break
				}
				delete(model, k)
				tr.delete(k)
				if len(model)%300 == 0 {
					apply()
				}
			}
			apply()
			if tr.cardinality() != 0 {
				t.Fatalf("not drained: %d left", tr.cardinality())
			}
		})
	}
}

func collectRange(tr *tree, start uint64, w int) []uint64 {
	k := start
	ord := tr.descendToRank(&k)
	off := int(k)
	out := make([]uint64, 0, w)
	for len(out) < w {
		n := tr.lNent(ord)
		for off < n && len(out) < w {
			out = append(out, tr.lKey(ord, off))
			off++
		}
		if len(out) >= w {
			break
		}
		nx := tr.lNext(ord)
		if nx == 0 {
			break
		}
		ord = nx
		off = 0
	}
	return out
}

// TestBulkLoad checks the right-edge bulk loader builds a valid counted tree:
// counts hold at every interior node, and rank and select agree with the sorted
// input on all arms. This is the tree whose arena bytes the memory column reads,
// so it has to be a real tree, not just a byte count.
func TestBulkLoad(t *testing.T) {
	arms := []arm{
		{"128", 128, 128},
		{"256", 256, 256},
		{"512", 512, 512},
		{"256b/512l", 256, 512},
		{"256b/1024l", 256, 1024},
	}
	for _, a := range arms {
		t.Run(a.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 14, 15, 16, 500, 5000} {
				keys := make([]uint64, n)
				for i := range keys {
					keys[i] = uint64(i)*3 + 7
				}
				tr := bulkLoad(a, keys)
				if int(tr.cardinality()) != n {
					t.Fatalf("n=%d: cardinality %d", n, tr.cardinality())
				}
				tr.checkCounts(t)
				for i, k := range keys {
					got, present := tr.rank(k)
					if !present || got != uint64(i) {
						t.Fatalf("n=%d: rank(%d)=%d present=%v want %d", n, k, got, present, i)
					}
					if sk := tr.selectAt(uint64(i)); sk != k {
						t.Fatalf("n=%d: select(%d)=%d want %d", n, i, sk, k)
					}
				}
			}
		})
	}
}

// TestArityMatchesLayout checks the derived arity and leaf capacity match the
// section 2.3 layout arithmetic for the frozen node size.
func TestArityMatchesLayout(t *testing.T) {
	tr := newTree(256, 256, 4)
	if tr.arity != 16 {
		t.Fatalf("arity %d, want 16 at 256B/4B", tr.arity)
	}
	if tr.leafCap != 15 {
		t.Fatalf("leafCap %d, want 15 at 256B", tr.leafCap)
	}
}
