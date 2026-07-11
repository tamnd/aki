package main

import (
	"sort"
	"testing"

	structs "github.com/tamnd/aki/engine/f3/struct"
)

// buildScores returns n distinct uniform scores and their sorted copy.
func buildScores(n int) (scores, sorted []uint64) {
	kg := &keygen{next: 1 << 48}
	scores = make([]uint64, n)
	for i := range scores {
		scores[i] = kg.key()
	}
	sorted = make([]uint64, n)
	copy(sorted, scores)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return scores, sorted
}

// TestPopArmsAgree drains half the tree through the naive and the cached-run
// kernels and checks both emit exactly the ascending prefix of the sorted keys,
// with the tree invariants intact after each drain.
func TestPopArmsAgree(t *testing.T) {
	const n = 3000
	scores, sorted := buildScores(n)
	ents := sortedEntries(scores)

	drain := func(pop func(tr *structs.Tree) uint64) []uint64 {
		tr := structs.BulkLoad(ents)
		out := make([]uint64, 0, n/2)
		for i := 0; i < n/2; i++ {
			out = append(out, pop(tr))
		}
		if err := tr.Check(noMembers{}); err != nil {
			t.Fatalf("tree invariant after drain: %v", err)
		}
		if tr.Len() != n-n/2 {
			t.Fatalf("cardinality after drain: %d, want %d", tr.Len(), n-n/2)
		}
		return out
	}

	got := drain(popMin)
	run := runCache{k: 31}
	cached := drain(run.pop)
	for i := 0; i < n/2; i++ {
		if got[i] != sorted[i] {
			t.Fatalf("naive pop %d: %d, want %d", i, got[i], sorted[i])
		}
		if cached[i] != sorted[i] {
			t.Fatalf("cached pop %d: %d, want %d", i, cached[i], sorted[i])
		}
	}
}

// TestPopMaxDrains checks the max side emits the descending suffix.
func TestPopMaxDrains(t *testing.T) {
	const n = 2000
	scores, sorted := buildScores(n)
	tr := structs.BulkLoad(sortedEntries(scores))
	for i := 0; i < n/2; i++ {
		if got, want := popMax(tr), sorted[n-1-i]; got != want {
			t.Fatalf("popmax %d: %d, want %d", i, got, want)
		}
	}
	if err := tr.Check(noMembers{}); err != nil {
		t.Fatalf("tree invariant after max drain: %v", err)
	}
}

// TestCacheStaysExactUnderChurn interleaves pops with edge and non-edge inserts
// and deletes under the absorb policy, checking every pop against a sorted-slice
// model, so the run cache's exactness invariant is proven, not assumed.
func TestCacheStaysExactUnderChurn(t *testing.T) {
	const n = 2000
	const ops = 10000
	scores, sorted := buildScores(n)
	tr := structs.BulkLoad(sortedEntries(scores))
	model := append([]uint64(nil), sorted...)
	cache := runCache{k: 31}
	kg := &keygen{next: 1 << 52}
	rng := xorshift(0x9e3779b97f4a7c15)
	var ref uint32 = 1 << 30

	for i := 0; i < ops; i++ {
		if len(model) < n/2 {
			break
		}
		switch rng.next() % 4 {
		case 0: // pop
			got := cache.pop(tr)
			if got != model[0] {
				t.Fatalf("op %d: pop %d, want model min %d", i, got, model[0])
			}
			model = model[1:]
		case 1: // uniform insert, occasionally forced below the cache bound
			key := kg.key()
			if rng.next()%4 == 0 && len(model) > 31 {
				// Land inside the cached prefix: strictly between two live keys.
				a, b := model[2], model[3]
				if b > a+1 {
					key = a + 1 + rng.next()%(b-a-1)
				}
			}
			pos := sort.Search(len(model), func(j int) bool { return model[j] >= key })
			if pos < len(model) && model[pos] == key {
				continue // rare synthetic collision, skip
			}
			ref++
			tr.Insert(key, nil, ref, noMembers{})
			cache.observeInsert(key, ref, true)
			model = append(model, 0)
			copy(model[pos+1:], model[pos:])
			model[pos] = key
		default: // delete a random live key, edge ones included
			j := int(rng.next() % uint64(len(model)))
			key := model[j]
			if _, ok := tr.Delete(key, nil, noMembers{}); !ok {
				t.Fatalf("op %d: live key %d not deletable", i, key)
			}
			cache.observeDelete(key, true)
			model = append(model[:j], model[j+1:]...)
		}
	}
	if err := tr.Check(noMembers{}); err != nil {
		t.Fatalf("tree invariant after churn: %v", err)
	}
	if tr.Len() != len(model) {
		t.Fatalf("cardinality %d, want %d", tr.Len(), len(model))
	}
}

// TestAdversarialAbsorb alternates a descending insert with a pop: every insert
// is the new minimum, the absorb policy must hand it straight back.
func TestAdversarialAbsorb(t *testing.T) {
	const n = 1000
	base := uint64(1) << 40
	ents := make([]structs.Entry, n)
	for i := range ents {
		ents[i] = structs.Entry{Score: base + uint64(i), Ref: uint32(i)}
	}
	tr := structs.BulkLoad(ents)
	cache := runCache{k: 31}
	next := base
	var ref uint32 = 1 << 30
	for i := 0; i < 500; i++ {
		next--
		ref++
		tr.Insert(next, nil, ref, noMembers{})
		cache.observeInsert(next, ref, true)
		if got := cache.pop(tr); got != next {
			t.Fatalf("adversarial pop %d: %d, want %d", i, got, next)
		}
	}
	if err := tr.Check(noMembers{}); err != nil {
		t.Fatalf("tree invariant after adversarial churn: %v", err)
	}
	if tr.Len() != n {
		t.Fatalf("cardinality %d, want %d", tr.Len(), n)
	}
}

// TestInterleavePoliciesAgree runs the harness's own interleave loop at a small
// shape under all three policies and checks they see the identical schedule.
func TestInterleavePoliciesAgree(t *testing.T) {
	scores, _ := buildScores(4000)
	ents := sortedEntries(scores)
	var miss [3]int
	for i, pol := range []policy{naive, invalidate, absorb} {
		kg := &keygen{next: 1 << 56}
		miss[i] = runInterleave(ents, 8000, 50, false, pol, kg, 31).misses
	}
	if miss[0] != miss[1] || miss[1] != miss[2] {
		t.Fatalf("policies diverged: misses %v", miss)
	}
}
