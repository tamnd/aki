package structs

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// TestDeleteAtModel drives a grow-then-churn schedule and, at every step, deletes
// the entry at a random rank, checks the returned (score, member) is the model's
// entry at that rank, and holds the tree to a valid counted B+ tree (tr.Check)
// with an honest count and leaf chain. This is the fuzz exit for the rank-routed
// window removal the ZREMRANGEBY* surgery loops over (spec 2064/f3/12 section
// 6.9): the descent routes on the counts alone, so the model match proves the
// route lands the intended rank and Check proves the count fixup and rebalance
// leave the structure whole after every delete.
func TestDeleteAtModel(t *testing.T) {
	rng := rand.New(rand.NewSource(0x616))
	ms := newMemStore()
	tr := NewTree()
	present := map[key]struct{}{}

	randKey := func() key {
		return key{score: uint64(rng.Intn(8)), member: fmt.Sprintf("m%d", rng.Intn(2000))}
	}
	sortedModel := func() []key {
		model := make([]key, 0, len(present))
		for k := range present {
			model = append(model, k)
		}
		sort.Slice(model, func(i, j int) bool { return lessKey(model[i], model[j]) })
		return model
	}
	insertBatch := func(n int) {
		for i := 0; i < n; i++ {
			k := randKey()
			if _, has := present[k]; has {
				continue
			}
			tr.Insert(k.score, []byte(k.member), ms.ref(k.member), ms)
			present[k] = struct{}{}
		}
	}
	deleteAt := func(rank int) {
		model := sortedModel()
		want := model[rank]
		score, ref, ok := tr.DeleteAt(uint64(rank))
		if !ok {
			t.Fatalf("DeleteAt(%d) on tree of %d returned ok=false", rank, len(model))
		}
		if score != want.score || string(ms.Member(ref)) != want.member {
			t.Fatalf("DeleteAt(%d)=(%d,%q), want %v", rank, score, ms.Member(ref), want)
		}
		delete(present, want)
	}

	for b := 0; b < 25; b++ {
		insertBatch(500)
		for i := 0; i < 120 && len(present) > 0; i++ {
			deleteAt(rng.Intn(len(present)))
		}
		if err := tr.Check(ms); err != nil {
			t.Fatalf("batch %d: invariant broken after DeleteAt: %v", b, err)
		}
		if tr.Len() != len(present) {
			t.Fatalf("batch %d: Len %d, model %d", b, tr.Len(), len(present))
		}
	}

	// Drain the rest one rank at a time, checking the model entry every step and the
	// invariants periodically, down to an empty single-leaf tree.
	for len(present) > 0 {
		deleteAt(rng.Intn(len(present)))
		if len(present)%97 == 0 {
			if err := tr.Check(ms); err != nil {
				t.Fatalf("drain at %d: invariant broken: %v", len(present), err)
			}
		}
	}
	if _, _, ok := tr.DeleteAt(0); ok {
		t.Fatal("DeleteAt(0) on empty tree returned ok=true")
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("drained tree invalid: %v", err)
	}
	if tr.Len() != 0 || tr.height != 1 {
		t.Fatalf("drained tree Len %d height %d, want 0 and 1", tr.Len(), tr.height)
	}
}

// TestDeleteAtBounds pins the out-of-range guard: any rank at or past the entry
// count leaves the tree untouched and reports ok=false, the empty-window contract
// the removeRange loop stops on.
func TestDeleteAtBounds(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	for i := 0; i < 50; i++ {
		m := fmt.Sprintf("m%02d", i)
		tr.Insert(uint64(i), []byte(m), ms.ref(m), ms)
	}
	for _, r := range []uint64{50, 51, 1000} {
		if _, _, ok := tr.DeleteAt(r); ok {
			t.Fatalf("DeleteAt(%d) on tree of 50 returned ok=true", r)
		}
	}
	if tr.Len() != 50 {
		t.Fatalf("Len %d after out-of-range deletes, want 50", tr.Len())
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("invariant broken by out-of-range deletes: %v", err)
	}
}
