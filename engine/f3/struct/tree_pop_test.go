package structs

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// The fused-pop tests (spec 2064/f3/12 section 6.7, lab 04 labs/f3/m2). They
// hold the five bars the lab froze for this slice: model-checked pops with
// Check after churn, an adversarial insert-below-the-min then pop, ZMPOP-style
// counts crossing leaf boundaries with the counts and links intact, drain to
// empty then refill and pop again, and cache-off versus cache-on producing
// byte-identical trees on the same schedule.

// TestPopMinMaxModel drives a grow-then-churn schedule and, at every phase, pops
// from an end and checks the popped key is the model extreme and the tree stays a
// valid counted B+ tree (tr.Check) with an honest count and leaf chain.
func TestPopMinMaxModel(t *testing.T) {
	rng := rand.New(rand.NewSource(0x709))
	ms := newMemStore()
	tr := NewTree()
	present := map[key]struct{}{}

	randKey := func() key {
		return key{score: uint64(rng.Intn(8)), member: fmt.Sprintf("m%d", rng.Intn(1500))}
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

	popMin := func() key {
		model := sortedModel()
		score, ref, ok := tr.PopMin()
		if !ok {
			t.Fatal("PopMin on non-empty tree returned ok=false")
		}
		want := model[0]
		if score != want.score || string(ms.Member(ref)) != want.member {
			t.Fatalf("PopMin=(%d,%q), want %v", score, ms.Member(ref), want)
		}
		delete(present, want)
		return want
	}
	popMax := func() key {
		model := sortedModel()
		score, ref, ok := tr.PopMax()
		if !ok {
			t.Fatal("PopMax on non-empty tree returned ok=false")
		}
		want := model[len(model)-1]
		if score != want.score || string(ms.Member(ref)) != want.member {
			t.Fatalf("PopMax=(%d,%q), want %v", score, ms.Member(ref), want)
		}
		delete(present, want)
		return want
	}

	for b := 0; b < 20; b++ {
		insertBatch(400)
		for i := 0; i < 90; i++ {
			if i%2 == 0 {
				popMin()
			} else {
				popMax()
			}
		}
		if err := tr.Check(ms); err != nil {
			t.Fatalf("batch %d: invariant broken after pops: %v", b, err)
		}
		if tr.Len() != len(present) {
			t.Fatalf("batch %d: Len %d, model %d", b, tr.Len(), len(present))
		}
	}

	// Drain the rest a pop at a time, alternating ends, checking the model extreme
	// every step and the invariants periodically.
	for len(present) > 0 {
		if len(present)%2 == 0 {
			popMin()
		} else {
			popMax()
		}
		if len(present)%101 == 0 {
			if err := tr.Check(ms); err != nil {
				t.Fatalf("drain at %d: invariant broken: %v", len(present), err)
			}
		}
	}
	if _, _, ok := tr.PopMin(); ok {
		t.Fatal("PopMin on empty tree returned ok=true")
	}
	if _, _, ok := tr.PopMax(); ok {
		t.Fatal("PopMax on empty tree returned ok=true")
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("drained tree invalid: %v", err)
	}
	if tr.Len() != 0 || tr.height != 1 {
		t.Fatalf("drained tree Len %d height %d, want 0 and 1", tr.Len(), tr.height)
	}
}

// TestPopAdversarialInsertBelowMin is the lab's adversarial trap: pop the min to
// a known key, insert a fresh key strictly below every present key, then pop the
// min again and require the fresh key back. It catches a pop that cached a stale
// edge entry instead of re-reading the leaf.
func TestPopAdversarialInsertBelowMin(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	// Seat a spread of keys at scores 100.. so there is always room below.
	for i := 0; i < 2000; i++ {
		m := fmt.Sprintf("m%d", i)
		tr.Insert(uint64(100+i), []byte(m), ms.ref(m), ms)
	}
	for round := 0; round < 50; round++ {
		score, _, ok := tr.PopMin()
		if !ok {
			t.Fatal("PopMin returned ok=false mid-test")
		}
		// Insert a key below the current minimum: score 0 is below every remaining
		// key (all at 100+). A distinct member each round.
		fresh := fmt.Sprintf("below%d", round)
		tr.Insert(uint64(round), []byte(fresh), ms.ref(fresh), ms)
		gotScore, gotRef, ok := tr.PopMin()
		if !ok {
			t.Fatal("PopMin after below-insert returned ok=false")
		}
		if gotScore != uint64(round) || string(ms.Member(gotRef)) != fresh {
			t.Fatalf("round %d: PopMin=(%d,%q), want (%d,%q); prev pop was %d",
				round, gotScore, ms.Member(gotRef), round, fresh, score)
		}
	}
	if err := tr.Check(ms); err != nil {
		t.Fatalf("invariant broken: %v", err)
	}
}

// TestPopNCrossLeafBoundaries pops counts that span many leaves (the ZMPOP drain)
// and checks the emitted run is exactly the model's front (or back) in order and
// the tree stays valid, so the per-step count fixup and leaf-chain relinking hold
// across every boundary the drain crosses.
func TestPopNCrossLeafBoundaries(t *testing.T) {
	const n = 5000
	for _, min := range []bool{true, false} {
		scores := distinctScores(n, 0x515)
		sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
		tr := NewTree()
		for i, s := range scores {
			tr.Insert(s, nil, uint32(i), nilMembers{})
		}
		// Pop in batches of 137, well over the 31-entry leaf, so every batch
		// crosses several leaf boundaries.
		popped := 0
		for tr.Len() > 0 {
			batch := 137
			if batch > tr.Len() {
				batch = tr.Len()
			}
			var got []uint64
			var got2 int
			if min {
				got2 = tr.PopMinN(batch, func(s uint64, _ uint32) { got = append(got, s) })
			} else {
				got2 = tr.PopMaxN(batch, func(s uint64, _ uint32) { got = append(got, s) })
			}
			if got2 != batch || len(got) != batch {
				t.Fatalf("min=%v: popped %d/%d, emitted %d", min, got2, batch, len(got))
			}
			for i := 0; i < batch; i++ {
				var want uint64
				if min {
					want = scores[popped+i]
				} else {
					want = scores[n-1-popped-i]
				}
				if got[i] != want {
					t.Fatalf("min=%v: pop[%d]=%d, want %d", min, popped+i, got[i], want)
				}
			}
			popped += batch
			if err := tr.Check(nilMembers{}); err != nil {
				t.Fatalf("min=%v: invariant broken at %d popped: %v", min, popped, err)
			}
		}
		if popped != n {
			t.Fatalf("min=%v: popped %d total, want %d", min, popped, n)
		}
	}
}

// TestPopDrainRefillPop drains a tree to empty with pops, refills it, and pops
// again, checking the second life is a clean valid tree matching its model. It
// catches a cache or free-list that did not reset on the drain-to-empty edge.
func TestPopDrainRefillPop(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	fill := func(tag string, n int) []key {
		model := make([]key, 0, n)
		for i := 0; i < n; i++ {
			m := fmt.Sprintf("%s-%d", tag, i)
			s := uint64(i % 50)
			tr.Insert(s, []byte(m), ms.ref(m), ms)
			model = append(model, key{score: s, member: m})
		}
		sort.Slice(model, func(i, j int) bool { return lessKey(model[i], model[j]) })
		return model
	}
	drainMin := func(model []key) {
		for i := 0; i < len(model); i++ {
			score, ref, ok := tr.PopMin()
			if !ok {
				t.Fatalf("drain PopMin returned ok=false at %d", i)
			}
			if score != model[i].score || string(ms.Member(ref)) != model[i].member {
				t.Fatalf("drain[%d]=(%d,%q), want %v", i, score, ms.Member(ref), model[i])
			}
		}
		if tr.Len() != 0 {
			t.Fatalf("post-drain Len %d, want 0", tr.Len())
		}
		if err := tr.Check(ms); err != nil {
			t.Fatalf("post-drain invariant broken: %v", err)
		}
	}
	drainMin(fill("first", 1200))
	drainMin(fill("second", 3300)) // refill bigger, drain again
	if tr.height != 1 || tr.Len() != 0 {
		t.Fatalf("final tree height %d Len %d, want 1 and 0", tr.height, tr.Len())
	}
}

// TestEdgeCacheParity is the lab's cache-off versus cache-on bar: the same
// mutation schedule against a cache-on tree and a cache-off tree must leave
// byte-identical arenas, since the edge-leaf cache is read-path only and never
// steers a mutation. Alongside it, TestEdgeCacheTracks proves the cache tracks
// the true end leaves after every op, the correctness the parity test alone does
// not exercise.
func TestEdgeCacheParity(t *testing.T) {
	on := NewTree()
	off := NewTree()
	off.edgeCache = false

	ms := newMemStore()
	rng := rand.New(rand.NewSource(0xed6e))
	step := func(tr *Tree, op int, k key) {
		switch op % 3 {
		case 0:
			tr.Insert(k.score, []byte(k.member), ms.ref(k.member), ms)
		case 1:
			tr.Delete(k.score, []byte(k.member), ms)
		default:
			// Read the ends to populate the cache on the on tree; a no-op on data.
			tr.leftmostLeaf()
			tr.rightmostLeaf()
		}
	}
	for i := 0; i < 6000; i++ {
		op := rng.Intn(3)
		k := key{score: uint64(rng.Intn(40)), member: fmt.Sprintf("m%d", rng.Intn(3000))}
		step(on, op, k)
		step(off, op, k)
	}
	// The arenas, node counts, and shape must match exactly.
	if !bytes.Equal(on.leaves, off.leaves) {
		t.Fatal("leaf arenas diverged between cache-on and cache-off")
	}
	if !bytes.Equal(on.branches, off.branches) {
		t.Fatal("branch arenas diverged between cache-on and cache-off")
	}
	if on.Len() != off.Len() || on.height != off.height || on.root != off.root {
		t.Fatalf("shape diverged: on(len=%d h=%d root=%d) off(len=%d h=%d root=%d)",
			on.Len(), on.height, on.root, off.Len(), off.height, off.root)
	}
	if err := on.Check(ms); err != nil {
		t.Fatalf("cache-on tree invalid: %v", err)
	}
}

// TestEdgeCacheTracks checks the cached end-leaf ordinals always match a fresh
// full descent after churn, so a read that trusts the cache reads the true ends.
func TestEdgeCacheTracks(t *testing.T) {
	ms := newMemStore()
	tr := NewTree()
	rng := rand.New(rand.NewSource(0x7ac))
	present := map[key]struct{}{}
	freshLeft := func() uint32 {
		ord := tr.root
		for level := tr.height; level > 1; level-- {
			ord = tr.bChild(ord, 0)
		}
		return ord
	}
	freshRight := func() uint32 {
		ord := tr.root
		for level := tr.height; level > 1; level-- {
			ord = tr.bChild(ord, tr.bNkeys(ord))
		}
		return ord
	}
	for i := 0; i < 8000; i++ {
		k := key{score: uint64(rng.Intn(30)), member: fmt.Sprintf("m%d", rng.Intn(2500))}
		if _, has := present[k]; has && rng.Intn(2) == 0 {
			tr.Delete(k.score, []byte(k.member), ms)
			delete(present, k)
		} else if !has {
			tr.Insert(k.score, []byte(k.member), ms.ref(k.member), ms)
			present[k] = struct{}{}
		}
		// The cached reads must equal a fresh descent every step.
		if got, want := tr.leftmostLeaf(), freshLeft(); got != want {
			t.Fatalf("step %d: leftmostLeaf cache %d, fresh %d", i, got, want)
		}
		if got, want := tr.rightmostLeaf(), freshRight(); got != want {
			t.Fatalf("step %d: rightmostLeaf cache %d, fresh %d", i, got, want)
		}
	}
}
