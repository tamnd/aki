package zset

import (
	"math/rand/v2"
	"sort"
	"testing"
)

// buildScattered fills a native store by incremental insert in random score
// order, the ZADD build the co-location plan targets: the slab is appended in
// insertion order while the tree keeps rank order, so the two diverge and
// scatteredInserts ends at the full cardinality. It returns the store and the
// rank-sorted model of what an ordered walk must yield.
func buildScattered(n int) (*nativeStore, []pairMS) {
	ns := newNativeStore(n)
	rng := rand.New(rand.NewPCG(7, uint64(n)))
	model := make([]pairMS, 0, n)
	for i := 0; i < n; i++ {
		m := "k" + itoa(i)
		s := float64(rng.IntN(n)) // random score, ties across members exercise the tiebreak
		ns.insert([]byte(m), s)
		model = append(model, pairMS{m, s})
	}
	sort.Slice(model, func(i, j int) bool { return lessStr(model[i].s, model[i].m, model[j].s, model[j].m) })
	return ns, model
}

// nativeMembers walks the whole store in ascending order and returns the member
// strings, the sequence an ordered ZRANGE streams.
func nativeMembers(ns *nativeStore) []string {
	out := make([]string, 0, ns.card())
	ns.walkRange(0, ns.card()-1, func(m []byte, _ uint64) {
		out = append(out, string(m))
	})
	return out
}

// assertSlabRankOrdered fails unless the slab is laid out in rank order with no
// gaps: visiting the tree in order, each record's loc must be the running offset
// and the offsets must tile the whole slab. This is what colocateSlab produces
// and what makes the member reads sequential.
func assertSlabRankOrdered(t *testing.T, ns *nativeStore) {
	t.Helper()
	var off uint32
	ns.tree.Each(func(_ uint64, ref uint32) bool {
		r := &ns.recs[ref]
		if r.loc != off {
			t.Fatalf("rank walk: rec loc %d, want %d", r.loc, off)
		}
		off += r.mlen
		return true
	})
	if int(off) != len(ns.slab) {
		t.Fatalf("rank layout covers %d bytes, slab is %d", off, len(ns.slab))
	}
}

// TestColocateIsTransparent is the correctness bar: reordering the slab must not
// change a single byte an ordered read emits. The same store is walked with the
// trigger disabled (scattered slab) and then armed (co-located slab), and both
// must equal the rank-sorted model across forward, reverse and WITHSCORES
// windows. After the armed walk the slab is asserted to be in rank order, so the
// reorder really happened and the output survived it.
func TestColocateIsTransparent(t *testing.T) {
	ns, model := buildScattered(colocateFloor + 2000)
	z := newZset()
	z.nat = ns
	z.enc = encSkiplist

	windows := [][2]int{{0, -1}, {0, 9}, {colocateFloor / 2, colocateFloor/2 + 50}, {-100, -1}}

	// Before: disable the trigger so the walk reads the scattered slab as built.
	ns.scatteredInserts = 0
	before := map[string][]string{}
	for _, w := range windows {
		for _, rev := range []bool{false, true} {
			for _, ws := range []bool{false, true} {
				key := itoa(w[0]) + "," + itoa(w[1]) + boolKey(rev) + boolKey(ws)
				before[key] = rangeStrings(t, z, w[0], w[1], rev, ws)
			}
		}
	}

	// Arm the trigger and walk once: the first ordered read co-locates the slab.
	ns.scatteredInserts = ns.card()
	nativeMembers(ns)
	if ns.scatteredInserts != 0 {
		t.Fatalf("armed walk did not co-locate: scatteredInserts = %d", ns.scatteredInserts)
	}
	assertSlabRankOrdered(t, ns)

	// After: every window is byte-for-byte what it was before, and what the model
	// says.
	for _, w := range windows {
		for _, rev := range []bool{false, true} {
			for _, ws := range []bool{false, true} {
				key := itoa(w[0]) + "," + itoa(w[1]) + boolKey(rev) + boolKey(ws)
				got := rangeStrings(t, z, w[0], w[1], rev, ws)
				if !eqStrings(got, before[key]) {
					t.Fatalf("window %v rev=%v ws=%v changed across co-location:\n before %v\n after  %v",
						w, rev, ws, before[key], got)
				}
				want := modelRange(model, w[0], w[1], rev, ws)
				if !eqStrings(got, want) {
					t.Fatalf("window %v rev=%v ws=%v: got %v want %v", w, rev, ws, got, want)
				}
			}
		}
	}
}

func boolKey(b bool) string {
	if b {
		return "T"
	}
	return "F"
}

// TestColocateTrigger checks the amortization predicate directly: no reorder
// below the cardinality floor, none until divergence reaches card/8, and none
// while dead records are pending (the churn rebuild owns the layout then).
func TestColocateTrigger(t *testing.T) {
	// Below the floor the arrays are in cache, so even full divergence does not
	// reorder.
	small, _ := buildScattered(colocateFloor - 1)
	small.scatteredInserts = small.card()
	nativeMembers(small)
	if small.scatteredInserts == 0 {
		t.Fatalf("small store co-located below the floor")
	}

	ns, _ := buildScattered(colocateFloor + 5000)
	card := ns.card()

	// Just under card/8: the walk leaves the scattered slab alone.
	ns.scatteredInserts = card/8 - 1
	nativeMembers(ns)
	if ns.scatteredInserts != card/8-1 {
		t.Fatalf("reordered under the card/8 threshold: scatteredInserts = %d", ns.scatteredInserts)
	}

	// At card/8 the next ordered read co-locates.
	ns.scatteredInserts = card/8 + 1
	nativeMembers(ns)
	if ns.scatteredInserts != 0 {
		t.Fatalf("did not co-locate at the card/8 threshold: scatteredInserts = %d", ns.scatteredInserts)
	}

	// A pending dead record blocks the reorder regardless of divergence.
	ns.deadRecs = 1
	ns.scatteredInserts = card
	nativeMembers(ns)
	if ns.scatteredInserts != card {
		t.Fatalf("co-located with dead records pending: scatteredInserts = %d", ns.scatteredInserts)
	}
}

// TestColocateInsertAccounting checks scatteredInserts tracks real writes: it
// counts up per insert, a rescore bumps it (the member's rank moved), and a real
// insert-driven walk co-locates once divergence crosses the threshold, leaving
// the slab rank-ordered and the divergence counter cleared.
func TestColocateInsertAccounting(t *testing.T) {
	ns, _ := buildScattered(colocateFloor + 1)
	if ns.scatteredInserts != ns.card() {
		t.Fatalf("build left scatteredInserts %d, want %d", ns.scatteredInserts, ns.card())
	}

	// First ordered read co-locates (full divergence), clearing the counter.
	nativeMembers(ns)
	if ns.scatteredInserts != 0 {
		t.Fatalf("build-then-read did not co-locate: scatteredInserts = %d", ns.scatteredInserts)
	}
	assertSlabRankOrdered(t, ns)

	// A rescore of an existing member counts as one unit of divergence.
	ns.rescore([]byte("k0"), 999999)
	if ns.scatteredInserts != 1 {
		t.Fatalf("rescore did not bump divergence: scatteredInserts = %d", ns.scatteredInserts)
	}
}

// TestColocatePreservesScan is the ZSCAN invariant across a slab reorder: the
// full member set a ZSCAN returns must be identical before and after a
// co-location, since colocateSlab moves bytes but never a record ordinal, and
// the scan rides ordinals. It scans the whole store, co-locates, scans again,
// and compares the sets.
func TestColocatePreservesScan(t *testing.T) {
	ns, model := buildScattered(colocateFloor + 500)

	scanAll := func() map[string]bool {
		seen := map[string]bool{}
		cursor := uint64(0)
		for {
			cursor = ns.scanPage(cursor, 512, nil, func(m []byte, _ uint64) {
				seen[string(m)] = true
			})
			if cursor == 0 {
				break
			}
		}
		return seen
	}

	// Scan before any reorder (trigger disabled so the scan itself does not move
	// the slab mid-walk).
	ns.scatteredInserts = 0
	pre := scanAll()

	// Co-locate, then scan again.
	ns.scatteredInserts = ns.card()
	nativeMembers(ns)
	assertSlabRankOrdered(t, ns)
	post := scanAll()

	if len(pre) != len(model) || len(post) != len(model) {
		t.Fatalf("scan coverage: pre %d post %d model %d", len(pre), len(post), len(model))
	}
	for _, p := range model {
		if !pre[p.m] || !post[p.m] {
			t.Fatalf("member %q missing: pre=%v post=%v", p.m, pre[p.m], post[p.m])
		}
	}
}
