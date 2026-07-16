package set

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

// The P11 merge kernels are checked against a map-based oracle across the shapes
// doc 11 section 12.2 names: empty, disjoint, equal, subset, and skewed 1:1000.
// Whatever split of members between the sorted run and the bounded tail an
// operand carries, and whatever tombstones a churn left, the kernel must return
// exactly the set the oracle does.

// idMember renders a stable distinct member for id.
func idMember(id int) []byte { return []byte(fmt.Sprintf("member-%08d", id)) }

// indexedFrom builds a native table over ids and engages the sorted arrays. When
// tailFrac > 0 the last tailFrac fraction of the ids are added after engagement
// so they land in the unsorted tail, exercising the run-merge-tail view.
func indexedFrom(ids []int, tailFrac float64) *htable {
	split := len(ids)
	if tailFrac > 0 {
		split = int(float64(len(ids)) * (1 - tailFrac))
	}
	h := newHashtable(len(ids))
	for _, id := range ids[:split] {
		h.add(idMember(id))
	}
	h.engageAlgebra()
	for _, id := range ids[split:] {
		h.add(idMember(id))
	}
	return h
}

// live returns the members the table actually holds, sorted, the oracle every
// kernel and invariant check compares against.
func live(h *htable) []string {
	var out []string
	h.each(func(m []byte) { out = append(out, string(m)) })
	sort.Strings(out)
	return out
}

func confirmOf(ha, hb *htable) func(a, b uint32) bool {
	return func(a, b uint32) bool { return bytes.Equal(ha.memberByOrd(a), hb.memberByOrd(b)) }
}

func runIntersect(ha, hb *htable) []string {
	sa, _, _ := ha.mergeStream(nil)
	sb, _, _ := hb.mergeStream(nil)
	var got []string
	mergeIntersect(&sa, &sb, func(a, b uint32) bool {
		if bytes.Equal(ha.memberByOrd(a), hb.memberByOrd(b)) {
			got = append(got, string(ha.memberByOrd(a)))
			return true
		}
		return false
	})
	sort.Strings(got)
	return got
}

func runDiff(ha, hb *htable) []string {
	sa, _, _ := ha.mergeStream(nil)
	sb, _, _ := hb.mergeStream(nil)
	var got []string
	mergeDiff(&sa, &sb, confirmOf(ha, hb), func(o uint32) { got = append(got, string(ha.memberByOrd(o))) })
	sort.Strings(got)
	return got
}

func runUnion(ha, hb *htable) []string {
	sa, _, _ := ha.mergeStream(nil)
	sb, _, _ := hb.mergeStream(nil)
	var got []string
	mergeUnion(&sa, &sb, confirmOf(ha, hb),
		func(o uint32) { got = append(got, string(ha.memberByOrd(o))) },
		func(o uint32) { got = append(got, string(hb.memberByOrd(o))) })
	sort.Strings(got)
	return got
}

// oracleSets computes the three algebra results over member-string sets.
func oracleSets(a, b []string) (inter, diff, union []string) {
	sa := map[string]bool{}
	for _, m := range a {
		sa[m] = true
	}
	sb := map[string]bool{}
	for _, m := range b {
		sb[m] = true
	}
	u := map[string]bool{}
	for m := range sa {
		u[m] = true
		if sb[m] {
			inter = append(inter, m)
		} else {
			diff = append(diff, m)
		}
	}
	for m := range sb {
		u[m] = true
	}
	for m := range u {
		union = append(union, m)
	}
	sort.Strings(inter)
	sort.Strings(diff)
	sort.Strings(union)
	return
}

func eqStrings(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d, want %d", name, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: at %d got %q want %q", name, i, got[i], want[i])
		}
	}
}

func TestMergeKernelsAgainstOracle(t *testing.T) {
	cases := []struct {
		name       string
		aN, bN     int
		aoff, boff int // id ranges [aoff, aoff+aN) and [boff, boff+bN); overlap is the intersection
	}{
		{"empty-empty", 0, 0, 0, 0},
		{"a-empty", 0, 300, 0, 0},
		{"b-empty", 300, 0, 0, 0},
		{"disjoint", 300, 300, 0, 5000},
		{"equal", 300, 300, 0, 0},
		{"subset", 150, 600, 0, 0},    // a fully inside b
		{"partial", 400, 400, 0, 200}, // half overlap
		{"skewed-1-1000", 10, 10000, 0, 0},
		{"skewed-partial", 200, 9000, 100, 0},
	}
	for _, tailFrac := range []float64{0, 0.05, 0.5} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/tail%.2f", tc.name, tailFrac), func(t *testing.T) {
				aIDs := make([]int, tc.aN)
				for i := range aIDs {
					aIDs[i] = tc.aoff + i
				}
				bIDs := make([]int, tc.bN)
				for i := range bIDs {
					bIDs[i] = tc.boff + i
				}
				ha := indexedFrom(aIDs, tailFrac)
				hb := indexedFrom(bIDs, tailFrac)

				wInter, wDiff, wUnion := oracleSets(live(ha), live(hb))
				eqStrings(t, "intersect", runIntersect(ha, hb), wInter)
				eqStrings(t, "diff", runDiff(ha, hb), wDiff)
				eqStrings(t, "union", runUnion(ha, hb), wUnion)

				sa, _, _ := ha.mergeStream(nil)
				sb, _, _ := hb.mergeStream(nil)
				if n := mergeIntersectCount(&sa, &sb, confirmOf(ha, hb), 0); n != len(wInter) {
					t.Fatalf("intersectCount: got %d want %d", n, len(wInter))
				}
			})
		}
	}
}

// TestIntersectCountLimit checks the SINTERCARD early exit: the count stops at
// the limit and never scans past it.
func TestIntersectCountLimit(t *testing.T) {
	aIDs := make([]int, 500)
	for i := range aIDs {
		aIDs[i] = i
	}
	ha := indexedFrom(aIDs, 0.3)
	hb := indexedFrom(aIDs, 0.3) // equal sets, intersection is 500
	for _, limit := range []int{1, 10, 100, 499, 500, 600} {
		sa, _, _ := ha.mergeStream(nil)
		sb, _, _ := hb.mergeStream(nil)
		got := mergeIntersectCount(&sa, &sb, confirmOf(ha, hb), limit)
		want := limit
		if want > 500 {
			want = 500
		}
		if got != want {
			t.Fatalf("limit %d: got %d want %d", limit, got, want)
		}
	}
}

// TestMergeAfterChurn checks the kernels stay correct after tombstones: remove a
// spread of members from each operand, then intersect, diff, and union against
// the oracle over what remains.
func TestMergeAfterChurn(t *testing.T) {
	aIDs := make([]int, 1000)
	bIDs := make([]int, 1000)
	for i := range aIDs {
		aIDs[i] = i
		bIDs[i] = i + 500 // 500 overlap
	}
	ha := indexedFrom(aIDs, 0.1)
	hb := indexedFrom(bIDs, 0.1)
	// Remove every third member of a and every fifth of b, hitting both run and
	// tail entries.
	for i := 0; i < 1000; i += 3 {
		ha.rem(idMember(aIDs[i]))
	}
	for i := 0; i < 1000; i += 5 {
		hb.rem(idMember(bIDs[i]))
	}
	wInter, wDiff, wUnion := oracleSets(live(ha), live(hb))
	eqStrings(t, "intersect", runIntersect(ha, hb), wInter)
	eqStrings(t, "diff", runDiff(ha, hb), wDiff)
	eqStrings(t, "union", runUnion(ha, hb), wUnion)
}
