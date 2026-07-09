package f1srv

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Doc 24 slice 2 wires the sorted-hash two-pointer merge behind SINTER/SINTERCARD (spec
// 2064/f1_rewrite_ltm/24). The merge is a separate code path from the always-correct doc-20 probe, so
// the bar is that it returns exactly what the probe returns on every eligible shape, at P=1 and across
// P>1 where the partitions fan out to workers. These tests build overlapping sets large enough to clear
// setMergeFloor and balanced enough to clear setMergeMaxRatio, drive the merge directly (asserting it
// actually engages, not silently falls back), compare it to a merge-off probe server, and hammer it
// concurrently under the race detector to shake the partition fan-out.

// newMergeServer builds a bare server with the sorted-hash folder on (as --set-algebra-merge does) and
// the partition count forced to p, so a test drives the routed SINTER/SINTERCARD merge in-process at any
// P without loading the quarter-million members a real engage would need. Config defaults mirror
// newPartServer so the two servers differ only in the merge flag.
func newMergeServer(t testing.TB, p int) *Server {
	t.Helper()
	cfg := Config{
		Addr:            "127.0.0.1:0",
		IndexBuckets:    1 << 12,
		ArenaBytes:      1 << 24,
		ReadBufSize:     4 << 10,
		IncrStripes:     64,
		SetAlgebraMerge: true,
	}
	srv := New(cfg)
	srv.forceP.Store(int64(p))
	return srv
}

// mergeFixture seeds two overlapping sets A and B on c and returns the intersection the merge must
// reproduce, sorted. The shared members carry a distinct prefix from each set's private members, so the
// intersection is exactly the shared block: no private member of A can collide with a private member of
// B, and the byte-confirm inside the merge rejects any bare hash collision. nShared/nA/nB are chosen by
// the caller to sit above setMergeFloor and within setMergeMaxRatio (or deliberately outside, to test
// the fallback).
func mergeFixture(t *testing.T, c *connState, nShared, nA, nB int) []string {
	t.Helper()
	shared := make([]string, nShared)
	for i := range shared {
		shared[i] = fmt.Sprintf("share:%06d", i)
	}
	amem := append([]string{}, shared...)
	for i := 0; i < nA; i++ {
		amem = append(amem, fmt.Sprintf("aonly:%06d", i))
	}
	bmem := append([]string{}, shared...)
	for i := 0; i < nB; i++ {
		bmem = append(bmem, fmt.Sprintf("bonly:%06d", i))
	}
	loadSet(t, c, "A", amem)
	loadSet(t, c, "B", bmem)
	want := append([]string{}, shared...)
	sort.Strings(want)
	return want
}

// bulksToSorted copies arena-stable member subslices into owned, sorted strings so a comparison holds
// after the merge's stripe locks drop.
func bulksToSorted(in [][]byte) []string {
	out := make([]string, len(in))
	for i, b := range in {
		out[i] = string(b)
	}
	sort.Strings(out)
	return out
}

// TestSetMergeIntersectEngagesAndMatches drives the merge directly at every P and asserts it engages
// (returns ok) and reproduces the exact intersection, then confirms the full-command SINTER/SINTERCARD
// replies on the merge server match a merge-off probe server byte for byte. Driving setMergeIntersect
// directly is what proves the merge path ran: a silent fall-through to the probe would return ok=false
// here and fail loudly, so a green test cannot be a probe in disguise.
func TestSetMergeIntersectEngagesAndMatches(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		msrv := newMergeServer(t, p)
		mc := bareConn(msrv)
		want := mergeFixture(t, mc, 1200, 800, 800) // |A|=|B|=2000, ratio 1, both above the floor
		keys := [][]byte{[]byte("A"), []byte("B")}

		unlock := mc.lockStripes(keys)
		merged, ok := mc.setMergeIntersect(keys)
		got := bulksToSorted(merged)
		n, okc := mc.setMergeIntersectCard(keys, 0)
		unlock()

		if !ok {
			t.Fatalf("P=%d setMergeIntersect did not engage on eligible sets", p)
		}
		if !eqStrs(got, want) {
			t.Fatalf("P=%d merge intersection has %d members, want %d (mismatch)", p, len(got), len(want))
		}
		if !okc {
			t.Fatalf("P=%d setMergeIntersectCard did not engage on eligible sets", p)
		}
		if n != len(want) {
			t.Fatalf("P=%d merge SINTERCARD = %d, want %d", p, n, len(want))
		}

		// The full command path on the merge server must match a merge-off probe server exactly. The
		// probe is the reference the whole slice is measured against.
		psrv := newPartServer(t, p)
		pc := bareConn(psrv)
		mergeFixture(t, pc, 1200, 800, 800)

		mInter := sortedFlatReply(t, call(mc, func(c *connState, a [][]byte) { c.cmdSInter(a) }, "SINTER", "A", "B"))
		pInter := sortedFlatReply(t, call(pc, func(c *connState, a [][]byte) { c.cmdSInter(a) }, "SINTER", "A", "B"))
		if strings.Join(mInter, "\x00") != strings.Join(pInter, "\x00") {
			t.Fatalf("P=%d SINTER merge reply differs from probe reply", p)
		}
		mCard := call(mc, func(c *connState, a [][]byte) { c.cmdSInterCard(a) }, "SINTERCARD", "2", "A", "B")
		pCard := call(pc, func(c *connState, a [][]byte) { c.cmdSInterCard(a) }, "SINTERCARD", "2", "A", "B")
		if mCard != pCard {
			t.Fatalf("P=%d SINTERCARD merge %q differs from probe %q", p, mCard, pCard)
		}

		psrv.Close()
		msrv.Close()
	}
}

// TestSetMergeIntersectCardLimit checks the merge honors SINTERCARD's LIMIT: a limit below the true
// intersection returns exactly the limit at every P, a limit above it returns the full count, and both
// still engage the merge. Because each partition intersection is disjoint the per-partition early stops
// sum to a total the driver caps again, so a miscount in the fan-out would drift the stop here.
func TestSetMergeIntersectCardLimit(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		srv := newMergeServer(t, p)
		c := bareConn(srv)
		want := mergeFixture(t, c, 1200, 800, 800) // intersection is 1200
		keys := [][]byte{[]byte("A"), []byte("B")}

		unlock := c.lockStripes(keys)
		below, okA := c.setMergeIntersectCard(keys, 500)
		above, okB := c.setMergeIntersectCard(keys, 5000)
		unlock()

		if !okA || !okB {
			t.Fatalf("P=%d SINTERCARD LIMIT did not engage the merge", p)
		}
		if below != 500 {
			t.Fatalf("P=%d SINTERCARD LIMIT 500 = %d, want 500", p, below)
		}
		if above != len(want) {
			t.Fatalf("P=%d SINTERCARD LIMIT 5000 = %d, want %d", p, above, len(want))
		}
		srv.Close()
	}
}

// TestSetMergeIneligibleFallsBack pins the eligibility fence: a wildly asymmetric pair (ratio past
// setMergeMaxRatio), a pair one of which is below setMergeFloor, and a three-source intersection all
// stay off the merge (setMergeEligible reports false), and the command still returns the correct result
// through the probe. This proves the fence is where the doc says it is and the fallback is wired.
func TestSetMergeIneligibleFallsBack(t *testing.T) {
	type shape struct {
		name             string
		nShared, nA, nB  int
		extraKey         bool // add a third source so len(keys) != 2
		wantMergeAttempt bool
	}
	shapes := []shape{
		{"asymmetric", 1100, 200, 20000, false, false}, // |A|=1300, |B|=21100, ratio > 8
		{"below-floor", 100, 50, 50, false, false},     // both sets far under 1024
		{"three-source", 1200, 800, 800, true, false},  // eligible sizes but three sources
		{"eligible", 1200, 800, 800, false, true},      // control: this one must engage
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			msrv := newMergeServer(t, 1)
			mc := bareConn(msrv)
			mergeFixture(t, mc, sh.nShared, sh.nA, sh.nB)
			keys := [][]byte{[]byte("A"), []byte("B")}
			if sh.extraKey {
				// A third source equal to A, so the true intersection is unchanged but len(keys) == 3.
				loadSet(t, mc, "C", nil)
				call(mc, func(c *connState, a [][]byte) { c.cmdSAdd(a) },
					append([]string{"SADD", "C"}, "share:000000")...)
				keys = [][]byte{[]byte("A"), []byte("B"), []byte("C")}
			}
			if _, ok := mc.setMergeEligible(keys); ok != sh.wantMergeAttempt {
				t.Fatalf("%s: setMergeEligible = %v, want %v", sh.name, ok, sh.wantMergeAttempt)
			}

			// Whatever the eligibility, the command result must match a merge-off probe server.
			psrv := newPartServer(t, 1)
			pc := bareConn(psrv)
			mergeFixture(t, pc, sh.nShared, sh.nA, sh.nB)
			var pkeys []string
			for _, k := range keys {
				pkeys = append(pkeys, string(k))
			}
			if sh.extraKey {
				call(pc, func(c *connState, a [][]byte) { c.cmdSAdd(a) }, "SADD", "C", "share:000000")
			}
			mInter := sortedFlatReply(t, call(mc, func(c *connState, a [][]byte) { c.cmdSInter(a) },
				append([]string{"SINTER"}, pkeys...)...))
			pInter := sortedFlatReply(t, call(pc, func(c *connState, a [][]byte) { c.cmdSInter(a) },
				append([]string{"SINTER"}, pkeys...)...))
			if strings.Join(mInter, "\x00") != strings.Join(pInter, "\x00") {
				t.Fatalf("%s: SINTER on merge server differs from probe server", sh.name)
			}
			psrv.Close()
			msrv.Close()
		})
	}
}

// diffWant returns the members SDIFF A B must produce for a mergeFixture seeded with these counts: the
// aonly block, since the shared block is removed and A has no other private members. It is sorted to
// compare against the merge's ascending-hash emission after bulksToSorted.
func diffWant(nA int) []string {
	out := make([]string, nA)
	for i := 0; i < nA; i++ {
		out[i] = fmt.Sprintf("aonly:%06d", i)
	}
	sort.Strings(out)
	return out
}

// unionWant returns the members SUNION A B must produce for a mergeFixture: the shared block plus both
// private blocks, deduplicated (the shared members appear once). It is sorted for the same reason.
func unionWant(nShared, nA, nB int) []string {
	out := make([]string, 0, nShared+nA+nB)
	for i := 0; i < nShared; i++ {
		out = append(out, fmt.Sprintf("share:%06d", i))
	}
	for i := 0; i < nA; i++ {
		out = append(out, fmt.Sprintf("aonly:%06d", i))
	}
	for i := 0; i < nB; i++ {
		out = append(out, fmt.Sprintf("bonly:%06d", i))
	}
	sort.Strings(out)
	return out
}

// TestSetMergeDiffEngagesAndMatches is the slice-3 diff twin of TestSetMergeIntersectEngagesAndMatches:
// it drives setMergeDiff directly at every P, asserts it engages and reproduces exactly A minus B, then
// confirms the full SDIFF command reply on the merge server matches a merge-off probe server. Diff is
// asymmetric (A\B != B\A), so the fixture's aonly block is the whole answer and any stray shared or
// bonly member would fail the exact compare.
func TestSetMergeDiffEngagesAndMatches(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		msrv := newMergeServer(t, p)
		mc := bareConn(msrv)
		mergeFixture(t, mc, 1200, 800, 800) // |A|=|B|=2000, ratio 1, both above the floor
		want := diffWant(800)
		keys := [][]byte{[]byte("A"), []byte("B")}

		unlock := mc.lockStripes(keys)
		merged, ok := mc.setMergeDiff(keys)
		got := bulksToSorted(merged)
		unlock()

		if !ok {
			t.Fatalf("P=%d setMergeDiff did not engage on eligible sets", p)
		}
		if !eqStrs(got, want) {
			t.Fatalf("P=%d merge diff has %d members, want %d (mismatch)", p, len(got), len(want))
		}

		psrv := newPartServer(t, p)
		pc := bareConn(psrv)
		mergeFixture(t, pc, 1200, 800, 800)

		mDiff := sortedFlatReply(t, call(mc, func(c *connState, a [][]byte) { c.cmdSDiff(a) }, "SDIFF", "A", "B"))
		pDiff := sortedFlatReply(t, call(pc, func(c *connState, a [][]byte) { c.cmdSDiff(a) }, "SDIFF", "A", "B"))
		if strings.Join(mDiff, "\x00") != strings.Join(pDiff, "\x00") {
			t.Fatalf("P=%d SDIFF merge reply differs from probe reply", p)
		}
		psrv.Close()
		msrv.Close()
	}
}

// TestSetMergeUnionEngagesAndMatches is the slice-3 union twin: it drives setMergeUnion directly at
// every P, asserts it engages and reproduces the deduplicated union, then confirms the full SUNION
// command reply matches a merge-off probe. Union is the one form that emits from both operands, so the
// shared block must appear exactly once (the byte-confirm in unionEmit drops the B copy of each shared
// member) and both private blocks in full.
func TestSetMergeUnionEngagesAndMatches(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		msrv := newMergeServer(t, p)
		mc := bareConn(msrv)
		mergeFixture(t, mc, 1200, 800, 800)
		want := unionWant(1200, 800, 800)
		keys := [][]byte{[]byte("A"), []byte("B")}

		unlock := mc.lockStripes(keys)
		merged, ok := mc.setMergeUnion(keys)
		got := bulksToSorted(merged)
		unlock()

		if !ok {
			t.Fatalf("P=%d setMergeUnion did not engage on eligible sets", p)
		}
		if !eqStrs(got, want) {
			t.Fatalf("P=%d merge union has %d members, want %d (mismatch)", p, len(got), len(want))
		}

		psrv := newPartServer(t, p)
		pc := bareConn(psrv)
		mergeFixture(t, pc, 1200, 800, 800)

		mUnion := sortedFlatReply(t, call(mc, func(c *connState, a [][]byte) { c.cmdSUnion(a) }, "SUNION", "A", "B"))
		pUnion := sortedFlatReply(t, call(pc, func(c *connState, a [][]byte) { c.cmdSUnion(a) }, "SUNION", "A", "B"))
		if strings.Join(mUnion, "\x00") != strings.Join(pUnion, "\x00") {
			t.Fatalf("P=%d SUNION merge reply differs from probe reply", p)
		}
		psrv.Close()
		msrv.Close()
	}
}

// TestSetMergeStoreForms drives the three STORE commands through the merge and asserts the stored set
// matches a merge-off probe server byte for byte, across P and across three destination shapes: a fresh
// destination, an aliased destination that is also a source (SINTERSTORE A A B, so the merge buffers the
// arena-stable result before clearing A), and a self-source pair (both sources the same key). The
// merge-first path in storeAlgebra subsumes aliasing by buffering the result before any destination
// write, so an aliased store must land exactly what a probe store lands. The stored set is read back
// with SMEMBERS so the comparison covers what was actually persisted, not just what the merge returned.
func TestSetMergeStoreForms(t *testing.T) {
	type form struct {
		cmd string
		fn  func(*connState, [][]byte)
	}
	forms := []form{
		{"SINTERSTORE", func(c *connState, a [][]byte) { c.cmdSInterStore(a) }},
		{"SUNIONSTORE", func(c *connState, a [][]byte) { c.cmdSUnionStore(a) }},
		{"SDIFFSTORE", func(c *connState, a [][]byte) { c.cmdSDiffStore(a) }},
	}
	// Each shape is the argv after the command name: destination then sources. "A A B" aliases the
	// destination onto a source; "A B B" repeats a source; "D A B" is a fresh destination.
	shapes := [][]string{
		{"D", "A", "B"},
		{"A", "A", "B"},
		{"A", "B", "B"},
	}
	for _, p := range []int{1, 2, 4, 8} {
		for _, f := range forms {
			for _, sh := range shapes {
				name := fmt.Sprintf("%s/P%d/%s", f.cmd, p, strings.Join(sh, "_"))
				t.Run(name, func(t *testing.T) {
					msrv := newMergeServer(t, p)
					mc := bareConn(msrv)
					mergeFixture(t, mc, 1200, 800, 800)
					psrv := newPartServer(t, p)
					pc := bareConn(psrv)
					mergeFixture(t, pc, 1200, 800, 800)

					argv := append([]string{f.cmd}, sh...)
					mCount := call(mc, f.fn, argv...)
					pCount := call(pc, f.fn, argv...)
					if mCount != pCount {
						t.Fatalf("%s: merge stored-count reply %q differs from probe %q", name, mCount, pCount)
					}

					dest := sh[0]
					mMembers := sortedFlatReply(t, call(mc, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", dest))
					pMembers := sortedFlatReply(t, call(pc, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", dest))
					if strings.Join(mMembers, "\x00") != strings.Join(pMembers, "\x00") {
						t.Fatalf("%s: stored set on merge server differs from probe server (%d vs %d members)",
							name, len(mMembers), len(pMembers))
					}
					psrv.Close()
					msrv.Close()
				})
			}
		}
	}
}

// TestSetMergeConcurrent runs many partitioned merges at once under the race detector: each goroutine
// owns a disjoint pair of large overlapping sets, seeds them, and runs SINTER and SINTERCARD through the
// merge, asserting the exact intersection every time. Each partitioned SINTER fans its partitions across
// workers, so a batch of them running together exercises the fan-out goroutines, the shared sorted-hash
// registry, and the synchronous fold under concurrency. A stale snapshot or a fan-out data race would
// surface as a wrong count or a race report.
func TestSetMergeConcurrent(t *testing.T) {
	const workers = 8
	srv := newMergeServer(t, 8)
	defer srv.Close()

	// Seed every worker's pair first so the concurrent phase is pure reads plus the merge's own fold.
	const nShared = 1100
	type pair struct{ a, b string }
	pairs := make([]pair, workers)
	for w := 0; w < workers; w++ {
		c := bareConn(srv)
		a := fmt.Sprintf("A%d", w)
		b := fmt.Sprintf("B%d", w)
		amem := make([]string, 0, nShared+700)
		bmem := make([]string, 0, nShared+700)
		for i := 0; i < nShared; i++ {
			m := fmt.Sprintf("w%d:share:%06d", w, i)
			amem = append(amem, m)
			bmem = append(bmem, m)
		}
		for i := 0; i < 700; i++ {
			amem = append(amem, fmt.Sprintf("w%d:aonly:%06d", w, i))
			bmem = append(bmem, fmt.Sprintf("w%d:bonly:%06d", w, i))
		}
		loadSet(t, c, a, amem)
		loadSet(t, c, b, bmem)
		pairs[w] = pair{a, b}
	}

	var wg sync.WaitGroup
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := bareConn(srv)
			keys := [][]byte{[]byte(pairs[w].a), []byte(pairs[w].b)}
			for iter := 0; iter < 20; iter++ {
				unlock := c.lockStripes(keys)
				merged, ok := c.setMergeIntersect(keys)
				got := len(merged)
				n, okc := c.setMergeIntersectCard(keys, 0)
				unlock()
				if !ok || !okc {
					errs[w] = fmt.Errorf("worker %d iter %d: merge did not engage", w, iter)
					return
				}
				if got != nShared || n != nShared {
					errs[w] = fmt.Errorf("worker %d iter %d: SINTER=%d SINTERCARD=%d, want %d", w, iter, got, n, nShared)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// newMixedMergeServer builds a merge-on server that reads per-key partition counts from the registry
// (forceP left 0), so a test can engage two sets to different P and drive the mixed-P re-partition merge
// (spec 2064/f1_rewrite_ltm/24 section 5.1). It mirrors newMergeServer minus the whole-server forceP so
// A and B can sit at different P.
func newMixedMergeServer(t testing.TB) *Server {
	t.Helper()
	cfg := Config{
		Addr:            "127.0.0.1:0",
		IndexBuckets:    1 << 12,
		ArenaBytes:      1 << 24,
		ReadBufSize:     4 << 10,
		IncrStripes:     64,
		SetAlgebraMerge: true,
	}
	return New(cfg)
}

// TestSetMergeMixedPartition drives the mixed-P re-partition merge: two eligible sets engaged to
// different partition counts must re-partition the smaller-P operand up into the larger P and return
// exactly what the merge-off probe returns, for SINTER, SDIFF, SUNION, and SINTERCARD. It runs both
// operand orders and a flat-vs-partitioned pair so the bigIsA flag, the SDIFF A/B bookkeeping, and the
// pSmall==1 bucket-split are all exercised. Driving setMergeEligible directly asserts the plan is
// genuinely the mixed path, not a silent fall-through to the probe.
func TestSetMergeMixedPartition(t *testing.T) {
	cases := []struct {
		name   string
		pA, pB int
	}{
		{"A-larger", 8, 4},
		{"B-larger", 4, 8},
		{"A-partitioned-B-flat", 8, 1},
		{"A-flat-B-partitioned", 1, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msrv := newMixedMergeServer(t)
			if tc.pA > 1 {
				msrv.engageP([]byte("A"), tc.pA)
			}
			if tc.pB > 1 {
				msrv.engageP([]byte("B"), tc.pB)
			}
			mc := bareConn(msrv)
			mergeFixture(t, mc, 1200, 800, 800) // |A|=|B|=2000, intersection 1200, ratio 1
			keys := [][]byte{[]byte("A"), []byte("B")}

			unlock := mc.lockStripes(keys)
			plan, elig := mc.setMergeEligible(keys)
			merged, ok := mc.setMergeIntersect(keys)
			got := bulksToSorted(merged)
			n, okc := mc.setMergeIntersectCard(keys, 0)
			unlock()

			if !elig || !plan.mixed {
				t.Fatalf("expected an eligible mixed plan, got elig=%v mixed=%v", elig, plan.mixed)
			}
			if !ok {
				t.Fatalf("setMergeIntersect did not engage on the mixed pair")
			}

			// The merge-off probe at P=1 is the reference for the whole slice.
			psrv := newPartServer(t, 1)
			pc := bareConn(psrv)
			want := mergeFixture(t, pc, 1200, 800, 800)
			if !eqStrs(got, want) {
				t.Fatalf("mixed intersect has %d members, want %d", len(got), len(want))
			}
			if !okc || n != len(want) {
				t.Fatalf("mixed SINTERCARD = %d (ok=%v), want %d", n, okc, len(want))
			}

			reads := []struct {
				name string
				fn   func(*connState, [][]byte)
			}{
				{"SINTER", func(c *connState, a [][]byte) { c.cmdSInter(a) }},
				{"SDIFF", func(c *connState, a [][]byte) { c.cmdSDiff(a) }},
				{"SUNION", func(c *connState, a [][]byte) { c.cmdSUnion(a) }},
			}
			for _, r := range reads {
				m := sortedFlatReply(t, call(mc, r.fn, r.name, "A", "B"))
				p := sortedFlatReply(t, call(pc, r.fn, r.name, "A", "B"))
				if strings.Join(m, "\x00") != strings.Join(p, "\x00") {
					t.Fatalf("%s mixed reply differs from probe", r.name)
				}
				mr := sortedFlatReply(t, call(mc, r.fn, r.name, "B", "A"))
				pr := sortedFlatReply(t, call(pc, r.fn, r.name, "B", "A"))
				if strings.Join(mr, "\x00") != strings.Join(pr, "\x00") {
					t.Fatalf("%s reversed mixed reply differs from probe", r.name)
				}
			}
			psrv.Close()
			msrv.Close()
		})
	}
}
