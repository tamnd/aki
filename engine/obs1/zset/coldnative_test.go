package zset

import (
	"bytes"
	"math"
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The cold-aware read and delete plumbing of the native band (spec 2064/f3/06
// sections 6 and 7, milestones/M7-slice-cold-chunk-zset-plan.md, PR D2 first half).
// The demote pass that retiers a member in production lands in the next slice; these
// tests hand-retier a contiguous score band into one cold chunk and prove every read
// path (score, rank, ordered walks, scan) and every delete path (rem, pop,
// removeRange, the churn rebuild) resolves a cold member's bytes through the pread
// choke without corrupting the resident members around it, and that a cold member
// stays cold across a rebuild rather than dragging its bytes resident.

// handDemote packs the given members, in the order given (which the caller keeps in
// score order the way the demote pass will), into one cold chunk, inserts the
// directory descriptor, and retiers each member's record to its chunk locator. The
// slab bytes are left in place (only the demote pass frees them); the retier is what
// routes the member's byte reads through the cold pread.
func handDemote(t *testing.T, n *nativeStore, cc *coldChunks, key []byte, members [][]byte) {
	t.Helper()
	var pk store.ChunkPacker
	for _, m := range members {
		ord, ok := n.tbl.Find(store.Hash(m), m, n)
		if !ok {
			t.Fatalf("handDemote: member %q absent", m)
		}
		pk.Add(m, scoreBytes(n.recs[ord].bits), 0)
	}
	payload, flags := pk.Finish()
	ord0, ok := n.tbl.Find(store.Hash(members[0]), members[0], n)
	if !ok {
		t.Fatalf("handDemote: first member %q absent", members[0])
	}
	disc := discOf(scoreKey(math.Float64frombits(n.recs[ord0].bits)), members[0])
	off, ok := cc.st.AppendChunk(kindZsetScore, flags, uint16(len(members)), key, disc, payload)
	if !ok {
		t.Fatal("handDemote: AppendChunk failed")
	}
	slot := uint32(len(cc.offs))
	cc.offs = append(cc.offs, off)
	cc.dir.Insert(disc, uint32(len(members)), off)
	for entry, m := range members {
		ord, ok := n.tbl.Find(store.Hash(m), m, n)
		if !ok {
			t.Fatalf("handDemote: member %q absent", m)
		}
		n.recs[ord].loc = packLoc(slot, uint32(entry)) | tierCold
	}
	n.cold = cc
}

// buildColdNative inserts n distinct members scored by their index, so the tree's
// ascending order is the insertion order and rank i holds member i.
func buildColdNative(count int) (*nativeStore, [][]byte) {
	n := newNativeStore(count)
	members := gen(0, count, 8)
	out := make([][]byte, count)
	for i, m := range members {
		out[i] = []byte(m)
		n.insert(out[i], float64(i))
	}
	return n, out
}

// walkAll collects every member and score the ordered walk streams, copying the
// bytes out immediately since a cold member aliases the shared pread scratch.
func walkAll(n *nativeStore) ([][]byte, []float64) {
	var ms [][]byte
	var scs []float64
	n.each(func(m []byte, s float64) {
		ms = append(ms, append([]byte(nil), m...))
		scs = append(scs, s)
	})
	return ms, scs
}

// TestNativeColdReadsTransparent demotes a middle score band and proves the reads
// answer identically to a fully resident store: the score and rank stay resident, the
// ordered walk streams every member in order with the cold bytes preadd, and a range
// window across the cold band reads back byte-for-byte.
func TestNativeColdReadsTransparent(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	cc := &coldChunks{st: st}
	// Demote ranks 3..6 (a contiguous score band), in score order.
	handDemote(t, n, cc, []byte("z"), members[3:7])

	// each streams all ten in order, cold members included.
	ms, scs := walkAll(n)
	if len(ms) != 10 {
		t.Fatalf("each streamed %d members, want 10", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) {
			t.Fatalf("rank %d: each = %q, want %q", i, ms[i], members[i])
		}
		if scs[i] != float64(i) {
			t.Fatalf("rank %d: score %v, want %d", i, scs[i], i)
		}
	}

	// score and rank stay resident and correct for a cold member.
	for _, i := range []int{3, 4, 5, 6} {
		sc, ok := n.score(members[i])
		if !ok || sc != float64(i) {
			t.Fatalf("cold member %d: score %v,%v want %d", i, sc, ok, i)
		}
		r, rsc, ok := n.rank(members[i])
		if !ok || r != i || rsc != float64(i) {
			t.Fatalf("cold member %d: rank %d,%v,%v want %d", i, r, rsc, ok, i)
		}
	}

	// a range window straddling the cold band reads back in order.
	var got [][]byte
	n.walkRange(2, 7, func(m []byte, _ uint64) {
		got = append(got, append([]byte(nil), m...))
	})
	for j, i := 0, 2; i <= 7; j, i = j+1, i+1 {
		if !bytes.Equal(got[j], members[i]) {
			t.Fatalf("walkRange rank %d = %q, want %q", i, got[j], members[i])
		}
	}
}

// TestNativeColdRemoveAndPop drives the delete paths over cold members: a rem of a
// cold member drops it and leaves the neighbors intact, and a pop draining across the
// cold band emits the right members and scores.
func TestNativeColdRemoveAndPop(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	cc := &coldChunks{st: st}
	handDemote(t, n, cc, []byte("z"), members[3:7])

	// Remove a cold member; it must leave, the rest must stay readable.
	if !n.rem(members[4]) {
		t.Fatal("rem of a cold member reported absent")
	}
	if _, ok := n.score(members[4]); ok {
		t.Fatal("removed cold member still scores")
	}
	if n.card() != 9 {
		t.Fatalf("card %d after one remove, want 9", n.card())
	}
	ms, _ := walkAll(n)
	if len(ms) != 9 {
		t.Fatalf("each streamed %d after remove, want 9", len(ms))
	}
	for _, m := range ms {
		if bytes.Equal(m, members[4]) {
			t.Fatal("removed cold member still walked")
		}
	}

	// Pop the three lowest (ranks 0,1,2 resident) then two more crossing into the
	// cold band (ranks 3,5 remain after 4 left): the drain must span the tier
	// boundary and emit ascending members and scores.
	var popM [][]byte
	var popS []float64
	n.pop(true, 5, func(m []byte, s float64) {
		popM = append(popM, append([]byte(nil), m...))
		popS = append(popS, s)
	})
	if len(popM) != 5 {
		t.Fatalf("popped %d, want 5", len(popM))
	}
	wantIdx := []int{0, 1, 2, 3, 5} // 4 was removed
	for j, i := range wantIdx {
		if !bytes.Equal(popM[j], members[i]) {
			t.Fatalf("pop %d = %q, want member %d %q", j, popM[j], i, members[i])
		}
		if popS[j] != float64(i) {
			t.Fatalf("pop %d score %v, want %d", j, popS[j], i)
		}
	}
	if n.card() != 4 {
		t.Fatalf("card %d after pop, want 4", n.card())
	}
}

// TestNativeColdRemoveRange deletes a rank window that covers cold members and
// asserts the survivors read back in order.
func TestNativeColdRemoveRange(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	cc := &coldChunks{st: st}
	handDemote(t, n, cc, []byte("z"), members[3:7])

	// Remove ranks [4, 7): members 4,5,6 (all cold) leave, 3 (cold) and 7.. stay.
	removed := n.removeRange(4, 7)
	if removed != 3 {
		t.Fatalf("removeRange removed %d, want 3", removed)
	}
	ms, scs := walkAll(n)
	wantIdx := []int{0, 1, 2, 3, 7, 8, 9}
	if len(ms) != len(wantIdx) {
		t.Fatalf("each streamed %d after removeRange, want %d", len(ms), len(wantIdx))
	}
	for j, i := range wantIdx {
		if !bytes.Equal(ms[j], members[i]) || scs[j] != float64(i) {
			t.Fatalf("survivor %d = %q/%v, want member %d", j, ms[j], scs[j], i)
		}
	}
}

// TestNativeColdRebuildKeepsCold forces a rebuild after a demote and proves a cold
// member survives it still cold: its record cell relocates but its locator and its
// packed bytes stay, so the reclaim never materializes the cold tier.
func TestNativeColdRebuildKeepsCold(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	cc := &coldChunks{st: st}
	handDemote(t, n, cc, []byte("z"), members[3:7])

	n.rebuild(n.tbl.Len())

	// Every member still reads back in order, cold ones included.
	ms, scs := walkAll(n)
	if len(ms) != 10 {
		t.Fatalf("each streamed %d after rebuild, want 10", len(ms))
	}
	for i := range ms {
		if !bytes.Equal(ms[i], members[i]) || scs[i] != float64(i) {
			t.Fatalf("rank %d after rebuild = %q/%v, want %q", i, ms[i], scs[i], members[i])
		}
	}
	// A demoted member is still cold: its relocated record carries the tier bit and
	// its bytes resolve through the pread, not the slab.
	for _, i := range []int{3, 4, 5, 6} {
		ord, ok := n.tbl.Find(store.Hash(members[i]), members[i], n)
		if !ok {
			t.Fatalf("cold member %d absent after rebuild", i)
		}
		if n.recs[ord].loc&tierCold == 0 {
			t.Fatalf("member %d came resident across the rebuild", i)
		}
	}
}

// TestNativeColdScan pages the whole store and proves the scan surfaces every member
// exactly once with the cold members preadd.
func TestNativeColdScan(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	cc := &coldChunks{st: st}
	handDemote(t, n, cc, []byte("z"), members[3:7])

	seen := map[string]bool{}
	cursor := uint64(0)
	for {
		cursor = n.scanPage(cursor, 3, nil, func(m []byte, _ uint64) {
			seen[string(append([]byte(nil), m...))] = true
		})
		if cursor == 0 {
			break
		}
	}
	if len(seen) != 10 {
		t.Fatalf("scan surfaced %d distinct members, want 10", len(seen))
	}
	for i, m := range members {
		if !seen[string(m)] {
			t.Fatalf("scan missed member %d %q", i, m)
		}
	}
}

// TestNativeColdBytesCountsDirectory proves the native footprint folds in the cold
// state's resident cost once a chunk lands, so the demote loop reads an honest figure.
func TestNativeColdBytesCountsDirectory(t *testing.T) {
	st := coldStore(t)
	n, members := buildColdNative(10)
	before := n.bytes()
	cc := &coldChunks{st: st}
	handDemote(t, n, cc, []byte("z"), members[3:7])
	if n.bytes() <= before {
		t.Fatalf("bytes %d did not grow past %d after a chunk landed", n.bytes(), before)
	}
	// sanity: the growth is exactly the cold state's resident footprint (the slab is
	// left in place by the hand-demote, so nothing else moved).
	if got, want := n.bytes()-before, int(cc.residentBytes()); got != want {
		t.Fatalf("bytes grew by %d, want the cold footprint %d", got, want)
	}
}
