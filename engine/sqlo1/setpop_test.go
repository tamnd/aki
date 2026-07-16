package sqlo1

import (
	"context"
	"fmt"
	"testing"
)

// srandCount collects one SRANDMEMBER count call.
func srandCount(t *testing.T, se *Set, key string, count int64, withReplacement bool) []string {
	t.Helper()
	var out []string
	begun := -1
	err := se.SRandMemberCount(context.Background(), []byte(key), count, withReplacement, func(n int64) {
		if begun != -1 {
			t.Fatalf("begin ran twice for SRANDMEMBER %s %d", key, count)
		}
		begun = int(n)
	}, func(m []byte) {
		out = append(out, string(m))
	})
	if err != nil {
		t.Fatalf("SRandMemberCount(%q, %d): %v", key, count, err)
	}
	if begun != len(out) {
		t.Fatalf("begin announced %d members, %d emitted", begun, len(out))
	}
	return out
}

// spopCount collects one SPOP count call.
func spopCount(t *testing.T, se *Set, key string, count int64) []string {
	t.Helper()
	var out []string
	begun := -1
	err := se.SPopCount(context.Background(), []byte(key), count, func(n int64) {
		if begun != -1 {
			t.Fatalf("begin ran twice for SPOP %s %d", key, count)
		}
		begun = int(n)
	}, func(m []byte) {
		out = append(out, string(m))
	})
	if err != nil {
		t.Fatalf("SPopCount(%q, %d): %v", key, count, err)
	}
	if begun != len(out) {
		t.Fatalf("begin announced %d members, %d emitted", begun, len(out))
	}
	return out
}

// smembers collects the full membership.
func smembers(t *testing.T, se *Set, key string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	announced := -1
	err := se.SMembers(context.Background(), []byte(key), func(n int) {
		announced = n
	}, func(m []byte) {
		out[string(m)] = true
	})
	if err != nil {
		t.Fatalf("SMembers(%q): %v", key, err)
	}
	if announced != len(out) {
		t.Fatalf("SMembers announced %d, emitted %d distinct", announced, len(out))
	}
	return out
}

// mustDistinctSubset checks a draw is duplicate-free and inside the
// universe.
func mustDistinctSubset(t *testing.T, got []string, universe map[string]bool) {
	t.Helper()
	seen := map[string]bool{}
	for _, m := range got {
		if seen[m] {
			t.Fatalf("member %q drawn twice in a distinct draw", m)
		}
		seen[m] = true
		if !universe[m] {
			t.Fatalf("draw invented member %q", m)
		}
	}
}

// TestSRandMember covers both grammar shapes over both rungs: single
// draws, positive distinct counts (clamped at the cardinality),
// negative with-replacement counts, and the zero count.
func TestSRandMember(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	// Absent key: single draw answers not-ok, counts answer zero.
	if _, ok, err := r.se.SRandMember(ctx, []byte("ghost")); err != nil || ok {
		t.Fatalf("SRandMember(ghost) = (ok=%v, err=%v), want absent", ok, err)
	}
	if got := srandCount(t, r.se, "ghost", 5, false); len(got) != 0 {
		t.Fatalf("SRANDMEMBER ghost 5 emitted %d members", len(got))
	}
	if got := srandCount(t, r.se, "ghost", 5, true); len(got) != 0 {
		t.Fatalf("SRANDMEMBER ghost -5 emitted %d members", len(got))
	}

	universe := map[string]bool{}
	for i := range 10 {
		m := fmt.Sprintf("m%02d", i)
		r.sadd("s", m)
		universe[m] = true
	}

	m, ok, err := r.se.SRandMember(ctx, []byte("s"))
	if err != nil || !ok || !universe[string(m)] {
		t.Fatalf("single draw = (%q, %v, %v), want a member", m, ok, err)
	}
	got := srandCount(t, r.se, "s", 4, false)
	if len(got) != 4 {
		t.Fatalf("count 4 emitted %d", len(got))
	}
	mustDistinctSubset(t, got, universe)
	if got = srandCount(t, r.se, "s", 25, false); len(got) != 10 {
		t.Fatalf("overcount clamps at the cardinality: emitted %d, want 10", len(got))
	}
	mustDistinctSubset(t, got, universe)
	got = srandCount(t, r.se, "s", 30, true)
	if len(got) != 30 {
		t.Fatalf("with replacement emitted %d, want exactly 30", len(got))
	}
	for _, m := range got {
		if !universe[m] {
			t.Fatalf("with-replacement draw invented %q", m)
		}
	}
	if got = srandCount(t, r.se, "s", 0, false); len(got) != 0 {
		t.Fatalf("count 0 emitted %d", len(got))
	}
	if r.scard("s") != 10 {
		t.Fatal("SRANDMEMBER mutated the set")
	}

	// Segmented rung: same contract through the fence machinery.
	big := map[string]bool{}
	for i := range 1200 {
		m := fmt.Sprintf("b%04d", i)
		r.sadd("big", m)
		big[m] = true
	}
	if r.encoding("big") != "hashtable" {
		t.Fatalf("big encodes %q, want hashtable", r.encoding("big"))
	}
	got = srandCount(t, r.se, "big", 50, false)
	if len(got) != 50 {
		t.Fatalf("segmented count 50 emitted %d", len(got))
	}
	mustDistinctSubset(t, got, big)
	got = srandCount(t, r.se, "big", 100, true)
	if len(got) != 100 {
		t.Fatalf("segmented with replacement emitted %d, want 100", len(got))
	}
	for _, m := range got {
		if !big[m] {
			t.Fatalf("segmented with-replacement draw invented %q", m)
		}
	}
	if r.scard("big") != 1200 {
		t.Fatal("SRANDMEMBER mutated the segmented set")
	}
}

// TestSPopSingle drains a small set one pop at a time: every pop is a
// distinct member, the last one deletes the key, and popping the
// ghost answers not-ok.
func TestSPopSingle(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	universe := map[string]bool{}
	for i := range 5 {
		m := fmt.Sprintf("p%d", i)
		r.sadd("s", m)
		universe[m] = true
	}
	popped := map[string]bool{}
	for i := range 5 {
		m, ok, err := r.se.SPop(ctx, []byte("s"))
		if err != nil || !ok {
			t.Fatalf("pop %d = (ok=%v, err=%v)", i, ok, err)
		}
		if !universe[string(m)] || popped[string(m)] {
			t.Fatalf("pop %d drew %q (invented or repeated)", i, m)
		}
		popped[string(m)] = true
		if r.scard("s") != int64(4-i) {
			t.Fatalf("card after pop %d = %d", i, r.scard("s"))
		}
	}
	if r.encoding("s") != "" {
		t.Fatal("key survived popping its last member")
	}
	if _, ok, err := r.se.SPop(ctx, []byte("s")); err != nil || ok {
		t.Fatalf("pop of the emptied key = (ok=%v, err=%v), want absent", ok, err)
	}

	// One segmented pop: member gone, count follows.
	for i := range 1200 {
		r.sadd("big", fmt.Sprintf("b%04d", i))
	}
	m, ok, err := r.se.SPop(ctx, []byte("big"))
	if err != nil || !ok {
		t.Fatalf("segmented pop = (ok=%v, err=%v)", ok, err)
	}
	gone := string(m)
	if r.sismember("big", gone) {
		t.Fatalf("popped member %q still present", gone)
	}
	if r.scard("big") != 1199 {
		t.Fatalf("card after segmented pop = %d, want 1199", r.scard("big"))
	}
}

// TestSPopCountInline covers the inline arm: a partial pop leaves the
// exact complement, the intset answer survives removals, the zero
// count removes nothing, and an overpop empties and deletes the key.
func TestSPopCountInline(t *testing.T) {
	r := newSetRig(t)

	universe := map[string]bool{}
	for i := range 20 {
		m := fmt.Sprintf("m%02d", i)
		r.sadd("s", m)
		universe[m] = true
	}
	if got := spopCount(t, r.se, "s", 0); len(got) != 0 {
		t.Fatalf("count 0 popped %d members", len(got))
	}
	if r.scard("s") != 20 {
		t.Fatal("count 0 mutated the set")
	}
	got := spopCount(t, r.se, "s", 5)
	if len(got) != 5 {
		t.Fatalf("popped %d, want 5", len(got))
	}
	mustDistinctSubset(t, got, universe)
	rest := smembers(t, r.se, "s")
	if len(rest) != 15 || r.scard("s") != 15 {
		t.Fatalf("complement holds %d members, card %d, want 15", len(rest), r.scard("s"))
	}
	for _, m := range got {
		if rest[m] {
			t.Fatalf("popped member %q still present", m)
		}
	}
	for m := range universe {
		if !rest[m] {
			gotIt := false
			for _, p := range got {
				if p == m {
					gotIt = true
				}
			}
			if !gotIt {
				t.Fatalf("member %q vanished without being popped", m)
			}
		}
	}

	// The intset answer never comes back but survives pops, SREM's
	// one-way rule.
	for i := range 12 {
		r.sadd("ints", fmt.Sprintf("%d", i+1))
	}
	if r.encoding("ints") != "intset" {
		t.Fatalf("ints encodes %q", r.encoding("ints"))
	}
	spopCount(t, r.se, "ints", 4)
	if r.encoding("ints") != "intset" {
		t.Fatalf("intset flag lost across a pop: %q", r.encoding("ints"))
	}

	// Overpop: everything emits and the key dies.
	got = spopCount(t, r.se, "ints", 99)
	if len(got) != 8 {
		t.Fatalf("overpop emitted %d, want the 8 left", len(got))
	}
	if r.encoding("ints") != "" {
		t.Fatal("key survived an overpop")
	}

	// Absent key: zero pops.
	if got = spopCount(t, r.se, "ghost", 3); len(got) != 0 {
		t.Fatalf("ghost pop emitted %d", len(got))
	}
}

// TestSPopCountSegmentedEdit pins the below-threshold arm: a small pop
// off a segmented set edits touched segments in place under the same
// plane (the rooth must not move), leaves the exact complement, and
// the cold view agrees.
func TestSPopCountSegmentedEdit(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	universe := map[string]bool{}
	for i := range 1200 {
		m := fmt.Sprintf("e%04d", i)
		r.sadd("s", m)
		universe[m] = true
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	fenceLen := int64(len(r.se.h.segRoot.fence))
	if 10 >= popRebuildFactor*fenceLen {
		t.Fatalf("fence of %d segments makes a pop of 10 rebuild; adjust sizes", fenceLen)
	}
	before := r.se.h.segRoot.rooth

	got := spopCount(t, r.se, "s", 10)
	if len(got) != 10 {
		t.Fatalf("popped %d, want 10", len(got))
	}
	mustDistinctSubset(t, got, universe)

	if _, _, _, err := r.se.h.stateOf(ctx, []byte("s")); err != nil {
		t.Fatalf("stateOf after: %v", err)
	}
	if r.se.h.segRoot.rooth != before {
		t.Fatal("edit-arm pop moved the rooth; the plane should stay")
	}
	if r.scard("s") != 1190 {
		t.Fatalf("card = %d, want 1190", r.scard("s"))
	}
	rest := smembers(t, r.se, "s")
	if len(rest) != 1190 {
		t.Fatalf("complement holds %d", len(rest))
	}
	for _, m := range got {
		if rest[m] {
			t.Fatalf("popped member %q still present", m)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	cnt, err := se2.SCard(ctx, []byte("s"))
	if err != nil || cnt != 1190 {
		t.Fatalf("cold SCard = (%d, %v)", cnt, err)
	}
	for _, m := range got {
		ok, err := se2.SIsMember(ctx, []byte("s"), []byte(m))
		if err != nil || ok {
			t.Fatalf("cold view still holds popped member %q (ok=%v, err=%v)", m, ok, err)
		}
	}
}

// TestSPopWholeSegment drives popSeg's whole-segment fast path
// directly: a pick set covering every entry of one segment must write
// the bare 12-byte empty image, and the set stays coherent once the
// root count is folded the way popEdit does it.
func TestSPopWholeSegment(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()
	h := r.se.h

	universe := map[string]bool{}
	for i := range 1200 {
		m := fmt.Sprintf("w%04d", i)
		r.sadd("s", m)
		universe[m] = true
	}
	st, _, _, err := h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	segid := h.segRoot.fence[0].segid

	// Every entry of the first segment becomes a pick; members copy
	// out first because the segment image dies at the next read.
	seg, err := h.readSeg(ctx, segid)
	if err != nil {
		t.Fatalf("readSeg: %v", err)
	}
	var members []string
	it := hashEntryIter{p: seg.entries, enc: seg.enc}
	for {
		f, _, _, ok, err := it.next()
		if err != nil {
			t.Fatalf("segment walk: %v", err)
		}
		if !ok {
			break
		}
		members = append(members, string(f))
	}
	if len(members) == 0 {
		t.Fatal("first segment is empty before the test starts")
	}
	picks := make([]popPick, len(members))
	for i, m := range members {
		picks[i] = popPick{segid: segid, fh: hashFH([]byte(m)), ei: i}
	}

	if err := r.se.popSeg(ctx, picks); err != nil {
		t.Fatalf("popSeg: %v", err)
	}
	seg, err = h.readSeg(ctx, segid)
	if err != nil {
		t.Fatalf("readSeg after: %v", err)
	}
	if seg.n != 0 || len(seg.entries) != 0 {
		t.Fatalf("emptied segment holds n=%d, %d entry bytes; want the bare header", seg.n, len(seg.entries))
	}

	// Fold the count the way popEdit does and check the set is whole.
	h.segRoot.count -= uint64(len(members))
	if err := h.writeSegRoot(ctx, []byte("s"), true); err != nil {
		t.Fatalf("writeSegRoot: %v", err)
	}
	want := int64(1200 - len(members))
	if r.scard("s") != want {
		t.Fatalf("card = %d, want %d", r.scard("s"), want)
	}
	rest := smembers(t, r.se, "s")
	if int64(len(rest)) != want {
		t.Fatalf("walk sees %d members, want %d", len(rest), want)
	}
	for _, m := range members {
		if rest[m] {
			t.Fatalf("emptied segment's member %q still visible", m)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	cnt, err := se2.SCard(ctx, []byte("s"))
	if err != nil || cnt != want {
		t.Fatalf("cold SCard = (%d, %v), want %d", cnt, err, want)
	}
}

// TestSPopCountRebuild pins the at-threshold arm: a large pop mints a
// fresh plane (the rooth must move), the survivors are the exact
// complement, the fh cursor still walks the new plane, and the cold
// view agrees.
func TestSPopCountRebuild(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	universe := map[string]bool{}
	for i := range 1200 {
		m := fmt.Sprintf("r%04d", i)
		r.sadd("s", m)
		universe[m] = true
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	fenceLen := int64(len(r.se.h.segRoot.fence))
	if 400 < popRebuildFactor*fenceLen {
		t.Fatalf("fence of %d segments keeps a pop of 400 on the edit arm; adjust sizes", fenceLen)
	}
	before := r.se.h.segRoot.rooth

	got := spopCount(t, r.se, "s", 400)
	if len(got) != 400 {
		t.Fatalf("popped %d, want 400", len(got))
	}
	mustDistinctSubset(t, got, universe)

	if _, _, _, err := r.se.h.stateOf(ctx, []byte("s")); err != nil {
		t.Fatalf("stateOf after: %v", err)
	}
	if r.se.h.segRoot.rooth == before {
		t.Fatal("rebuild pop kept the old rooth; the plane should be fresh")
	}
	if r.se.h.segRoot.rootgen != 1 {
		t.Fatalf("fresh plane at rootgen %d, want 1", r.se.h.segRoot.rootgen)
	}
	rest := smembers(t, r.se, "s")
	if len(rest) != 800 || r.scard("s") != 800 {
		t.Fatalf("complement holds %d members, card %d, want 800", len(rest), r.scard("s"))
	}
	for _, m := range got {
		if rest[m] {
			t.Fatalf("popped member %q survived the rebuild", m)
		}
	}

	// The shared fh cursor walks the packed plane end to end.
	scanned := map[string]bool{}
	cursor := uint64(0)
	for step := 0; ; step++ {
		if step > 400 {
			t.Fatal("SSCAN over the rebuilt plane did not finish")
		}
		next, err := r.se.SScan(ctx, []byte("s"), cursor, 40, func(m []byte) {
			scanned[string(m)] = true
		})
		if err != nil {
			t.Fatalf("SScan: %v", err)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(scanned) != 800 {
		t.Fatalf("SSCAN saw %d members, want 800", len(scanned))
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	cnt, err := se2.SCard(ctx, []byte("s"))
	if err != nil || cnt != 800 {
		t.Fatalf("cold SCard = (%d, %v)", cnt, err)
	}
	for m := range rest {
		ok, err := se2.SIsMember(ctx, []byte("s"), []byte(m))
		if err != nil || !ok {
			t.Fatalf("cold view lost survivor %q (ok=%v, err=%v)", m, ok, err)
		}
		break
	}
}

// TestSPopRebuildPaged runs the rebuild arm with a paged fence on both
// sides: the source pages its fence, the pop count clears the paged
// threshold proxy, and the survivor plane is big enough to page again.
func TestSPopRebuildPaged(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	// Fat members keep entries-per-segment low so the fence pages at a
	// member count a test can afford, pageRigHash's trick.
	pad := make([]byte, 54)
	for i := range pad {
		pad[i] = 'x'
	}
	n := 16000
	for i := range n {
		r.sadd("s", fmt.Sprintf("f%05d-%s", i, pad))
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	if !r.se.h.segRoot.paged {
		t.Fatalf("source fence is flat at %d segments; adjust sizes", len(r.se.h.segRoot.fence))
	}
	count := popRebuildFactor * int64(len(r.se.h.segRoot.pidx)) * hashFencePageMax
	if int64(n)-count < int64(129*64) {
		t.Fatalf("pop of %d leaves too few survivors to page the target; adjust sizes", count)
	}

	got := spopCount(t, r.se, "s", count)
	if int64(len(got)) != count {
		t.Fatalf("popped %d, want %d", len(got), count)
	}
	if _, _, _, err := r.se.h.stateOf(ctx, []byte("s")); err != nil {
		t.Fatalf("stateOf after: %v", err)
	}
	if !r.se.h.segRoot.paged {
		t.Fatal("survivor plane should still page its fence")
	}
	want := int64(n) - count
	if r.scard("s") != want {
		t.Fatalf("card = %d, want %d", r.scard("s"), want)
	}
	rest := smembers(t, r.se, "s")
	if int64(len(rest)) != want {
		t.Fatalf("complement holds %d, want %d", len(rest), want)
	}
	for _, m := range got {
		if rest[m] {
			t.Fatalf("popped member survived the paged rebuild: %q", m)
		}
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	se2 := r.reopen()
	cnt, err := se2.SCard(ctx, []byte("s"))
	if err != nil || cnt != want {
		t.Fatalf("cold SCard = (%d, %v), want %d", cnt, err, want)
	}
}

// popMembersAt probes membership in the state a crash after batch p
// recovers to.
func popMembersAt(t *testing.T, r *setRig, p int, members []string) map[string]bool {
	t.Helper()
	ms := r.rs.replayPrefix(t, p)
	tr := NewTiered(ms, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     uint64(p) + 300,
		NowMs:    func() int64 { return 1 << 41 },
	})
	se, err := NewSet(tr, HashConfig{})
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, m := range members {
		ok, err := se.SIsMember(context.Background(), []byte("s"), []byte(m))
		if err != nil {
			t.Fatalf("prefix %d: SIsMember(%q): %v", p, m, err)
		}
		if ok {
			present[m] = true
		}
	}
	return present
}

// TestSPopCrashPrefixEdit cuts the edit arm's drain at every batch
// boundary: survivors must be present at every prefix (a pop may
// replay as unfinished, never as overdone), and the full replay holds
// exactly the complement. Count exactness across a torn segment-root
// pair is the store's W3 reconciliation, proven in the sqlo1b matrix;
// the placeholder store replays raw batches, so the prefix sweep here
// asserts membership.
func TestSPopCrashPrefixEdit(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	n := 600
	members := make([]string, n)
	for i := range n {
		members[i] = fmt.Sprintf("c%03d", i)
		r.sadd("s", members[i])
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	if 10 >= popRebuildFactor*int64(len(r.se.h.segRoot.fence)) {
		t.Fatal("pop of 10 would rebuild; adjust sizes")
	}
	setup := len(r.rs.batches)

	r.tr.dr.maxOps = 1
	got := spopCount(t, r.se, "s", 10)
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	popped := map[string]bool{}
	for _, m := range got {
		popped[m] = true
	}

	for p := setup; p <= len(r.rs.batches); p++ {
		present := popMembersAt(t, r, p, members)
		for _, m := range members {
			if !popped[m] && !present[m] {
				t.Fatalf("prefix %d: survivor %q lost by a batch cut", p, m)
			}
		}
	}
	final := popMembersAt(t, r, len(r.rs.batches), members)
	if len(final) != n-10 {
		t.Fatalf("full replay holds %d members, want %d", len(final), n-10)
	}
	for m := range popped {
		if final[m] {
			t.Fatalf("full replay still holds popped member %q", m)
		}
	}
}

// TestSPopCrashPrefixRebuild cuts the rebuild arm the same way. The
// plane-before-root barrier makes every prefix a whole plane: either
// the old root over the intact old plane or the new root over the
// landed new one (the genbump rides the root's own batch), so this
// sweep can assert full walk coherence at every cut, not just
// membership.
func TestSPopCrashPrefixRebuild(t *testing.T) {
	r := newSetRig(t)
	ctx := context.Background()

	n := 600
	members := make([]string, n)
	for i := range n {
		members[i] = fmt.Sprintf("r%03d", i)
		r.sadd("s", members[i])
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	st, _, _, err := r.se.h.stateOf(ctx, []byte("s"))
	if err != nil || st != hashSegState {
		t.Fatalf("stateOf = (%v, %v), want segmented", st, err)
	}
	if 300 < popRebuildFactor*int64(len(r.se.h.segRoot.fence)) {
		t.Fatal("pop of 300 stays on the edit arm; adjust sizes")
	}
	setup := len(r.rs.batches)

	r.tr.dr.maxOps = 1
	got := spopCount(t, r.se, "s", 300)
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	popped := map[string]bool{}
	for _, m := range got {
		popped[m] = true
	}

	for p := setup; p <= len(r.rs.batches); p++ {
		ms := r.rs.replayPrefix(t, p)
		tr := NewTiered(ms, TieredConfig{
			Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
			PromoteP: -1,
			Seed:     uint64(p) + 300,
			NowMs:    func() int64 { return 1 << 41 },
		})
		se, err := NewSet(tr, HashConfig{})
		if err != nil {
			t.Fatal(err)
		}
		visible := map[string]bool{}
		announced := -1
		err = se.SMembers(ctx, []byte("s"), func(c int) { announced = c }, func(m []byte) {
			visible[string(m)] = true
		})
		if err != nil {
			t.Fatalf("prefix %d: SMembers: %v", p, err)
		}
		if announced != len(visible) {
			t.Fatalf("prefix %d: announced %d, walked %d", p, announced, len(visible))
		}
		if len(visible) != n && len(visible) != n-300 {
			t.Fatalf("prefix %d: %d members visible, want the old plane's %d or the new one's %d", p, len(visible), n, n-300)
		}
		for _, m := range members {
			if !popped[m] && !visible[m] {
				t.Fatalf("prefix %d: survivor %q lost", p, m)
			}
		}
		if p == len(r.rs.batches) && len(visible) != n-300 {
			t.Fatalf("full replay holds %d members, want %d", len(visible), n-300)
		}
	}
}
