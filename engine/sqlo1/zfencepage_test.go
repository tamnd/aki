package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// fatMember builds a member long enough that a handful fill a run:
// ~700 encoded bytes against zRunMax 4032 puts five entries in a full
// run, so a few dozen adds climb the whole paging ladder under
// test-shrunk fanouts.
func fatMember(prefix string, i int) []byte {
	m := []byte(strings.Repeat("f", 700))
	copy(m, fmt.Sprintf("%s:%04d:", prefix, i))
	return m
}

// TestZFencePageCodec round-trips the three paged codecs and holds
// the reject tables: level bytes, sentinel rules, ordering, exact
// lengths, and the mint ceiling.
func TestZFencePageCodec(t *testing.T) {
	idx := []zIdxEnt{
		{lo: 0, pageid: 7, runs: 3, count: 0},
		{lo: 500, pageid: 9, runs: 2, count: 41},
		{lo: 500, pageid: 12, runs: 1, count: 3},
	}
	tail := appendZTailPaged(nil, idx)
	got, err := decodeZTailPaged(tail, nil)
	if err != nil {
		t.Fatalf("decodeZTailPaged: %v", err)
	}
	if len(got) != len(idx) {
		t.Fatalf("root index round-trips %d entries, want %d", len(got), len(idx))
	}
	for i := range idx {
		if got[i] != idx[i] {
			t.Fatalf("root index entry %d = %+v, want %+v", i, got[i], idx[i])
		}
	}

	up := appendZUpperPage(nil, idx)
	got, err = decodeZUpperPage(up, nil, 20, true)
	if err != nil {
		t.Fatalf("decodeZUpperPage: %v", err)
	}
	if len(got) != len(idx) || got[1] != idx[1] {
		t.Fatalf("upper page round-trip = %+v, want %+v", got, idx)
	}

	fence := []zFenceEnt{
		{lo: 0, segid: 3, count: 0},
		{lo: 90, segid: 4, count: 7},
		{lo: 90, segid: 6, count: 2},
	}
	leaf := appendZLeafPage(nil, fence)
	gotf, err := decodeZLeafPage(leaf, nil, 20, true)
	if err != nil {
		t.Fatalf("decodeZLeafPage: %v", err)
	}
	if len(gotf) != len(fence) || gotf[2] != fence[2] {
		t.Fatalf("leaf page round-trip = %+v, want %+v", gotf, fence)
	}

	tailRejects := []struct {
		name string
		mut  func(p []byte) []byte
	}{
		{"flat flag", func(p []byte) []byte { p[0] = 0; return p }},
		{"unknown flag byte", func(p []byte) []byte { p[1] = 9; return p }},
		{"zero entries", func(p []byte) []byte { p[2], p[3] = 0, 0; return p[:zTailHdrLen] }},
		{"length mismatch", func(p []byte) []byte { return p[:len(p)-1] }},
		{"sentinel lo", func(p []byte) []byte { p[zTailHdrLen] = 1; return p }},
		{"zero lo past sentinel", func(p []byte) []byte {
			copy(p[zTailHdrLen+zIdxEntLen:], make([]byte, 8))
			return p
		}},
		{"out of order", func(p []byte) []byte { p[zTailHdrLen+2*zIdxEntLen] = 0x10; p[zTailHdrLen+2*zIdxEntLen+1] = 0; return p }},
		{"empty subtree past sentinel", func(p []byte) []byte {
			copy(p[zTailHdrLen+2*zIdxEntLen+16:zTailHdrLen+3*zIdxEntLen], make([]byte, 8))
			return p
		}},
		{"zero runs", func(p []byte) []byte {
			p[zTailHdrLen+zIdxEntLen+14] = 0
			p[zTailHdrLen+zIdxEntLen+15] = 0
			return p
		}},
	}
	for _, tc := range tailRejects {
		p := tc.mut(appendZTailPaged(nil, idx))
		if _, err := decodeZTailPaged(p, nil); err == nil {
			t.Fatalf("decodeZTailPaged accepted %s", tc.name)
		}
	}

	if _, err := decodeZUpperPage(appendZUpperPage(nil, idx), nil, 20, false); err == nil {
		t.Fatal("decodeZUpperPage accepted a sentinel entry outside the first subtree")
	}
	if _, err := decodeZUpperPage(appendZUpperPage(nil, idx), nil, 10, true); err == nil {
		t.Fatal("decodeZUpperPage accepted a pageid past the mint counter")
	}
	badLevel := appendZUpperPage(nil, idx)
	badLevel[2] = zPageLeaf
	if _, err := decodeZUpperPage(badLevel, nil, 20, true); err == nil {
		t.Fatal("decodeZUpperPage accepted a leaf level byte")
	}

	if _, err := decodeZLeafPage(appendZLeafPage(nil, fence), nil, 20, false); err == nil {
		t.Fatal("decodeZLeafPage accepted a sentinel entry outside the first leaf")
	}
	if _, err := decodeZLeafPage(appendZLeafPage(nil, fence), nil, 5, true); err == nil {
		t.Fatal("decodeZLeafPage accepted a segid past the mint counter")
	}
	badLevel = appendZLeafPage(nil, fence)
	badLevel[2] = zPageUpper
	if _, err := decodeZLeafPage(badLevel, nil, 20, true); err == nil {
		t.Fatal("decodeZLeafPage accepted an upper level byte")
	}
}

// TestZFencePagedLadder climbs the whole paged ladder through the
// real dual surface under shrunken fanouts: transition, leaf splits,
// upper splits, ranks against the reference at every rung, score
// moves, then removals that walk the ladder back down through run,
// leaf, and upper deaths, hot and cold, ending in key death and a
// flat rebirth.
func TestZFencePagedLadder(t *testing.T) {
	defer SetZFenceCapsForTest(3, 5, 3, 4)()
	r := newZsetRig(t)
	ctx := context.Background()
	key := []byte("board")

	scores := map[string]float64{}
	zadd := func(m []byte, sc float64) {
		t.Helper()
		if _, _, _, ok, err := r.z.ZAdd(ctx, key, m, sc, ZAddFlags{}); err != nil || !ok {
			t.Fatalf("ZAdd(%q, %g) = (ok=%v, err=%v)", m[:12], sc, ok, err)
		}
		scores[string(m)] = sc
	}
	zrem := func(m []byte) {
		t.Helper()
		if existed, err := r.z.ZRem(ctx, key, m); err != nil || !existed {
			t.Fatalf("ZRem(%q) = (%v, %v)", m[:12], existed, err)
		}
		delete(scores, string(m))
	}
	state := func() {
		t.Helper()
		if _, err := r.z.zscoreState(ctx, key); err != nil {
			t.Fatalf("zscoreState: %v", err)
		}
	}

	rng := rand.New(rand.NewSource(41))
	order := rng.Perm(70)
	for _, i := range order {
		zadd(fatMember("p", i), float64(i)+0.25)
	}
	state()
	if !r.z.zpaged {
		t.Fatal("fence still flat after 70 fat adds under a 3-run flat cap")
	}
	if len(r.z.zridx) < 2 {
		t.Fatalf("root index holds %d uppers, the upper split never fired", len(r.z.zridx))
	}
	runs, count := zidxSum(r.z.zridx)
	if count != 70 {
		t.Fatalf("root index sums %d members, want 70", count)
	}
	t.Logf("after adds: %d uppers, %d runs, %d members", len(r.z.zridx), runs, count)
	checkZRanks(t, r.z, "board", scores)

	// Score moves across pages: the dual del+add lands in one command
	// with page rewrites on both ends.
	for i := 0; i < 70; i += 5 {
		zadd(fatMember("p", i), float64(70-i)+0.75)
	}
	checkZRanks(t, r.z, "board", scores)

	// Small members at one score band pack many to a run, then thin
	// out, so paged lazy merges fire inside a leaf.
	for i := range 40 {
		zadd([]byte(fmt.Sprintf("s:%03d", i)), 30.5)
	}
	for i := range 35 {
		zrem([]byte(fmt.Sprintf("s:%03d", i)))
	}
	checkZRanks(t, r.z, "board", scores)

	// The ladder walks back down: removing everything above the low
	// scores empties the tail runs, their leaves, and finally an
	// upper.
	state()
	uppersBefore := len(r.z.zridx)
	for m, sc := range scores {
		if sc > 10 {
			zrem([]byte(m))
		}
	}
	state()
	if len(r.z.zridx) >= uppersBefore {
		t.Fatalf("root index still holds %d uppers after the top removals, want under %d", len(r.z.zridx), uppersBefore)
	}
	if !r.z.zpaged {
		t.Fatal("fence un-paged itself, paged mode is one-way")
	}
	checkZRanks(t, r.z, "board", scores)

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	cold := r.reopen()
	checkZRanks(t, cold, "board", scores)

	// Key death through the paged rung, then a flat rebirth.
	for m := range scores {
		zrem([]byte(m))
	}
	if card, err := r.z.ZCard(ctx, key); err != nil || card != 0 {
		t.Fatalf("ZCard after death = (%d, %v), want 0", card, err)
	}
	if _, ok, err := r.z.Encoding(ctx, key); err != nil || ok {
		t.Fatalf("Encoding after death = (ok=%v, err=%v), want absent", ok, err)
	}
	zadd([]byte("reborn"), 1.5)
	if enc, ok, err := r.z.Encoding(ctx, key); err != nil || !ok || enc != "listpack" {
		t.Fatalf("reborn Encoding = (%q, %v, %v), want listpack", enc, ok, err)
	}
	checkZRanks(t, r.z, "board", scores)
}

// TestZFenceChainAcrossPages pins the equal-separator chain when it
// spans leaves and uppers: every member on one score, the fence full
// of equal separators, routing resolved only by first-entry reads
// over global run positions.
func TestZFenceChainAcrossPages(t *testing.T) {
	defer SetZFenceCapsForTest(2, 4, 2, 8)()
	r := newZsetRig(t)
	ctx := context.Background()
	key := []byte("chain")

	scores := map[string]float64{}
	const n = 48
	for i := range n {
		m := fatMember("c", i)
		if _, _, _, ok, err := r.z.ZAdd(ctx, key, m, 7.0, ZAddFlags{}); err != nil || !ok {
			t.Fatalf("ZAdd(%d) = (ok=%v, err=%v)", i, ok, err)
		}
		scores[string(m)] = 7.0
	}
	if _, err := r.z.zscoreState(ctx, key); err != nil {
		t.Fatalf("zscoreState: %v", err)
	}
	if !r.z.zpaged || len(r.z.zridx) < 2 {
		t.Fatalf("chain board paged=%v with %d uppers, want a paged fence spanning uppers", r.z.zpaged, len(r.z.zridx))
	}
	checkZRanks(t, r.z, "chain", scores)

	for i := 0; i < n; i += 3 {
		m := fatMember("c", i)
		if existed, err := r.z.ZRem(ctx, key, m); err != nil || !existed {
			t.Fatalf("ZRem(%d) = (%v, %v)", i, existed, err)
		}
		delete(scores, string(m))
	}
	checkZRanks(t, r.z, "chain", scores)

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	checkZRanks(t, r.reopen(), "chain", scores)
}

// TestZFenceThirdLevel drives the fence to the two-level format edge:
// the add that would need a third level fails with
// errZFenceThirdLevel before either family writes, the board stays
// exactly as it was, and adds that still have room keep landing.
func TestZFenceThirdLevel(t *testing.T) {
	defer SetZFenceCapsForTest(2, 3, 2, 2)()
	r := newZsetRig(t)
	ctx := context.Background()
	key := []byte("edge")

	scores := map[string]float64{}
	var wall []byte
	for i := range 400 {
		m := fatMember("e", i)
		_, _, _, ok, err := r.z.ZAdd(ctx, key, m, float64(i)+1, ZAddFlags{})
		if err != nil {
			if !errors.Is(err, errZFenceThirdLevel) {
				t.Fatalf("add %d failed with %v, want errZFenceThirdLevel", i, err)
			}
			wall = m
			break
		}
		if !ok {
			t.Fatalf("ZAdd(%d) not ok", i)
		}
		scores[string(m)] = float64(i) + 1
	}
	if wall == nil {
		t.Fatal("400 fat adds never hit the two-level edge under (2,3,2,2) caps")
	}

	// The rejected command tore nothing: the member side never gained
	// the pair, the card agrees, and every prior rank still holds.
	if _, ok, err := r.z.ZScore(ctx, key, wall); err != nil || ok {
		t.Fatalf("rejected member visible on the member side (ok=%v, err=%v), the dual write tore", ok, err)
	}
	card, err := r.z.ZCard(ctx, key)
	if err != nil || int(card) != len(scores) {
		t.Fatalf("ZCard after the edge = (%d, %v), want %d", card, err, len(scores))
	}
	checkZRanks(t, r.z, "edge", scores)

	// The sentinel run sits below every landed score and has room: a
	// tiny low add still lands at the full-fence corner.
	if _, _, _, ok, err := r.z.ZAdd(ctx, key, []byte("tiny"), 0.5, ZAddFlags{}); err != nil || !ok {
		t.Fatalf("tiny add at the edge = (ok=%v, err=%v)", ok, err)
	}
	scores["tiny"] = 0.5
	checkZRanks(t, r.z, "edge", scores)

	// Ranks survive the edge cold.
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	checkZRanks(t, r.reopen(), "edge", scores)
}
