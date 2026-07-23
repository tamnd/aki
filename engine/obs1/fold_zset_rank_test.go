package obs1_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
)

// The zset rank and range plane over a folded segment (spec 2064/obs1
// doc 08 section 5): ZCARD and the rank prefix sums are RAM-only over
// the kind-restricted plan's counts, ZRANK resolves with one boundary
// block, and every range form streams the score runs from a floored
// start. The frames are the demoter-shaped dual frames of the
// fold_zset_test fixture, so the runs under test carry the composite
// score-key-led discs the directory lifts.

// zsetRunFetcher returns the plan fetch function plus a counter of
// distinct block GETs, the request bill every assertion below reads.
func zsetRunFetcher(t *testing.T, fx *foldFixture) (func(obs1.DirRef) ([]byte, error), *int) {
	t.Helper()
	fetches := 0
	seen := map[string]bool{}
	ctx := t.Context()
	return func(ref obs1.DirRef) ([]byte, error) {
		blk := fmt.Sprintf("%s@%d", ref.ObjKey, ref.Block.Offset)
		if !seen[blk] {
			seen[blk] = true
			fetches++
		}
		off, n := ref.Block.BlockSpan()
		raw, _, err := fx.sim.GetRange(ctx, ref.ObjKey, off, n)
		if err != nil {
			return nil, err
		}
		return obs1.ParseSegmentBlock(raw, ref.Block)
	}, &fetches
}

// collect drains an iterator into a slice, checking errp at the end.
func collectPairs(t *testing.T, it func() (obs1.ZsetPair, bool), errp *error, limit int) []obs1.ZsetPair {
	t.Helper()
	var out []obs1.ZsetPair
	for limit != 0 {
		p, ok := it()
		if !ok {
			break
		}
		out = append(out, p)
		limit--
	}
	if *errp != nil {
		t.Fatalf("stream error: %v", *errp)
	}
	return out
}

func TestFolderZsetRankAndRange(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	pairs := zsetFixture(60)

	fx.folder.Add(zsetDualFrames("zr", pairs, 16))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("zr"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("zr has no locator")
	}
	scoreKind := uint8(kindZsetScoreChunk | 0x80)
	refs := dir.CollChunksKind(loc, fp, scoreKind)
	if len(refs) < 3 {
		t.Fatalf("planned %d score runs, want the pack split into several", len(refs))
	}
	for i, r := range refs {
		if r.ChunkKind != scoreKind {
			t.Fatalf("run %d kind 0x%02x crossed the kind-restricted plan", i, r.ChunkKind)
		}
		if r.Count == 0 {
			t.Fatalf("run %d carries no count; rank math needs it", i)
		}
	}

	// ZCARD: the counts alone, no fetch anywhere.
	if got := obs1.ZsetCard(refs); got != len(pairs) {
		t.Fatalf("ZsetCard = %d, want %d", got, len(pairs))
	}

	fetch, fetches := zsetRunFetcher(t, fx)

	// ZRANK for every member: floor by the score coordinate, then the rank
	// is the floor's base plus the in-run offset, read off one stream that
	// starts at the floor run. Each rank resolves within the floor run for
	// this fixture's distinct scores, so the bill is the boundary block.
	for want, p := range pairs {
		key := obs1.ZsetScoreKey(p.score)
		idx, base := obs1.ZsetRankFloor(refs, key)
		if idx > 0 && refs[idx].FirstDisc > key {
			t.Fatalf("rank floor for %q landed past its coordinate", p.member)
		}
		before := *fetches
		var serr error
		it := obs1.ZsetRunIter(refs, idx, fetch, nil, nowMs, &serr)
		got := -1
		for off := 0; ; off++ {
			sp, ok := it()
			if !ok {
				break
			}
			if string(sp.Member) == p.member {
				got = base + off
				break
			}
		}
		if serr != nil {
			t.Fatalf("rank stream for %q: %v", p.member, serr)
		}
		if got != want {
			t.Fatalf("rank of %q = %d, want %d", p.member, got, want)
		}
		if d := *fetches - before; d > 1 {
			t.Fatalf("rank of %q billed %d new blocks, want at most the boundary block", p.member, d)
		}
	}

	// ZRANGE by rank across a run boundary: locate the start run by prefix
	// sums, skip the in-run offset in RAM, stream the window.
	startRank, stopRank := int(refs[0].Count)-2, int(refs[0].Count)+2
	idx, base, ok := obs1.ZsetRunAtRank(refs, startRank)
	if !ok {
		t.Fatalf("rank %d has no run", startRank)
	}
	var serr error
	it := obs1.ZsetRunIter(refs, idx, fetch, nil, nowMs, &serr)
	for skip := startRank - base; skip > 0; skip-- {
		if _, ok := it(); !ok {
			t.Fatal("stream ended inside the skip")
		}
	}
	window := collectPairs(t, it, &serr, stopRank-startRank+1)
	if len(window) != stopRank-startRank+1 {
		t.Fatalf("range window streamed %d, want %d", len(window), stopRank-startRank+1)
	}
	for i, sp := range window {
		want := pairs[startRank+i]
		if string(sp.Member) != want.member || math.Float64frombits(sp.Bits) != want.score {
			t.Fatalf("window[%d] = %q/%v, want %q/%v", i, sp.Member, math.Float64frombits(sp.Bits), want.member, want.score)
		}
	}
	if _, _, ok := obs1.ZsetRunAtRank(refs, len(pairs)); ok {
		t.Fatal("a rank past the cardinality resolved a run")
	}

	// ZRANGEBYSCORE: floor the min, stream while the coordinate holds.
	minS, maxS := pairs[10].score, pairs[20].score
	minKey, maxKey := obs1.ZsetScoreKey(minS), obs1.ZsetScoreKey(maxS)
	idx, _ = obs1.ZsetRankFloor(refs, minKey)
	serr = nil
	it = obs1.ZsetRunIter(refs, idx, fetch, nil, nowMs, &serr)
	var byScore []obs1.ZsetPair
	for {
		sp, ok := it()
		if !ok || sp.Key > maxKey {
			break
		}
		if sp.Key >= minKey {
			byScore = append(byScore, sp)
		}
	}
	if serr != nil {
		t.Fatalf("byscore stream: %v", serr)
	}
	if len(byScore) != 11 {
		t.Fatalf("BYSCORE [%v,%v] streamed %d, want 11", minS, maxS, len(byScore))
	}
	for i, sp := range byScore {
		if string(sp.Member) != pairs[10+i].member {
			t.Fatalf("BYSCORE[%d] = %q, want %q", i, sp.Member, pairs[10+i].member)
		}
	}

	// The prefetch seam: every block after the first is announced before
	// the stream needs it, and each announcement names the next distinct
	// block in plan order.
	var announced []obs1.DirRef
	serr = nil
	it = obs1.ZsetRunIter(refs, 0, fetch, func(r obs1.DirRef) { announced = append(announced, r) }, nowMs, &serr)
	all := collectPairs(t, it, &serr, -1)
	if len(all) != len(pairs) {
		t.Fatalf("full stream yielded %d, want %d", len(all), len(pairs))
	}
	distinct := map[string]bool{}
	for _, r := range refs {
		distinct[fmt.Sprintf("%s@%d", r.ObjKey, r.Block.Offset)] = true
	}
	if want := len(distinct) - 1; len(announced) < want {
		t.Fatalf("prefetch announced %d blocks, want at least %d (every block after the first)", len(announced), want)
	}
}

// TestFolderZsetLexUniform holds the score-uniform lex mode: with one
// score, the composite discs order by member bytes, so the stream is the
// lex order and a BYLEX window filters it exactly. The run coordinates
// all lift the same score key, which is why the floor cannot skip runs
// here and the stream starts at run zero; the stable plan order is the
// demoter's emission order, which is the lex order.
func TestFolderZsetLexUniform(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	var pairs []zsetPair
	for i := 0; i < 40; i++ {
		pairs = append(pairs, zsetPair{member: fmt.Sprintf("lex%03d", i), score: 0})
	}

	fx.folder.Add(zsetDualFrames("zl", pairs, 12))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("zl"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("zl has no locator")
	}
	refs := dir.CollChunksKind(loc, fp, uint8(kindZsetScoreChunk|0x80))
	if len(refs) < 2 {
		t.Fatalf("planned %d runs, want the pack split", len(refs))
	}

	fetch, _ := zsetRunFetcher(t, fx)
	var serr error
	it := obs1.ZsetRunIter(refs, 0, fetch, nil, nowMs, &serr)
	lo, hi := []byte("lex010"), []byte("lex020")
	var got [][]byte
	var prev []byte
	for {
		sp, ok := it()
		if !ok {
			break
		}
		if prev != nil && bytes.Compare(sp.Member, prev) < 0 {
			t.Fatalf("uniform-score stream out of lex order at %q", sp.Member)
		}
		prev = append(prev[:0], sp.Member...)
		if bytes.Compare(sp.Member, lo) >= 0 && bytes.Compare(sp.Member, hi) <= 0 {
			got = append(got, append([]byte(nil), sp.Member...))
		}
	}
	if serr != nil {
		t.Fatalf("lex stream: %v", serr)
	}
	if len(got) != 11 {
		t.Fatalf("BYLEX [%s,%s] yielded %d members, want 11", lo, hi, len(got))
	}
	for i, m := range got {
		if want := fmt.Sprintf("lex%03d", 10+i); string(m) != want {
			t.Fatalf("BYLEX[%d] = %q, want %q", i, m, want)
		}
	}
}
