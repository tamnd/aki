package obs1_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The scan planner (spec 2064/obs1 doc 05 section 3): coalesce a plan's
// adjacent blocks into large ranges, fan the GETs, stream the blocks in
// order, and launch the next range at the midpoint. The fetcher's Fetch
// and Prefetch match the run iterators' seams, so these tests drive the
// zset, list, stream, and PEL planes through it against real folded
// segments and hold the GET classes to the plan.

func mkblk(off uint64, stored uint32) obs1.SegmentBlockEntry {
	return obs1.SegmentBlockEntry{Offset: off, StoredLen: stored, RawLen: stored}
}

func mkref(obj string, b obs1.SegmentBlockEntry, count uint16) obs1.DirRef {
	return obs1.DirRef{ObjKey: obj, Block: b, Count: count}
}

func TestScanRangesCoalesce(t *testing.T) {
	// Block spans are stored+16 bytes (header and crc); lay four adjacent
	// blocks on object a, a gapped fifth, and one on object b.
	b0 := mkblk(0, 100)
	b1 := mkblk(116, 100)
	b2 := mkblk(232, 100)
	b3 := mkblk(348, 100)
	gap := mkblk(1000, 100)
	other := mkblk(0, 50)

	refs := []obs1.DirRef{
		mkref("a", b0, 5),
		mkref("a", b0, 7), // second chunk of the same block folds in
		mkref("a", b1, 5),
		mkref("a", b1, 0), // a manifest drop plans nothing
		mkref("a", b2, 5),
		mkref("a", b3, 5),
		mkref("a", gap, 5),
		mkref("b", other, 5),
	}

	// A target of two spans splits the adjacent quad in half.
	ranges := obs1.ScanRanges(refs, 232)
	want := []struct {
		obj    string
		off, n int64
		blocks int
	}{
		{"a", 0, 232, 2},
		{"a", 232, 232, 2},
		{"a", 1000, 116, 1},
		{"b", 0, 66, 1},
	}
	if len(ranges) != len(want) {
		t.Fatalf("planned %d ranges, want %d", len(ranges), len(want))
	}
	for i, w := range want {
		r := ranges[i]
		if r.Obj != w.obj || r.Off != w.off || r.N != w.n || len(r.Blocks) != w.blocks {
			t.Fatalf("range %d = %s@%d+%d (%d blocks), want %s@%d+%d (%d blocks)",
				i, r.Obj, r.Off, r.N, len(r.Blocks), w.obj, w.off, w.n, w.blocks)
		}
	}

	// The default target swallows the whole adjacent run.
	if ranges := obs1.ScanRanges(refs, 0); len(ranges) != 3 {
		t.Fatalf("default target planned %d ranges, want 3", len(ranges))
	}
}

// bigScanFixture folds a multi-block zset, list, and stream (with a PEL
// chunk) under one segment and returns the lookup surface. Values are
// padded so each collection alone spans several 128 KiB blocks.
func bigScanFixture(t *testing.T) (fx *foldFixture, km *obs1.Keymap, dir *obs1.Directory, zpairs []zsetPair, elems []string, entries []streamEnt, pel []pelFixEnt) {
	t.Helper()
	fx, km, dir = newFoldDirFixture(t)

	zpairs = make([]zsetPair, 1500)
	for i := range zpairs {
		zpairs[i] = zsetPair{member: fmt.Sprintf("m%04d-%0100d", i, i), score: float64(i)}
	}
	elems = make([]string, 2000)
	for i := range elems {
		elems[i] = fmt.Sprintf("e%04d-%0100d", i, i)
	}
	entries = make([]streamEnt, 400)
	for i := range entries {
		entries[i] = streamEnt{
			id:     obs1.StreamRunID{Ms: 1000 + uint64(i)},
			fields: [][2]string{{"f", fmt.Sprintf("v%03d-%0500d", i, i)}},
		}
	}
	pel = []pelFixEnt{
		{id: entries[3].id, consumer: "c1", deliveries: 1, delivered: 71},
		{id: entries[9].id, consumer: "c2", deliveries: 2, delivered: 72},
	}

	buf := zsetDualFrames("big:z", zpairs, 24)
	buf = append(buf, listFrames("big:l", elems, 24)...)
	buf = append(buf, streamRunFrames("big:s", entries, 24)...)
	buf = append(buf, pelChunkFrame("big:s", obs1.StreamPelTag([]byte("g")), entries[0].id, pel)...)
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })
	return fx, km, dir, zpairs, elems, entries, pel
}

// scanRefs resolves one collection's kind-restricted plan.
func scanRefs(t *testing.T, km *obs1.Keymap, dir *obs1.Directory, key string, kind uint8) []obs1.DirRef {
	t.Helper()
	fp := obs1.Fingerprint([]byte(key))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatalf("%s has no locator", key)
	}
	refs := dir.CollChunksKind(loc, fp, kind)
	if len(refs) == 0 {
		t.Fatalf("%s planned no chunks", key)
	}
	return refs
}

func distinctBlocks(refs []obs1.DirRef) int {
	seen := map[string]bool{}
	for _, r := range refs {
		seen[fmt.Sprintf("%s@%d", r.ObjKey, r.Block.Offset)] = true
	}
	return len(seen)
}

func TestScanFetcherEveryType(t *testing.T) {
	fx, km, dir, zpairs, elems, entries, pel := bigScanFixture(t)
	ctx := context.Background()
	// A one-block target makes every block its own range, so even the
	// two-block collections plan several ranges and the midpoint
	// readahead is observable.
	target := int64(obs1.SegmentBlockSize + 4096)

	checkStats := func(t *testing.T, f *obs1.ScanFetcher, ranges, blocks int) {
		t.Helper()
		s := f.Stats()
		if int(s.RangeGETs+s.ReadaheadGETs) != ranges {
			t.Fatalf("billed %d GETs (%d scan, %d readahead), want the %d planned ranges",
				s.RangeGETs+s.ReadaheadGETs, s.RangeGETs, s.ReadaheadGETs, ranges)
		}
		if ranges > 1 && s.ReadaheadGETs == 0 {
			t.Fatal("a multi-range scan launched no readahead")
		}
		if int(s.Blocks) != blocks {
			t.Fatalf("served %d blocks, want the plan's %d distinct", s.Blocks, blocks)
		}
	}

	t.Run("zset", func(t *testing.T) {
		refs := scanRefs(t, km, dir, "big:z", uint8(kindZsetScoreChunk|store.ChunkKindBit))
		ranges := obs1.ScanRanges(refs, target)
		if len(ranges) < 2 {
			t.Fatalf("fixture planned %d ranges, want several", len(ranges))
		}
		f := obs1.NewScanFetcher(ctx, fx.sim, ranges, 0)
		var serr error
		it := obs1.ZsetRunIter(refs, 0, f.Fetch, f.Prefetch, 0, &serr)
		i := 0
		for {
			p, ok := it()
			if !ok {
				break
			}
			if i >= len(zpairs) {
				t.Fatal("scan yielded extra pairs")
			}
			want := zpairs[i]
			if !bytes.Equal(p.Member, []byte(want.member)) || p.Bits != zsetScoreBits(want.score) {
				t.Fatalf("pair %d = %q, want %q", i, p.Member, want.member)
			}
			i++
		}
		if serr != nil {
			t.Fatalf("zset scan: %v", serr)
		}
		if i != len(zpairs) {
			t.Fatalf("scan yielded %d pairs, want %d", i, len(zpairs))
		}
		checkStats(t, f, len(ranges), distinctBlocks(refs))
	})

	t.Run("list", func(t *testing.T) {
		refs := scanRefs(t, km, dir, "big:l", uint8(kindListChunk|store.ChunkKindBit))
		ranges := obs1.ScanRanges(refs, target)
		if len(ranges) < 2 {
			t.Fatalf("fixture planned %d ranges, want several", len(ranges))
		}
		f := obs1.NewScanFetcher(ctx, fx.sim, ranges, 0)
		var serr error
		it := obs1.ListRunIter(refs, 0, f.Fetch, f.Prefetch, 0, &serr)
		i := 0
		for {
			e, ok := it()
			if !ok {
				break
			}
			if i >= len(elems) || !bytes.Equal(e, []byte(elems[i])) {
				t.Fatalf("element %d diverged", i)
			}
			i++
		}
		if serr != nil {
			t.Fatalf("list scan: %v", serr)
		}
		if i != len(elems) {
			t.Fatalf("scan yielded %d elements, want %d", i, len(elems))
		}
		checkStats(t, f, len(ranges), distinctBlocks(refs))
	})

	t.Run("stream", func(t *testing.T) {
		refs := scanRefs(t, km, dir, "big:s", uint8(kindStreamChunk|store.ChunkKindBit))
		ranges := obs1.ScanRanges(refs, target)
		if len(ranges) < 2 {
			t.Fatalf("fixture planned %d ranges, want several", len(ranges))
		}
		f := obs1.NewScanFetcher(ctx, fx.sim, ranges, 0)
		var serr error
		it := obs1.StreamRunIter(refs, 0, f.Fetch, f.Prefetch, &serr)
		i := 0
		for {
			e, ok := it()
			if !ok {
				break
			}
			if i >= len(entries) {
				t.Fatal("scan yielded extra entries")
			}
			checkStreamEntry(t, e, entries[i])
			i++
		}
		if serr != nil {
			t.Fatalf("stream scan: %v", serr)
		}
		if i != len(entries) {
			t.Fatalf("scan yielded %d entries, want %d", i, len(entries))
		}
		checkStats(t, f, len(ranges), distinctBlocks(refs))
	})

	t.Run("pel", func(t *testing.T) {
		refs := scanRefs(t, km, dir, "big:s", uint8(kindStreamPelChunk|store.ChunkKindBit))
		gr := obs1.StreamPelRefs(refs, obs1.StreamPelTag([]byte("g")))
		ranges := obs1.ScanRanges(gr, target)
		f := obs1.NewScanFetcher(ctx, fx.sim, ranges, 0)
		var serr error
		it := obs1.StreamPelIter(gr, f.Fetch, f.Prefetch, &serr)
		i := 0
		for {
			e, ok := it()
			if !ok {
				break
			}
			if i >= len(pel) || e.ID != pel[i].id || !bytes.Equal(e.Consumer, []byte(pel[i].consumer)) {
				t.Fatalf("pel entry %d diverged", i)
			}
			i++
		}
		if serr != nil {
			t.Fatalf("pel scan: %v", serr)
		}
		if i != len(pel) {
			t.Fatalf("scan yielded %d pel entries, want %d", i, len(pel))
		}
	})
}

// TestScanFetcherPrime holds the full-collection mode to its fan: primed
// ranges launch as scan GETs up front and the widened window keeps every
// later launch classed, so a primed scan that covers its plan bills no
// unlaunched stragglers.
func TestScanFetcherPrime(t *testing.T) {
	fx, km, dir, _, elems, _, _ := bigScanFixture(t)
	refs := scanRefs(t, km, dir, "big:l", uint8(kindListChunk|store.ChunkKindBit))
	target := int64(obs1.SegmentBlockSize + 4096)
	ranges := obs1.ScanRanges(refs, target)
	if len(ranges) < 2 {
		t.Fatalf("fixture planned %d ranges, want several", len(ranges))
	}

	f := obs1.NewScanFetcher(context.Background(), fx.sim, ranges, 0)
	f.Prime(obs1.ScanFanDefault)
	primed := min(obs1.ScanFanDefault, len(ranges))
	if s := f.Stats(); int(s.RangeGETs) != primed || s.ReadaheadGETs != 0 {
		t.Fatalf("prime launched %d scan and %d readahead GETs, want %d and 0", s.RangeGETs, s.ReadaheadGETs, primed)
	}

	var serr error
	it := obs1.ListRunIter(refs, 0, f.Fetch, f.Prefetch, 0, &serr)
	i := 0
	for {
		e, ok := it()
		if !ok {
			break
		}
		if !bytes.Equal(e, []byte(elems[i])) {
			t.Fatalf("element %d diverged", i)
		}
		i++
	}
	if serr != nil {
		t.Fatalf("primed scan: %v", serr)
	}
	if i != len(elems) {
		t.Fatalf("primed scan yielded %d elements, want %d", i, len(elems))
	}
	s := f.Stats()
	if int(s.RangeGETs+s.ReadaheadGETs) != len(ranges) {
		t.Fatalf("billed %d GETs, want the %d planned ranges", s.RangeGETs+s.ReadaheadGETs, len(ranges))
	}
}

// zsetScoreBits mirrors the fixture builder's payload: raw IEEE-754 bits.
func zsetScoreBits(s float64) uint64 { return math.Float64bits(s) }
