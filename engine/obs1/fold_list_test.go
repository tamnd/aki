package obs1_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The list position-run plane over a folded segment (spec 2064/obs1 doc 08
// section 6): LLEN is RAM-only over the kind-restricted plan's counts,
// LINDEX resolves with one block through the index-to-run prefix sums, and
// LRANGE streams the runs in position order from a planned start. The frames
// are demoter-shaped: valueless packed pairs under 8-byte virtual-position
// discs that advance by the run counts, the emission the list demote pass
// ships.

// kindListChunk is the list collection kind, format like kindZsetScoreChunk.
const kindListChunk = 0x03

// listFrames packs elems into position-run frames of perChunk elements, the
// demote emission's shape: valueless pairs, discs the biased position of each
// run's first element.
func listFrames(key string, elems []string, perChunk int) []byte {
	var buf []byte
	var pk store.ChunkPacker
	base := uint64(1) << 62
	for i := 0; i < len(elems); i += perChunk {
		end := min(i+perChunk, len(elems))
		pk.Reset()
		for _, e := range elems[i:end] {
			pk.Add([]byte(e), nil, 0)
		}
		payload, flags := pk.Finish()
		var disc [8]byte
		binary.BigEndian.PutUint64(disc[:], base+uint64(i))
		buf = store.AppendRunChunk(buf, kindListChunk|store.ChunkKindBit, flags, uint16(pk.Count()), []byte(key), disc[:], payload)
	}
	return buf
}

func listFixture(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("elem%03d", i)
	}
	return out
}

func TestFolderListPositionRuns(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)
	elems := listFixture(60)

	fx.folder.Add(listFrames("lp", elems, 16))
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("lp"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("lp has no locator")
	}
	listKind := uint8(kindListChunk | store.ChunkKindBit)
	refs := dir.CollChunksKind(loc, fp, listKind)
	if len(refs) < 3 {
		t.Fatalf("planned %d position runs, want the pack split into several", len(refs))
	}
	for i, r := range refs {
		if r.ChunkKind != listKind {
			t.Fatalf("run %d kind 0x%02x crossed the kind-restricted plan", i, r.ChunkKind)
		}
		if r.Count == 0 {
			t.Fatalf("run %d carries no count; positional math needs it", i)
		}
	}

	// LLEN: the counts alone, no fetch anywhere.
	if got := obs1.ListRunCard(refs); got != len(elems) {
		t.Fatalf("ListRunCard = %d, want %d", got, len(elems))
	}

	fetch, fetches := zsetRunFetcher(t, fx)

	// LINDEX for every position: run by prefix sums, in-RAM skip, one block.
	for want := range elems {
		idx, base, ok := obs1.ListRunAtIndex(refs, want)
		if !ok {
			t.Fatalf("index %d has no run", want)
		}
		before := *fetches
		var serr error
		it := obs1.ListRunIter(refs, idx, fetch, nil, nowMs, &serr)
		for skip := want - base; skip > 0; skip-- {
			if _, ok := it(); !ok {
				t.Fatalf("index %d: stream ended inside the skip", want)
			}
		}
		v, ok := it()
		if !ok || serr != nil {
			t.Fatalf("index %d: stream ended or errored (%v)", want, serr)
		}
		if !bytes.Equal(v, []byte(elems[want])) {
			t.Fatalf("LINDEX %d = %q, want %q", want, v, elems[want])
		}
		if d := *fetches - before; d > 1 {
			t.Fatalf("LINDEX %d billed %d new blocks, want at most one", want, d)
		}
	}
	if _, _, ok := obs1.ListRunAtIndex(refs, len(elems)); ok {
		t.Fatal("an index past the cardinality resolved a run")
	}

	// LRANGE across a run boundary: plan the start, skip in RAM, stream the
	// window.
	startIdx, stopIdx := int(refs[0].Count)-2, int(refs[0].Count)+2
	idx, base, ok := obs1.ListRunAtIndex(refs, startIdx)
	if !ok {
		t.Fatalf("index %d has no run", startIdx)
	}
	var serr error
	it := obs1.ListRunIter(refs, idx, fetch, nil, nowMs, &serr)
	for skip := startIdx - base; skip > 0; skip-- {
		if _, ok := it(); !ok {
			t.Fatal("stream ended inside the skip")
		}
	}
	for i := startIdx; i <= stopIdx; i++ {
		v, ok := it()
		if !ok {
			t.Fatalf("window ended at %d, want through %d", i, stopIdx)
		}
		if !bytes.Equal(v, []byte(elems[i])) {
			t.Fatalf("window[%d] = %q, want %q", i, v, elems[i])
		}
	}
	if serr != nil {
		t.Fatalf("window stream: %v", serr)
	}

	// The whole list streams exactly, and the prefetch seam announces every
	// distinct block past the first in plan order.
	var announced []obs1.DirRef
	serr = nil
	it = obs1.ListRunIter(refs, 0, fetch, func(r obs1.DirRef) { announced = append(announced, r) }, nowMs, &serr)
	var all [][]byte
	for {
		v, ok := it()
		if !ok {
			break
		}
		all = append(all, v)
	}
	if serr != nil {
		t.Fatalf("full stream: %v", serr)
	}
	if len(all) != len(elems) {
		t.Fatalf("full stream yielded %d, want %d", len(all), len(elems))
	}
	for i, v := range all {
		if !bytes.Equal(v, []byte(elems[i])) {
			t.Fatalf("stream[%d] = %q, want %q", i, v, elems[i])
		}
	}
	distinct := map[string]bool{}
	for _, r := range refs {
		distinct[fmt.Sprintf("%s@%d", r.ObjKey, r.Block.Offset)] = true
	}
	if want := len(distinct) - 1; len(announced) < want {
		t.Fatalf("prefetch announced %d blocks, want at least %d", len(announced), want)
	}
}

// TestFolderListValueBearingGuard folds a run whose pairs wrongly carry
// values and holds the stream to the misfile guard: a position run is
// valueless by contract, so a value-bearing pair is an error, not data.
func TestFolderListValueBearingGuard(t *testing.T) {
	fx, km, dir := newFoldDirFixture(t)
	const nowMs = int64(1_700_000_000_000)

	var pk store.ChunkPacker
	pk.Add([]byte("a"), []byte("x"), 0)
	payload, flags := pk.Finish()
	var disc [8]byte
	binary.BigEndian.PutUint64(disc[:], uint64(1)<<62)
	buf := store.AppendRunChunk(nil, kindListChunk|store.ChunkKindBit, flags, 1, []byte("lb"), disc[:], payload)
	fx.folder.Add(buf)
	fx.folder.Flush()
	waitFor(t, "publish", func() bool { return len(fx.folder.Ledger()) == 1 })

	fp := obs1.Fingerprint([]byte("lb"))
	loc, ok := km.Lookup(fp)
	if !ok {
		t.Fatal("lb has no locator")
	}
	refs := dir.CollChunksKind(loc, fp, uint8(kindListChunk|store.ChunkKindBit))
	if len(refs) != 1 {
		t.Fatalf("planned %d runs, want 1", len(refs))
	}
	fetch, _ := zsetRunFetcher(t, fx)
	var serr error
	it := obs1.ListRunIter(refs, 0, fetch, nil, nowMs, &serr)
	if _, ok := it(); ok || serr == nil {
		t.Fatalf("value-bearing run streamed (err %v), want the misfile guard", serr)
	}
}
