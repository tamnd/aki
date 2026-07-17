package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

// pagedRoot decodes key's root and fails unless it is on the paged
// rung, handing back the page index for shape assertions.
func (r *streamRig) pagedRoot(key string) streamRoot {
	r.t.Helper()
	v, _, _, ok, err := r.tr.LookupEntry(context.Background(), []byte(key))
	if err != nil || !ok {
		r.t.Fatalf("LookupEntry(%q): ok=%v err=%v", key, ok, err)
	}
	sr, err := decodeStreamRoot(v, nil, nil)
	if err != nil {
		r.t.Fatalf("decode stream root %q: %v", key, err)
	}
	if !sr.paged {
		r.t.Fatalf("stream %q is not paged", key)
	}
	return sr
}

// TestStreamFenceTransition drives the flat-to-paged transition at the
// production fanouts: fat entries cut one run each until the flat cap,
// and the cut past it moves the whole fence into pages, with the full
// audit after every add and a cold reopen at the end.
func TestStreamFenceTransition(t *testing.T) {
	r := newStreamRig(t)
	fat := strings.Repeat("x", 5000)
	for range streamFenceMaxRuns {
		r.nowMs++
		r.add("s", xidAuto, streamID{}, "blob", fat)
	}
	if paged, err := r.x.StreamFencePagedForTest(context.Background(), []byte("s")); err != nil || paged {
		t.Fatalf("paged before the cap: %v, %v", paged, err)
	}
	r.nowMs++
	r.add("s", xidAuto, streamID{}, "blob", fat)
	if paged, err := r.x.StreamFencePagedForTest(context.Background(), []byte("s")); err != nil || !paged {
		t.Fatalf("paged after the cap: %v, %v", paged, err)
	}
	// The tail page keeps growing in place: fat entries cut runs onto
	// it, and the audit walks the pages after every add.
	for range 3 {
		r.nowMs++
		r.add("s", xidAuto, streamID{}, "blob", fat)
	}
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	r.checkRange("s", streamID{}, full, -1, false)
	r.checkRange("s", streamID{}, full, -1, true)

	// A cold runtime reads the identical paged stream.
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr2 := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     13,
		NowMs:    func() int64 { return r.nowMs },
	})
	x2, err := NewStream(tr2, StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cold := &streamRig{t: t, rs: r.rs, tr: tr2, x: x2, nowMs: r.nowMs, model: r.model}
	cold.check("s")
	cold.checkRange("s", streamID{}, full, -1, false)
}

// TestStreamPagedLadder walks every paged rung at dialed caps (three
// flat runs, two per page, four index slots) with medium entries, two
// per run, so cuts and tail amendments alternate: the transition, the
// paged tail amendment, in-place tail page growth, fresh pages, and the
// third-level refusal, which must be side-effect free while small
// appends keep serving.
func TestStreamPagedLadder(t *testing.T) {
	defer SetStreamFenceCapsForTest(3, 2, 4)()
	r := newStreamRig(t)
	ctx := context.Background()
	med := strings.Repeat("m", 1800)
	addMed := func() {
		r.t.Helper()
		r.nowMs++
		r.add("s", xidAuto, streamID{}, "v", med)
	}

	// Adds 1-6 stay flat: three runs of two entries.
	for range 6 {
		addMed()
	}
	if paged, err := r.x.StreamFencePagedForTest(ctx, []byte("s")); err != nil || paged {
		t.Fatalf("paged before the cap: %v, %v", paged, err)
	}
	// Add 7 cuts the fourth run and pages the fence.
	addMed()
	if paged, err := r.x.StreamFencePagedForTest(ctx, []byte("s")); err != nil || !paged {
		t.Fatalf("paged after the cap: %v, %v", paged, err)
	}
	if sr := r.pagedRoot("s"); len(sr.pidx) != 2 {
		t.Fatalf("transition built %d pages, want 2", len(sr.pidx))
	}
	// Adds 8-16 alternate paged tail amendments with cuts that grow the
	// tail page in place and spill fresh pages, up to a full index of
	// full pages.
	for range 9 {
		addMed()
	}
	sr := r.pagedRoot("s")
	if len(sr.pidx) != 4 {
		t.Fatalf("ladder ended on %d pages, want 4", len(sr.pidx))
	}
	for i, pe := range sr.pidx {
		if pe.count != 4 {
			t.Fatalf("page %d holds %d entries, want 4", i, pe.count)
		}
	}

	// The ladder's end: a cut has no page to go to, and the refusal
	// leaves no trace.
	_, _, err := r.x.Add(ctx, []byte("s"), xidAuto, streamID{}, r.nowMs+1, false, [][]byte{[]byte("blob"), []byte(strings.Repeat("x", 5000))})
	if !errors.Is(err, errStreamFenceThirdLevel) {
		t.Fatalf("third-level err = %v", err)
	}
	r.check("s")
	// A small entry still amends the tail run, so the stream keeps
	// serving at the wall.
	r.nowMs++
	r.add("s", xidAuto, streamID{}, "f", "v")
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	r.checkRange("s", streamID{}, full, -1, false)
	r.checkRange("s", streamID{}, full, -1, true)
}

// TestStreamPagedRangeModel compares the paged Range against the model
// over windows inside one page, across page boundaries, over interior
// pages, capped by count, and in both directions, with explicit IDs
// making the windows legible.
func TestStreamPagedRangeModel(t *testing.T) {
	defer SetStreamFenceCapsForTest(4, 3, 64)()
	r := newStreamRig(t)
	med := strings.Repeat("y", 1800)
	// Sixty entries, two per run, thirty runs across ten pages.
	for i := 1; i <= 60; i++ {
		fv := [][]byte{[]byte("n"), []byte(fmt.Sprintf("%d", i)), []byte("pad"), []byte(med)}
		if _, _, err := r.x.Add(context.Background(), []byte("s"), xidExplicit, streamID{ms: uint64(i), seq: 1}, r.nowMs, false, fv); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
		r.model = append(r.model, streamModelEnt{id: streamID{ms: uint64(i), seq: 1}, fv: fv})
	}
	r.check("s")
	if sr := r.pagedRoot("s"); len(sr.pidx) != 10 {
		t.Fatalf("scenario built %d pages, want 10", len(sr.pidx))
	}
	full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	for _, rev := range []bool{false, true} {
		r.checkRange("s", streamID{}, full, -1, rev)
		// Inside one page, mid-run bounds, and the single-entry window.
		r.checkRange("s", streamID{ms: 13}, streamID{ms: 16, seq: math.MaxUint64}, -1, rev)
		r.checkRange("s", streamID{ms: 14, seq: 1}, streamID{ms: 14, seq: 1}, -1, rev)
		// Across one page boundary and across many, with interior pages
		// answering from index counts.
		r.checkRange("s", streamID{ms: 11}, streamID{ms: 20, seq: math.MaxUint64}, -1, rev)
		r.checkRange("s", streamID{ms: 8, seq: 2}, streamID{ms: 55}, -1, rev)
		// Count caps under and over a page's worth.
		r.checkRange("s", streamID{}, full, 4, rev)
		r.checkRange("s", streamID{ms: 10}, full, 17, rev)
		// Empty windows on both sides.
		r.checkRange("s", streamID{ms: 100}, full, -1, rev)
		r.checkRange("s", streamID{}, streamID{ms: 0, seq: math.MaxUint64}, -1, rev)
	}

	// The identical windows from a cold runtime.
	if err := r.tr.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr2 := NewTiered(r.rs, TieredConfig{
		Budget:   Budget{Entries: 4096, Arenas: 64 << 20},
		PromoteP: -1,
		Seed:     14,
		NowMs:    func() int64 { return r.nowMs },
	})
	x2, err := NewStream(tr2, StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cold := &streamRig{t: t, rs: r.rs, tr: tr2, x: x2, nowMs: r.nowMs, model: r.model}
	cold.check("s")
	for _, rev := range []bool{false, true} {
		cold.checkRange("s", streamID{}, full, -1, rev)
		cold.checkRange("s", streamID{ms: 11}, streamID{ms: 20, seq: math.MaxUint64}, -1, rev)
	}
}
