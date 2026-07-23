package sqlo1

import (
	"context"
	"encoding/binary"
	"testing"
)

// TestStreamPelPageCodec proves the kind 6 payload round-trips and the
// decoder rejects every malformed shape: a short buffer, a nonzero
// reserved field, an empty page, a length that does not match the row
// count, an empty row, an unminted segid, and out-of-order bases.
func TestStreamPelPageCodec(t *testing.T) {
	ents := []streamPelFenceEnt{
		{base: streamID{ms: 1}, segid: 3, count: 2},
		{base: streamID{ms: 5, seq: 7}, segid: 9, count: 1},
		{base: streamID{ms: 8}, segid: 4, count: 6},
	}
	v := appendStreamPelFencePage(nil, ents)
	got, sum, err := decodeStreamPelFencePage(v, 100, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 || got[0] != ents[0] || got[1] != ents[1] || got[2] != ents[2] {
		t.Fatalf("round trip = %+v", got)
	}
	if sum != 9 {
		t.Fatalf("sum = %d, want 9", sum)
	}

	reject := func(name string, mut func(b []byte) []byte, nextSegid uint64) {
		t.Helper()
		b := mut(append([]byte(nil), v...))
		if _, _, err := decodeStreamPelFencePage(b, nextSegid, nil); err == nil {
			t.Errorf("%s: decode accepted", name)
		}
	}
	reject("short header", func(b []byte) []byte { return b[:3] }, 100)
	reject("reserved", func(b []byte) []byte { b[2] = 1; return b }, 100)
	reject("empty", func(b []byte) []byte {
		binary.LittleEndian.PutUint16(b, 0)
		return b[:streamPelPageHdrLen]
	}, 100)
	reject("length short", func(b []byte) []byte { return b[:len(b)-1] }, 100)
	reject("length long", func(b []byte) []byte { return append(b, 0) }, 100)
	reject("zero count", func(b []byte) []byte {
		binary.LittleEndian.PutUint32(b[streamPelPageHdrLen+24:], 0)
		return b
	}, 100)
	reject("unminted segid", func(b []byte) []byte { return b }, 9)
	reject("out of order", func(b []byte) []byte {
		binary.LittleEndian.PutUint64(b[streamPelPageHdrLen+streamPelFenceEntLen:], 0)
		return b
	}, 100)

	panics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", name)
			}
		}()
		fn()
	}
	panics("empty page", func() { appendStreamPelFencePage(nil, nil) })
	panics("zero count", func() {
		appendStreamPelFencePage(nil, []streamPelFenceEnt{{base: streamID{ms: 1}, segid: 1}})
	})
	panics("out of order", func() {
		appendStreamPelFencePage(nil, []streamPelFenceEnt{
			{base: streamID{ms: 2}, segid: 1, count: 1},
			{base: streamID{ms: 1}, segid: 2, count: 1},
		})
	})
}

// TestStreamGroupRecPagedCodec proves the paged flag round-trips
// through the group record: the top bit of pel_fence_n marks the
// trailing rows as the page index, and a paged record with no index
// rows rejects.
func TestStreamGroupRecPagedCodec(t *testing.T) {
	g := streamGroup{
		name: []byte("g"),
		last: streamID{ms: 9},
		read: 4,
		cons: []streamConsumer{{name: []byte("c"), pel: 5, seenMs: 1, activeMs: 1}},
		pelIdx: []streamPelFenceEnt{
			{base: streamID{ms: 1}, segid: 11, count: 3},
			{base: streamID{ms: 4}, segid: 12, count: 2},
		},
		pelPaged: true,
	}
	v := appendStreamGroup(nil, &g)
	got, err := decodeStreamGroup(v, nil, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.pelPaged || len(got.pelIdx) != 2 || len(got.pelf) != 0 {
		t.Fatalf("paged decode = paged %v idx %d pelf %d", got.pelPaged, len(got.pelIdx), len(got.pelf))
	}
	if got.pelIdx[0] != g.pelIdx[0] || got.pelIdx[1] != g.pelIdx[1] {
		t.Fatalf("idx rows = %+v", got.pelIdx)
	}

	// The paged flag with a zero row count is meaningless: a paged
	// fence always holds at least one page.
	bad := appendStreamGroup(nil, &streamGroup{
		name: []byte("g"),
		cons: []streamConsumer{{name: []byte("c"), seenMs: 1, activeMs: -1}},
	})
	// pel_fence_n sits 26 bytes into the leading fixed header.
	binary.LittleEndian.PutUint16(bad[26:], 0x8000)
	if _, err := decodeStreamGroup(bad, nil, nil); err == nil {
		t.Fatal("paged record with no index rows accepted")
	}
}

// pelPageState settles the hot tier and reports the group's paged
// flag, the live kind 5 and kind 6 subkey counts on the store, and the
// materialized fence and index sizes, the orphan-free oracle every
// ladder step asserts against.
func (r *streamRig) pelPageState(key, group string) (paged bool, segs, pages, slots, idx int) {
	r.t.Helper()
	ctx := context.Background()
	if _, _, err := r.x.stateOf(ctx, []byte(key)); err != nil {
		r.t.Fatalf("stateOf: %v", err)
	}
	_, g0, err := r.x.findGroup(ctx, []byte(group))
	if err != nil {
		r.t.Fatalf("findGroup: %v", err)
	}
	g := r.x.copyGroupOwned(&g0)
	if err := r.x.pelfLoad(ctx, &g); err != nil {
		r.t.Fatalf("pelfLoad: %v", err)
	}
	paged, slots, idx = g.pelPaged, len(g.pelf), len(g.pelIdx)
	if err := r.tr.Flush(ctx); err != nil {
		r.t.Fatalf("Flush: %v", err)
	}
	if _, err := r.rs.MemStore.Scan(ctx, nil, func(rec Record) bool {
		if len(rec.Key) == SubkeySize {
			switch rec.Key[8] {
			case streamSubkindPelSeg:
				segs++
			case streamSubkindPelPage:
				pages++
			}
		}
		return true
	}); err != nil {
		r.t.Fatalf("Scan: %v", err)
	}
	return paged, segs, pages, slots, idx
}

// TestStreamPelPagedLadder drives the fence through its whole life at
// dialed caps against the flat model oracle: inline growth, the
// transition into pages, reads and claims over the paged fence, the
// side-effect free page index refusal, ack shrink with page drops, the
// flip back inline at half the cap, and a destroy that leaves no
// orphan segment or page. Every settle point cross-checks the store
// for orphans: the live kind 5 and 6 counts must match the fence the
// record references.
func TestStreamPelPagedLadder(t *testing.T) {
	defer SetStreamPelCapsForTest(4096, 3, 4)()
	defer SetStreamPelPageMaxForTest(2)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for ms := uint64(1); ms <= 40; ms++ {
		r.add(key, xidExplicit, streamID{ms: ms}, "f", "v")
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	deliver := func(cons string, count int64) {
		t.Helper()
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte(cons), count, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
			model[id] = pelModelEnt{cons: cons, dcount: 1, dtime: r.nowMs}
		})
		if err != nil {
			t.Fatalf("deliver %d: %v", count, err)
		}
		pelAudit(t, r.x, key, "g", model)
	}
	state := func(wantPaged bool, wantSlots int) {
		t.Helper()
		paged, segs, pages, slots, idx := r.pelPageState(key, "g")
		if paged != wantPaged || slots != wantSlots {
			t.Fatalf("state = paged %v slots %d, want %v %d", paged, slots, wantPaged, wantSlots)
		}
		if segs != slots {
			t.Fatalf("store holds %d segments, fence has %d slots", segs, slots)
		}
		if !paged && (pages != 0 || idx != 0) {
			t.Fatalf("inline fence with %d pages on store, idx %d", pages, idx)
		}
		if paged && pages != idx {
			t.Fatalf("store holds %d pages, index references %d", pages, idx)
		}
	}

	// Twelve entries cut four 3-entry segments, the inline cap exactly.
	deliver("c1", 12)
	state(false, 4)

	// The fifth segment transitions the fence into pages: five slots
	// re-chunk into three 2-row pages.
	deliver("c1", 3)
	state(true, 5)

	// Grow to the index cap: seven slots (the kept short page from the
	// transition fragments the index, so the cap binds here).
	deliver("c2", 6)
	state(true, 7)

	// The next segment needs a fifth page and the index refuses whole:
	// the model is untouched and the group's cursor did not move.
	err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("c1"), 3, false, r.nowMs, func(int) {}, func(streamID, [][]byte) {})
	if err != errStreamPelFenceFull {
		t.Fatalf("overflow deliver err = %v, want errStreamPelFenceFull", err)
	}
	pelAudit(t, r.x, key, "g", model)
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		if g.last != (streamID{ms: 21}) {
			t.Errorf("refused delivery moved last to %v", g.last)
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}
	state(true, 7)

	// The pending surface reads through the pages: summary bounds and
	// an extended walk over the whole window.
	total, minID, maxID, _, err := r.x.PendingSummary(ctx, []byte(key), []byte("g"))
	if err != nil || total != 21 || minID != (streamID{ms: 1}) || maxID != (streamID{ms: 21}) {
		t.Fatalf("summary = %d %v..%v, %v", total, minID, maxID, err)
	}
	walked := 0
	if err := r.x.PendingExt(ctx, []byte(key), []byte("g"), streamID{}, streamID{ms: 40}, 100, nil, 0, r.nowMs, func(id streamID, cons []byte, idle int64, dcount uint32) {
		walked++
	}); err != nil {
		t.Fatalf("PendingExt: %v", err)
	}
	if walked != 21 {
		t.Fatalf("extended walk saw %d rows, want 21", walked)
	}

	// An ack inside an interior segment shrinks its slot in place; the
	// page rewrites and the count stays exact.
	ack := func(ids ...uint64) {
		t.Helper()
		sids := make([]streamID, len(ids))
		for i, ms := range ids {
			sids[i] = streamID{ms: ms}
			delete(model, streamID{ms: ms})
		}
		if n, err := r.x.Ack(ctx, []byte(key), []byte("g"), sids); err != nil || int(n) != len(ids) {
			t.Fatalf("Ack(%v) = %d, %v", ids, n, err)
		}
		pelAudit(t, r.x, key, "g", model)
	}
	ack(8)
	state(true, 7)

	// Killing two whole segments drops their slots and re-chunks the
	// middle; the dead pages leave the store.
	ack(4, 5, 6, 10, 11, 12)
	state(true, 5)

	// A claim over the paged fence moves ownership through the shared
	// finalize pass; a FORCE of the acked entry re-mints its row into
	// an existing segment.
	o := streamClaimOpts{retry: -1, force: true}
	got, err := r.x.Claim(ctx, []byte(key), []byte("g"), []byte("c2"), 0, []streamID{{ms: 8}, {ms: 13}}, &o, r.nowMs)
	if err != nil || len(got) != 2 {
		t.Fatalf("claim = %v, %v", got, err)
	}
	model[streamID{ms: 8}] = pelModelEnt{cons: "c2", dcount: 2, dtime: r.nowMs}
	model[streamID{ms: 13}] = pelModelEnt{cons: "c2", dcount: 2, dtime: r.nowMs}
	pelAudit(t, r.x, key, "g", model)

	// An autoclaim walks the paged fence with its cursor.
	_, claimed, _, err := r.x.AutoClaim(ctx, []byte(key), []byte("g"), []byte("c1"), 0, streamID{}, 5, true, r.nowMs)
	if err != nil || len(claimed) != 5 {
		t.Fatalf("autoclaim = %v, %v", claimed, err)
	}
	for _, id := range claimed {
		m := model[id]
		m.cons = "c1"
		m.dtime = r.nowMs
		model[id] = m
	}
	pelAudit(t, r.x, key, "g", model)

	// Acking four whole segments leaves one slot, at or under half the
	// cap: the fence flips back inline and every page dies.
	ack(1, 2, 3, 7, 8, 9, 13, 14, 15, 16, 17, 18)
	state(false, 1)

	// Regrow into pages, then destroy: no segment or page survives.
	deliver("c1", 12)
	state(true, 5)
	destroyed, err := r.x.GroupDestroy(ctx, []byte(key), []byte("g"))
	if err != nil || !destroyed {
		t.Fatalf("GroupDestroy = %v, %v", destroyed, err)
	}
	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	segs, pages := 0, 0
	if _, err := r.rs.MemStore.Scan(ctx, nil, func(rec Record) bool {
		if len(rec.Key) == SubkeySize {
			switch rec.Key[8] {
			case streamSubkindPelSeg:
				segs++
			case streamSubkindPelPage:
				pages++
			}
		}
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if segs != 0 || pages != 0 {
		t.Fatalf("destroy left %d segments and %d pages", segs, pages)
	}
	r.check(key)
}
