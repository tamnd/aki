package sqlo1

import (
	"context"
	"fmt"
	"math"
	"testing"
)

// pendingRig builds a stream with n entries at ms 1..n, a group g at
// 0, and delivers everything to c1 at the rig's starting clock, the
// shared base for the pending-surface oracles. The caps come from the
// caller so each test picks its own segment geometry.
func pendingRig(t *testing.T, key string, n int) (*streamRig, map[streamID]pelModelEnt) {
	t.Helper()
	r := newStreamRig(t)
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", fmt.Sprintf("v%d", i))
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("c1"), -1, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
		model[id] = pelModelEnt{cons: "c1", dcount: 1, dtime: r.nowMs}
	})
	if err != nil {
		t.Fatalf("ReadGroupNew: %v", err)
	}
	if len(model) != n {
		t.Fatalf("delivered %d entries, want %d", len(model), n)
	}
	return r, model
}

type pendingExtRow struct {
	id     streamID
	cons   string
	idle   int64
	dcount uint32
}

// TestStreamPendingSummaryExt proves both XPENDING read forms against
// staged deliveries: the summary's total, ID window, and name-ordered
// consumer table, and the extended form's range, count, consumer, and
// idle filters, at caps small enough that the walk crosses segments.
func TestStreamPendingSummaryExt(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 20; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", "v")
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("empty"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate empty: %v", err)
	}
	deliver := func(cons string, count int64) {
		t.Helper()
		n := 0
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte(cons), count, false, r.nowMs, func(int) {}, func(streamID, [][]byte) { n++ })
		if err != nil || int64(n) != count {
			t.Fatalf("deliver(%s, %d) = %d, %v", cons, count, n, err)
		}
	}
	// Three waves at three timestamps: c2 owns 1..10 at t, c1 owns
	// 11..16 at t+50, c3 owns 17..20 at t+100.
	deliver("c2", 10)
	r.nowMs += 50
	deliver("c1", 6)
	r.nowMs += 50
	deliver("c3", 4)

	total, minID, maxID, cons, err := r.x.PendingSummary(ctx, []byte(key), []byte("g"))
	if err != nil {
		t.Fatalf("PendingSummary: %v", err)
	}
	if total != 20 || minID != (streamID{ms: 1}) || maxID != (streamID{ms: 20}) {
		t.Fatalf("summary = %d [%v, %v]", total, minID, maxID)
	}
	wantCons := []struct {
		name string
		n    uint64
	}{{"c1", 6}, {"c2", 10}, {"c3", 4}}
	if len(cons) != len(wantCons) {
		t.Fatalf("summary consumers = %d rows, want %d", len(cons), len(wantCons))
	}
	for i, w := range wantCons {
		if string(cons[i].name) != w.name || cons[i].n != w.n {
			t.Fatalf("summary consumer %d = %s/%d, want %s/%d", i, cons[i].name, cons[i].n, w.name, w.n)
		}
	}
	// The empty group answers zero with no window and no consumers.
	total, _, _, cons, err = r.x.PendingSummary(ctx, []byte(key), []byte("empty"))
	if err != nil || total != 0 || cons != nil {
		t.Fatalf("empty summary = %d, %v, %v", total, cons, err)
	}
	if _, _, _, _, err := r.x.PendingSummary(ctx, []byte("nk"), []byte("g")); err != errStreamNoGroup {
		t.Fatalf("missing key summary err = %v", err)
	}
	if _, _, _, _, err := r.x.PendingSummary(ctx, []byte(key), []byte("ng")); err != errStreamNoGroup {
		t.Fatalf("missing group summary err = %v", err)
	}

	ext := func(start, end streamID, count int64, consumer string, minIdle int64) []pendingExtRow {
		t.Helper()
		var c []byte
		if consumer != "" {
			c = []byte(consumer)
		}
		var rows []pendingExtRow
		err := r.x.PendingExt(ctx, []byte(key), []byte("g"), start, end, count, c, minIdle, r.nowMs, func(id streamID, cons []byte, idle int64, dcount uint32) {
			rows = append(rows, pendingExtRow{id: id, cons: string(cons), idle: idle, dcount: dcount})
		})
		if err != nil {
			t.Fatalf("PendingExt: %v", err)
		}
		return rows
	}
	max := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
	full := ext(streamID{}, max, 100, "", 0)
	if len(full) != 20 {
		t.Fatalf("full walk = %d rows", len(full))
	}
	for i, row := range full {
		id := streamID{ms: uint64(i + 1)}
		wantCons, wantIdle := "c2", int64(100)
		if i >= 16 {
			wantCons, wantIdle = "c3", 0
		} else if i >= 10 {
			wantCons, wantIdle = "c1", 50
		}
		if row.id != id || row.cons != wantCons || row.idle != wantIdle || row.dcount != 1 {
			t.Fatalf("row %d = %+v, want %v %s idle=%d", i, row, id, wantCons, wantIdle)
		}
	}
	if rows := ext(streamID{ms: 5}, streamID{ms: 12}, 100, "", 0); len(rows) != 8 || rows[0].id != (streamID{ms: 5}) || rows[7].id != (streamID{ms: 12}) {
		t.Fatalf("window walk = %v", rows)
	}
	if rows := ext(streamID{}, max, 3, "", 0); len(rows) != 3 || rows[2].id != (streamID{ms: 3}) {
		t.Fatalf("count walk = %v", rows)
	}
	if rows := ext(streamID{}, max, 100, "c1", 0); len(rows) != 6 || rows[0].id != (streamID{ms: 11}) {
		t.Fatalf("consumer walk = %v", rows)
	}
	if rows := ext(streamID{}, max, 100, "nobody", 0); rows != nil {
		t.Fatalf("missing consumer walk = %v", rows)
	}
	if rows := ext(streamID{}, max, 100, "", 60); len(rows) != 10 || rows[9].id != (streamID{ms: 10}) {
		t.Fatalf("min-idle 60 walk = %v", rows)
	}
	if rows := ext(streamID{}, max, 100, "", 30); len(rows) != 16 {
		t.Fatalf("min-idle 30 walk = %d rows", len(rows))
	}
	if rows := ext(streamID{}, max, 0, "", 0); rows != nil {
		t.Fatalf("count 0 walk = %v", rows)
	}
	if rows := ext(streamID{}, max, -5, "", 0); rows != nil {
		t.Fatalf("negative count walk = %v", rows)
	}
	if err := r.x.PendingExt(ctx, []byte("nk"), []byte("g"), streamID{}, max, 10, nil, 0, r.nowMs, nil); err != errStreamNoGroup {
		t.Fatalf("missing key ext err = %v", err)
	}
}

// TestStreamClaimOracle drives XCLAIM through every pinned side rule
// against the pel model: ownership moves, the three delivery-count
// behaviors, the dtime resolution with its clamp, the min-idle
// filter, FORCE minting, the deleted-entry drop, LASTID, and the
// ghost consumer.
func TestStreamClaimOracle(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	key := "k"
	r, model := pendingRig(t, key, 12)
	ctx := context.Background()
	t0 := r.nowMs
	id := func(ms uint64) streamID { return streamID{ms: ms} }
	claim := func(cons string, minIdle int64, o *streamClaimOpts, ids ...streamID) []streamID {
		t.Helper()
		if o == nil {
			o = &streamClaimOpts{retry: -1}
		}
		got, err := r.x.Claim(ctx, []byte(key), []byte("g"), []byte(cons), minIdle, ids, o, r.nowMs)
		if err != nil {
			t.Fatalf("Claim(%s, %v): %v", cons, ids, err)
		}
		return got
	}
	eq := func(got []streamID, want ...streamID) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("claimed %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("claimed %v, want %v", got, want)
			}
		}
	}

	r.nowMs += 100
	now := r.nowMs
	// A plain claim moves ownership, bumps the count, restamps, and
	// answers the IDs in argument order.
	eq(claim("c2", 0, nil, id(3), id(2)), id(3), id(2))
	model[id(2)] = pelModelEnt{cons: "c2", dcount: 2, dtime: now}
	model[id(3)] = pelModelEnt{cons: "c2", dcount: 2, dtime: now}
	pelAudit(t, r.x, key, "g", model)

	// The min-idle filter skips per entry: 2 was just claimed so its
	// idle is zero, 4 still wears the delivery stamp.
	eq(claim("c3", 1000, nil, id(4)))
	eq(claim("c3", 50, nil, id(2), id(4)), id(4))
	model[id(4)] = pelModelEnt{cons: "c3", dcount: 2, dtime: now}
	pelAudit(t, r.x, key, "g", model)

	// JUSTID freezes the count, RETRYCOUNT overwrites it, and a wider
	// value saturates at the u32 the record stores.
	eq(claim("c3", 0, &streamClaimOpts{retry: -1, justid: true}, id(5)), id(5))
	model[id(5)] = pelModelEnt{cons: "c3", dcount: 1, dtime: now}
	eq(claim("c2", 0, &streamClaimOpts{retry: 7}, id(6)), id(6))
	model[id(6)] = pelModelEnt{cons: "c2", dcount: 7, dtime: now}
	eq(claim("c2", 0, &streamClaimOpts{retry: math.MaxUint32 + 5}, id(7)), id(7))
	model[id(7)] = pelModelEnt{cons: "c2", dcount: math.MaxUint32, dtime: now}
	pelAudit(t, r.x, key, "g", model)

	// dtime resolution: IDLE subtracts from now, TIME lands verbatim,
	// and anything outside [0, now] clamps to now.
	eq(claim("c1", 0, &streamClaimOpts{retry: -1, setIdle: true, idle: 40}, id(8)), id(8))
	model[id(8)] = pelModelEnt{cons: "c1", dcount: 2, dtime: now - 40}
	eq(claim("c1", 0, &streamClaimOpts{retry: -1, setTime: true, time: t0 + 10}, id(9)), id(9))
	model[id(9)] = pelModelEnt{cons: "c1", dcount: 2, dtime: t0 + 10}
	eq(claim("c1", 0, &streamClaimOpts{retry: -1, setTime: true, time: now + 999}, id(10)), id(10))
	model[id(10)] = pelModelEnt{cons: "c1", dcount: 2, dtime: now}
	eq(claim("c1", 0, &streamClaimOpts{retry: -1, setIdle: true, idle: -50}, id(11)), id(11))
	model[id(11)] = pelModelEnt{cons: "c1", dcount: 2, dtime: now}
	pelAudit(t, r.x, key, "g", model)

	// A live but unpending entry skips without FORCE; with FORCE the
	// minted row starts at one and the plain bump lands on top, the
	// pinned count of two, while FORCE JUSTID stays at one. A dead ID
	// never mints.
	r.add(key, xidExplicit, id(13), "f", "v13")
	r.add(key, xidExplicit, id(14), "f", "v14")
	eq(claim("c2", 0, nil, id(13)))
	eq(claim("c2", 0, &streamClaimOpts{retry: -1, force: true}, id(13)), id(13))
	model[id(13)] = pelModelEnt{cons: "c2", dcount: 2, dtime: now}
	eq(claim("c3", 0, &streamClaimOpts{retry: -1, force: true, justid: true}, id(14)), id(14))
	model[id(14)] = pelModelEnt{cons: "c3", dcount: 1, dtime: now}
	eq(claim("c2", 0, &streamClaimOpts{retry: -1, force: true}, id(999)))
	pelAudit(t, r.x, key, "g", model)

	// A pending entry whose stream entry was trimmed away drops from
	// the PEL no matter the filter, and the claiming consumer still
	// mints its ghost row.
	if n := r.trim(key, true, 0, id(2), false, -1); n != 1 {
		t.Fatalf("trim = %d, want 1", n)
	}
	eq(claim("ghost", 1_000_000, nil, id(1)))
	delete(model, id(1))
	pelAudit(t, r.x, key, "g", model)
	seen := false
	if err := r.x.FullGroupsInfo(ctx, []byte(key), -1, func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool, rows []streamPelRow, consRows [][]streamPelRow) {
		for i := range g.cons {
			if string(g.cons[i].name) == "ghost" {
				seen = true
				if g.cons[i].pel != 0 {
					t.Errorf("ghost consumer pel = %d", g.cons[i].pel)
				}
			}
		}
	}); err != nil {
		t.Fatalf("FullGroupsInfo: %v", err)
	}
	if !seen {
		t.Fatal("ghost consumer not minted")
	}

	// LASTID advances the cursor and resets the entries-read counter.
	eq(claim("c1", 0, &streamClaimOpts{retry: -1, setLast: true, last: id(50)}, id(12)), id(12))
	model[id(12)] = pelModelEnt{cons: "c1", dcount: 2, dtime: now}
	pelAudit(t, r.x, key, "g", model)
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		if g.last != id(50) || g.read != -1 {
			t.Errorf("after LASTID last=%v read=%d, want 50-0/-1", g.last, g.read)
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}
}

// TestStreamClaimForceMint drives the FORCE insert paths that build
// PEL structure from nothing: the first-ever pending entry mints the
// fence, a below-base insert moves the base, filling past the entry
// cap splits the segment, and a full fence refuses with no side
// effects.
func TestStreamClaimForceMint(t *testing.T) {
	defer SetStreamPelCapsForTest(4096, 4, 100)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for _, ms := range []uint64{10, 20, 30, 40, 50} {
		r.add(key, xidExplicit, streamID{ms: ms}, "f", "v")
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	force := func(ms uint64) {
		t.Helper()
		o := streamClaimOpts{retry: -1, force: true}
		got, err := r.x.Claim(ctx, []byte(key), []byte("g"), []byte("c1"), 0, []streamID{{ms: ms}}, &o, r.nowMs)
		if err != nil || len(got) != 1 {
			t.Fatalf("force claim %d = %v, %v", ms, got, err)
		}
		model[streamID{ms: ms}] = pelModelEnt{cons: "c1", dcount: 2, dtime: r.nowMs}
		pelAudit(t, r.x, key, "g", model)
	}
	// First-ever mint, then a base move, then fill to the cap.
	force(30)
	force(10)
	if _, minID, _, _, err := r.x.PendingSummary(ctx, []byte(key), []byte("g")); err != nil || minID != (streamID{ms: 10}) {
		t.Fatalf("after base move min = %v, %v", minID, err)
	}
	force(20)
	force(40)
	// The fifth insert overflows the four-entry cap and splits.
	force(50)
	if _, _, err := r.x.stateOf(ctx, []byte(key)); err != nil {
		t.Fatalf("stateOf: %v", err)
	}
	_, g, err := r.x.findGroup(ctx, []byte("g"))
	if err != nil || len(g.pelf) != 2 {
		t.Fatalf("after split fence = %d slots, %v", len(g.pelf), err)
	}

	// A two-slot fence at a two-entry cap with single-row pages: the
	// next split has nowhere to put its second half even paged, and
	// the refusal leaves the PEL untouched. A fresh rig, since the
	// shared one models a single stream.
	restore := SetStreamPelCapsForTest(4096, 2, 2)
	defer restore()
	defer SetStreamPelPageMaxForTest(1)()
	r2 := newStreamRig(t)
	key2 := "k2"
	for ms := uint64(1); ms <= 5; ms++ {
		r2.add(key2, xidExplicit, streamID{ms: ms}, "f", "v")
	}
	if err := r2.x.GroupCreate(ctx, []byte(key2), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate k2: %v", err)
	}
	model2 := map[streamID]pelModelEnt{}
	err = r2.x.ReadGroupNew(ctx, []byte(key2), []byte("g"), []byte("c1"), 4, false, r2.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
		model2[id] = pelModelEnt{cons: "c1", dcount: 1, dtime: r2.nowMs}
	})
	if err != nil || len(model2) != 4 {
		t.Fatalf("k2 delivery = %d, %v", len(model2), err)
	}
	o := streamClaimOpts{retry: -1, force: true}
	if _, err := r2.x.Claim(ctx, []byte(key2), []byte("g"), []byte("c1"), 0, []streamID{{ms: 5}}, &o, r2.nowMs); err != errStreamPelFenceFull {
		t.Fatalf("full-fence force claim err = %v", err)
	}
	pelAudit(t, r2.x, key2, "g", model2)
}

// TestStreamAutoClaimOracle proves the XAUTOCLAIM walk: the inclusive
// cursor and its resume, the drained 0-0, the count-times-ten attempt
// budget, the JUSTID freeze, the min-idle skip, and trimmed entries
// landing in the deleted reply while leaving the PEL.
func TestStreamAutoClaimOracle(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	key := "k"
	r, model := pendingRig(t, key, 25)
	ctx := context.Background()
	auto := func(cons string, minIdle int64, start streamID, count int64, justid bool) (streamID, []streamID, []streamID) {
		t.Helper()
		cursor, claimed, deleted, err := r.x.AutoClaim(ctx, []byte(key), []byte("g"), []byte(cons), minIdle, start, count, justid, r.nowMs)
		if err != nil {
			t.Fatalf("AutoClaim(%s, %v): %v", cons, start, err)
		}
		return cursor, claimed, deleted
	}
	ids := func(got []streamID, wantMs ...uint64) {
		t.Helper()
		if len(got) != len(wantMs) {
			t.Fatalf("got %v, want ms %v", got, wantMs)
		}
		for i, ms := range wantMs {
			if got[i] != (streamID{ms: ms}) {
				t.Fatalf("got %v, want ms %v", got, wantMs)
			}
		}
	}

	r.nowMs += 100
	// The first window claims ten and parks the cursor on the next
	// unexamined entry; the resume from that cursor drains the rest.
	cursor, claimed, deleted := auto("c2", 0, streamID{}, 10, false)
	ids(claimed, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	ids(deleted)
	if cursor != (streamID{ms: 11}) {
		t.Fatalf("cursor = %v, want 11-0", cursor)
	}
	for ms := uint64(1); ms <= 10; ms++ {
		model[streamID{ms: ms}] = pelModelEnt{cons: "c2", dcount: 2, dtime: r.nowMs}
	}
	pelAudit(t, r.x, key, "g", model)
	cursor, claimed, _ = auto("c2", 0, cursor, 100, false)
	if cursor != (streamID{}) || len(claimed) != 15 || claimed[0] != (streamID{ms: 11}) {
		t.Fatalf("resume = %v, %v", cursor, claimed)
	}
	for ms := uint64(11); ms <= 25; ms++ {
		model[streamID{ms: ms}] = pelModelEnt{cons: "c2", dcount: 2, dtime: r.nowMs}
	}
	pelAudit(t, r.x, key, "g", model)

	// JUSTID moves ownership and stamps time without a count bump.
	r.nowMs += 10
	_, claimed, _ = auto("c3", 0, streamID{}, 5, true)
	ids(claimed, 1, 2, 3, 4, 5)
	for ms := uint64(1); ms <= 5; ms++ {
		model[streamID{ms: ms}] = pelModelEnt{cons: "c3", dcount: 2, dtime: r.nowMs}
	}
	pelAudit(t, r.x, key, "g", model)

	// Nothing is idle enough: the walk drains without claiming, so the
	// cursor answers 0-0, unless the attempt budget runs out first, in
	// which case it parks where the walk stopped, entry eleven at a
	// count of one.
	cursor, claimed, _ = auto("c1", 1_000_000, streamID{}, 5, false)
	if cursor != (streamID{}) || claimed != nil {
		t.Fatalf("idle-skip walk = %v, %v", cursor, claimed)
	}
	cursor, claimed, _ = auto("c1", 1_000_000, streamID{}, 1, false)
	if cursor != (streamID{ms: 11}) || claimed != nil {
		t.Fatalf("attempt-budget walk = %v, %v", cursor, claimed)
	}
	pelAudit(t, r.x, key, "g", model)

	// Trimmed entries drop into the deleted reply and leave the PEL;
	// the survivors claim as usual.
	if n := r.trim(key, true, 0, streamID{ms: 4}, false, -1); n != 3 {
		t.Fatalf("trim = %d, want 3", n)
	}
	r.nowMs += 10
	cursor, claimed, deleted = auto("c1", 0, streamID{}, 100, false)
	ids(deleted, 1, 2, 3)
	if cursor != (streamID{}) || len(claimed) != 22 || claimed[0] != (streamID{ms: 4}) {
		t.Fatalf("post-trim walk = %v, %d claimed", cursor, len(claimed))
	}
	for ms := uint64(1); ms <= 3; ms++ {
		delete(model, streamID{ms: ms})
	}
	for ms := uint64(4); ms <= 25; ms++ {
		e := model[streamID{ms: ms}]
		model[streamID{ms: ms}] = pelModelEnt{cons: "c1", dcount: e.dcount + 1, dtime: r.nowMs}
	}
	pelAudit(t, r.x, key, "g", model)

	// A start past the tail examines nothing.
	cursor, claimed, deleted = auto("c1", 0, streamID{ms: 26}, 10, false)
	if cursor != (streamID{}) || claimed != nil || deleted != nil {
		t.Fatalf("past-tail walk = %v, %v, %v", cursor, claimed, deleted)
	}
	if _, _, _, err := r.x.AutoClaim(ctx, []byte("nk"), []byte("g"), []byte("c1"), 0, streamID{}, 10, false, r.nowMs); err != errStreamNoGroup {
		t.Fatalf("missing key err = %v", err)
	}
}
