package sqlo1

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// TestStreamPelSegCodec proves the kind 5 payload round-trips
// byte-identically and that decode rejects every non-canonical byte
// string, the same canonical-form law every other stream codec holds.
func TestStreamPelSegCodec(t *testing.T) {
	ents := []streamPelEnt{
		{id: streamID{ms: 5, seq: 0}, cidx: 0, dcount: 1, dtime: 1_000_000},
		{id: streamID{ms: 5, seq: 3}, cidx: 2, dcount: 7, dtime: 1_000_050},
		{id: streamID{ms: 7, seq: 2}, cidx: 255, dcount: 1, dtime: 0},
		{id: streamID{ms: 7, seq: 3}, cidx: 1, dcount: 4_000_000_000, dtime: 1 << 60},
		{id: streamID{ms: 1 << 50, seq: 9}, cidx: 0, dcount: 1, dtime: 3},
	}
	enc := appendStreamPelSeg(nil, ents)

	// The width helper prices exactly what the encoder writes.
	want := streamPelSegHdrLen
	prev := streamID{}
	for i := range ents {
		want += streamPelEntLen(prev, ents[i].id)
		prev = ents[i].id
	}
	if len(enc) != want {
		t.Fatalf("encoded %d bytes, streamPelEntLen sums to %d", len(enc), want)
	}

	dec, err := decodeStreamPelSeg(enc, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(dec) != len(ents) {
		t.Fatalf("decoded %d entries, want %d", len(dec), len(ents))
	}
	for i := range ents {
		if dec[i] != ents[i] {
			t.Fatalf("entry %d = %+v, want %+v", i, dec[i], ents[i])
		}
	}
	if re := appendStreamPelSeg(nil, dec); !bytes.Equal(re, enc) {
		t.Fatalf("re-encode differs:\n  %x\n  %x", re, enc)
	}

	// One entry with id 1-1: header 4, dms 1 byte, dseq 1 byte, then
	// the 14 fixed bytes, so the mutation offsets below are stable.
	one := appendStreamPelSeg(nil, []streamPelEnt{{id: streamID{ms: 1, seq: 1}, cidx: 3, dcount: 2, dtime: 9}})
	if len(one) != 20 {
		t.Fatalf("single-entry segment is %d bytes, want 20", len(one))
	}
	mut := func(off int, b byte) []byte {
		v := append([]byte(nil), one...)
		v[off] = b
		return v
	}
	rejects := []struct {
		name string
		v    []byte
	}{
		{"nil", nil},
		{"short header", one[:3]},
		{"zero count", mut(0, 0)},
		{"reserved header", mut(2, 1)},
		{"count over payload", mut(0, 2)},
		{"truncated entry", one[:len(one)-1]},
		{"trailing byte", append(append([]byte(nil), one...), 0)},
		{"reserved flags", mut(7, 1)},
		{"negative dtime", mut(19, 0x80)},
		{"non-minimal dms", append([]byte{1, 0, 0, 0, 0x81, 0x00}, one[5:]...)},
		{"non-advancing", func() []byte {
			v := appendStreamPelSeg(nil, []streamPelEnt{{id: streamID{ms: 1, seq: 1}}})
			v[0] = 2
			v = append(v, 0, 0)
			return append(v, make([]byte, 14)...)
		}()},
	}
	for _, tc := range rejects {
		if _, err := decodeStreamPelSeg(tc.v, nil); err == nil {
			t.Errorf("%s: decode accepted %x", tc.name, tc.v)
		}
	}

	// The writer-contract violations panic: they are encoder bugs, not
	// wire input.
	panics := func(name string, fn func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", name)
			}
		}()
		fn()
	}
	panics("empty", func() { appendStreamPelSeg(nil, nil) })
	panics("out of order", func() {
		appendStreamPelSeg(nil, []streamPelEnt{{id: streamID{ms: 2}}, {id: streamID{ms: 1}}})
	})
	panics("flags", func() { appendStreamPelSeg(nil, []streamPelEnt{{id: streamID{ms: 1}, flags: 1}}) })
	panics("negative dtime", func() { appendStreamPelSeg(nil, []streamPelEnt{{id: streamID{ms: 1}, dtime: -1}}) })
}

// pelModelEnt is the oracle's row: what one pending entry must look
// like through the real fence-and-segment read path.
type pelModelEnt struct {
	cons   string
	dcount uint32
	dtime  int64
}

// pelAudit compares group's live PEL against the model exactly: same
// ID set, same owner, same delivery bookkeeping, IDs in order, the
// consumer partition consistent with the flat rows, and the pel
// counters summing to the pending count.
func pelAudit(t *testing.T, x *Stream, key, group string, model map[streamID]pelModelEnt) {
	t.Helper()
	seen := false
	err := x.FullGroupsInfo(context.Background(), []byte(key), -1, func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool, rows []streamPelRow, consRows [][]streamPelRow) {
		if string(g.name) != group {
			return
		}
		seen = true
		if int(pending) != len(model) {
			t.Errorf("group %s pending = %d, model has %d", group, pending, len(model))
		}
		var sum uint64
		for i := range g.cons {
			sum += g.cons[i].pel
		}
		if sum != pending {
			t.Errorf("group %s consumer pel sum = %d, pending = %d", group, sum, pending)
		}
		prev := streamID{}
		flat := 0
		for _, r := range rows {
			if !prev.less(r.id) {
				t.Errorf("group %s pending rows out of order at %v", group, r.id)
			}
			prev = r.id
			m, ok := model[r.id]
			if !ok {
				t.Errorf("group %s has %v pending, model does not", group, r.id)
				continue
			}
			if string(g.cons[r.cidx].name) != m.cons || r.dcount != m.dcount || r.dtime != m.dtime {
				t.Errorf("group %s %v = %s#%d/%d, model %s#%d/%d",
					group, r.id, g.cons[r.cidx].name, r.dcount, r.dtime, m.cons, m.dcount, m.dtime)
			}
			flat++
		}
		if flat != len(model) {
			t.Errorf("group %s rendered %d rows, model has %d", group, flat, len(model))
		}
		part := 0
		for ci := range consRows {
			for _, r := range consRows[ci] {
				if r.cidx != ci {
					t.Errorf("group %s consumer partition leaks %v into %s", group, r.id, g.cons[ci].name)
				}
				part++
			}
		}
		if part != len(model) {
			t.Errorf("group %s consumer partition has %d rows, model has %d", group, part, len(model))
		}
	})
	if err != nil {
		t.Fatalf("FullGroupsInfo(%q): %v", key, err)
	}
	if !seen {
		t.Fatalf("group %s not rendered", group)
	}
}

// TestStreamPelDeliverAckOracle drives deliveries and acks against a
// model at segment caps small enough that every delivery amends a tail
// and cuts fresh segments, and every ack rewrites and drops them. The
// rig's entry check runs after the churn, the X-I3 proof: PEL traffic
// leaves the entry runs byte-identical.
func TestStreamPelDeliverAckOracle(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 60; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", fmt.Sprintf("v%d", i))
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	// A second group proves ack isolation: its PEL never moves.
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g2"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate g2: %v", err)
	}
	other := map[streamID]pelModelEnt{}
	deliver := func(group, cons string, count int64, noack bool, model map[streamID]pelModelEnt) []streamID {
		t.Helper()
		var ids []streamID
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte(group), []byte(cons), count, noack, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
			ids = append(ids, id)
		})
		if err != nil {
			t.Fatalf("ReadGroupNew(%s, %s): %v", group, cons, err)
		}
		if !noack {
			for _, id := range ids {
				model[id] = pelModelEnt{cons: cons, dcount: 1, dtime: r.nowMs}
			}
		}
		return ids
	}
	if got := deliver("g2", "z", 10, false, other); len(got) != 10 {
		t.Fatalf("g2 delivery = %d ids, want 10", len(got))
	}

	model := map[streamID]pelModelEnt{}
	rng := rand.New(rand.NewSource(7))
	cons := []string{"c1", "c2", "c3"}
	delivered := 0
	for i, n := range []int64{7, 3, 9, 12, 1, 8} {
		r.nowMs += 10
		ids := deliver("g", cons[i%3], n, false, model)
		if int64(len(ids)) != n {
			t.Fatalf("delivery %d = %d ids, want %d", i, len(ids), n)
		}
		delivered += len(ids)
		pelAudit(t, r.x, key, "g", model)
	}
	// The fresh-stream front repair makes entries-read exact, so lag is
	// added minus delivered.
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		if string(g.name) != "g" {
			return
		}
		if g.read != int64(delivered) || !lagOK || lag != int64(60-delivered) {
			t.Errorf("g read=%d lag=%d/%v, want read=%d lag=%d", g.read, lag, lagOK, delivered, 60-delivered)
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}

	// Random ack waves: each acked set answers its size, re-acking
	// answers zero, and the model tracks every removal.
	for wave := 0; wave < 6 && len(model) > 0; wave++ {
		var live []streamID
		for id := range model {
			live = append(live, id)
		}
		sort.Slice(live, func(i, j int) bool { return live[i].less(live[j]) })
		rng.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
		k := 1 + rng.Intn(len(live))
		batch := live[:k]
		n, err := r.x.Ack(ctx, []byte(key), []byte("g"), batch)
		if err != nil {
			t.Fatalf("Ack wave %d: %v", wave, err)
		}
		if n != int64(k) {
			t.Fatalf("Ack wave %d = %d, want %d", wave, n, k)
		}
		for _, id := range batch {
			delete(model, id)
		}
		if n, err := r.x.Ack(ctx, []byte(key), []byte("g"), batch); err != nil || n != 0 {
			t.Fatalf("re-Ack wave %d = %d, %v", wave, n, err)
		}
		pelAudit(t, r.x, key, "g", model)
		pelAudit(t, r.x, key, "g2", other)
	}

	// Duplicate IDs in one call count once; unpending IDs count zero.
	r.nowMs += 10
	ids := deliver("g", "c1", 2, false, model)
	dup := []streamID{ids[0], ids[0], {ms: 999999, seq: 0}}
	if n, err := r.x.Ack(ctx, []byte(key), []byte("g"), dup); err != nil || n != 1 {
		t.Fatalf("duplicate Ack = %d, %v, want 1", n, err)
	}
	delete(model, ids[0])
	pelAudit(t, r.x, key, "g", model)

	// NOACK sweeps the rest of the stream, cursor moves, PEL does not.
	r.add(key, xidExplicit, streamID{ms: 61}, "f", "v61")
	r.nowMs += 10
	if got := deliver("g", "c2", -1, true, model); len(got) != 19 || got[len(got)-1] != (streamID{ms: 61}) {
		t.Fatalf("NOACK delivery = %v", got)
	}
	pelAudit(t, r.x, key, "g", model)

	// A fresh multi-segment batch, then ack everything: the fence
	// empties and every segment key dies.
	for i := 62; i <= 80; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", "v")
	}
	r.nowMs += 10
	deliver("g", "c3", -1, false, model)
	pelAudit(t, r.x, key, "g", model)
	_, g0, err := r.x.findGroup(ctx, []byte("g"))
	if err != nil {
		t.Fatalf("findGroup: %v", err)
	}
	segids := make([]uint64, 0, len(g0.pelf))
	for _, fe := range g0.pelf {
		segids = append(segids, fe.segid)
	}
	if len(segids) < 2 {
		t.Fatalf("only %d segments live before the final ack; the caps did not bind", len(segids))
	}
	var rest []streamID
	for id := range model {
		rest = append(rest, id)
	}
	if n, err := r.x.Ack(ctx, []byte(key), []byte("g"), rest); err != nil || n != int64(len(rest)) {
		t.Fatalf("final Ack = %d, %v, want %d", n, err, len(rest))
	}
	pelAudit(t, r.x, key, "g", map[streamID]pelModelEnt{})
	pelAudit(t, r.x, key, "g2", other)
	if _, g1, err := r.x.findGroup(ctx, []byte("g")); err != nil || len(g1.pelf) != 0 {
		t.Fatalf("fence after full ack = %d slots, %v", len(g1.pelf), err)
	}
	var kbuf [SubkeySize]byte
	for _, segid := range segids {
		putStreamPelKey(kbuf[:], r.x.root.rooth, segid)
		if _, ok, err := r.tr.Get(ctx, kbuf[:]); err != nil || ok {
			t.Fatalf("segment %d survived the full ack (ok=%v err=%v)", segid, ok, err)
		}
	}
	r.check(key)
}

// TestStreamPelHistoryOracle drives the specific-ID form: a consumer
// re-reads only its own pending entries above the exclusive start,
// every re-read bumps the delivery bookkeeping, a trimmed pending
// entry surfaces as missing, and an unknown consumer auto-creates.
func TestStreamPelHistoryOracle(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 8; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", fmt.Sprintf("v%d", i))
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	deliverTo := func(cons string, count int64) {
		t.Helper()
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte(cons), count, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
			model[id] = pelModelEnt{cons: cons, dcount: 1, dtime: r.nowMs}
		})
		if err != nil {
			t.Fatalf("ReadGroupNew(%s): %v", cons, err)
		}
	}
	deliverTo("c1", 5)
	r.nowMs += 5
	deliverTo("c2", 3)

	type hrow struct {
		id      streamID
		val     string
		missing bool
	}
	history := func(cons string, after streamID, count int64) []hrow {
		t.Helper()
		var rows []hrow
		err := r.x.ReadGroupHistory(ctx, []byte(key), []byte("g"), []byte(cons), after, count, r.nowMs, func(int) {}, func(id streamID, fv [][]byte, missing bool) {
			h := hrow{id: id, missing: missing}
			if !missing {
				h.val = string(fv[1])
			}
			rows = append(rows, h)
		})
		if err != nil {
			t.Fatalf("ReadGroupHistory(%s): %v", cons, err)
		}
		return rows
	}
	bump := func(cons string, ids ...uint64) {
		t.Helper()
		for _, ms := range ids {
			id := streamID{ms: ms}
			m := model[id]
			if m.cons != cons {
				t.Fatalf("bump %v owned by %s, not %s", id, m.cons, cons)
			}
			m.dcount++
			m.dtime = r.nowMs
			model[id] = m
		}
	}

	// c1 from zero sees its five, values intact, all bumped to 2.
	r.nowMs += 5
	got := history("c1", streamID{}, -1)
	if len(got) != 5 {
		t.Fatalf("c1 history = %d rows, want 5", len(got))
	}
	for i, h := range got {
		want := hrow{id: streamID{ms: uint64(i + 1)}, val: fmt.Sprintf("v%d", i+1)}
		if h != want {
			t.Fatalf("c1 history[%d] = %+v, want %+v", i, h, want)
		}
	}
	bump("c1", 1, 2, 3, 4, 5)
	pelAudit(t, r.x, key, "g", model)

	// The start is exclusive, COUNT windows the walk, and only the
	// returned rows bump.
	r.nowMs += 5
	got = history("c1", streamID{ms: 2}, 2)
	if len(got) != 2 || got[0].id != (streamID{ms: 3}) || got[1].id != (streamID{ms: 4}) {
		t.Fatalf("c1 history after 2 count 2 = %+v", got)
	}
	bump("c1", 3, 4)
	pelAudit(t, r.x, key, "g", model)

	// c2 sees only its own; c1's entries never leak.
	r.nowMs += 5
	got = history("c2", streamID{}, -1)
	if len(got) != 3 || got[0].id != (streamID{ms: 6}) || got[2].id != (streamID{ms: 8}) {
		t.Fatalf("c2 history = %+v", got)
	}
	bump("c2", 6, 7, 8)
	pelAudit(t, r.x, key, "g", model)

	// An unknown consumer answers empty but lands in the table.
	r.nowMs += 5
	if got := history("c3", streamID{}, -1); len(got) != 0 {
		t.Fatalf("c3 history = %+v, want none", got)
	}
	found := false
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		for i := range g.cons {
			c := &g.cons[i]
			if string(c.name) == "c3" {
				found = true
				if c.seenMs != r.nowMs || c.activeMs != -1 || c.pel != 0 {
					t.Errorf("c3 = seen=%d active=%d pel=%d", c.seenMs, c.activeMs, c.pel)
				}
			}
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}
	if !found {
		t.Fatalf("history did not auto-create c3")
	}

	// Trimming a pending entry leaves its PEL row; history surfaces it
	// as missing and still bumps it.
	if _, err := r.x.Trim(ctx, []byte(key), true, 0, streamID{ms: 2}, false, 0); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	r.model = r.model[1:]
	r.nowMs += 5
	got = history("c1", streamID{}, 1)
	if len(got) != 1 || !got[0].missing || got[0].id != (streamID{ms: 1}) {
		t.Fatalf("trimmed history row = %+v", got)
	}
	bump("c1", 1)
	pelAudit(t, r.x, key, "g", model)
	r.check(key)
}

// TestStreamPelFenceRefusal proves the two refusals are side-effect
// free: the fence page index cap and the u8 consumer index both refuse
// the delivery before any state moves. Single-row pages make the index
// bind at the same two-slot point the inline cap used to.
func TestStreamPelFenceRefusal(t *testing.T) {
	defer SetStreamPelCapsForTest(4096, 2, 2)()
	defer SetStreamPelPageMaxForTest(1)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 6; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", "v")
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("c1"), 4, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
		model[id] = pelModelEnt{cons: "c1", dcount: 1, dtime: r.nowMs}
	})
	if err != nil {
		t.Fatalf("fill delivery: %v", err)
	}
	if len(model) != 4 {
		t.Fatalf("fill delivered %d, want 4", len(model))
	}

	// Two segments of two entries fill the two fence slots; the next
	// delivery needs a third and refuses whole.
	if err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("c1"), 1, false, r.nowMs, func(int) {}, func(streamID, [][]byte) {}); err != errStreamPelFenceFull {
		t.Fatalf("overflow delivery err = %v, want errStreamPelFenceFull", err)
	}
	pelAudit(t, r.x, key, "g", model)
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		if g.last != (streamID{ms: 4}) {
			t.Errorf("refused delivery moved last to %v", g.last)
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}

	// Acking a slot free lets the same delivery through.
	if n, err := r.x.Ack(ctx, []byte(key), []byte("g"), []streamID{{ms: 1}, {ms: 2}}); err != nil || n != 2 {
		t.Fatalf("freeing Ack = %d, %v", n, err)
	}
	delete(model, streamID{ms: 1})
	delete(model, streamID{ms: 2})
	err = r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("c1"), 1, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
		model[id] = pelModelEnt{cons: "c1", dcount: 1, dtime: r.nowMs}
	})
	if err != nil {
		t.Fatalf("post-ack delivery: %v", err)
	}
	pelAudit(t, r.x, key, "g", model)
	r.check(key)
}

// TestStreamPelConsumerCap proves consumer index 255 delivers and 256
// refuses with the dedicated error, before any state moves.
func TestStreamPelConsumerCap(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	r.add(key, xidExplicit, streamID{ms: 1}, "f", "v")
	r.add(key, xidExplicit, streamID{ms: 2}, "f", "v")
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	for i := range 255 {
		if _, err := r.x.GroupCreateConsumer(ctx, []byte(key), []byte("g"), fmt.Appendf(nil, "c%03d", i), r.nowMs); err != nil {
			t.Fatalf("GroupCreateConsumer %d: %v", i, err)
		}
	}
	// The 256th consumer takes index 255, the last that fits.
	n := 0
	err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("edge"), 1, false, r.nowMs, func(k int) { n = k }, func(streamID, [][]byte) {})
	if err != nil || n != 1 {
		t.Fatalf("index 255 delivery = %d, %v", n, err)
	}
	if err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte("over"), 1, false, r.nowMs, func(int) {}, func(streamID, [][]byte) {}); err != errStreamPelConsumerCap {
		t.Fatalf("index 256 delivery err = %v, want errStreamPelConsumerCap", err)
	}
	if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		if g.last != (streamID{ms: 1}) || len(g.cons) != 256 {
			t.Errorf("refused delivery left last=%v cons=%d", g.last, len(g.cons))
		}
	}); err != nil {
		t.Fatalf("GroupsInfo: %v", err)
	}
}

// TestStreamPelDelConsumerSweep proves XGROUP DELCONSUMER discards the
// victim's pending entries across segments and reindexes the owners
// above it, so the survivors' rows still name the right consumers.
func TestStreamPelDelConsumerSweep(t *testing.T) {
	defer SetStreamPelCapsForTest(96, 1024, 100)()
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 24; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", "v")
	}
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("g"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate: %v", err)
	}
	model := map[streamID]pelModelEnt{}
	deliverTo := func(cons string, count int64) {
		t.Helper()
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte("g"), []byte(cons), count, false, r.nowMs, func(int) {}, func(id streamID, fv [][]byte) {
			model[id] = pelModelEnt{cons: cons, dcount: 1, dtime: r.nowMs}
		})
		if err != nil {
			t.Fatalf("ReadGroupNew(%s): %v", cons, err)
		}
	}
	// Interleave so every segment mixes owners.
	for range 4 {
		deliverTo("a", 3)
		deliverTo("b", 2)
		deliverTo("c", 1)
	}
	pelAudit(t, r.x, key, "g", model)

	// Deleting a, index 0, forces the reindex of everyone above it.
	n, err := r.x.GroupDelConsumer(ctx, []byte(key), []byte("g"), []byte("a"))
	if err != nil || n != 12 {
		t.Fatalf("DelConsumer a = %d, %v, want 12", n, err)
	}
	for id, m := range model {
		if m.cons == "a" {
			delete(model, id)
		}
	}
	pelAudit(t, r.x, key, "g", model)

	// Deleting the rest empties the fence and the segments.
	if n, err := r.x.GroupDelConsumer(ctx, []byte(key), []byte("g"), []byte("b")); err != nil || n != 8 {
		t.Fatalf("DelConsumer b = %d, %v, want 8", n, err)
	}
	if n, err := r.x.GroupDelConsumer(ctx, []byte(key), []byte("g"), []byte("c")); err != nil || n != 4 {
		t.Fatalf("DelConsumer c = %d, %v, want 4", n, err)
	}
	pelAudit(t, r.x, key, "g", map[streamID]pelModelEnt{})
	if _, g1, err := r.x.findGroup(ctx, []byte("g")); err != nil || len(g1.pelf) != 0 {
		t.Fatalf("fence after sweep = %d slots, %v", len(g1.pelf), err)
	}
	r.check(key)
}

// TestStreamPelEntriesReadRepair pins the entries-read edge repairs
// against the 8.8 behavior: a fresh group rebases at the front, a
// mid-stream cursor stays unknown until a delivery reaches the tail,
// and a trimmed front still rebases when no tombstone blocks it.
func TestStreamPelEntriesReadRepair(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	key := "k"
	for i := 1; i <= 5; i++ {
		r.add(key, xidExplicit, streamID{ms: uint64(i)}, "f", "v")
	}
	readOf := func(group string) (int64, int64, bool) {
		t.Helper()
		var read, lag int64
		var lagOK bool
		if err := r.x.GroupsInfo(ctx, []byte(key), func(int) {}, func(g *streamGroup, pending uint64, l int64, lok bool) {
			if string(g.name) == group {
				read, lag, lagOK = g.read, l, lok
			}
		}); err != nil {
			t.Fatalf("GroupsInfo: %v", err)
		}
		return read, lag, lagOK
	}
	deliver := func(group string, count int64) int {
		t.Helper()
		n := 0
		err := r.x.ReadGroupNew(ctx, []byte(key), []byte(group), []byte("c"), count, true, r.nowMs, func(k int) { n = k }, func(streamID, [][]byte) {})
		if err != nil {
			t.Fatalf("ReadGroupNew(%s): %v", group, err)
		}
		return n
	}

	// From zero on an untrimmed stream: the front rebase makes the
	// counter exact immediately.
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("gz"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate gz: %v", err)
	}
	if n := deliver("gz", 2); n != 2 {
		t.Fatalf("gz delivery = %d", n)
	}
	if read, lag, lagOK := readOf("gz"); read != 2 || lag != 3 || !lagOK {
		t.Fatalf("gz read=%d lag=%d/%v, want 2/3", read, lag, lagOK)
	}

	// Mid-stream: unknown until the delivery reaches the tail, then
	// pinned to entries-added.
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("gm"), true, streamID{ms: 3}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate gm: %v", err)
	}
	if n := deliver("gm", 1); n != 1 {
		t.Fatalf("gm delivery = %d", n)
	}
	if read, _, lagOK := readOf("gm"); read != -1 || lagOK {
		t.Fatalf("gm mid-stream read=%d lagOK=%v, want unknown", read, lagOK)
	}
	if n := deliver("gm", -1); n != 1 {
		t.Fatalf("gm tail delivery = %d", n)
	}
	if read, lag, lagOK := readOf("gm"); read != 5 || lag != 0 || !lagOK {
		t.Fatalf("gm tail read=%d lag=%d/%v, want 5/0", read, lag, lagOK)
	}

	// A trimmed front with the tombstone below the first entry still
	// rebases: three live of five added, cursor at zero.
	if _, err := r.x.Trim(ctx, []byte(key), true, 0, streamID{ms: 3}, false, 0); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	r.model = r.model[2:]
	if err := r.x.GroupCreate(ctx, []byte(key), []byte("gt"), true, streamID{}, false, false, -1); err != nil {
		t.Fatalf("GroupCreate gt: %v", err)
	}
	if n := deliver("gt", 1); n != 1 {
		t.Fatalf("gt delivery = %d", n)
	}
	if read, lag, lagOK := readOf("gt"); read != 3 || lag != 2 || !lagOK {
		t.Fatalf("gt read=%d lag=%d/%v, want 3/2", read, lag, lagOK)
	}
	r.check(key)
}
