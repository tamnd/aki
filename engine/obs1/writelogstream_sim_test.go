package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogStreamEmission drives the stream seam methods and checks
// the frame runs they emit: a creating XADD leads with a collnew, a
// trimming XADD trails its xadd with an xtrim in the same run, XTRIM and
// XDEL frame counts and id lists post-decision, XSETID frames all three
// resulting values, and no stream emission ever frames a colldrop.
func TestWriteLogStreamEmission(t *testing.T) {
	const node = uint64(0xD9)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	wl.SetGroup(0, 1, 1)

	bs := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	alpha := []byte("alpha")

	// Creating XADD: collnew then xadd, seqs 1 and 2, the mark is the
	// last.
	if g, seq, err := wl.StreamAdd(alpha, true, 1700000000000, 0, bs("f1", "v1", "f2", "v2"), 0); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating StreamAdd mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A plain append: one xadd, seq 3.
	if _, seq, err := wl.StreamAdd(alpha, false, 1700000000000, 1, bs("f", "v"), 0); err != nil || seq != 3 {
		t.Fatalf("StreamAdd mark = (%d, %v), want seq 3", seq, err)
	}
	// A trimming append: xadd then xtrim as one run, seqs 4 and 5.
	if _, seq, err := wl.StreamAdd(alpha, false, 1700000000001, 0, bs("f", "v"), 2); err != nil || seq != 5 {
		t.Fatalf("trimming StreamAdd mark = (%d, %v), want seq 5", seq, err)
	}
	// A bare XTRIM: one xtrim, seq 6.
	if _, seq, err := wl.StreamTrim(alpha, 1); err != nil || seq != 6 {
		t.Fatalf("StreamTrim mark = (%d, %v), want seq 6", seq, err)
	}
	// An XDEL of two entries: one xdel, seq 7, and no colldrop even when
	// the deletes empty the stream, since the stream persists.
	if _, seq, err := wl.StreamDel(alpha, []uint64{1700000000000, 1700000000001}, []uint64{1, 0}); err != nil || seq != 7 {
		t.Fatalf("StreamDel mark = (%d, %v), want seq 7", seq, err)
	}
	// XSETID: one xsetid with the three resulting values, seq 8.
	if _, seq, err := wl.StreamSetID(alpha, 1800000000000, 5, 42, 1700000000001, 0); err != nil || seq != 8 {
		t.Fatalf("StreamSetID mark = (%d, %v), want seq 8", seq, err)
	}

	// No-effect and malformed emissions are the encode bug row and burn
	// no seq: the next emission still takes seq 9.
	if _, _, err := wl.StreamAdd(alpha, false, 1, 1, nil, 0); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("pairless StreamAdd gave %v", err)
	}
	if _, _, err := wl.StreamAdd(alpha, false, 1, 1, bs("f", "v", "orphan"), 0); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("odd-paired StreamAdd gave %v", err)
	}
	if _, _, err := wl.StreamTrim(alpha, 0); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("zero StreamTrim gave %v", err)
	}
	if _, _, err := wl.StreamDel(alpha, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty StreamDel gave %v", err)
	}
	if _, _, err := wl.StreamDel(alpha, []uint64{1, 2}, []uint64{0}); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("mismatched StreamDel gave %v", err)
	}
	if _, seq, err := wl.StreamAdd(alpha, false, 1700000000002, 0, bs("f9", "v9"), 0); err != nil || seq != 9 {
		t.Fatalf("StreamAdd after refusals = (%d, %v), want seq 9", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 9); err != nil {
		t.Fatalf("Wait group 0: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 1 {
		t.Fatalf("sections = %+v, want group 0 alone", secs)
	}
	g0 := secs[0].Frames
	if len(g0) != 9 {
		t.Fatalf("frame count = %d, want 9", len(g0))
	}
	ops := make([]obs1.Op, len(g0))
	for i, f := range g0 {
		if f.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq = %d, want %d", i, f.Seq, i+1)
		}
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp frame %d: %v", i, err)
		}
		ops[i] = op
	}
	if cn := ops[0].(obs1.CollNew); cn.Type != obs1.CollStream || len(cn.Hints) != 0 {
		t.Fatalf("frame 1 = %+v, want a hintless stream collnew", cn)
	}
	xa := ops[1].(obs1.CollDelta).Sub.(obs1.XAdd)
	if xa.IDMs != 1700000000000 || xa.IDSeq != 0 || len(xa.Pairs) != 2 || string(xa.Pairs[1].Field) != "f2" || string(xa.Pairs[1].Value) != "v2" {
		t.Fatalf("frame 2 = %+v", xa)
	}
	if xa := ops[2].(obs1.CollDelta).Sub.(obs1.XAdd); xa.IDSeq != 1 || string(xa.Pairs[0].Field) != "f" {
		t.Fatalf("frame 3 = %+v", xa)
	}
	if xa := ops[3].(obs1.CollDelta).Sub.(obs1.XAdd); xa.IDMs != 1700000000001 {
		t.Fatalf("frame 4 = %+v", xa)
	}
	if xt := ops[4].(obs1.CollDelta).Sub.(obs1.XTrim); xt.Count != 2 {
		t.Fatalf("frame 5 = %+v, want the trim clause's count 2", xt)
	}
	if xt := ops[5].(obs1.CollDelta).Sub.(obs1.XTrim); xt.Count != 1 {
		t.Fatalf("frame 6 = %+v", xt)
	}
	xd := ops[6].(obs1.CollDelta).Sub.(obs1.XDel)
	if len(xd.IDMs) != 2 || xd.IDMs[0] != 1700000000000 || xd.IDSeq[0] != 1 || xd.IDMs[1] != 1700000000001 || xd.IDSeq[1] != 0 {
		t.Fatalf("frame 7 = %+v", xd)
	}
	xs := ops[7].(obs1.CollDelta).Sub.(obs1.XSetID)
	if xs.LastMs != 1800000000000 || xs.LastSeq != 5 || xs.EntriesAdded != 42 || xs.MaxDelMs != 1700000000001 || xs.MaxDelSeq != 0 {
		t.Fatalf("frame 8 = %+v", xs)
	}
	if xa := ops[8].(obs1.CollDelta).Sub.(obs1.XAdd); string(xa.Pairs[0].Field) != "f9" {
		t.Fatalf("frame 9 = %+v", xa)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 5 {
		t.Fatalf("wal_encode_errors = %d, want the five refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
