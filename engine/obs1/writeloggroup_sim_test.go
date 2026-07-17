package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogGroupEmission drives the consumer-group seam methods and
// checks the frame runs they emit: an MKSTREAM group create leads with a
// collnew in the same run, every sub-op frames its post-decision result
// under the stream key, and refused emissions burn no seq.
func TestWriteLogGroupEmission(t *testing.T) {
	const node = uint64(0xDA)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	wl.SetGroup(0, 1, 1)

	alpha := []byte("alpha")
	grp := []byte("g1")
	con := []byte("c1")

	// MKSTREAM create: collnew then gnew as one run, seqs 1 and 2, the
	// mark is the last.
	if g, seq, err := wl.StreamGroupNew(alpha, true, grp, 0, 0, 0, true); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating StreamGroupNew mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A plain create on an existing stream: one gnew, seq 3.
	if _, seq, err := wl.StreamGroupNew(alpha, false, []byte("g2"), 1700000000000, 4, 7, true); err != nil || seq != 3 {
		t.Fatalf("StreamGroupNew mark = (%d, %v), want seq 3", seq, err)
	}
	// SETID with an unknown lag basis: one gsetid, seq 4.
	if _, seq, err := wl.StreamGroupSetID(alpha, grp, 1700000000001, 2, 0, false); err != nil || seq != 4 {
		t.Fatalf("StreamGroupSetID mark = (%d, %v), want seq 4", seq, err)
	}
	// CREATECONSUMER that created: one gconsumernew, seq 5.
	if _, seq, err := wl.StreamConsumerNew(alpha, grp, con, 1700000000002); err != nil || seq != 5 {
		t.Fatalf("StreamConsumerNew mark = (%d, %v), want seq 5", seq, err)
	}
	// An XREADGROUP delivery of two entries: one gdeliver, seq 6.
	if _, seq, err := wl.StreamDeliver(alpha, grp, con, false, 1700000000003, []uint64{10, 10}, []uint64{0, 1}); err != nil || seq != 6 {
		t.Fatalf("StreamDeliver mark = (%d, %v), want seq 6", seq, err)
	}
	// XACK of one of them: one gack, seq 7.
	if _, seq, err := wl.StreamAck(alpha, grp, []uint64{10}, []uint64{0}); err != nil || seq != 7 {
		t.Fatalf("StreamAck mark = (%d, %v), want seq 7", seq, err)
	}
	// XCLAIM moving the survivor and dropping an id whose log entry is
	// gone: gclaim then gack as one run, seqs 8 and 9, the mark is the
	// last.
	if _, seq, err := wl.StreamClaim(alpha, grp, []byte("c2"), false, []uint64{10}, []uint64{1}, []int64{1700000000004}, []uint16{2}, []uint64{9}, []uint64{5}); err != nil || seq != 9 {
		t.Fatalf("StreamClaim mark = (%d, %v), want seq 9", seq, err)
	}
	// XNACK back to unowned: one gclaim with the unowned flag, seq 10.
	if _, seq, err := wl.StreamClaim(alpha, grp, nil, true, []uint64{10}, []uint64{1}, []int64{0}, []uint16{2}, nil, nil); err != nil || seq != 10 {
		t.Fatalf("unowned StreamClaim mark = (%d, %v), want seq 10", seq, err)
	}
	// DELCONSUMER: one gconsumerdel, seq 11.
	if _, seq, err := wl.StreamConsumerDel(alpha, grp, con); err != nil || seq != 11 {
		t.Fatalf("StreamConsumerDel mark = (%d, %v), want seq 11", seq, err)
	}
	// DESTROY that returned 1: one gdrop, seq 12.
	if _, seq, err := wl.StreamGroupDrop(alpha, grp); err != nil || seq != 12 {
		t.Fatalf("StreamGroupDrop mark = (%d, %v), want seq 12", seq, err)
	}

	// Refused emissions are the encode bug row and burn no seq: the next
	// emission still takes seq 13.
	if _, _, err := wl.StreamAck(alpha, grp, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty StreamAck gave %v", err)
	}
	if _, _, err := wl.StreamDeliver(alpha, grp, con, false, 1, []uint64{1, 2}, []uint64{0}); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("mismatched StreamDeliver gave %v", err)
	}
	if _, _, err := wl.StreamClaim(alpha, grp, con, true, []uint64{1}, []uint64{0}, []int64{5}, []uint16{1}, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("unowned StreamClaim naming a consumer gave %v", err)
	}
	if _, _, err := wl.StreamClaim(alpha, grp, con, false, nil, nil, nil, nil, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("both-halves-empty StreamClaim gave %v", err)
	}
	if _, seq, err := wl.StreamGroupDrop(alpha, []byte("g2")); err != nil || seq != 13 {
		t.Fatalf("StreamGroupDrop after refusals = (%d, %v), want seq 13", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 13); err != nil {
		t.Fatalf("Wait group 0: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 1 {
		t.Fatalf("sections = %+v, want group 0 alone", secs)
	}
	g0 := secs[0].Frames
	if len(g0) != 13 {
		t.Fatalf("frame count = %d, want 13", len(g0))
	}
	ops := make([]obs1.Op, len(g0))
	for i, f := range g0 {
		if f.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq = %d, want %d", i, f.Seq, i+1)
		}
		if string(f.Key) != "alpha" {
			t.Fatalf("frame %d key = %q, want the stream key", i, f.Key)
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
	if gn := ops[1].(obs1.GroupDelta).Sub.(obs1.GNew); string(gn.Group) != "g1" || gn.LastMs != 0 || !gn.ReadValid {
		t.Fatalf("frame 2 = %+v", gn)
	}
	gn := ops[2].(obs1.GroupDelta).Sub.(obs1.GNew)
	if string(gn.Group) != "g2" || gn.LastMs != 1700000000000 || gn.LastSeq != 4 || gn.EntriesRead != 7 || !gn.ReadValid {
		t.Fatalf("frame 3 = %+v", gn)
	}
	if gs := ops[3].(obs1.GroupDelta).Sub.(obs1.GSetID); gs.LastMs != 1700000000001 || gs.LastSeq != 2 || gs.ReadValid {
		t.Fatalf("frame 4 = %+v", gs)
	}
	if cnew := ops[4].(obs1.GroupDelta).Sub.(obs1.GConsumerNew); string(cnew.Consumer) != "c1" || cnew.SeenMs != 1700000000002 {
		t.Fatalf("frame 5 = %+v", cnew)
	}
	gd := ops[5].(obs1.GroupDelta).Sub.(obs1.GDeliver)
	if string(gd.Consumer) != "c1" || gd.NoAck || gd.TimeMs != 1700000000003 || len(gd.IDMs) != 2 || gd.IDSeq[1] != 1 {
		t.Fatalf("frame 6 = %+v", gd)
	}
	if ga := ops[6].(obs1.GroupDelta).Sub.(obs1.GAck); len(ga.IDMs) != 1 || ga.IDMs[0] != 10 || ga.IDSeq[0] != 0 {
		t.Fatalf("frame 7 = %+v", ga)
	}
	gc := ops[7].(obs1.GroupDelta).Sub.(obs1.GClaim)
	if string(gc.Consumer) != "c2" || gc.Unowned || gc.TimeMs[0] != 1700000000004 || gc.Counts[0] != 2 {
		t.Fatalf("frame 8 = %+v", gc)
	}
	if ga := ops[8].(obs1.GroupDelta).Sub.(obs1.GAck); len(ga.IDMs) != 1 || ga.IDMs[0] != 9 || ga.IDSeq[0] != 5 {
		t.Fatalf("frame 9 = %+v, want the claim path's dropped id as a gack", ga)
	}
	gc = ops[9].(obs1.GroupDelta).Sub.(obs1.GClaim)
	if !gc.Unowned || len(gc.Consumer) != 0 || gc.IDSeq[0] != 1 || gc.Counts[0] != 2 {
		t.Fatalf("frame 10 = %+v", gc)
	}
	if cd := ops[10].(obs1.GroupDelta).Sub.(obs1.GConsumerDel); string(cd.Group) != "g1" || string(cd.Consumer) != "c1" {
		t.Fatalf("frame 11 = %+v", cd)
	}
	if dr := ops[11].(obs1.GroupDelta).Sub.(obs1.GDrop); string(dr.Group) != "g1" {
		t.Fatalf("frame 12 = %+v", dr)
	}
	if dr := ops[12].(obs1.GroupDelta).Sub.(obs1.GDrop); string(dr.Group) != "g2" {
		t.Fatalf("frame 13 = %+v", dr)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 4 {
		t.Fatalf("wal_encode_errors = %d, want the four refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
