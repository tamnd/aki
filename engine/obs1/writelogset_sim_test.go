package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogSetEmission drives the set seam methods and checks the
// frame runs they emit: a creating write leads with a collnew, an
// emptying removal trails with a colldrop, a co-located SMOVE's two-key
// effect rides one atomic run while a cross-group move splits into a
// destination-first pair, and a STORE replacement frames keydel, collnew,
// and the result members as one run.
func TestWriteLogSetEmission(t *testing.T) {
	const node = uint64(0xD7)
	store := sim.New(sim.Config{})
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0)
	rig.grant(t, node, 1, 1)
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{})
	wl.SetGroup(0, 1, 1)
	wl.SetGroup(1, 1, 1)

	bs := func(ss ...string) [][]byte {
		out := make([][]byte, len(ss))
		for i, s := range ss {
			out[i] = []byte(s)
		}
		return out
	}
	// testMapKey routes by first letter: alpha and echo share group 0,
	// bravo lands on group 1, which is what the two SMOVE arms need.
	alpha, echo, bravo := []byte("alpha"), []byte("echo"), []byte("bravo")

	// Creating write: collnew then sadd, seqs 1 and 2, the mark is the last.
	if g, seq, err := wl.SetAdd(alpha, true, bs("m1", "m2")); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating SetAdd mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A plain add: one sadd, seq 3.
	if _, seq, err := wl.SetAdd(alpha, false, bs("m3")); err != nil || seq != 3 {
		t.Fatalf("SetAdd mark = (%d, %v), want seq 3", seq, err)
	}
	// A non-emptying removal: one srem, seq 4.
	if _, seq, err := wl.SetRem(alpha, bs("m1"), false); err != nil || seq != 4 {
		t.Fatalf("SetRem mark = (%d, %v), want seq 4", seq, err)
	}
	// A co-located move (alpha and echo share group 0): one run of
	// collnew(echo), sadd(echo), srem(alpha), seqs 5-7, srcSeq 0.
	dg, ds, sg, ss, err := wl.SetMove(alpha, echo, []byte("m2"), false, true)
	if err != nil || dg != 0 || ds != 7 || ss != 0 {
		t.Fatalf("same-group SetMove = (%d, %d, %d, %d, %v), want group 0 seq 7 and no source mark", dg, ds, sg, ss, err)
	}
	// A cross-group move (echo group 0, bravo group 1), emptying the
	// source: the destination run buffers first (bravo seqs 1-2), then
	// the source run (echo... alpha's group: srem seq 8, colldrop seq 9).
	dg, ds, sg, ss, err = wl.SetMove(echo, bravo, []byte("m2"), true, true)
	if err != nil || dg != 1 || ds != 2 || sg != 0 || ss != 9 {
		t.Fatalf("cross-group SetMove = (%d, %d, %d, %d, %v), want dst group 1 seq 2, src group 0 seq 9", dg, ds, sg, ss, err)
	}
	// A STORE replacing a live set: collnew (reset-to-empty) then the
	// result as one sadd, seqs 10 and 11.
	if _, seq, err := wl.SetStore(alpha, false, true, bs("x", "y")); err != nil || seq != 11 {
		t.Fatalf("replacing SetStore mark = (%d, %v), want seq 11", seq, err)
	}
	// A STORE emptying a destination that also held a shadow string:
	// keydel then colldrop, seqs 12 and 13.
	if _, seq, err := wl.SetStore(alpha, true, true, nil); err != nil || seq != 13 {
		t.Fatalf("emptying SetStore mark = (%d, %v), want seq 13", seq, err)
	}

	// No-effect emissions are the encode bug row and burn no seq: the
	// next emission still takes seq 14.
	if _, _, err := wl.SetStore(alpha, false, false, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("no-effect SetStore gave %v", err)
	}
	if _, _, err := wl.SetAdd(alpha, false, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty-member SetAdd gave %v", err)
	}
	if _, seq, err := wl.SetAdd(alpha, false, bs("m9")); err != nil || seq != 14 {
		t.Fatalf("SetAdd after refusals = (%d, %v), want seq 14", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 14); err != nil {
		t.Fatalf("Wait group 0: %v", err)
	}
	if err := wl.Marks().Wait(ctx, 1, 2); err != nil {
		t.Fatalf("Wait group 1: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 2 {
		t.Fatalf("sections = %+v, want groups 0 and 1", secs)
	}
	byGroup := map[uint16][]obs1.WALFrame{}
	for _, s := range secs {
		byGroup[s.Group] = s.Frames
	}
	g0, g1 := byGroup[0], byGroup[1]
	if len(g0) != 14 || len(g1) != 2 {
		t.Fatalf("frame counts = %d and %d, want 14 and 2", len(g0), len(g1))
	}
	ops := make([]obs1.Op, len(g0))
	for i, f := range g0 {
		if f.Seq != uint64(i+1) {
			t.Fatalf("group 0 frame %d seq = %d, want %d", i, f.Seq, i+1)
		}
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp group 0 frame %d: %v", i, err)
		}
		ops[i] = op
	}
	if cn := ops[0].(obs1.CollNew); cn.Type != obs1.CollSet || len(cn.Hints) != 0 {
		t.Fatalf("frame 1 = %+v, want a hintless set collnew", cn)
	}
	if sa := ops[1].(obs1.CollDelta).Sub.(obs1.SAdd); len(sa.Members) != 2 || string(sa.Members[0]) != "m1" {
		t.Fatalf("frame 2 = %+v", sa)
	}
	if sa := ops[2].(obs1.CollDelta).Sub.(obs1.SAdd); string(sa.Members[0]) != "m3" {
		t.Fatalf("frame 3 = %+v", sa)
	}
	if sr := ops[3].(obs1.CollDelta).Sub.(obs1.SRem); string(sr.Members[0]) != "m1" {
		t.Fatalf("frame 4 = %+v", sr)
	}
	// The co-located move's run: destination frames then source frames,
	// each carrying its own key inside one section.
	if string(g0[4].Key) != "echo" || string(g0[5].Key) != "echo" || string(g0[6].Key) != "alpha" {
		t.Fatalf("move run keys = %q %q %q", g0[4].Key, g0[5].Key, g0[6].Key)
	}
	if cn := ops[4].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("frame 5 = %+v", cn)
	}
	if sa := ops[5].(obs1.CollDelta).Sub.(obs1.SAdd); string(sa.Members[0]) != "m2" {
		t.Fatalf("frame 6 = %+v", sa)
	}
	if sr := ops[6].(obs1.CollDelta).Sub.(obs1.SRem); string(sr.Members[0]) != "m2" {
		t.Fatalf("frame 7 = %+v", sr)
	}
	// The cross-group move's source side: srem then the emptying colldrop.
	if string(g0[7].Key) != "echo" || string(g0[8].Key) != "echo" {
		t.Fatalf("cross move source keys = %q %q", g0[7].Key, g0[8].Key)
	}
	if sr := ops[7].(obs1.CollDelta).Sub.(obs1.SRem); string(sr.Members[0]) != "m2" {
		t.Fatalf("frame 8 = %+v", sr)
	}
	if _, ok := ops[8].(obs1.CollDrop); !ok {
		t.Fatalf("frame 9 = %+v, want a colldrop", ops[8])
	}
	// The replacing STORE: collnew over the live set, then the result.
	if cn := ops[9].(obs1.CollNew); cn.Type != obs1.CollSet {
		t.Fatalf("frame 10 = %+v", cn)
	}
	if sa := ops[10].(obs1.CollDelta).Sub.(obs1.SAdd); len(sa.Members) != 2 || string(sa.Members[1]) != "y" {
		t.Fatalf("frame 11 = %+v", sa)
	}
	// The emptying STORE: keydel for the shadow string, then colldrop.
	if _, ok := ops[11].(obs1.KeyDel); !ok {
		t.Fatalf("frame 12 = %+v, want a keydel", ops[11])
	}
	if _, ok := ops[12].(obs1.CollDrop); !ok {
		t.Fatalf("frame 13 = %+v, want a colldrop", ops[12])
	}
	if sa := ops[13].(obs1.CollDelta).Sub.(obs1.SAdd); string(sa.Members[0]) != "m9" {
		t.Fatalf("frame 14 = %+v", sa)
	}
	// The cross-group move's destination side, on group 1's section.
	for i, f := range g1 {
		if f.Seq != uint64(i+1) || string(f.Key) != "bravo" {
			t.Fatalf("group 1 frame %d = seq %d key %q", i, f.Seq, f.Key)
		}
	}
	if cn, err := obs1.DecodeOp(g1[0]); err != nil || cn.(obs1.CollNew).Type != obs1.CollSet {
		t.Fatalf("group 1 frame 1 = %+v, %v", cn, err)
	}
	if op, err := obs1.DecodeOp(g1[1]); err != nil || string(op.(obs1.CollDelta).Sub.(obs1.SAdd).Members[0]) != "m2" {
		t.Fatalf("group 1 frame 2 = %+v, %v", op, err)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 2 {
		t.Fatalf("wal_encode_errors = %d, want the two refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
