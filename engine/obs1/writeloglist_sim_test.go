package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogListEmission drives the list seam methods and checks the
// frame runs they emit: a creating push leads with a collnew, an
// emptying pop trails with a colldrop, LTRIM frames as sided pops or one
// bare colldrop on the clamp-fail form, LREM carries ascending
// pre-removal positions, and LMOVE's three shapes ride the run layouts
// the SMOVE seam set: same-key one run pop then push, same-group one
// destination-first run, cross-group destination run then source run.
func TestWriteLogListEmission(t *testing.T) {
	const node = uint64(0xD9)
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
	// bravo lands on group 1, which is what the LMOVE arms need.
	alpha, echo, bravo := []byte("alpha"), []byte("echo"), []byte("bravo")

	// Creating push: collnew then lpush, seqs 1 and 2, the mark is the last.
	if g, seq, err := wl.ListPush(alpha, true, true, bs("v1", "v2")); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating ListPush mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A plain tail push: one rpush, seq 3.
	if _, seq, err := wl.ListPush(alpha, false, false, bs("v3")); err != nil || seq != 3 {
		t.Fatalf("ListPush mark = (%d, %v), want seq 3", seq, err)
	}
	// A non-emptying head pop of two: one lpop, seq 4.
	if _, seq, err := wl.ListPop(alpha, true, 2, false); err != nil || seq != 4 {
		t.Fatalf("ListPop mark = (%d, %v), want seq 4", seq, err)
	}
	// An emptying tail pop: rpop then colldrop, seqs 5 and 6.
	if _, seq, err := wl.ListPop(alpha, false, 1, true); err != nil || seq != 6 {
		t.Fatalf("emptying ListPop mark = (%d, %v), want seq 6", seq, err)
	}
	// An element overwrite: one lset, seq 7.
	if _, seq, err := wl.ListSet(alpha, 2, []byte("w")); err != nil || seq != 7 {
		t.Fatalf("ListSet mark = (%d, %v), want seq 7", seq, err)
	}
	// A two-sided trim: lpop then rpop as one run, seqs 8 and 9.
	if _, seq, err := wl.ListTrim(alpha, 1, 2, false); err != nil || seq != 9 {
		t.Fatalf("two-sided ListTrim mark = (%d, %v), want seq 9", seq, err)
	}
	// A tail-only trim: one rpop, seq 10.
	if _, seq, err := wl.ListTrim(alpha, 0, 3, false); err != nil || seq != 10 {
		t.Fatalf("tail ListTrim mark = (%d, %v), want seq 10", seq, err)
	}
	// The clamp-fail trim clears the list: one bare colldrop, seq 11.
	if _, seq, err := wl.ListTrim(alpha, 0, 0, true); err != nil || seq != 11 {
		t.Fatalf("clearing ListTrim mark = (%d, %v), want seq 11", seq, err)
	}
	// A non-emptying LREM: one lrem, seq 12.
	if _, seq, err := wl.ListRem(alpha, []uint32{0, 2, 5}, false); err != nil || seq != 12 {
		t.Fatalf("ListRem mark = (%d, %v), want seq 12", seq, err)
	}
	// An emptying LREM: lrem then colldrop, seqs 13 and 14.
	if _, seq, err := wl.ListRem(alpha, []uint32{1}, true); err != nil || seq != 14 {
		t.Fatalf("emptying ListRem mark = (%d, %v), want seq 14", seq, err)
	}
	// An insert at its resolved position: one lins, seq 15.
	if _, seq, err := wl.ListInsert(alpha, 3, []byte("z")); err != nil || seq != 15 {
		t.Fatalf("ListInsert mark = (%d, %v), want seq 15", seq, err)
	}
	// The same-key rotation: one run of lpop then rpush, seqs 16 and 17,
	// srcSeq 0.
	dg, ds, sg, ss, err := wl.ListMove(alpha, alpha, true, false, []byte("r"), false, false)
	if err != nil || dg != 0 || ds != 17 || ss != 0 {
		t.Fatalf("same-key ListMove = (%d, %d, %d, %d, %v), want group 0 seq 17 and no source mark", dg, ds, sg, ss, err)
	}
	// A co-located move (alpha and echo share group 0): one run of
	// collnew(echo), lpush(echo), rpop(alpha), seqs 18-20, srcSeq 0.
	dg, ds, sg, ss, err = wl.ListMove(alpha, echo, false, true, []byte("m"), false, true)
	if err != nil || dg != 0 || ds != 20 || ss != 0 {
		t.Fatalf("same-group ListMove = (%d, %d, %d, %d, %v), want group 0 seq 20 and no source mark", dg, ds, sg, ss, err)
	}
	// A cross-group move (echo group 0, bravo group 1), emptying the
	// source: the destination run buffers first (bravo seqs 1-2), then
	// the source run (echo lpop seq 21, colldrop seq 22).
	dg, ds, sg, ss, err = wl.ListMove(echo, bravo, true, false, []byte("m"), true, true)
	if err != nil || dg != 1 || ds != 2 || sg != 0 || ss != 22 {
		t.Fatalf("cross-group ListMove = (%d, %d, %d, %d, %v), want dst group 1 seq 2, src group 0 seq 22", dg, ds, sg, ss, err)
	}

	// No-effect and malformed emissions are the encode bug row and burn
	// no seq: the next emission still takes seq 23.
	if _, _, err := wl.ListTrim(alpha, 0, 0, false); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("no-effect ListTrim gave %v", err)
	}
	if _, _, err := wl.ListPush(alpha, false, true, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty ListPush gave %v", err)
	}
	if _, _, err := wl.ListPop(alpha, true, 0, false); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("zero-count ListPop gave %v", err)
	}
	if _, _, err := wl.ListRem(alpha, nil, false); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty ListRem gave %v", err)
	}
	if _, _, err := wl.ListRem(alpha, []uint32{2, 1}, false); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("descending ListRem gave %v", err)
	}
	if _, _, err := wl.ListInsert(alpha, -1, []byte("v")); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("negative ListInsert gave %v", err)
	}
	if _, seq, err := wl.ListPush(alpha, false, false, bs("v9")); err != nil || seq != 23 {
		t.Fatalf("ListPush after refusals = (%d, %v), want seq 23", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 23); err != nil {
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
	if len(g0) != 23 || len(g1) != 2 {
		t.Fatalf("frame counts = %d and %d, want 23 and 2", len(g0), len(g1))
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
	if cn := ops[0].(obs1.CollNew); cn.Type != obs1.CollList || len(cn.Hints) != 0 {
		t.Fatalf("frame 1 = %+v, want a hintless list collnew", cn)
	}
	if lp := ops[1].(obs1.CollDelta).Sub.(obs1.LPush); len(lp.Values) != 2 || string(lp.Values[0]) != "v1" {
		t.Fatalf("frame 2 = %+v", lp)
	}
	if rp := ops[2].(obs1.CollDelta).Sub.(obs1.RPush); string(rp.Values[0]) != "v3" {
		t.Fatalf("frame 3 = %+v", rp)
	}
	if lp := ops[3].(obs1.CollDelta).Sub.(obs1.LPop); lp.Count != 2 {
		t.Fatalf("frame 4 = %+v", lp)
	}
	if rp := ops[4].(obs1.CollDelta).Sub.(obs1.RPop); rp.Count != 1 {
		t.Fatalf("frame 5 = %+v", rp)
	}
	if _, ok := ops[5].(obs1.CollDrop); !ok {
		t.Fatalf("frame 6 = %+v, want a colldrop", ops[5])
	}
	if ls := ops[6].(obs1.CollDelta).Sub.(obs1.LSet); ls.Index != 2 || string(ls.Value) != "w" {
		t.Fatalf("frame 7 = %+v", ls)
	}
	// The two-sided trim's run: head pop then tail pop.
	if lp := ops[7].(obs1.CollDelta).Sub.(obs1.LPop); lp.Count != 1 {
		t.Fatalf("frame 8 = %+v", lp)
	}
	if rp := ops[8].(obs1.CollDelta).Sub.(obs1.RPop); rp.Count != 2 {
		t.Fatalf("frame 9 = %+v", rp)
	}
	if rp := ops[9].(obs1.CollDelta).Sub.(obs1.RPop); rp.Count != 3 {
		t.Fatalf("frame 10 = %+v", rp)
	}
	if _, ok := ops[10].(obs1.CollDrop); !ok {
		t.Fatalf("frame 11 = %+v, want the clamp-fail colldrop", ops[10])
	}
	if lr := ops[11].(obs1.CollDelta).Sub.(obs1.LRem); len(lr.Indices) != 3 || lr.Indices[2] != 5 {
		t.Fatalf("frame 12 = %+v", lr)
	}
	if lr := ops[12].(obs1.CollDelta).Sub.(obs1.LRem); len(lr.Indices) != 1 || lr.Indices[0] != 1 {
		t.Fatalf("frame 13 = %+v", lr)
	}
	if _, ok := ops[13].(obs1.CollDrop); !ok {
		t.Fatalf("frame 14 = %+v, want a colldrop", ops[13])
	}
	if li := ops[14].(obs1.CollDelta).Sub.(obs1.LIns); li.Index != 3 || string(li.Value) != "z" {
		t.Fatalf("frame 15 = %+v", li)
	}
	// The same-key rotation's run: pop first, push second, both on alpha.
	if string(g0[15].Key) != "alpha" || string(g0[16].Key) != "alpha" {
		t.Fatalf("rotation run keys = %q %q", g0[15].Key, g0[16].Key)
	}
	if lp := ops[15].(obs1.CollDelta).Sub.(obs1.LPop); lp.Count != 1 {
		t.Fatalf("frame 16 = %+v", lp)
	}
	if rp := ops[16].(obs1.CollDelta).Sub.(obs1.RPush); string(rp.Values[0]) != "r" {
		t.Fatalf("frame 17 = %+v", rp)
	}
	// The co-located move's run: destination frames then source frame,
	// each carrying its own key inside one section.
	if string(g0[17].Key) != "echo" || string(g0[18].Key) != "echo" || string(g0[19].Key) != "alpha" {
		t.Fatalf("move run keys = %q %q %q", g0[17].Key, g0[18].Key, g0[19].Key)
	}
	if cn := ops[17].(obs1.CollNew); cn.Type != obs1.CollList {
		t.Fatalf("frame 18 = %+v", cn)
	}
	if lp := ops[18].(obs1.CollDelta).Sub.(obs1.LPush); string(lp.Values[0]) != "m" {
		t.Fatalf("frame 19 = %+v", lp)
	}
	if rp := ops[19].(obs1.CollDelta).Sub.(obs1.RPop); rp.Count != 1 {
		t.Fatalf("frame 20 = %+v", rp)
	}
	// The cross-group move's source side: lpop then the emptying colldrop.
	if string(g0[20].Key) != "echo" || string(g0[21].Key) != "echo" {
		t.Fatalf("cross move source keys = %q %q", g0[20].Key, g0[21].Key)
	}
	if lp := ops[20].(obs1.CollDelta).Sub.(obs1.LPop); lp.Count != 1 {
		t.Fatalf("frame 21 = %+v", lp)
	}
	if _, ok := ops[21].(obs1.CollDrop); !ok {
		t.Fatalf("frame 22 = %+v, want a colldrop", ops[21])
	}
	if rp := ops[22].(obs1.CollDelta).Sub.(obs1.RPush); string(rp.Values[0]) != "v9" {
		t.Fatalf("frame 23 = %+v", rp)
	}
	// The cross-group move's destination side, on group 1's section.
	for i, f := range g1 {
		if f.Seq != uint64(i+1) || string(f.Key) != "bravo" {
			t.Fatalf("group 1 frame %d = seq %d key %q", i, f.Seq, f.Key)
		}
	}
	if cn, err := obs1.DecodeOp(g1[0]); err != nil || cn.(obs1.CollNew).Type != obs1.CollList {
		t.Fatalf("group 1 frame 1 = %+v, %v", cn, err)
	}
	if op, err := obs1.DecodeOp(g1[1]); err != nil || string(op.(obs1.CollDelta).Sub.(obs1.RPush).Values[0]) != "m" {
		t.Fatalf("group 1 frame 2 = %+v, %v", op, err)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 6 {
		t.Fatalf("wal_encode_errors = %d, want the six refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
