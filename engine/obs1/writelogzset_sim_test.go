package obs1_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogZSetEmission drives the sorted-set seam methods and checks
// the frame runs they emit: a creating write leads with a collnew, an
// emptying removal trails with a colldrop, a STORE replacement frames
// keydel, collnew, and the result pairs as one run, and the parallel
// score and member slices survive byte- and bit-exactly through the
// zadd encoding.
func TestWriteLogZSetEmission(t *testing.T) {
	const node = uint64(0xD8)
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

	// Creating write: collnew then zadd, seqs 1 and 2, the mark is the
	// last. A negative zero score must survive bit-exactly.
	if g, seq, err := wl.ZSetAdd(alpha, true, []float64{1.5, math.Copysign(0, -1)}, bs("m1", "m2")); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating ZSetAdd mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A plain upsert: one zadd, seq 3.
	if _, seq, err := wl.ZSetAdd(alpha, false, []float64{7}, bs("m3")); err != nil || seq != 3 {
		t.Fatalf("ZSetAdd mark = (%d, %v), want seq 3", seq, err)
	}
	// A non-emptying removal: one zrem, seq 4.
	if _, seq, err := wl.ZSetRem(alpha, bs("m1"), false); err != nil || seq != 4 {
		t.Fatalf("ZSetRem mark = (%d, %v), want seq 4", seq, err)
	}
	// An emptying removal: zrem then colldrop, seqs 5 and 6.
	if _, seq, err := wl.ZSetRem(alpha, bs("m2", "m3"), true); err != nil || seq != 6 {
		t.Fatalf("emptying ZSetRem mark = (%d, %v), want seq 6", seq, err)
	}
	// A STORE replacing a live sorted set: collnew (reset-to-empty) then
	// the result as one zadd, seqs 7 and 8.
	if _, seq, err := wl.ZSetStore(alpha, false, true, []float64{2, 3}, bs("x", "y")); err != nil || seq != 8 {
		t.Fatalf("replacing ZSetStore mark = (%d, %v), want seq 8", seq, err)
	}
	// A STORE emptying a destination that also held a shadow string:
	// keydel then colldrop, seqs 9 and 10.
	if _, seq, err := wl.ZSetStore(alpha, true, true, nil, nil); err != nil || seq != 10 {
		t.Fatalf("emptying ZSetStore mark = (%d, %v), want seq 10", seq, err)
	}

	// No-effect and malformed emissions are the encode bug row and burn
	// no seq: the next emission still takes seq 11.
	if _, _, err := wl.ZSetStore(alpha, false, false, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("no-effect ZSetStore gave %v", err)
	}
	if _, _, err := wl.ZSetAdd(alpha, false, nil, nil); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty ZSetAdd gave %v", err)
	}
	if _, _, err := wl.ZSetAdd(alpha, false, []float64{1, 2}, bs("m")); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("mismatched ZSetAdd gave %v", err)
	}
	if _, _, err := wl.ZSetAdd(alpha, false, []float64{math.NaN()}, bs("m")); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("NaN ZSetAdd gave %v", err)
	}
	if _, seq, err := wl.ZSetAdd(alpha, false, []float64{9}, bs("m9")); err != nil || seq != 11 {
		t.Fatalf("ZSetAdd after refusals = (%d, %v), want seq 11", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 11); err != nil {
		t.Fatalf("Wait group 0: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 1 {
		t.Fatalf("sections = %+v, want group 0 alone", secs)
	}
	g0 := secs[0].Frames
	if len(g0) != 11 {
		t.Fatalf("frame count = %d, want 11", len(g0))
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
	if cn := ops[0].(obs1.CollNew); cn.Type != obs1.CollZSet || len(cn.Hints) != 0 {
		t.Fatalf("frame 1 = %+v, want a hintless zset collnew", cn)
	}
	za := ops[1].(obs1.CollDelta).Sub.(obs1.ZAdd)
	if len(za.Entries) != 2 || string(za.Entries[0].Member) != "m1" || za.Entries[0].Score != 1.5 {
		t.Fatalf("frame 2 = %+v", za)
	}
	if bits := math.Float64bits(za.Entries[1].Score); bits != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatalf("frame 2 entry 2 score bits = %x, want negative zero", bits)
	}
	if za := ops[2].(obs1.CollDelta).Sub.(obs1.ZAdd); string(za.Entries[0].Member) != "m3" || za.Entries[0].Score != 7 {
		t.Fatalf("frame 3 = %+v", za)
	}
	if zr := ops[3].(obs1.CollDelta).Sub.(obs1.ZRem); string(zr.Members[0]) != "m1" {
		t.Fatalf("frame 4 = %+v", zr)
	}
	if zr := ops[4].(obs1.CollDelta).Sub.(obs1.ZRem); len(zr.Members) != 2 || string(zr.Members[1]) != "m3" {
		t.Fatalf("frame 5 = %+v", zr)
	}
	if _, ok := ops[5].(obs1.CollDrop); !ok {
		t.Fatalf("frame 6 = %+v, want a colldrop", ops[5])
	}
	// The replacing STORE: collnew over the live sorted set, then the
	// result pairs.
	if cn := ops[6].(obs1.CollNew); cn.Type != obs1.CollZSet {
		t.Fatalf("frame 7 = %+v", cn)
	}
	if za := ops[7].(obs1.CollDelta).Sub.(obs1.ZAdd); len(za.Entries) != 2 || string(za.Entries[1].Member) != "y" || za.Entries[1].Score != 3 {
		t.Fatalf("frame 8 = %+v", za)
	}
	// The emptying STORE: keydel for the shadow string, then colldrop.
	if _, ok := ops[8].(obs1.KeyDel); !ok {
		t.Fatalf("frame 9 = %+v, want a keydel", ops[8])
	}
	if _, ok := ops[9].(obs1.CollDrop); !ok {
		t.Fatalf("frame 10 = %+v, want a colldrop", ops[9])
	}
	if za := ops[10].(obs1.CollDelta).Sub.(obs1.ZAdd); string(za.Entries[0].Member) != "m9" {
		t.Fatalf("frame 11 = %+v", za)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 4 {
		t.Fatalf("wal_encode_errors = %d, want the four refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
