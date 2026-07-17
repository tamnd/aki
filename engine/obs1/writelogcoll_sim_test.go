package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestWriteLogHashEmission drives the hash seam methods and checks the
// frame runs they emit: a creating write leads with a collnew, an
// emptying removal trails with a colldrop, a TTL-preserving write chases
// its hset with the restoring hexpire, and every multi-frame emission
// returns the run's last seq as its mark.
func TestWriteLogHashEmission(t *testing.T) {
	const node = uint64(0xD6)
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
	key := []byte("alpha")

	// Creating write: collnew then hset, seqs 1 and 2, the mark is the last.
	if g, seq, err := wl.HashSet(key, true, bs("f1", "v1", "f2", "v2"), 0); err != nil || g != 0 || seq != 2 {
		t.Fatalf("creating HashSet mark = (%d, %d, %v), want group 0 seq 2", g, seq, err)
	}
	// A deadline set: one hexpire, seq 3.
	if _, seq, err := wl.HashExpire(key, 5000, bs("f1")); err != nil || seq != 3 {
		t.Fatalf("HashExpire mark = (%d, %v), want seq 3", seq, err)
	}
	// A TTL-preserving write: hset then the restoring hexpire, seqs 4 and 5.
	if _, seq, err := wl.HashSet(key, false, bs("f1", "7"), 5000); err != nil || seq != 5 {
		t.Fatalf("preserving HashSet mark = (%d, %v), want seq 5", seq, err)
	}
	// HPERSIST: hexpire at 0, seq 6.
	if _, seq, err := wl.HashExpire(key, 0, bs("f1")); err != nil || seq != 6 {
		t.Fatalf("persist HashExpire mark = (%d, %v), want seq 6", seq, err)
	}
	// An emptying removal: hdel then colldrop, seqs 7 and 8.
	if _, seq, err := wl.HashDel(key, bs("f1", "f2"), true); err != nil || seq != 8 {
		t.Fatalf("dropping HashDel mark = (%d, %v), want seq 8", seq, err)
	}

	// Malformed pairs are the encode bug row and burn no seq: the next
	// emission still takes seq 9.
	if _, _, err := wl.HashSet(key, false, bs("odd"), 0); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("odd pair tail gave %v", err)
	}
	if _, _, err := wl.HashSet(key, false, nil, 0); err == nil || err.Error() != "ERR internal: wal encode" {
		t.Fatalf("empty pair list gave %v", err)
	}
	if _, seq, err := wl.HashDel(key, bs("f1"), false); err != nil || seq != 9 {
		t.Fatalf("HashDel after refusals = (%d, %v), want seq 9", seq, err)
	}

	wl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := wl.Marks().Wait(ctx, 0, 9); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	secs := walObject(t, store, node, 1)
	if len(secs) != 1 || secs[0].Group != 0 || len(secs[0].Frames) != 9 {
		t.Fatalf("sections = %+v, want one group 0 section of 9 frames", secs)
	}
	ops := make([]obs1.Op, 9)
	for i, f := range secs[0].Frames {
		if f.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq = %d, want %d", i, f.Seq, i+1)
		}
		if string(f.Key) != "alpha" {
			t.Fatalf("frame %d key = %q", i, f.Key)
		}
		op, err := obs1.DecodeOp(f)
		if err != nil {
			t.Fatalf("DecodeOp frame %d: %v", i, err)
		}
		ops[i] = op
	}
	if cn := ops[0].(obs1.CollNew); cn.Type != obs1.CollHash || len(cn.Hints) != 0 {
		t.Fatalf("frame 1 = %+v, want a hintless hash collnew", cn)
	}
	hs := ops[1].(obs1.CollDelta).Sub.(obs1.HSet)
	if len(hs.Pairs) != 2 || string(hs.Pairs[0].Field) != "f1" || string(hs.Pairs[1].Value) != "v2" {
		t.Fatalf("frame 2 = %+v", hs)
	}
	if he := ops[2].(obs1.CollDelta).Sub.(obs1.HExpire); he.AtMs != 5000 || len(he.Fields) != 1 {
		t.Fatalf("frame 3 = %+v", he)
	}
	if hs := ops[3].(obs1.CollDelta).Sub.(obs1.HSet); string(hs.Pairs[0].Value) != "7" {
		t.Fatalf("frame 4 = %+v", hs)
	}
	if he := ops[4].(obs1.CollDelta).Sub.(obs1.HExpire); he.AtMs != 5000 || string(he.Fields[0]) != "f1" {
		t.Fatalf("frame 5 = %+v, want the restored deadline", he)
	}
	if he := ops[5].(obs1.CollDelta).Sub.(obs1.HExpire); he.AtMs != 0 {
		t.Fatalf("frame 6 = %+v, want the cleared deadline", he)
	}
	if hd := ops[6].(obs1.CollDelta).Sub.(obs1.HDel); len(hd.Fields) != 2 {
		t.Fatalf("frame 7 = %+v", hd)
	}
	if _, ok := ops[7].(obs1.CollDrop); !ok {
		t.Fatalf("frame 8 = %+v, want a colldrop", ops[7])
	}
	if hd := ops[8].(obs1.CollDelta).Sub.(obs1.HDel); len(hd.Fields) != 1 {
		t.Fatalf("frame 9 = %+v", hd)
	}

	rows := durabilityRows(t, wl)
	if rows["wal_encode_errors"] != 2 {
		t.Fatalf("wal_encode_errors = %d, want the two refused emissions", rows["wal_encode_errors"])
	}
	if err := wl.Close(); err != nil {
		t.Fatal(err)
	}
}
