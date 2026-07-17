package obs1_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// TestFlusherAppendRun proves a multi-frame run appends atomically: the
// frames land contiguous in one object's section, a mid-run encode error
// leaves the buffer exactly as it was, and admission judges the run's
// total against the cap, all or nothing.
func TestFlusherAppendRun(t *testing.T) {
	const node = uint64(11)
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: node,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()

	run := []obs1.WALFrame{
		opFrame(t, 3, 1, "h", obs1.CollNew{Type: obs1.CollHash}),
		opFrame(t, 3, 2, "h", obs1.CollDelta{Sub: obs1.HSet{
			Pairs: []obs1.FieldValue{{Field: []byte("f"), Value: []byte("v")}},
		}}),
	}
	if err := fl.AppendRun(2, 1, run); err != nil {
		t.Fatal(err)
	}
	// A single append rides the same open buffer behind the run.
	if err := fl.AppendOp(2, 1, opFrame(t, 3, 3, "h", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	fl.Barrier()
	waitCall(t, snk)
	secs := walObject(t, store, node, 1)
	if len(secs) != 1 || secs[0].Group != 2 || len(secs[0].Frames) != 3 {
		t.Fatalf("object 1 sections = %+v, want one group 2 section of 3 frames", secs)
	}
	for i, f := range secs[0].Frames {
		if f.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq = %d, want %d", i, f.Seq, i+1)
		}
	}

	// Rejects: an empty run, seqs not strictly increasing within the run,
	// and a run head at or below the group's high seq.
	if err := fl.AppendRun(2, 1, nil); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("empty run gave %v", err)
	}
	flat := []obs1.WALFrame{
		opFrame(t, 3, 4, "h", obs1.KeyDel{}),
		opFrame(t, 3, 4, "h", obs1.KeyDel{}),
	}
	if err := fl.AppendRun(2, 1, flat); err == nil || !strings.Contains(err.Error(), "strictly increasing") {
		t.Fatalf("flat run seqs gave %v", err)
	}
	stale := []obs1.WALFrame{
		opFrame(t, 3, 3, "h", obs1.KeyDel{}),
		opFrame(t, 3, 4, "h", obs1.KeyDel{}),
	}
	if err := fl.AppendRun(2, 1, stale); err == nil || !strings.Contains(err.Error(), "strictly increasing") {
		t.Fatalf("stale run head gave %v", err)
	}

	// All or nothing: the second frame's oversized key fails the encode
	// mid-run, and the rollback leaves no half-run behind, so seq 4 is
	// still free and the next flush carries exactly one frame.
	torn := []obs1.WALFrame{
		opFrame(t, 3, 4, "h", obs1.KeyDel{}),
		{Kind: obs1.OpKeyDel, Slot: 3, Seq: 5, Key: make([]byte, 0x10000)},
	}
	if err := fl.AppendRun(2, 1, torn); err == nil || !strings.Contains(err.Error(), "caps keys") {
		t.Fatalf("torn run gave %v", err)
	}
	if err := fl.AppendOp(2, 1, opFrame(t, 3, 4, "h", obs1.KeyDel{})); err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	fl.Barrier()
	waitCall(t, snk)
	secs = walObject(t, store, node, 2)
	if len(secs) != 1 || len(secs[0].Frames) != 1 || secs[0].Frames[0].Seq != 4 {
		t.Fatalf("object 2 sections = %+v, want one frame at seq 4", secs)
	}
}

// TestFlusherAppendRunCapUnit proves cap admission treats the run as one
// unit: a run whose total tops the cap is refused whole even though each
// frame alone would fit, and the buffer stays usable.
func TestFlusherAppendRunCapUnit(t *testing.T) {
	store := sim.New(sim.Config{})
	snk := newChanSink()
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: snk, Prefix: "p", Node: 12,
		FlushSize: 200, FlushAge: time.Hour, CapBytes: 256,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fl.Close()

	val := make([]byte, 40)
	run := make([]obs1.WALFrame, 6)
	for i := range run {
		run[i] = opFrame(t, 0, uint64(i+1), "k", obs1.StrSet{Value: val})
	}
	if err := fl.AppendRun(1, 1, run); !errors.Is(err, obs1.ErrWALFull) {
		t.Fatalf("over-cap run gave %v, want ErrWALFull", err)
	}
	if err := fl.AppendRun(1, 1, run[:2]); err != nil {
		t.Fatalf("in-cap run after refusal: %v", err)
	}
}
