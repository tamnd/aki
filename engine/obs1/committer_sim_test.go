package obs1_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// commitRig is the slice 3 wiring: flusher into committer into a real
// chain appender over the sim, lease fold computing verdicts, verdicts
// moving the watermarks.
type commitRig struct {
	store *sim.Sim
	fold  *obs1.LeaseFold
	marks *obs1.Watermarks
	ap    *obs1.ChainAppender
}

func newCommitRig(t *testing.T, store *sim.Sim, node uint64) *commitRig {
	t.Helper()
	fold := obs1.NewLeaseFold()
	marks := obs1.NewWatermarks()
	fold.OnCommit = marks.ApplyVerdict
	ap, err := obs1.NewChainAppender(store, "p", 0, node, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	return &commitRig{store: store, fold: fold, marks: marks, ap: ap}
}

func (r *commitRig) grant(t *testing.T, group uint16, node uint64, epoch uint32) {
	t.Helper()
	if _, err := r.ap.Append(context.Background(), []obs1.ChainRecord{
		obs1.GrantRecord{Group: group, Node: node, Epoch: epoch},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterEndToEnd(t *testing.T) {
	const node = uint64(0xC1)
	store := sim.New(sim.Config{})
	rig := newCommitRig(t, store, node)
	rig.grant(t, 1, node, 1)
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{Chain: rig.ap, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: cm, Prefix: "p", Node: node,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		if err := fl.AppendOp(1, 1, opFrame(t, 0, seq, "k", obs1.KeyDel{})); err != nil {
			t.Fatal(err)
		}
	}
	fl.Barrier()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rig.marks.Wait(ctx, 1, 5); err != nil {
		t.Fatalf("Wait for committed seq 5: %v", err)
	}
	if got := rig.marks.Committed(1); got != 5 {
		t.Fatalf("Committed(1) = %d, want 5", got)
	}
	if err := fl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cm.Close(); err != nil {
		t.Fatal(err)
	}
	st := cm.Stats()
	if st.Records != 1 || st.Batches != 1 {
		t.Fatalf("stats = %+v, want one record in one batch", st)
	}
	if dead := rig.fold.Stats.SectionsDead; dead != 0 {
		t.Fatalf("%d sections dead, all were under a live lease", dead)
	}
}

func TestCommitterFencedCommitHoldsWatermark(t *testing.T) {
	const node = uint64(0xC2)
	const other = uint64(0xEE)
	store := sim.New(sim.Config{})
	rig := newCommitRig(t, store, node)
	rig.grant(t, 3, other, 1)
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{Chain: rig.ap, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: cm, Prefix: "p", Node: node,
		FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendOp(3, 1, opFrame(t, 0, 1, "k", obs1.KeyDel{})); err != nil {
		t.Fatal(err)
	}
	fl.Barrier()
	if err := fl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cm.Close(); err != nil {
		t.Fatal(err)
	}
	if st := cm.Stats(); st.Records != 1 {
		t.Fatalf("stats = %+v, the fenced commit still lands on the chain", st)
	}
	if got := rig.marks.Committed(3); got != 0 {
		t.Fatalf("Committed(3) = %d after a fenced commit, want 0", got)
	}
	if dead := rig.fold.Stats.SectionsDead; dead != 1 {
		t.Fatalf("SectionsDead = %d, want 1", dead)
	}
}

// gatedChain stalls the first append until released and records every
// batch, so coalescing is asserted deterministically, no timing.
type gatedChain struct {
	inner   obs1.ChainWriter
	entered chan struct{}
	gate    chan struct{}

	mu      sync.Mutex
	batches [][]obs1.ChainRecord
}

func (g *gatedChain) Append(ctx context.Context, recs []obs1.ChainRecord) (obs1.ChainPos, error) {
	g.mu.Lock()
	g.batches = append(g.batches, recs)
	n := len(g.batches)
	g.mu.Unlock()
	if n == 1 {
		g.entered <- struct{}{}
		<-g.gate
	}
	return g.inner.Append(ctx, recs)
}

func walIndex(group uint16, epoch uint32, seq uint64) []obs1.WALIndexEntry {
	return []obs1.WALIndexEntry{{
		Group: group, Epoch: epoch, Offset: 32,
		StoredLen: 24, RawLen: 24, NFrames: 1,
		FirstSeq: seq, LastSeq: seq,
	}}
}

func TestCommitterCoalescesBehindSlowAppend(t *testing.T) {
	const node = uint64(0xC3)
	store := sim.New(sim.Config{})
	rig := newCommitRig(t, store, node)
	rig.grant(t, 1, node, 1)
	gc := &gatedChain{
		inner:   rig.ap,
		entered: make(chan struct{}, 1),
		gate:    make(chan struct{}),
	}
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{Chain: gc, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	if err := cm.WALFlushed(1, 100, walIndex(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gc.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first append never started")
	}
	for seq := uint64(2); seq <= 5; seq++ {
		if err := cm.WALFlushed(seq, 100, walIndex(1, 1, seq)); err != nil {
			t.Fatal(err)
		}
	}
	close(gc.gate)
	if err := cm.Close(); err != nil {
		t.Fatal(err)
	}
	st := cm.Stats()
	if st.Records != 5 || st.Batches != 2 {
		t.Fatalf("stats = %+v, want 5 records in 2 batches", st)
	}
	if len(gc.batches[0]) != 1 || len(gc.batches[1]) != 4 {
		t.Fatalf("batch sizes = %d and %d, want 1 then 4", len(gc.batches[0]), len(gc.batches[1]))
	}
	seen := uint64(0)
	for _, b := range gc.batches {
		for _, r := range b {
			c := r.(obs1.CommitRecord)
			if c.WALSeq != seen+1 {
				t.Fatalf("WAL %d committed after %d, order broke", c.WALSeq, seen)
			}
			seen = c.WALSeq
		}
	}
	if got := rig.marks.Committed(1); got != 5 {
		t.Fatalf("Committed(1) = %d, want 5", got)
	}
}

type failChain struct{ err error }

func (f failChain) Append(context.Context, []obs1.ChainRecord) (obs1.ChainPos, error) {
	return obs1.ChainPos{}, f.err
}

func TestCommitterAppendErrorFatal(t *testing.T) {
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{
		Chain: failChain{err: errors.New("chain says no")},
		Node:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cm.WALFlushed(1, 100, walIndex(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for cm.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("Err() never surfaced the append failure")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := cm.WALFlushed(2, 100, walIndex(1, 1, 2)); err == nil || !strings.Contains(err.Error(), "chain says no") {
		t.Fatalf("delivery after failure gave %v", err)
	}
	if err := cm.Close(); err == nil || !strings.Contains(err.Error(), "chain says no") {
		t.Fatalf("Close after failure gave %v", err)
	}
}

func TestCommitterOnCommittedOrder(t *testing.T) {
	const node = uint64(0xC4)
	store := sim.New(sim.Config{})
	rig := newCommitRig(t, store, node)
	rig.grant(t, 1, node, 1)
	var mu sync.Mutex
	var seqs []uint64
	cm, err := obs1.NewCommitter(obs1.CommitterConfig{
		Chain: rig.ap, Node: node,
		OnCommitted: func(walSeq uint64, pos obs1.ChainPos) {
			if pos.Seq == 0 {
				t.Errorf("WAL %d committed at the zero chain position", walSeq)
			}
			mu.Lock()
			seqs = append(seqs, walSeq)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 8; seq++ {
		if err := cm.WALFlushed(seq, 100, walIndex(1, 1, seq)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.Close(); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 8 {
		t.Fatalf("OnCommitted heard %d seqs, want 8", len(seqs))
	}
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("OnCommitted order = %v", seqs)
		}
	}
	if err := cm.WALFlushed(9, 100, walIndex(1, 1, 9)); !errors.Is(err, obs1.ErrCommitterClosed) {
		t.Fatalf("delivery after Close gave %v", err)
	}
	if err := cm.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestWatermarksWait(t *testing.T) {
	w := obs1.NewWatermarks()
	verdict := func(group uint16, seq uint64, live bool) obs1.CommitVerdict {
		return obs1.CommitVerdict{
			Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{
				Group: group, Epoch: 1, NFrames: 1, FirstSeq: seq, LastSeq: seq,
			}}},
			Live: []bool{live},
		}
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- w.Wait(ctx, 1, 3)
	}()
	if err := w.ApplyVerdict(verdict(1, 3, false)); err != nil {
		t.Fatal(err)
	}
	if got := w.Committed(1); got != 0 {
		t.Fatalf("dead section moved the watermark to %d", got)
	}
	if err := w.ApplyVerdict(verdict(1, 2, true)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v at watermark 2, wanted 3", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := w.ApplyVerdict(verdict(1, 3, true)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Wait: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Wait(ctx, 1, 99); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Wait gave %v", err)
	}
	if got := w.Committed(2); got != 0 {
		t.Fatalf("untouched group reads %d", got)
	}
}

// TestWatermarksNotify covers the callback face Wait's blocking form
// cannot serve: an already-covered mark fires before Notify returns, a
// pending one fires from the covering ApplyVerdict and not from a dead
// section or a short advance, and callbacks past the advance stay
// registered.
func TestWatermarksNotify(t *testing.T) {
	w := obs1.NewWatermarks()
	verdict := func(group uint16, seq uint64, live bool) obs1.CommitVerdict {
		return obs1.CommitVerdict{
			Commit: obs1.CommitRecord{Sections: []obs1.CommitSection{{
				Group: group, Epoch: 1, NFrames: 1, FirstSeq: seq, LastSeq: seq,
			}}},
			Live: []bool{live},
		}
	}
	fired := make(chan int, 8)
	w.Notify(1, 2, func() { fired <- 2 })
	w.Notify(1, 5, func() { fired <- 5 })
	select {
	case n := <-fired:
		t.Fatalf("callback %d fired before any commit", n)
	default:
	}
	if err := w.ApplyVerdict(verdict(1, 3, false)); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-fired:
		t.Fatalf("callback %d fired on a dead section", n)
	default:
	}
	if err := w.ApplyVerdict(verdict(1, 3, true)); err != nil {
		t.Fatal(err)
	}
	if n := <-fired; n != 2 {
		t.Fatalf("first firing = %d, want the covered mark 2", n)
	}
	select {
	case n := <-fired:
		t.Fatalf("callback %d fired at watermark 3", n)
	default:
	}
	// Already covered: fires on the caller's goroutine before Notify
	// returns, no verdict needed.
	inline := false
	w.Notify(1, 3, func() { inline = true })
	if !inline {
		t.Fatal("already-covered Notify did not fire inline")
	}
	if err := w.ApplyVerdict(verdict(1, 7, true)); err != nil {
		t.Fatal(err)
	}
	if n := <-fired; n != 5 {
		t.Fatalf("second firing = %d, want the surviving mark 5", n)
	}
	// A different group's registration is untouched by all of the above.
	w.Notify(2, 1, func() { fired <- 100 })
	select {
	case n := <-fired:
		t.Fatalf("callback %d fired for an untouched group", n)
	default:
	}
	if err := w.ApplyVerdict(verdict(2, 1, true)); err != nil {
		t.Fatal(err)
	}
	if n := <-fired; n != 100 {
		t.Fatalf("group 2 firing = %d", n)
	}
}
