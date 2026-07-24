package obs1_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// hoNode is a mgrNode with a TailWindow between the appender and the
// fold, the taker-capable composition.
type hoNode struct {
	fold *obs1.LeaseFold
	gate *obs1.LeaseGate
	ap   *obs1.ChainAppender
	mgr  *obs1.LeaseManager
	win  *obs1.TailWindow
}

func newHoNode(t *testing.T, s obs1.Store, self uint64, clk *fakeClock) hoNode {
	t.Helper()
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(0, 0)
	win, err := obs1.NewTailWindow(fold, fold)
	if err != nil {
		t.Fatal(err)
	}
	ap, err := obs1.NewChainAppender(s, "db/t", 0, self, 1, obs1.ChainPos{}, win)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate, Now: clk.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hoNode{fold, gate, ap, mgr, win}
}

func hoWALKey(node, seq uint64) string {
	return fmt.Sprintf("db/t/wal/%016x/%016d", node, seq)
}

// hoFlush builds one WAL object for group with frames [first..last], PUTs
// it under the node's namespace, and returns the commit record a final
// flush would publish, the doc 02 section 4.3 step 3 shape.
func hoFlush(t *testing.T, s obs1.Store, node uint64, walSeq uint64, group uint16, epoch uint32, first, last uint64) obs1.CommitRecord {
	t.Helper()
	ctx := context.Background()
	frames := make([]obs1.WALFrame, 0, last-first+1)
	for seq := first; seq <= last; seq++ {
		frames = append(frames, obs1.WALFrame{
			Kind: 0x01, Slot: 9, Seq: seq,
			Key:     []byte(fmt.Sprintf("hk%06d", seq)),
			Payload: []byte(fmt.Sprintf("hv%06d", seq)),
		})
	}
	body, err := obs1.AppendWAL(nil, node, []obs1.WALSection{{Group: group, Epoch: epoch, Frames: frames}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, hoWALKey(node, walSeq), body); err != nil {
		t.Fatal(err)
	}
	off, flen, err := obs1.ParseTail(body[len(body)-obs1.TailSize:])
	if err != nil {
		t.Fatal(err)
	}
	entries, err := obs1.ParseWALFooter(body[off : off+uint64(flen)])
	if err != nil {
		t.Fatal(err)
	}
	rec := obs1.CommitRecord{WALNode: node, WALSeq: walSeq, WALSize: uint64(len(body))}
	for _, e := range entries {
		rec.Sections = append(rec.Sections, e.CommitSection())
	}
	return rec
}

// TestHandoffReleaseRidesTheFlushBatch runs the holder's steps 2-4 and
// the taker's steps 5-6 over one sim chain: the final flush's commit and
// the release land in one chain object, the taker's window retains the
// flushed section as live, and TakeGroup replays exactly those frames.
func TestHandoffReleaseRidesTheFlushBatch(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 61})
	clk := newFakeClock()
	const group = uint16(9)

	a := newHoNode(t, s, 1, clk)
	b := newHoNode(t, s, 2, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}

	before := s.Usage()
	err := a.mgr.Handoff(ctx, group, func(ctx context.Context) ([]obs1.ChainRecord, error) {
		return []obs1.ChainRecord{hoFlush(t, s, 1, 1, group, 1, 1, 3)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	after := s.Usage()
	// Step 4's one-object claim: the WAL PUT plus a single chain PUT.
	if got := after.PutRequests - before.PutRequests; got != 2 {
		t.Fatalf("handoff issued %d PUTs, want 2 (WAL + one chain batch)", got)
	}
	if held := a.mgr.Held(); len(held) != 0 {
		t.Fatalf("holder still believes it holds %v", held)
	}

	// Taker: follow sees commit then release, takes at epoch 2, replays.
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	b.mgr.Reconcile()
	if won, err := b.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("take: %v %v", won, err)
	}
	if _, epoch, _ := b.fold.Holder(group); epoch != 2 {
		t.Fatalf("taker's epoch = %d, want 2", epoch)
	}
	var got []uint64
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: b.win,
		Apply: func(g uint16, f obs1.WALFrame) error {
			if g != group {
				t.Fatalf("frame for group %d", g)
			}
			got = append(got, f.Seq)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 3 || st.Applied != 3 || st.WALGets != 1 {
		t.Fatalf("take stats %+v", st)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("replayed seqs %v", got)
	}

	// The old holder catches up and both folds agree.
	if err := a.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	a.mgr.Reconcile()
	if a.fold.StateSum() != b.fold.StateSum() {
		t.Fatal("folds disagree after handoff")
	}
}

// TestHandoffFlushFailureStaysSafe: a failed final flush leaves the
// group ours on the chain and suspended at the gate, and the retry
// completes the handoff.
func TestHandoffFlushFailureStaysSafe(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 62})
	clk := newFakeClock()
	const group = uint16(4)

	a := newHoNode(t, s, 1, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}

	boom := fmt.Errorf("final flush lost its bucket")
	err := a.mgr.Handoff(ctx, group, func(ctx context.Context) ([]obs1.ChainRecord, error) {
		return nil, boom
	})
	if err != boom {
		t.Fatalf("handoff err = %v, want the flush error", err)
	}
	if held := a.mgr.Held(); len(held) != 1 || held[0] != group {
		t.Fatalf("held = %v after failed flush, the lease must survive", held)
	}
	if node, _, ok := a.fold.Holder(group); !ok || node != 1 {
		t.Fatal("chain no longer shows us holding after a failed flush")
	}

	if err := a.mgr.Handoff(ctx, group, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := a.fold.Holder(group); ok {
		t.Fatal("group still held on the chain after the retry released it")
	}
	// Shedding a group we no longer hold is a no-op, not an error.
	if err := a.mgr.Handoff(ctx, group, nil); err != nil {
		t.Fatal(err)
	}
}

// TestTailWindowLiveOnlyAndTrim: epoch-stale sections never enter the
// window, and a checkpoint record folding through trims everything at or
// below its position.
func TestTailWindowLiveOnlyAndTrim(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 63})
	clk := newFakeClock()
	const group = uint16(7)

	a := newHoNode(t, s, 1, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	live := hoFlush(t, s, 1, 1, group, 1, 1, 2)
	stale := hoFlush(t, s, 1, 2, group, 99, 3, 4)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{live, stale}); err != nil {
		t.Fatal(err)
	}
	if got := a.win.Retained(); got != 1 {
		t.Fatalf("window retains %d sections, want only the live one", got)
	}

	// More commits, then a checkpoint through the first batch's position:
	// the early section trims, the later one survives.
	cut := a.fold.Applied()
	later := hoFlush(t, s, 1, 3, group, 1, 3, 5)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{later}); err != nil {
		t.Fatal(err)
	}
	if got := a.win.Retained(); got != 2 {
		t.Fatalf("window retains %d, want 2 before the trim", got)
	}
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{obs1.CheckpointRecord{Pos: cut}}); err != nil {
		t.Fatal(err)
	}
	secs := a.win.Sections(group)
	if len(secs) != 1 || secs[0].Sec.FirstSeq != 3 {
		t.Fatalf("after trim window holds %+v, want only the post-checkpoint section", secs)
	}
}

// TestTakeGroupCursorDiscipline: a leading gap above the manifest's fold
// cursor is an error, the same gap at or below the cursor is trimmed
// history, and frames the cursor already covers replay anyway when still
// present (the #1118 resident-record rule).
func TestTakeGroupCursorDiscipline(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 64})
	clk := newFakeClock()
	const group = uint16(5)

	a := newHoNode(t, s, 1, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	rec := hoFlush(t, s, 1, 1, group, 1, 5, 6)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
		t.Fatal(err)
	}

	_, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: a.win,
	})
	if err == nil || !strings.Contains(err.Error(), "gap") {
		t.Fatalf("gap above a zero cursor must fail, got %v", err)
	}

	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: a.win,
		Manifest: obs1.Manifest{Group: group, FoldSeq: 4}, HasManifest: true, Warm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 2 || st.Applied != 6 {
		t.Fatalf("cursor-floored take stats %+v", st)
	}

	// A cursor past the whole section: frames replay anyway, and the
	// applied cursor never moves backwards below the floor.
	st, err = obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: a.win,
		Manifest: obs1.Manifest{Group: group, FoldSeq: 8}, HasManifest: true, Warm: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 2 || st.Applied != 8 {
		t.Fatalf("past-cursor take stats %+v", st)
	}
}

// TestBalancerShedsViaHandoff wires the balancer's Shed hook to
// LeaseManager.Handoff, the doc 02 section 4.6 rule that sheds move via
// graceful handoff: the fleet converges to the rendezvous placement with
// every shed riding a release batch, and the taker replays each group it
// takes from its own retained window.
func TestBalancerShedsViaHandoff(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 65})
	clk := newFakeClock()
	const nGroups = 8

	a := newHoNode(t, s, 1, clk)
	b := newHoNode(t, s, 2, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	for g := 0; g < nGroups; g++ {
		if won, err := a.mgr.Acquire(ctx, uint16(g)); err != nil || !won {
			t.Fatalf("seed acquire %d: %v %v", g, won, err)
		}
	}
	// Every group carries one flushed frame so each shed has a real
	// final flush and each take a real replay.
	walSeq := uint64(1)
	for g := 0; g < nGroups; g++ {
		rec := hoFlush(t, s, 1, walSeq, uint16(g), 1, 1, 1)
		walSeq++
		if _, err := a.ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}

	balA, err := obs1.NewBalancer(a.mgr, nGroups, time.Second, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	balB, err := obs1.NewBalancer(b.mgr, nGroups, time.Second, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	balA.Shed = func(ctx context.Context, group uint16) error {
		return a.mgr.Handoff(ctx, group, nil)
	}
	taken := map[uint16]uint64{}
	wantB := 0
	for g := 0; g < nGroups; g++ {
		if pref, _ := obs1.PreferredNode(uint16(g), a.fold.Members()); pref == 2 {
			wantB++
		}
	}
	if wantB == 0 {
		t.Fatal("placement never prefers node 2; test inputs are useless")
	}

	for round := 0; round < wantB+2; round++ {
		if err := balA.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
		b.mgr.Reconcile()
		heldBefore := map[uint16]bool{}
		for _, g := range b.mgr.Held() {
			heldBefore[g] = true
		}
		if err := balB.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		for _, g := range b.mgr.Held() {
			if heldBefore[g] {
				continue
			}
			st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
				Store: s, Prefix: "db/t", Group: g, Window: b.win,
			})
			if err != nil {
				t.Fatalf("take group %d: %v", g, err)
			}
			taken[g] = st.FramesApplied
		}
		if err := a.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
		a.mgr.Reconcile()
	}

	if got := len(b.mgr.Held()); got != wantB {
		t.Fatalf("node 2 holds %d, placement wants %d", got, wantB)
	}
	if len(taken) != wantB {
		t.Fatalf("replayed %d groups, want %d", len(taken), wantB)
	}
	for g, n := range taken {
		if n != 1 {
			t.Fatalf("group %d replayed %d frames, want its one flushed frame", g, n)
		}
	}
	if a.fold.StateSum() != b.fold.StateSum() {
		t.Fatal("folds disagree after the rebalance")
	}
}

// TestPrewarmGroupFanMatchesSerial: the fanned rebuild is the serial
// rebuild with the GETs prefetched, so the rebuilt keymap and directory
// are identical.
func TestPrewarmGroupFanMatchesSerial(t *testing.T) {
	fx, _, _ := newFoldDirFixture(t)
	ctx := context.Background()

	fx.folder.Add(frames("p1", "v1", "p2", "v2", "p3", "v3"))
	fx.folder.Flush()
	waitFor(t, "segment 1", func() bool { return len(fx.folder.Ledger()) == 1 })
	fx.folder.Delete([]byte("p2"))
	fx.folder.Flush()
	waitFor(t, "segment 2", func() bool { return len(fx.folder.Ledger()) == 2 })
	m := residentManifest(fx.folder.Ledger())

	kmSerial, dirSerial := obs1.NewKeymap(), obs1.NewDirectory()
	stSerial, err := obs1.RebuildResident(ctx, fx.sim, "db/t", m, dirSerial, kmSerial)
	if err != nil {
		t.Fatal(err)
	}
	kmFan, dirFan := obs1.NewKeymap(), obs1.NewDirectory()
	stFan, err := obs1.PrewarmGroup(ctx, fx.sim, "db/t", m, dirFan, kmFan, 8)
	if err != nil {
		t.Fatal(err)
	}
	if stFan != stSerial {
		t.Fatalf("fan stats %+v, serial %+v", stFan, stSerial)
	}
	if kmFan.Len() != kmSerial.Len() || dirFan.Segments() != dirSerial.Segments() {
		t.Fatalf("fan rebuilt %d keys %d segments, serial %d and %d",
			kmFan.Len(), dirFan.Segments(), kmSerial.Len(), dirSerial.Segments())
	}
	for _, key := range []string{"p1", "p3"} {
		fp := obs1.Fingerprint([]byte(key))
		a, aok := kmSerial.Lookup(fp)
		b, bok := kmFan.Lookup(fp)
		if !aok || !bok || a != b {
			t.Fatalf("%s serial %+v ok=%v, fan %+v ok=%v", key, a, aok, b, bok)
		}
	}
	if _, ok := kmFan.Lookup(obs1.Fingerprint([]byte("p2"))); ok {
		t.Fatal("tombstoned p2 present after the fanned rebuild")
	}
}
