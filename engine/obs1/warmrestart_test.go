package obs1_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// newWrNode is newToNode with the incarnation chosen by the caller, the
// warm-restart composition: a restarted process builds this stack fresh,
// primes nothing from RAM, and follows the chain before rejoining.
func newWrNode(t *testing.T, s obs1.Store, self uint64, inc uint32, clk *fakeClock) toNode {
	t.Helper()
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(0, 0)
	win, err := obs1.NewTailWindow(fold, fold)
	if err != nil {
		t.Fatal(err)
	}
	live, err := obs1.NewLiveness(win, obs1.DefaultLeaseTTL, obs1.DefaultSkewBound, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ap, err := obs1.NewChainAppender(s, "db/t", 0, self, inc, obs1.ChainPos{}, live)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate, Now: clk.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return toNode{fold, gate, ap, mgr, win, live, obs1.NewTakeoverJudge(fold, live, 0)}
}

func rejoinMember(node uint64, inc uint32) obs1.Member {
	return obs1.Member{Node: node, Incarnation: inc, Resp: "r", Mesh: "m", Weight: 1, Version: "dev"}
}

// TestWarmRestartAdoptsWithoutGrant is doc 02 section 4.5's happy path: a
// new incarnation of the same node id follows the chain, rejoins with one
// member record, adopts every lease the fold still shows as its own at
// the unchanged epoch with no grant appended, replays its own flushed
// frames from the retained window like a taker would, and its
// post-restart commits fold live under the new incarnation.
func TestWarmRestartAdoptsWithoutGrant(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 76})
	clk := newFakeClock()
	const group = uint16(4)

	old := newWrNode(t, s, 1, 1, clk)
	obs := newWrNode(t, s, 2, 1, clk)
	if _, err := old.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := old.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	rec := hoFlush(t, s, 1, 1, group, 1, 1, 3)
	if _, err := old.ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
		t.Fatal(err)
	}

	// The process dies and restarts inside the TTL: a fresh stack under
	// the same node id at incarnation 2, trusting only the chain.
	clk.advance(1000 * time.Millisecond)
	fresh := newWrNode(t, s, 1, 2, clk)
	if err := fresh.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if held := fresh.mgr.Held(); len(held) != 0 {
		t.Fatalf("fresh manager believes it holds %v before rejoining", held)
	}
	before := fresh.ap.Tail()
	if err := fresh.mgr.Rejoin(ctx, rejoinMember(1, 2)); err != nil {
		t.Fatal(err)
	}
	if after := fresh.ap.Tail(); after.Seq != before.Seq+1 {
		t.Fatalf("rejoin moved the tail %d to %d, want exactly one join batch", before.Seq, after.Seq)
	}
	if held := fresh.mgr.Held(); len(held) != 1 || held[0] != group {
		t.Fatalf("held after rejoin = %v, want just group %d", held, group)
	}
	if node, epoch, ok := fresh.fold.Holder(group); !ok || node != 1 || epoch != 1 {
		t.Fatalf("holder after rejoin = %d at %d ok=%v, the epoch must not move", node, epoch, ok)
	}
	if fresh.gate.Suspended(group, clk.now().UnixMilli()) {
		t.Fatal("adopted group suspended right after rejoin")
	}

	// The pre-crash flushed frames replay from the retained window, the
	// taker's path, with nothing trusted from the old process.
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: fresh.win,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 3 || st.Applied != 3 {
		t.Fatalf("take stats %+v, want the old life's 3 flushed frames", st)
	}

	// The new incarnation serves: its commit folds live everywhere.
	live := hoFlush(t, s, 1, 2, group, 1, 4, 5)
	if _, err := fresh.ap.Append(ctx, []obs1.ChainRecord{live}); err != nil {
		t.Fatal(err)
	}
	if fresh.fold.Stats.SectionsDead != 0 || fresh.fold.Stats.CommitsIncStale != 0 {
		t.Fatalf("new life's own commit fenced: %+v", fresh.fold.Stats)
	}
	if err := obs.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if obs.fold.Stats.SectionsDead != 0 || obs.fold.Stats.CommitsIncStale != 0 {
		t.Fatalf("observer fenced the new life's commit: %+v", obs.fold.Stats)
	}
	if fresh.fold.StateSum() != obs.fold.StateSum() {
		t.Fatal("folds disagree after the warm restart")
	}
}

// TestIncarnationFenceKillsPreCrashCommit is the doc 02 section 4.5
// fence: a pre-crash in-flight commit of the old incarnation lands after
// the rejoin bumped the member row, and even though the lease table still
// names the writer at the matching epoch, its sections fold dead at every
// folder and never enter a retained window. Without the incarnation check
// this commit would fold live, the exact hole the slice closes.
func TestIncarnationFenceKillsPreCrashCommit(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 77})
	clk := newFakeClock()
	const group = uint16(5)

	old := newWrNode(t, s, 1, 1, clk)
	obs := newWrNode(t, s, 2, 1, clk)
	if _, err := old.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := old.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	first := hoFlush(t, s, 1, 1, group, 1, 1, 2)
	if _, err := old.ap.Append(ctx, []obs1.ChainRecord{first}); err != nil {
		t.Fatal(err)
	}

	// The restart happens while the old process still has a commit in
	// flight; the new incarnation rejoins first.
	fresh := newWrNode(t, s, 1, 2, clk)
	if err := fresh.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fresh.mgr.Rejoin(ctx, rejoinMember(1, 2)); err != nil {
		t.Fatal(err)
	}

	// The old life's PUT lands now. Writer matches the holder and the
	// epoch matches the lease, so only the incarnation says it is dead.
	stale := hoFlush(t, s, 1, 2, group, 1, 3, 4)
	if _, err := old.ap.Append(ctx, []obs1.ChainRecord{stale}); err != nil {
		t.Fatal(err)
	}
	if old.fold.Stats.CommitsIncStale != 1 || old.fold.Stats.SectionsDead != 1 {
		t.Fatalf("old life's own fold: %+v, want the commit fenced by incarnation", old.fold.Stats)
	}
	if secs := old.win.Sections(group); len(secs) != 1 || secs[0].Sec.FirstSeq != 1 {
		t.Fatalf("old life's window retains %+v, the fenced section must not enter", secs)
	}
	for _, n := range []toNode{fresh, obs} {
		if err := n.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
		if n.fold.Stats.CommitsIncStale != 1 || n.fold.Stats.SectionsDead != 1 {
			t.Fatalf("a folder disagrees on the fence: %+v", n.fold.Stats)
		}
	}

	// The replay sees only the first flush, never the fenced frames.
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: fresh.win,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 2 || st.Applied != 2 {
		t.Fatalf("take stats %+v, the fenced frames must not replay", st)
	}
	if fresh.fold.StateSum() != obs.fold.StateSum() || fresh.fold.StateSum() != old.fold.StateSum() {
		t.Fatal("folds disagree after the fence")
	}
}

// TestRejoinValidates: a rejoin for someone else's node id or with a
// member row that does not match what the appender stamps is refused
// before anything lands, since a mismatched row would make the node
// fence its own commits.
func TestRejoinValidates(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 78})
	clk := newFakeClock()

	n := newWrNode(t, s, 1, 2, clk)
	if err := n.mgr.Rejoin(ctx, rejoinMember(2, 2)); err == nil || !strings.Contains(err.Error(), "node") {
		t.Fatalf("foreign node id accepted: %v", err)
	}
	if err := n.mgr.Rejoin(ctx, rejoinMember(1, 1)); err == nil || !strings.Contains(err.Error(), "incarnation") {
		t.Fatalf("mismatched incarnation accepted: %v", err)
	}
	if tail := n.ap.Tail(); tail.Seq != 0 {
		t.Fatalf("a refused rejoin landed a batch at %d", tail.Seq)
	}
}
