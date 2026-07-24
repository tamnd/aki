package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// toNode is an hoNode with the failure-detection view: a Liveness in the
// apply chain and a TakeoverJudge over it, the crash-taker composition.
type toNode struct {
	fold  *obs1.LeaseFold
	gate  *obs1.LeaseGate
	ap    *obs1.ChainAppender
	mgr   *obs1.LeaseManager
	win   *obs1.TailWindow
	live  *obs1.Liveness
	judge *obs1.TakeoverJudge
}

func newToNode(t *testing.T, s obs1.Store, self uint64, clk *fakeClock) toNode {
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
	ap, err := obs1.NewChainAppender(s, "db/t", 0, self, 1, obs1.ChainPos{}, live)
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

// survivorView is the member table filtered to the nodes this observer
// does not suspect, self always included, the balancer's liveMembers
// filter for the takeover planner.
func survivorView(n toNode, self uint64, at time.Time) []obs1.Member {
	var out []obs1.Member
	for _, m := range n.fold.Members() {
		if m.Node == self || !n.live.Suspect(m.Node, at) {
			out = append(out, m)
		}
	}
	return out
}

// TestTakeoverJudgeFullTTLWait walks the freeness case (b) discipline on
// a shared clock: no eligibility before the chain-observed staleness, a
// full TTL of watched staleness on top of it, a restart whenever the
// holder acts, and no case (b) at all for a released group.
func TestTakeoverJudgeFullTTLWait(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 71})
	clk := newFakeClock()
	const group = uint16(6)

	a := newToNode(t, s, 1, clk)
	b := newToNode(t, s, 2, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("eligible with a live holder")
	}

	// Chain-observed staleness alone must not be enough.
	clk.advance(3600 * time.Millisecond)
	if !b.live.Suspect(1, clk.now()) {
		t.Fatal("holder not suspect past ttl plus skew")
	}
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("eligible the instant staleness was first observed")
	}
	clk.advance(2900 * time.Millisecond)
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("eligible before a full ttl of watched staleness")
	}
	clk.advance(200 * time.Millisecond)
	if !b.judge.Eligible(group, clk.now()) {
		t.Fatal("not eligible after staleness plus a full watched ttl")
	}

	// The holder acts: the watch drops and the wait starts over.
	if err := a.mgr.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("eligible right after the holder heartbeat")
	}
	clk.advance(3600 * time.Millisecond)
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("the heartbeat must have restarted the full-ttl watch")
	}
	clk.advance(3000 * time.Millisecond)
	if !b.judge.Eligible(group, clk.now()) {
		t.Fatal("not eligible after the restarted watch ran its ttl")
	}

	// A released group is freeness case (a), never a takeover.
	if err := a.mgr.Release(ctx, group); err != nil {
		t.Fatal(err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("a released group is plain Acquire business")
	}
}

// TestCrashTakeoverReplaysAndServes is doc 02 section 4.4's three steps:
// the taker waits out the discipline, grants itself epoch plus one,
// replays the crashed holder's flushed frames from its retained window,
// and the woken holder demotes with the redirect on its next reconcile.
func TestCrashTakeoverReplaysAndServes(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 72})
	clk := newFakeClock()
	const group = uint16(9)

	a := newToNode(t, s, 1, clk)
	b := newToNode(t, s, 2, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	rec := hoFlush(t, s, 1, 1, group, 1, 1, 3)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{rec}); err != nil {
		t.Fatal(err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}

	// The holder crashes here. Discipline, then the plan names the group.
	clk.advance(3600 * time.Millisecond)
	obs1.PlanTakeover(2, 16, survivorView(b, 2, clk.now()), b.judge, clk.now())
	clk.advance(3000 * time.Millisecond)
	at := clk.now()
	sv := survivorView(b, 2, at)
	if len(sv) != 1 || sv[0].Node != 2 {
		t.Fatalf("survivors = %+v, want only the taker", sv)
	}
	plan := obs1.PlanTakeover(2, 16, sv, b.judge, at)
	if len(plan) != 1 || plan[0] != group {
		t.Fatalf("plan = %v, want just group %d", plan, group)
	}

	if won, err := b.mgr.Takeover(ctx, group); err != nil || !won {
		t.Fatalf("takeover: %v %v", won, err)
	}
	if node, epoch, ok := b.fold.Holder(group); !ok || node != 2 || epoch != 2 {
		t.Fatalf("holder after takeover = %d at %d", node, epoch)
	}
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: b.win,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 3 || st.Applied != 3 {
		t.Fatalf("take stats %+v, want the crashed holder's 3 flushed frames", st)
	}

	// The crashed node wakes, catches up, and demotes with the redirect.
	if err := a.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	a.mgr.Reconcile()
	if held := a.mgr.Held(); len(held) != 0 {
		t.Fatalf("woken holder still believes it holds %v", held)
	}
	if ep, ok := a.gate.Demoted(group); !ok || ep != "r" {
		t.Fatalf("demotion endpoint = %q ok=%v, want the taker's resp", ep, ok)
	}
	if a.fold.StateSum() != b.fold.StateSum() {
		t.Fatal("folds disagree after the takeover")
	}
}

// TestZombieWindowBound is doc 02 section 3.4 stated as assertions: an
// honest zombie self-suspends before the earliest possible takeover
// grant, and a mis-clocked one that acks anyway commits sections that
// fold dead at every folder, never enter a retained window, and never
// reach the taker's state.
func TestZombieWindowBound(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 73})
	clk := newFakeClock()
	const group = uint16(7)

	a := newToNode(t, s, 1, clk)
	b := newToNode(t, s, 2, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	live := hoFlush(t, s, 1, 1, group, 1, 1, 2)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{live}); err != nil {
		t.Fatal(err)
	}
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}

	// Inside the belief window the holder serves and no taker can move.
	clk.advance(2400 * time.Millisecond)
	if a.gate.Suspended(group, clk.now().UnixMilli()) {
		t.Fatal("holder suspended inside its believed deadline")
	}
	if b.judge.Eligible(group, clk.now()) {
		t.Fatal("taker eligible inside the lease ttl")
	}

	// By first-staleness the honest holder has already self-suspended:
	// belief deadline minus skew sits at 2500ms, staleness at 3500ms.
	clk.advance(1200 * time.Millisecond)
	if !a.gate.Suspended(group, clk.now().UnixMilli()) {
		t.Fatal("honest holder not self-suspended past its deadline")
	}
	b.judge.Eligible(group, clk.now())
	clk.advance(3000 * time.Millisecond)
	if !b.judge.Eligible(group, clk.now()) {
		t.Fatal("taker not eligible after the full discipline")
	}
	if won, err := b.mgr.Takeover(ctx, group); err != nil || !won {
		t.Fatalf("takeover: %v %v", won, err)
	}

	// The mis-clocked zombie acks anyway: its commit lands on the chain
	// and folds dead at its own fold first.
	zombie := hoFlush(t, s, 1, 2, group, 1, 3, 4)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{zombie}); err != nil {
		t.Fatal(err)
	}
	if a.fold.Stats.SectionsDead != 1 {
		t.Fatalf("zombie's own fold killed %d sections, want 1", a.fold.Stats.SectionsDead)
	}
	if secs := a.win.Sections(group); len(secs) != 1 || secs[0].Sec.FirstSeq != 1 {
		t.Fatalf("zombie's window retains %+v, dead section must not enter", secs)
	}

	// Every other folder agrees, and the taker replays only real data.
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if b.fold.Stats.SectionsDead != 1 {
		t.Fatalf("taker's fold killed %d sections, want 1", b.fold.Stats.SectionsDead)
	}
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: s, Prefix: "db/t", Group: group, Window: b.win,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 2 || st.Applied != 2 {
		t.Fatalf("take stats %+v, the zombie's frames must not replay", st)
	}
	a.mgr.Reconcile()
	if held := a.mgr.Held(); len(held) != 0 {
		t.Fatalf("zombie still believes it holds %v after reconciling", held)
	}
	if ep, ok := a.gate.Demoted(group); !ok || ep != "r" {
		t.Fatalf("zombie demotion endpoint = %q ok=%v", ep, ok)
	}
	if a.fold.StateSum() != b.fold.StateSum() {
		t.Fatal("folds disagree after the zombie window closed")
	}
}

// TestTakeoverRaceOneWinner: two disciplined takers race on the chain
// CAS; exactly one grant folds and the loser abandons with nothing held,
// the doc 02 section 3.2 race rule.
func TestTakeoverRaceOneWinner(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 74})
	clk := newFakeClock()
	const group = uint16(3)

	a := newToNode(t, s, 1, clk)
	b := newToNode(t, s, 2, clk)
	c := newToNode(t, s, 3, clk)
	if _, err := a.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2), joinRec(3)}); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, group); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	for _, n := range []toNode{b, c} {
		if err := n.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
	}

	clk.advance(3600 * time.Millisecond)
	b.judge.Eligible(group, clk.now())
	c.judge.Eligible(group, clk.now())
	clk.advance(3000 * time.Millisecond)
	if !b.judge.Eligible(group, clk.now()) || !c.judge.Eligible(group, clk.now()) {
		t.Fatal("both takers should be disciplined and eligible")
	}

	if won, err := b.mgr.Takeover(ctx, group); err != nil || !won {
		t.Fatalf("first taker: %v %v", won, err)
	}
	won, err := c.mgr.Takeover(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if won {
		t.Fatal("both takers won the same group")
	}
	if held := c.mgr.Held(); len(held) != 0 {
		t.Fatalf("loser holds %v", held)
	}
	if c.fold.Stats.GrantsRejected != 1 {
		t.Fatalf("loser's grant rejected %d times, want exactly 1", c.fold.Stats.GrantsRejected)
	}
	for _, n := range []toNode{a, b} {
		if err := n.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []toNode{a, b, c} {
		if node, epoch, ok := n.fold.Holder(group); !ok || node != 2 || epoch != 2 {
			t.Fatalf("a folder sees holder %d at %d ok=%v", node, epoch, ok)
		}
	}
	if a.fold.StateSum() != b.fold.StateSum() || b.fold.StateSum() != c.fold.StateSum() {
		t.Fatal("folds disagree after the race")
	}
}

// TestPlanTakeoverSpreadsAcrossSurvivors: a crashed node's groups
// partition across the survivors by rendezvous, disjoint and complete,
// and every planned takeover wins, the doc 02 section 4.4 G/N spread.
func TestPlanTakeoverSpreadsAcrossSurvivors(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 75})
	clk := newFakeClock()
	const nGroups = 8

	t1 := newToNode(t, s, 1, clk)
	t2 := newToNode(t, s, 2, clk)
	h := newToNode(t, s, 3, clk)
	if _, err := h.ap.Append(ctx, []obs1.ChainRecord{joinRec(1), joinRec(2), joinRec(3)}); err != nil {
		t.Fatal(err)
	}
	for g := 0; g < nGroups; g++ {
		if won, err := h.mgr.Acquire(ctx, uint16(g)); err != nil || !won {
			t.Fatalf("seed acquire %d: %v %v", g, won, err)
		}
	}
	for _, n := range []toNode{t1, t2} {
		if err := n.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// The holder crashes; the survivors keep each other live with
	// heartbeats while the discipline runs.
	clk.advance(3600 * time.Millisecond)
	if err := t1.mgr.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := t2.mgr.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := t1.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	obs1.PlanTakeover(1, nGroups, survivorView(t1, 1, clk.now()), t1.judge, clk.now())
	obs1.PlanTakeover(2, nGroups, survivorView(t2, 2, clk.now()), t2.judge, clk.now())
	clk.advance(3000 * time.Millisecond)
	at := clk.now()

	sv1 := survivorView(t1, 1, at)
	if len(sv1) != 2 {
		t.Fatalf("survivors = %+v, the crashed node must be filtered", sv1)
	}
	plan1 := obs1.PlanTakeover(1, nGroups, sv1, t1.judge, at)
	plan2 := obs1.PlanTakeover(2, nGroups, survivorView(t2, 2, at), t2.judge, at)
	if len(plan1) == 0 || len(plan2) == 0 {
		t.Fatalf("plans %v and %v; placement never splits, test inputs are useless", plan1, plan2)
	}
	seen := map[uint16]int{}
	for _, g := range plan1 {
		seen[g]++
	}
	for _, g := range plan2 {
		seen[g]++
	}
	if len(seen) != nGroups {
		t.Fatalf("plans cover %d of %d groups: %v %v", len(seen), nGroups, plan1, plan2)
	}
	for g, n := range seen {
		if n != 1 {
			t.Fatalf("group %d planned by %d survivors", g, n)
		}
	}

	for _, g := range plan1 {
		if won, err := t1.mgr.Takeover(ctx, g); err != nil || !won {
			t.Fatalf("takeover %d: %v %v", g, won, err)
		}
	}
	for _, g := range plan2 {
		if won, err := t2.mgr.Takeover(ctx, g); err != nil || !won {
			t.Fatalf("takeover %d: %v %v", g, won, err)
		}
	}
	if err := t1.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(t1.fold.HeldBy(1)); got != len(plan1) {
		t.Fatalf("node 1 holds %d, planned %d", got, len(plan1))
	}
	if got := len(t1.fold.HeldBy(2)); got != len(plan2) {
		t.Fatalf("node 2 holds %d, planned %d", got, len(plan2))
	}
	if t1.fold.StateSum() != t2.fold.StateSum() {
		t.Fatal("folds disagree after the spread")
	}
}
