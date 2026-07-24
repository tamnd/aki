package fleetsim_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/fleetsim"
)

const (
	nGroups = 12
	step    = 100 * time.Millisecond
)

func newFleet(t *testing.T, seed uint64, nodes int) *fleetsim.Fleet {
	t.Helper()
	f, err := fleetsim.New(fleetsim.Config{
		Seed: seed, Nodes: nodes, NGroups: nGroups, BalanceEvery: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for id := uint64(1); id <= uint64(nodes); id++ {
		if err := f.Join(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

// converge ticks until observer's fold covers every group, returning
// the simulated time it took.
func converge(t *testing.T, ctx context.Context, f *fleetsim.Fleet, observer uint64, cap time.Duration) time.Duration {
	t.Helper()
	for elapsed := time.Duration(0); elapsed < cap; elapsed += step {
		f.Tick(ctx, step)
		if _, full := f.Coverage(observer); full {
			return elapsed + step
		}
	}
	t.Fatalf("no full coverage inside %v of simulated time", cap)
	return 0
}

// snapshot reads every group's holder and epoch from one fold.
func snapshot(f *fleetsim.Fleet, observer uint64) (map[uint16]uint64, map[uint16]uint32) {
	holders := make(map[uint16]uint64)
	epochs := make(map[uint16]uint32)
	for g := 0; g < nGroups; g++ {
		if node, epoch, ok := f.Node(observer).Fold.Holder(uint16(g)); ok {
			holders[uint16(g)] = node
			epochs[uint16(g)] = epoch
		}
	}
	return holders, epochs
}

func agree(t *testing.T, f *fleetsim.Fleet, ids ...uint64) {
	t.Helper()
	base := f.Node(ids[0]).Fold.StateSum()
	for _, id := range ids[1:] {
		if f.Node(id).Fold.StateSum() != base {
			t.Fatalf("node %d's fold disagrees with node %d's", id, ids[0])
		}
	}
}

// TestColdBootConverges is the doc 02 section 6 full-fleet cold boot:
// every node joins an empty chain and races grants from the first tick,
// the CAS serializes, and the fleet settles at the rendezvous placement
// and stays there.
func TestColdBootConverges(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 81, 3)
	took := converge(t, ctx, f, 1, 15*time.Second)
	t.Logf("cold boot to full coverage in %v simulated", took)

	members := f.Node(1).Fold.Members()
	holders, _ := snapshot(f, 1)
	perNode := map[uint64]int{}
	for g, node := range holders {
		perNode[node]++
		if pref, ok := obs1.PreferredNode(g, members); !ok || pref != node {
			t.Fatalf("group %d settled on node %d, rendezvous prefers %d", g, node, pref)
		}
	}
	for id := uint64(1); id <= 3; id++ {
		if perNode[id] == 0 {
			t.Fatalf("node %d ended with no groups: %v", id, perNode)
		}
	}

	// A balanced fleet stops moving: more ticks change nothing.
	wantH, wantE := snapshot(f, 1)
	f.Run(ctx, 5*time.Second, step)
	gotH, gotE := snapshot(f, 1)
	for g := 0; g < nGroups; g++ {
		if gotH[uint16(g)] != wantH[uint16(g)] || gotE[uint16(g)] != wantE[uint16(g)] {
			t.Fatalf("group %d moved after convergence", g)
		}
	}
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	agree(t, f, 1, 2, 3)
	for id := uint64(1); id <= 3; id++ {
		if f.Node(id).Errs != 0 {
			t.Fatalf("node %d hit %d duty-cycle errors on a clean bucket", id, f.Node(id).Errs)
		}
	}
}

// TestWriteOutageFreezesOwnership: with the bucket refusing writes,
// disciplined takers really try and really fail, nothing folds, nothing
// fences, and ownership stays exactly where the chain left it; healing
// the bucket lets the takeover land.
func TestWriteOutageFreezesOwnership(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 82, 3)
	converge(t, ctx, f, 1, 15*time.Second)
	holders, _ := snapshot(f, 1)
	var crashedGroups []uint16
	for g, node := range holders {
		if node == 3 {
			crashedGroups = append(crashedGroups, g)
		}
	}
	if len(crashedGroups) == 0 {
		t.Fatal("node 3 holds nothing, the crash proves nothing")
	}

	// Crash node 3, let the discipline run almost out on a healthy
	// bucket, then kill writes just before eligibility.
	f.Crash(3)
	f.Run(ctx, 6*time.Second, step)
	errsBefore := f.Node(1).Errs + f.Node(2).Errs
	f.SetFault(fleetsim.WriteOutage())
	f.Run(ctx, 3*time.Second, step)

	if f.Node(1).Errs+f.Node(2).Errs == errsBefore {
		t.Fatal("no duty-cycle failures during a write outage; nobody tried")
	}
	for _, g := range crashedGroups {
		node, epoch, ok := f.Node(1).Fold.Holder(g)
		if !ok || node != 3 || epoch != 1 {
			t.Fatalf("group %d moved during the outage: node %d epoch %d", g, node, epoch)
		}
	}

	// Heal: the already-disciplined takeovers land.
	f.SetFault(nil)
	for i := 0; i < 30; i++ {
		f.Tick(ctx, step)
	}
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, g := range crashedGroups {
		node, epoch, ok := f.Node(1).Fold.Holder(g)
		if !ok || node == 3 || epoch != 2 {
			t.Fatalf("group %d after heal: node %d epoch %d ok=%v", g, node, epoch, ok)
		}
	}
	agree(t, f, 1, 2)
}

// TestReadOutageStallsFollowersOnly: holders keep appending, followers
// stall on their own cursors, and nothing diverges because nobody folds
// what they cannot read; healing catches everyone up.
func TestReadOutageStallsFollowersOnly(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 83, 3)
	converge(t, ctx, f, 1, 15*time.Second)
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	holders, _ := snapshot(f, 1)
	var g uint16
	found := false
	for gr, node := range holders {
		if node == 1 {
			g, found = gr, true
			break
		}
	}
	if !found {
		t.Fatal("node 1 holds nothing")
	}

	f.SetFault(fleetsim.ReadOutage())
	if err := f.FlushData(ctx, 1, g, 1, 2); err != nil {
		t.Fatalf("the write path must stay clean in a read outage: %v", err)
	}
	f.Run(ctx, time.Second, step)
	if len(f.Node(1).Win.Sections(g)) == 0 {
		t.Fatal("holder's own window missed its own flush")
	}
	if len(f.Node(2).Win.Sections(g)) != 0 {
		t.Fatal("a follower folded a commit it could not have read")
	}
	if f.Node(2).Errs == 0 {
		t.Fatal("follower reported no errors while blind")
	}

	f.SetFault(nil)
	f.Run(ctx, time.Second, step)
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.Node(2).Win.Sections(g)) == 0 {
		t.Fatal("follower never caught up after the heal")
	}
	for id := uint64(1); id <= 3; id++ {
		if f.Node(id).Fold.Stats.SectionsDead != 0 {
			t.Fatalf("node %d folded %d sections dead", id, f.Node(id).Fold.Stats.SectionsDead)
		}
	}
	agree(t, f, 1, 2, 3)
}

// TestStormInsideTTLNoDemotions: a SlowDown wave that scatters failures
// for less than a lease TTL costs retries and nothing else; no holder
// demotes, no epoch moves, no takeover starts.
func TestStormInsideTTLNoDemotions(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 84, 3)
	converge(t, ctx, f, 1, 15*time.Second)
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	wantH, wantE := snapshot(f, 1)
	errsBefore := f.Node(1).Errs + f.Node(2).Errs + f.Node(3).Errs

	f.SetFault(fleetsim.Storm(3))
	f.Run(ctx, 1500*time.Millisecond, step)
	f.SetFault(nil)
	f.Run(ctx, 2*time.Second, step)

	if f.Node(1).Errs+f.Node(2).Errs+f.Node(3).Errs == errsBefore {
		t.Fatal("the storm never landed a failure")
	}
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	gotH, gotE := snapshot(f, 1)
	for g := 0; g < nGroups; g++ {
		if gotH[uint16(g)] != wantH[uint16(g)] || gotE[uint16(g)] != wantE[uint16(g)] {
			t.Fatalf("group %d moved through a storm shorter than the ttl", g)
		}
	}
	agree(t, f, 1, 2, 3)
}

// TestCrashTakeoverSpreadsAndReplays is the Gate D3 mechanics at fleet
// scale: a crashed node's groups spread across the survivors by
// rendezvous inside the takeover discipline's bound, and the flushed
// data replays from a survivor's retained window.
func TestCrashTakeoverSpreadsAndReplays(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 85, 3)
	converge(t, ctx, f, 1, 15*time.Second)
	holders, _ := snapshot(f, 1)
	var crashedGroups []uint16
	for g, node := range holders {
		if node == 3 {
			crashedGroups = append(crashedGroups, g)
		}
	}
	if len(crashedGroups) == 0 {
		t.Fatal("node 3 holds nothing")
	}
	flushed := crashedGroups[0]
	if err := f.FlushData(ctx, 3, flushed, 1, 3); err != nil {
		t.Fatal(err)
	}
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}

	f.Crash(3)
	recovered := time.Duration(0)
	for elapsed := time.Duration(0); ; elapsed += step {
		if elapsed > 10*time.Second {
			t.Fatal("survivors never recovered full coverage")
		}
		f.Tick(ctx, step)
		full := true
		for _, g := range crashedGroups {
			if node, _, ok := f.Node(1).Fold.Holder(g); !ok || node == 3 {
				full = false
				break
			}
		}
		if full {
			recovered = elapsed + step
			break
		}
	}
	// Doc 02: staleness observed at ttl plus skew, a full watched ttl on
	// top, plus at most a heartbeat of pre-crash quiet. 8s is that bound
	// with tick slack.
	if recovered > 8*time.Second {
		t.Fatalf("recovery took %v of simulated time", recovered)
	}
	t.Logf("crash to full coverage in %v simulated", recovered)

	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	members := survivorsOf(f, 1)
	for _, g := range crashedGroups {
		node, epoch, _ := f.Node(1).Fold.Holder(g)
		if epoch != 2 {
			t.Fatalf("group %d recovered at epoch %d", g, epoch)
		}
		if pref, ok := obs1.PreferredNode(g, members); !ok || pref != node {
			t.Fatalf("group %d went to node %d, survivors' rendezvous prefers %d", g, node, pref)
		}
	}

	// The taker replays the crashed holder's flushed frames.
	taker, _, _ := f.Node(1).Fold.Holder(flushed)
	st, err := obs1.TakeGroup(ctx, obs1.TakeConfig{
		Store: f.Store, Prefix: "db/t", Group: flushed, Window: f.Node(taker).Win,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.FramesApplied != 3 || st.Applied != 3 {
		t.Fatalf("take stats %+v, want the crashed holder's 3 frames", st)
	}
	agree(t, f, 1, 2)
}

// survivorsOf is the observer's member table minus the nodes it
// suspects, the placement view the assertions compare against.
func survivorsOf(f *fleetsim.Fleet, observer uint64) []obs1.Member {
	n := f.Node(observer)
	at := f.Clk.Now()
	var out []obs1.Member
	for _, m := range n.Fold.Members() {
		if m.Node == observer || !n.Live.Suspect(m.Node, at) {
			out = append(out, m)
		}
	}
	return out
}

// TestWarmRestartKeepsCoverage: a node that restarts inside its lease
// TTL rejoins at the next incarnation, adopts everything it held at
// unchanged epochs, and the fleet never notices; its next flush folds
// live everywhere.
func TestWarmRestartKeepsCoverage(t *testing.T) {
	ctx := context.Background()
	f := newFleet(t, 86, 3)
	converge(t, ctx, f, 1, 15*time.Second)
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}
	wantH, wantE := snapshot(f, 1)
	held := f.Node(2).Mgr.Held()
	if len(held) == 0 {
		t.Fatal("node 2 holds nothing")
	}

	f.Crash(2)
	f.Run(ctx, time.Second, step)
	n2, err := f.Restart(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n2.Inc != 2 {
		t.Fatalf("restart came back at incarnation %d", n2.Inc)
	}
	got := n2.Mgr.Held()
	if len(got) != len(held) {
		t.Fatalf("restart adopted %v, held %v before the crash", got, held)
	}
	if err := f.FlushData(ctx, 2, held[0], 1, 2); err != nil {
		t.Fatal(err)
	}
	f.Run(ctx, 3*time.Second, step)
	if err := f.FollowAll(ctx); err != nil {
		t.Fatal(err)
	}

	gotH, gotE := snapshot(f, 1)
	for g := 0; g < nGroups; g++ {
		if gotH[uint16(g)] != wantH[uint16(g)] || gotE[uint16(g)] != wantE[uint16(g)] {
			t.Fatalf("group %d moved across a warm restart", g)
		}
	}
	for id := uint64(1); id <= 3; id++ {
		st := f.Node(id).Fold.Stats
		if st.SectionsDead != 0 || st.CommitsIncStale != 0 {
			t.Fatalf("node %d fenced the restarted node's live commit: %+v", id, st)
		}
	}
	agree(t, f, 1, 2, 3)
}
