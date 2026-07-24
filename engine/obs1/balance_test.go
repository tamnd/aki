package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func members(weights map[uint64]uint16) []obs1.Member {
	var out []obs1.Member
	for node, w := range weights {
		out = append(out, obs1.Member{Node: node, Incarnation: 1, Weight: w})
	}
	return out
}

func TestPreferredNodeSpread(t *testing.T) {
	mem := members(map[uint64]uint16{1: 100, 2: 100, 3: 100})
	const groups = 128
	counts := map[uint64]int{}
	for g := 0; g < groups; g++ {
		node, ok := obs1.PreferredNode(uint16(g), mem)
		if !ok {
			t.Fatalf("group %d unplaced", g)
		}
		counts[node]++
	}
	// Equal weights should land near a third each; wide bounds, the
	// scores are deterministic so this can never flake.
	for node, n := range counts {
		if n < groups/6 || n > groups/2+groups/6 {
			t.Fatalf("node %d took %d of %d groups", node, n, groups)
		}
	}

	// The same inputs give the same placement everywhere.
	for g := 0; g < groups; g++ {
		a, _ := obs1.PreferredNode(uint16(g), mem)
		b, _ := obs1.PreferredNode(uint16(g), mem)
		if a != b {
			t.Fatalf("group %d placement unstable", g)
		}
	}
}

func TestPreferredNodeWeights(t *testing.T) {
	mem := members(map[uint64]uint16{1: 200, 2: 100})
	const groups = 1024
	counts := map[uint64]int{}
	for g := 0; g < groups; g++ {
		node, _ := obs1.PreferredNode(uint16(g), mem)
		counts[node]++
	}
	// Twice the weight expects twice the groups: node 1 near 683 of
	// 1024. Bounds generous and deterministic.
	if counts[1] < groups/2 || counts[1] > groups*4/5 {
		t.Fatalf("weight 200 node took %d of %d", counts[1], groups)
	}

	// Weight zero is out of the running entirely.
	drained := members(map[uint64]uint16{1: 0, 2: 100})
	for g := 0; g < 64; g++ {
		node, ok := obs1.PreferredNode(uint16(g), drained)
		if !ok || node != 2 {
			t.Fatalf("drained node still preferred for group %d", g)
		}
	}
	if _, ok := obs1.PreferredNode(0, members(map[uint64]uint16{1: 0})); ok {
		t.Fatal("placement with no eligible members")
	}
}

func TestPreferredNodeMinimalDisruption(t *testing.T) {
	three := members(map[uint64]uint16{1: 100, 2: 100, 3: 100})
	two := members(map[uint64]uint16{1: 100, 2: 100})
	const groups = 256
	for g := 0; g < groups; g++ {
		before, _ := obs1.PreferredNode(uint16(g), three)
		after, _ := obs1.PreferredNode(uint16(g), two)
		if before != 3 && after != before {
			t.Fatalf("group %d moved from %d to %d though node 3 was not involved", g, before, after)
		}
	}
}

func TestBalancerConverges(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 41})
	clk := newFakeClock()
	const nGroups = 16

	a := newMgrNode(t, s, 1, 0, 0, clk)
	b := newMgrNode(t, s, 2, 0, 0, clk)

	// Both nodes are members at equal weight; A grabs every group first,
	// the join-then-imbalance shape of the doc 10 elasticity scenario.
	joins := []obs1.ChainRecord{
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 1, Incarnation: 1, Weight: 100}},
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 2, Incarnation: 1, Weight: 100}},
	}
	if _, err := a.ap.Append(ctx, joins); err != nil {
		t.Fatal(err)
	}
	for g := 0; g < nGroups; g++ {
		if won, err := a.mgr.Acquire(ctx, uint16(g)); err != nil || !won {
			t.Fatalf("seed acquire %d: %v %v", g, won, err)
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
	// Shedding goes through release until the handoff slice exists; the
	// hook shape is the handoff seam.
	sheds := 0
	balA.Shed = func(ctx context.Context, group uint16) error {
		sheds++
		return a.mgr.Release(ctx, group)
	}

	wantB := 0
	for g := 0; g < nGroups; g++ {
		if pref, _ := obs1.PreferredNode(uint16(g), a.fold.Members()); pref == 2 {
			wantB++
		}
	}
	if wantB == 0 {
		t.Fatal("placement never prefers node 2; test inputs are useless")
	}

	// One shed and one take per round: A sheds a group that prefers B,
	// B takes it next tick. Convergence needs wantB rounds.
	for round := 0; round < wantB+2; round++ {
		if err := balA.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
		b.mgr.Reconcile()
		if err := balB.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if err := a.ap.Follow(ctx); err != nil {
			t.Fatal(err)
		}
		a.mgr.Reconcile()
	}

	if sheds != wantB {
		t.Fatalf("shed %d groups, placement wanted %d moved", sheds, wantB)
	}
	if got := len(b.mgr.Held()); got != wantB {
		t.Fatalf("node 2 holds %d, placement wants %d", got, wantB)
	}
	if got := len(a.mgr.Held()); got != nGroups-wantB {
		t.Fatalf("node 1 holds %d, placement wants %d", got, nGroups-wantB)
	}

	// Balanced fleet: further ticks move nothing.
	before := sheds
	heldB := len(b.mgr.Held())
	if err := balA.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := balB.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if sheds != before || len(b.mgr.Held()) != heldB {
		t.Fatal("balanced fleet still moving groups")
	}
}

func TestBalancerDueAndAlive(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 42})
	clk := newFakeClock()

	n := newMgrNode(t, s, 1, 0, 0, clk)
	joins := []obs1.ChainRecord{
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 1, Incarnation: 1, Weight: 100}},
		obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 2, Incarnation: 1, Weight: 100}},
	}
	if _, err := n.ap.Append(ctx, joins); err != nil {
		t.Fatal(err)
	}

	bal, err := obs1.NewBalancer(n.mgr, 16, time.Second, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	if !bal.Due(clk.now()) {
		t.Fatal("fresh balancer not due")
	}
	if err := bal.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if bal.Due(clk.now()) {
		t.Fatal("due right after a tick")
	}
	clk.advance(2 * time.Second)
	if !bal.Due(clk.now()) {
		t.Fatal("not due after interval plus jitter")
	}

	// With node 2 suspect, every group prefers self: the take path sees
	// only live members, and still moves one group per tick.
	bal.Alive = func(node uint64) bool { return node == 1 }
	before := len(n.mgr.Held())
	if err := bal.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(n.mgr.Held()); got != before+1 {
		t.Fatalf("held went %d to %d in one tick, want exactly one take", before, got)
	}
}
