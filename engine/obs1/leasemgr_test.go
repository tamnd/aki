package obs1_test

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// mgrNode is one node's full lease machinery on the shared store: fold,
// gate, appender, manager, all on the injected clock.
type mgrNode struct {
	fold *obs1.LeaseFold
	gate *obs1.LeaseGate
	ap   *obs1.ChainAppender
	mgr  *obs1.LeaseManager
}

func newMgrNode(t *testing.T, s obs1.Store, self uint64, ttl, skew time.Duration, clk *fakeClock) mgrNode {
	t.Helper()
	fold := obs1.NewLeaseFold()
	gate := obs1.NewLeaseGate(ttl, skew)
	ap, err := obs1.NewChainAppender(s, "db/t", 0, self, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{
		Self: self, Appender: ap, Fold: fold, Gate: gate, Now: clk.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgrNode{fold, gate, ap, mgr}
}

func TestLeaseManagerRejects(t *testing.T) {
	s := sim.New(sim.Config{Seed: 31})
	fold := obs1.NewLeaseFold()
	ap, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, fold)
	if err != nil {
		t.Fatal(err)
	}
	gate := obs1.NewLeaseGate(0, 0)
	if _, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{Appender: ap, Fold: fold, Gate: gate}); err == nil {
		t.Fatal("zero self accepted")
	}
	if _, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{Self: 1, Fold: fold, Gate: gate}); err == nil {
		t.Fatal("nil appender accepted")
	}
	if _, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{Self: 1, Appender: ap, Gate: gate}); err == nil {
		t.Fatal("nil fold accepted")
	}
	if _, err := obs1.NewLeaseManager(obs1.LeaseManagerConfig{Self: 1, Appender: ap, Fold: fold}); err == nil {
		t.Fatal("nil gate accepted")
	}
}

func TestLeaseManagerAcquire(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 32})
	clk := newFakeClock()
	const ttl, skew = 3 * time.Second, 500 * time.Millisecond

	n := newMgrNode(t, s, 1, ttl, skew, clk)
	nowMs := clk.now().UnixMilli()
	if !n.gate.Suspended(7, nowMs) {
		t.Fatal("unowned group serving")
	}

	won, err := n.mgr.Acquire(ctx, 7)
	if err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	if node, epoch, ok := n.fold.Holder(7); !ok || node != 1 || epoch != 1 {
		t.Fatalf("holder after acquire: %d %d %v", node, epoch, ok)
	}
	if n.gate.Suspended(7, nowMs) {
		t.Fatal("acquired group suspended")
	}
	if got := n.mgr.Held(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("held: %v", got)
	}

	// A second acquire of the same group is a no-op, no extra append.
	puts := s.Usage().PutRequests
	if won, err := n.mgr.Acquire(ctx, 7); err != nil || !won {
		t.Fatalf("re-acquire: %v %v", won, err)
	}
	if s.Usage().PutRequests != puts {
		t.Fatal("re-acquire appended")
	}
}

func TestLeaseManagerAcquireContested(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 33})
	clk := newFakeClock()

	a := newMgrNode(t, s, 1, 0, 0, clk)
	b := newMgrNode(t, s, 2, 0, 0, clk)

	wonA, err := a.mgr.Acquire(ctx, 3)
	if err != nil || !wonA {
		t.Fatalf("a acquire: %v %v", wonA, err)
	}
	// B catches up during its own append and finds the epoch moved.
	wonB, err := b.mgr.Acquire(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if wonB {
		t.Fatal("both nodes won the group")
	}
	if node, epoch, ok := b.fold.Holder(3); !ok || node != 1 || epoch != 1 {
		t.Fatalf("holder in loser's fold: %d %d %v", node, epoch, ok)
	}
	if got := b.mgr.Held(); len(got) != 0 {
		t.Fatalf("loser held: %v", got)
	}
	if b.fold.Stats.GrantsRejected == 0 {
		t.Fatal("losing grant folded")
	}

	// A group visibly held by someone else does not even append.
	puts := s.Usage().PutRequests
	if won, err := b.mgr.Acquire(ctx, 3); err != nil || won {
		t.Fatalf("acquire of held group: %v %v", won, err)
	}
	if s.Usage().PutRequests != puts {
		t.Fatal("acquire of held group appended")
	}
}

func TestLeaseManagerHeartbeatAndSuspension(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 34})
	clk := newFakeClock()
	const ttl, skew = 3 * time.Second, 500 * time.Millisecond

	n := newMgrNode(t, s, 1, ttl, skew, clk)
	if won, err := n.mgr.Acquire(ctx, 5); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}

	// Quiet past the believed deadline minus skew: the group suspends,
	// doc 02 section 3.5.
	clk.advance(ttl)
	if !n.gate.Suspended(5, clk.now().UnixMilli()) {
		t.Fatal("group serving past its believed deadline")
	}

	// The retry lands before anyone granted the group away: un-suspend at
	// the same epoch, nothing replayed.
	if err := n.mgr.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if n.gate.Suspended(5, clk.now().UnixMilli()) {
		t.Fatal("group suspended after a landed heartbeat")
	}
	if _, epoch, _ := n.fold.Holder(5); epoch != 1 {
		t.Fatalf("epoch moved on un-suspend: %d", epoch)
	}
}

func TestLeaseManagerHeartbeatDue(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 35})
	clk := newFakeClock()

	n := newMgrNode(t, s, 1, 0, 0, clk)
	if !n.mgr.HeartbeatDue(clk.now()) {
		t.Fatal("fresh manager not due")
	}
	if err := n.mgr.Heartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if n.mgr.HeartbeatDue(clk.now()) {
		t.Fatal("due right after a heartbeat")
	}
	clk.advance(obs1.DefaultHeartbeatEvery)
	if !n.mgr.HeartbeatDue(clk.now()) {
		t.Fatal("not due after the interval")
	}
	// A data commit is a heartbeat: NoteAppend suppresses the cadence.
	n.mgr.NoteAppend(clk.now())
	if n.mgr.HeartbeatDue(clk.now()) {
		t.Fatal("due despite a fresh commit")
	}
}

func TestLeaseManagerForeignGrantDemotes(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 36})
	clk := newFakeClock()

	a := newMgrNode(t, s, 1, 0, 0, clk)
	b := newMgrNode(t, s, 2, 0, 0, clk)

	// B is a member with a known RESP endpoint, so A's demotion can name
	// the redirect target.
	joinB := obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{
		Node: 2, Incarnation: 1, Resp: "b:6379",
	}}
	if _, err := b.ap.Append(ctx, []obs1.ChainRecord{joinB}); err != nil {
		t.Fatal(err)
	}
	if err := a.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if won, err := a.mgr.Acquire(ctx, 9); err != nil || !won {
		t.Fatalf("a acquire: %v %v", won, err)
	}

	// B grants the group to itself at the next epoch, the takeover shape;
	// the freeness discipline is B's business, the fold moves either way.
	if err := b.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	grant := obs1.GrantRecord{Group: 9, Node: 2, Epoch: b.fold.NextEpoch(9)}
	if _, err := b.ap.Append(ctx, []obs1.ChainRecord{grant}); err != nil {
		t.Fatal(err)
	}

	if err := a.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	a.mgr.Reconcile()
	if got := a.mgr.Held(); len(got) != 0 {
		t.Fatalf("held after foreign grant: %v", got)
	}
	ep, ok := a.gate.Demoted(9)
	if !ok || ep != "b:6379" {
		t.Fatalf("demotion endpoint: %q %v", ep, ok)
	}
}

func TestLeaseManagerRelease(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 37})
	clk := newFakeClock()

	n := newMgrNode(t, s, 1, 0, 0, clk)
	if won, err := n.mgr.Acquire(ctx, 4); err != nil || !won {
		t.Fatalf("acquire: %v %v", won, err)
	}
	if err := n.mgr.Release(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := n.fold.Holder(4); ok {
		t.Fatal("released group still held on the chain")
	}
	if !n.gate.Suspended(4, clk.now().UnixMilli()) {
		t.Fatal("released group serving")
	}
	if got := n.mgr.Held(); len(got) != 0 {
		t.Fatalf("held after release: %v", got)
	}
	// Releasing a group we do not hold is a no-op.
	if err := n.mgr.Release(ctx, 4); err != nil {
		t.Fatal(err)
	}

	// The epoch survives the release, so the next grant fences the past.
	if won, err := n.mgr.Acquire(ctx, 4); err != nil || !won {
		t.Fatalf("re-acquire: %v %v", won, err)
	}
	if _, epoch, ok := n.fold.Holder(4); !ok || epoch != 2 {
		t.Fatalf("epoch after re-acquire: %d %v", epoch, ok)
	}
}

func TestCheckpointObjectCadence(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 38})
	clk := newFakeClock()
	if err := obs1.CreateRoot(ctx, s, "db/t", false, obs1.Root{G: 128, D: 1}); err != nil {
		t.Fatal(err)
	}

	// Generous record and age bounds so only the object bound can fire:
	// heartbeats are one record per object, and the boot lab priced boot
	// by trailing objects, so the cadence must count them.
	ck, err := obs1.NewCheckpointer(obs1.NewLeaseFold(), 1, 0, 1<<20, 8, time.Hour, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ap, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, ck)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, err := ap.Append(ctx, hb()); err != nil {
			t.Fatal(err)
		}
	}
	if ck.Due() {
		t.Fatal("due before the object bound")
	}
	if _, err := ap.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if !ck.Due() {
		t.Fatal("not due at the object bound")
	}
	if _, err := ck.WriteCheckpoint(ctx, s, "db/t", false, ap); err != nil {
		t.Fatal(err)
	}
	if ck.Due() {
		t.Fatal("still due after the checkpoint folded")
	}
}
