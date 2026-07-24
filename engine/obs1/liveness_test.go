package obs1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

func joinRec(node uint64) obs1.MemberRecord {
	return obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{
		Node: node, Incarnation: 1, Resp: "r", Mesh: "m", Weight: 1, Version: "dev",
	}}
}

func leaveRec(node uint64) obs1.MemberRecord {
	return obs1.MemberRecord{Op: obs1.MemberLeave, Member: obs1.Member{Node: node, Incarnation: 1}}
}

type nopApplier struct{}

func (nopApplier) ApplyChain(obs1.ChainPos, obs1.Header, obs1.ChainBatch) error { return nil }

func TestLivenessRejects(t *testing.T) {
	clk := newFakeClock()
	if _, err := obs1.NewLiveness(nil, time.Second, 0, clk.now); err == nil {
		t.Fatal("nil inner accepted")
	}
	if _, err := obs1.NewLiveness(nopApplier{}, time.Second, 0, nil); err == nil {
		t.Fatal("nil clock accepted")
	}
	if _, err := obs1.NewLiveness(nopApplier{}, 0, time.Second, clk.now); err == nil {
		t.Fatal("zero ttl accepted")
	}
}

type failApplier struct{ err error }

func (f failApplier) ApplyChain(obs1.ChainPos, obs1.Header, obs1.ChainBatch) error { return f.err }

func TestLivenessInnerErrorSkipsStamp(t *testing.T) {
	clk := newFakeClock()
	boom := errors.New("boom")
	l, err := obs1.NewLiveness(failApplier{boom}, time.Second, 0, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	pos := obs1.ChainPos{Seq: 1}
	if err := l.ApplyChain(pos, obs1.Header{Writer: 7}, obs1.ChainBatch{}); !errors.Is(err, boom) {
		t.Fatalf("apply error: %v", err)
	}
	if _, ok := l.LastSeen(7); ok {
		t.Fatal("failed apply stamped the writer")
	}
}

func TestLivenessSuspectAndRecover(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 21})
	clk := newFakeClock()
	const ttl, skew = 10 * time.Second, 2 * time.Second

	live, err := obs1.NewLiveness(obs1.NewLeaseFold(), ttl, skew, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, live)
	if err != nil {
		t.Fatal(err)
	}
	b, err := obs1.NewChainAppender(s, "db/t", 0, 2, 1, obs1.ChainPos{}, obs1.NewLeaseFold())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Append(ctx, []obs1.ChainRecord{joinRec(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Append(ctx, []obs1.ChainRecord{joinRec(2)}); err != nil {
		t.Fatal(err)
	}
	if err := a.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	now := clk.now()
	if live.Suspect(1, now) || live.Suspect(2, now) {
		t.Fatal("fresh nodes suspect")
	}

	// Node 2 goes silent; node 1 keeps heartbeating. Just inside the
	// horizon nobody is suspect, one tick past it node 2 is.
	clk.advance(ttl + skew)
	if _, err := a.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if live.Suspect(2, clk.now()) {
		t.Fatal("suspect at exactly the horizon")
	}
	clk.advance(time.Second)
	now = clk.now()
	if live.Suspect(1, now) {
		t.Fatal("heartbeating node suspect")
	}
	if !live.Suspect(2, now) {
		t.Fatal("silent node not suspect past the horizon")
	}
	row, ok := live.LastSeen(2)
	if !ok || row.Pos.Seq != 2 {
		t.Fatalf("last seen for node 2: %+v %v", row, ok)
	}

	// Node 2 reappears on the chain; following it clears the verdict.
	if _, err := b.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if err := a.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if live.Suspect(2, clk.now()) {
		t.Fatal("suspect after reappearing")
	}
}

func TestLivenessLeaveIsNotSuspect(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 22})
	clk := newFakeClock()

	fold := obs1.NewLeaseFold()
	live, err := obs1.NewLiveness(fold, 10*time.Second, 0, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	a, err := obs1.NewChainAppender(s, "db/t", 0, 1, 1, obs1.ChainPos{}, live)
	if err != nil {
		t.Fatal(err)
	}

	recs := []obs1.ChainRecord{joinRec(1), joinRec(2)}
	if _, err := a.Append(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if _, ok := live.LastSeen(2); !ok {
		t.Fatal("joined node has no row")
	}

	if _, err := a.Append(ctx, []obs1.ChainRecord{leaveRec(2)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := live.LastSeen(2); ok {
		t.Fatal("departed node kept a row")
	}
	// The member table dropped the node too, so however long the clock
	// runs, the departed node never makes the verdict list; node 1 does,
	// since it also went silent, which is the contrast the test wants.
	clk.advance(time.Hour)
	if sus := live.Suspects(fold.Members(), clk.now()); len(sus) != 1 || sus[0] != 1 {
		t.Fatalf("suspects after leave: %v", sus)
	}
}

func TestLivenessObserversConverge(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 23})
	clk := newFakeClock()
	const ttl = 10 * time.Second

	type observer struct {
		fold *obs1.LeaseFold
		live *obs1.Liveness
		ap   *obs1.ChainAppender
	}
	mk := func(writer uint64) observer {
		fold := obs1.NewLeaseFold()
		live, err := obs1.NewLiveness(fold, ttl, 0, clk.now)
		if err != nil {
			t.Fatal(err)
		}
		ap, err := obs1.NewChainAppender(s, "db/t", 0, writer, 1, obs1.ChainPos{}, live)
		if err != nil {
			t.Fatal(err)
		}
		return observer{fold, live, ap}
	}
	x, y := mk(1), mk(2)

	recs := []obs1.ChainRecord{joinRec(1), joinRec(2), joinRec(3)}
	if _, err := x.ap.Append(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if _, err := y.ap.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if err := x.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}

	// Node 3 joined and never acted again; nodes 1 and 2 keep appending
	// past the horizon. Both observers read the same chain positions and
	// reach the same verdict.
	clk.advance(ttl + time.Second)
	if _, err := x.ap.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if _, err := y.ap.Append(ctx, hb()); err != nil {
		t.Fatal(err)
	}
	if err := x.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := y.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}

	now := clk.now()
	sx := x.live.Suspects(x.fold.Members(), now)
	sy := y.live.Suspects(y.fold.Members(), now)
	if len(sx) != 1 || sx[0] != 3 {
		t.Fatalf("observer x suspects: %v", sx)
	}
	if len(sy) != 1 || sy[0] != 3 {
		t.Fatalf("observer y suspects: %v", sy)
	}
	if x.fold.StateSum() != y.fold.StateSum() {
		t.Fatal("folds diverged")
	}
}

func TestLivenessPrimed(t *testing.T) {
	clk := newFakeClock()
	live, err := obs1.NewLiveness(nopApplier{}, 10*time.Second, 0, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	members := []obs1.Member{{Node: 1, Incarnation: 1}, {Node: 2, Incarnation: 1}}
	pos := obs1.ChainPos{Seq: 40}
	live.Primed(members, pos)

	now := clk.now()
	if live.Suspect(1, now) || live.Suspect(2, now) {
		t.Fatal("primed nodes suspect at boot")
	}
	row, ok := live.LastSeen(1)
	if !ok || row.Pos != pos {
		t.Fatalf("primed row: %+v %v", row, ok)
	}
	clk.advance(11 * time.Second)
	if sus := live.Suspects(members, clk.now()); len(sus) != 2 {
		t.Fatalf("suspects past horizon: %v", sus)
	}
}
