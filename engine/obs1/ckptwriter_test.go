package obs1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// fakeClock is the injected observer clock; nothing in these tests reads
// the wall clock.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock               { return &fakeClock{t: time.UnixMilli(1_000_000)} }
func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// ckptNode builds a fold wrapped in a checkpointer wired as a real
// appender's applier on the shared store.
func ckptNode(t *testing.T, s obs1.Store, prefix string, id uint64, maxRecords int, maxAge time.Duration, clk *fakeClock) (*obs1.ChainAppender, *obs1.Checkpointer) {
	t.Helper()
	c, err := obs1.NewCheckpointer(obs1.NewLeaseFold(), id, 0, maxRecords, maxRecords, maxAge, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	a, err := obs1.NewChainAppender(s, prefix, 0, id, 1, obs1.ChainPos{}, c)
	if err != nil {
		t.Fatal(err)
	}
	return a, c
}

func makeRoot(t *testing.T, s obs1.Store, prefix string) {
	t.Helper()
	if err := obs1.CreateRoot(context.Background(), s, prefix, false, obs1.Root{G: 8, D: 1}); err != nil {
		t.Fatal(err)
	}
}

func mustAppendTo(t *testing.T, a *obs1.ChainAppender, recs ...obs1.ChainRecord) {
	t.Helper()
	if _, err := a.Append(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
}

// TestCadenceByRecords: the record count triggers, only an own crossing
// batch raises Due, and the written checkpoint round-trips with the
// fold's tables while the 0x06 record resets everyone's cadence.
func TestCadenceByRecords(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 21})
	const prefix = "db/ck"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a7, c7 := ckptNode(t, s, prefix, 7, 4, time.Hour, clk)
	a9, c9 := ckptNode(t, s, prefix, 9, 4, time.Hour, clk)

	mustAppendTo(t, a7, grant(1, 7, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, grant(2, 7, 1))
	if c7.Due() {
		t.Fatal("due after 3 records, threshold is 4")
	}
	mustAppendTo(t, a7, commit(7, 2, 1))
	if !c7.Due() {
		t.Fatal("own 4th record crossed the threshold, want due")
	}

	// The follower sees the same 4 records cross but none of them are
	// its own, so the checkpoint is not its to write.
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if c9.Due() {
		t.Fatal("foreign records crossed the threshold, follower must not be due")
	}

	pos, err := c7.WriteCheckpoint(ctx, s, prefix, false, a7)
	if err != nil {
		t.Fatal(err)
	}
	if pos.Seq != 4 {
		t.Fatalf("checkpoint through seq %d, want 4", pos.Seq)
	}
	if c7.Due() {
		t.Fatal("own 0x06 record must reset the cadence")
	}
	ck, _, err := obs1.LoadCheckpoint(ctx, s, prefix, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Through.Seq != 4 || len(ck.Leases) != 2 {
		t.Fatalf("checkpoint through %d with %d leases, want 4 with 2", ck.Through.Seq, len(ck.Leases))
	}
	root, err := obs1.LoadRoot(ctx, s, prefix, false)
	if err != nil {
		t.Fatal(err)
	}
	if root.CkptSeq != 4 {
		t.Fatalf("root points at ckpt %d, want 4", root.CkptSeq)
	}

	// The 0x06 record resets the follower too.
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if c9.Due() {
		t.Fatal("follower saw the 0x06, cadence must have reset")
	}
	wantHolder(t, c9.Fold(), 1, 7, 1)
}

// TestCadenceByTime: the age trigger fires on the first own append past
// the window, even with the record count far from its threshold.
func TestCadenceByTime(t *testing.T) {
	s := sim.New(sim.Config{Seed: 22})
	const prefix = "db/ct"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a, c := ckptNode(t, s, prefix, 7, 1<<20, time.Minute, clk)

	mustAppendTo(t, a, grant(1, 7, 1))
	if c.Due() {
		t.Fatal("fresh chain, not due")
	}
	clk.advance(59 * time.Second)
	mustAppendTo(t, a, commit(7, 1, 1))
	if c.Due() {
		t.Fatal("59s elapsed, window is 60s")
	}
	clk.advance(2 * time.Second)
	mustAppendTo(t, a, commit(7, 1, 1))
	if !c.Due() {
		t.Fatal("own append past the window, want due")
	}
	if _, err := c.WriteCheckpoint(context.Background(), s, prefix, false, a); err != nil {
		t.Fatal(err)
	}
	if c.Due() {
		t.Fatal("cadence must reset after the checkpoint")
	}
}

// TestTriggerInherited: if the responsible node never writes, the count
// keeps climbing and the next node to append inherits the trigger.
func TestTriggerInherited(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 23})
	const prefix = "db/ci"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a7, _ := ckptNode(t, s, prefix, 7, 4, time.Hour, clk)
	a9, c9 := ckptNode(t, s, prefix, 9, 4, time.Hour, clk)

	mustAppendTo(t, a7, grant(1, 7, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	// Node 7 crossed and is due but dies here without writing.
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if c9.Due() {
		t.Fatal("crossing batch was foreign")
	}
	mustAppendTo(t, a9, grant(2, 9, 1))
	if !c9.Due() {
		t.Fatal("own append past the uncleared threshold must inherit the trigger")
	}
}

// TestCheckpointRaceHarmless: two nodes checkpoint the same seq; the
// loser's object CAS 412s, its 0x06 still lands, the root no-ops, and a
// cold folder reads the chain clean.
func TestCheckpointRaceHarmless(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 24})
	const prefix = "db/cr"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a7, c7 := ckptNode(t, s, prefix, 7, 4, time.Hour, clk)
	a9, c9 := ckptNode(t, s, prefix, 9, 4, time.Hour, clk)

	mustAppendTo(t, a7, grant(1, 7, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	// Both believe seq 4 is the summary point; 7 wins the object CAS.
	if _, err := c7.WriteCheckpoint(ctx, s, prefix, false, a7); err != nil {
		t.Fatal(err)
	}
	pos, err := c9.WriteCheckpoint(ctx, s, prefix, false, a9)
	if err != nil {
		t.Fatalf("losing the checkpoint CAS must be success: %v", err)
	}
	if pos.Seq != 4 {
		t.Fatalf("loser summarized through %d, want 4", pos.Seq)
	}
	root, err := obs1.LoadRoot(ctx, s, prefix, false)
	if err != nil {
		t.Fatal(err)
	}
	if root.CkptSeq != 4 {
		t.Fatalf("root ckpt %d, want 4", root.CkptSeq)
	}
	// A cold folder replays the whole chain, both 0x06 records included;
	// the winner follows to the shared tail first so applied positions
	// compare like for like.
	if err := a7.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	cold := obs1.NewLeaseFold()
	ac, err := obs1.NewChainAppender(s, prefix, 0, 11, 1, obs1.ChainPos{}, cold)
	if err != nil {
		t.Fatal(err)
	}
	if err := ac.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if cold.StateSum() != c7.Fold().StateSum() {
		t.Fatal("cold replay disagrees with the winner's fold")
	}
}

// TestDeadlineStamps: DeadlineMS is this observer's arrival time of the
// group's last renewal plus the TTL; released rows stay at zero.
func TestDeadlineStamps(t *testing.T) {
	s := sim.New(sim.Config{Seed: 25})
	const prefix = "db/cd"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a, c := ckptNode(t, s, prefix, 7, 1<<20, time.Hour, clk)

	mustAppendTo(t, a, grant(1, 7, 1))
	mustAppendTo(t, a, grant(2, 7, 1))
	clk.advance(500 * time.Millisecond)
	mustAppendTo(t, a, obs1.HeartbeatRecord{})
	renewedAt := clk.now()
	clk.advance(200 * time.Millisecond)
	mustAppendTo(t, a, obs1.ReleaseRecord{Group: 2, Epoch: 1})

	ck := c.Snapshot()
	if len(ck.Leases) != 2 {
		t.Fatalf("%d leases, want 2", len(ck.Leases))
	}
	want := uint64(renewedAt.Add(obs1.DefaultLeaseTTL).UnixMilli())
	if l := ck.Leases[0]; l.Group != 1 || l.DeadlineMS != want {
		t.Fatalf("group 1 deadline %d, want %d", l.DeadlineMS, want)
	}
	if l := ck.Leases[1]; l.Node != 0 || l.DeadlineMS != 0 {
		t.Fatalf("released row node %d deadline %d, want 0 and 0", l.Node, l.DeadlineMS)
	}
}

// TestTrimBehindSecondNewest: the trim floor is the second-newest
// checkpoint, the previous checkpoint object survives its own trim
// round, and both boot paths still replay after deletion.
func TestTrimBehindSecondNewest(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 26})
	const prefix = "db/tr"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a, c := ckptNode(t, s, prefix, 7, 4, time.Hour, clk)

	if from, floor := c.Trimmable(); from != 0 || floor != 0 {
		t.Fatal("nothing trimmable before two checkpoints exist")
	}
	fill := func() {
		for range 4 {
			mustAppendTo(t, a, commit(7, 1, 1))
		}
	}
	mustAppendTo(t, a, grant(1, 7, 1))
	mustAppendTo(t, a, commit(7, 1, 1))
	fill() // seqs 3-6 cross at 4 records since the start
	if !c.Due() {
		t.Fatal("want due")
	}
	first, err := c.WriteCheckpoint(ctx, s, prefix, false, a) // ckpt A, 0x06 at 7
	if err != nil {
		t.Fatal(err)
	}
	if from, floor := c.Trimmable(); from != 0 || floor != 0 {
		t.Fatal("one checkpoint is not enough to trim")
	}
	fill()                                                     // seqs 8-11
	second, err := c.WriteCheckpoint(ctx, s, prefix, false, a) // ckpt B, 0x06 at 12
	if err != nil {
		t.Fatal(err)
	}

	from, floor := c.Trimmable()
	if from != 0 || floor != first.Seq {
		t.Fatalf("trimmable [%d, %d), want [0, %d)", from, floor, first.Seq)
	}
	if err := obs1.TrimChain(ctx, s, prefix, 0, from, floor); err != nil {
		t.Fatal(err)
	}
	c.Trimmed(floor)
	if f, fl := c.Trimmable(); f != 0 || fl != 0 {
		t.Fatalf("trim range must be empty after Trimmed, got [%d, %d)", f, fl)
	}

	// Everything below ckpt A is gone, ckpt A itself is retained, and a
	// reader replaying from it still reaches the tail.
	if _, _, err := s.Get(ctx, prefix+"/chain/00/0000000000000001"); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("seq 1 should be deleted, got %v", err)
	}
	ckA, _, err := obs1.LoadCheckpoint(ctx, s, prefix, 0, first.Seq)
	if err != nil {
		t.Fatalf("previous checkpoint must be retained: %v", err)
	}
	mid := obs1.NewLeaseFold()
	if err := mid.Prime(ckA); err != nil {
		t.Fatal(err)
	}
	am, err := obs1.NewChainAppender(s, prefix, 0, 12, 1, ckA.Through, mid)
	if err != nil {
		t.Fatal(err)
	}
	if err := am.Follow(ctx); err != nil {
		t.Fatalf("replay from the retained floor broke: %v", err)
	}

	// A third checkpoint moves the floor; this trim round deletes ckpt A
	// along with the chain range, and ckpt B survives.
	fill() // seqs 13-16
	if _, err := c.WriteCheckpoint(ctx, s, prefix, false, a); err != nil {
		t.Fatal(err)
	}
	from, floor = c.Trimmable()
	if from != first.Seq || floor != second.Seq {
		t.Fatalf("trimmable [%d, %d), want [%d, %d)", from, floor, first.Seq, second.Seq)
	}
	if err := obs1.TrimChain(ctx, s, prefix, 0, from, floor); err != nil {
		t.Fatal(err)
	}
	c.Trimmed(floor)
	if _, _, err := obs1.LoadCheckpoint(ctx, s, prefix, 0, first.Seq); !errors.Is(err, obs1.ErrNotFound) {
		t.Fatalf("ckpt A is older than the second-newest and should be deleted, got %v", err)
	}
	if _, _, err := obs1.LoadCheckpoint(ctx, s, prefix, 0, second.Seq); err != nil {
		t.Fatalf("ckpt B is the second-newest and must be retained: %v", err)
	}
}

// TestBootPrimeFollow: the BootChain, Prime, Follow order works through
// the checkpointer, and the booted node's cadence and floor start at the
// checkpoint it primed from.
func TestBootPrimeFollow(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 27})
	const prefix = "db/bp"
	makeRoot(t, s, prefix)
	clk := newFakeClock()
	a7, c7 := ckptNode(t, s, prefix, 7, 4, time.Hour, clk)

	mustAppendTo(t, a7, grant(1, 7, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	mustAppendTo(t, a7, commit(7, 1, 1))
	pos, err := c7.WriteCheckpoint(ctx, s, prefix, false, a7)
	if err != nil {
		t.Fatal(err)
	}
	mustAppendTo(t, a7, commit(7, 1, 1)) // tail past the checkpoint

	cb, err := obs1.NewCheckpointer(obs1.NewLeaseFold(), 9, 0, 4, 4096, time.Hour, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ab, ck, err := obs1.BootChain(ctx, s, prefix, false, 0, 9, 1, cb)
	if err != nil {
		t.Fatal(err)
	}
	if ck.Through != pos {
		t.Fatalf("boot found ckpt %+v, want %+v", ck.Through, pos)
	}
	if err := cb.Prime(ck); err != nil {
		t.Fatal(err)
	}
	if err := ab.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	wantHolder(t, cb.Fold(), 1, 7, 1)
	if cb.Fold().Applied().Seq != pos.Seq+2 {
		t.Fatalf("booted fold applied %d, want %d", cb.Fold().Applied().Seq, pos.Seq+2)
	}
	// The primed position is the newest known checkpoint, so nothing is
	// trimmable until a second one lands, and the boot-time stamp puts a
	// deadline on the inherited lease.
	if from, floor := cb.Trimmable(); from != 0 || floor != 0 {
		t.Fatalf("booted node computed trim range [%d, %d) from one checkpoint", from, floor)
	}
	if l := cb.Snapshot().Leases[0]; l.DeadlineMS == 0 {
		t.Fatal("inherited lease must carry the observer's boot stamp")
	}
}
