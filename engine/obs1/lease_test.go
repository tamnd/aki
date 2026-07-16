// The lease fold against doc 02 section 3: epoch ladder, release rules,
// commit fencing, renewal positions, member incarnation guards, epoch
// history, checkpoint priming, determinism, and the holder-side guard.
// Every batch goes through real chain bytes, so the fold is tested the
// way the appender feeds it.
package obs1_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// foldApply round-trips records through AppendChainBatch and ParseChainBatch
// and folds them at seq.
func foldApply(t *testing.T, f *obs1.LeaseFold, seq uint64, writer uint64, recs ...obs1.ChainRecord) {
	t.Helper()
	if err := foldApplyErr(f, seq, writer, recs...); err != nil {
		t.Fatalf("apply seq %d: %v", seq, err)
	}
}

func foldApplyErr(f *obs1.LeaseFold, seq uint64, writer uint64, recs ...obs1.ChainRecord) error {
	b, err := obs1.AppendChainBatch(nil, writer, obs1.ChainBatch{BatchID: seq, Incarnation: 1, Records: recs})
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	batch, h, err := obs1.ParseChainBatch(b)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	return f.ApplyChain(obs1.ChainPos{Seq: seq}, h, batch)
}

func grant(g uint16, node uint64, epoch uint32) obs1.GrantRecord {
	return obs1.GrantRecord{Group: g, Node: node, Epoch: epoch}
}

func commit(writer uint64, g uint16, epoch uint32) obs1.CommitRecord {
	return obs1.CommitRecord{
		WALNode: writer, WALSeq: 1, WALSize: 128,
		Sections: []obs1.CommitSection{{
			Group: g, Epoch: epoch, Offset: 32, StoredLen: 64,
			NFrames: 1, FirstSeq: 1, LastSeq: 1,
		}},
	}
}

func wantHolder(t *testing.T, f *obs1.LeaseFold, g uint16, node uint64, epoch uint32) {
	t.Helper()
	gotNode, gotEpoch, ok := f.Holder(g)
	if !ok || gotNode != node || gotEpoch != epoch {
		t.Fatalf("group %d holder = (%d, %d, %v), want (%d, %d, true)", g, gotNode, gotEpoch, ok, node, epoch)
	}
}

func TestGrantEpochLadder(t *testing.T) {
	f := obs1.NewLeaseFold()
	foldApply(t, f, 1, 7, grant(3, 7, 2)) // fresh group, first grant must be epoch 1
	if _, _, ok := f.Holder(3); ok {
		t.Fatal("epoch 2 grant on a never-leased group folded")
	}
	foldApply(t, f, 2, 7, grant(3, 7, 1))
	wantHolder(t, f, 3, 7, 1)
	foldApply(t, f, 3, 9, grant(3, 9, 3)) // skips an epoch, rejected
	wantHolder(t, f, 3, 7, 1)
	foldApply(t, f, 4, 9, grant(3, 9, 2)) // cur+1, folds; freeness is writer discipline
	wantHolder(t, f, 3, 9, 2)
	foldApply(t, f, 5, 9, grant(4, 0, 1)) // node id 0 never folds
	if _, _, ok := f.Holder(4); ok {
		t.Fatal("grant naming node 0 folded")
	}
	if f.Stats.GrantsRejected != 3 {
		t.Fatalf("GrantsRejected = %d, want 3", f.Stats.GrantsRejected)
	}
}

func TestReleaseRules(t *testing.T) {
	f := obs1.NewLeaseFold()
	foldApply(t, f, 1, 7, grant(3, 7, 1))
	foldApply(t, f, 2, 7, obs1.ReleaseRecord{Group: 3, Epoch: 2}) // wrong epoch
	wantHolder(t, f, 3, 7, 1)
	foldApply(t, f, 3, 9, obs1.ReleaseRecord{Group: 3, Epoch: 1}) // wrong writer
	wantHolder(t, f, 3, 7, 1)
	foldApply(t, f, 4, 7, obs1.ReleaseRecord{Group: 3, Epoch: 1})
	if _, _, ok := f.Holder(3); ok {
		t.Fatal("group 3 still held after its holder released it")
	}
	foldApply(t, f, 5, 7, obs1.ReleaseRecord{Group: 3, Epoch: 1}) // double release
	if f.Stats.ReleasesRejected != 3 {
		t.Fatalf("ReleasesRejected = %d, want 3", f.Stats.ReleasesRejected)
	}
	foldApply(t, f, 6, 9, grant(3, 9, 2)) // released group is free at epoch+1
	wantHolder(t, f, 3, 9, 2)
	ls := f.Leases()
	if len(ls) != 1 || ls[0].Node != 9 || ls[0].Epoch != 2 {
		t.Fatalf("lease table = %+v", ls)
	}
}

func TestCommitFence(t *testing.T) {
	f := obs1.NewLeaseFold()
	var verdicts []obs1.CommitVerdict
	f.OnCommit = func(v obs1.CommitVerdict) error {
		verdicts = append(verdicts, v)
		return nil
	}
	foldApply(t, f, 1, 7, grant(3, 7, 1))
	foldApply(t, f, 2, 9, grant(3, 9, 2))  // takeover: node 7 is now a zombie
	foldApply(t, f, 3, 7, commit(7, 3, 1)) // zombie commit under the old epoch
	foldApply(t, f, 4, 9, commit(9, 3, 2)) // the owner's commit
	foldApply(t, f, 5, 7, commit(7, 3, 2)) // right epoch, wrong writer
	foldApply(t, f, 6, 9, obs1.ReleaseRecord{Group: 3, Epoch: 2})
	foldApply(t, f, 7, 9, commit(9, 3, 2)) // former holder after release
	want := []bool{false, true, false, false}
	if len(verdicts) != len(want) {
		t.Fatalf("got %d verdicts, want %d", len(verdicts), len(want))
	}
	for i, v := range verdicts {
		if len(v.Live) != 1 || v.Live[0] != want[i] {
			t.Fatalf("verdict %d Live = %v, want [%v]", i, v.Live, want[i])
		}
	}
	if f.Stats.SectionsDead != 3 {
		t.Fatalf("SectionsDead = %d, want 3", f.Stats.SectionsDead)
	}
}

func TestRenewalPositions(t *testing.T) {
	f := obs1.NewLeaseFold()
	foldApply(t, f, 1, 7, grant(3, 7, 1))
	foldApply(t, f, 2, 9, grant(5, 9, 1))
	foldApply(t, f, 3, 7, obs1.HeartbeatRecord{}) // renews 7's groups only
	if p, _ := f.LastRenewal(3); p.Seq != 3 {
		t.Fatalf("group 3 renewal at %d, want 3", p.Seq)
	}
	if p, _ := f.LastRenewal(5); p.Seq != 2 {
		t.Fatalf("group 5 renewal at %d, want 2", p.Seq)
	}
	foldApply(t, f, 4, 9, commit(9, 5, 1)) // a commit renews too
	if p, _ := f.LastRenewal(5); p.Seq != 4 {
		t.Fatalf("group 5 renewal at %d, want 4", p.Seq)
	}
	foldApply(t, f, 5, 9, grant(6, 9, 1)) // a grant renews only the granted group
	if p, _ := f.LastRenewal(5); p.Seq != 4 {
		t.Fatalf("group 5 renewal moved to %d on another group's grant", 5)
	}
	if held := f.HeldBy(9); len(held) != 2 || held[0] != 5 || held[1] != 6 {
		t.Fatalf("HeldBy(9) = %v", held)
	}
}

func TestMemberIncarnationGuards(t *testing.T) {
	f := obs1.NewLeaseFold()
	m := func(node uint64, inc uint32) obs1.Member {
		return obs1.Member{Node: node, Incarnation: inc, Resp: "r:1", Mesh: "m:1", Weight: 1, Version: "v1"}
	}
	foldApply(t, f, 1, 7, obs1.MemberRecord{Op: obs1.MemberJoin, Member: m(7, 2)})
	foldApply(t, f, 2, 7, obs1.MemberRecord{Op: obs1.MemberJoin, Member: m(7, 1)}) // delayed old join
	if got := f.Members(); len(got) != 1 || got[0].Incarnation != 2 {
		t.Fatalf("members = %+v", got)
	}
	foldApply(t, f, 3, 9, obs1.MemberRecord{Op: obs1.MemberLeave, Member: m(7, 1)}) // delayed old leave
	if got := f.Members(); len(got) != 1 {
		t.Fatal("stale leave removed a rejoined member")
	}
	foldApply(t, f, 4, 9, obs1.MemberRecord{Op: obs1.MemberLeave, Member: m(7, 2)})
	if got := f.Members(); len(got) != 0 {
		t.Fatalf("members after leave = %+v", got)
	}
	if f.Stats.MembersStale != 2 {
		t.Fatalf("MembersStale = %d, want 2", f.Stats.MembersStale)
	}
}

func TestEpochHistorySpans(t *testing.T) {
	f := obs1.NewLeaseFold()
	foldApply(t, f, 1, 7, grant(3, 7, 1))
	foldApply(t, f, 2, 7, obs1.HeartbeatRecord{})
	foldApply(t, f, 3, 7, obs1.HeartbeatRecord{})
	foldApply(t, f, 4, 7, obs1.HeartbeatRecord{})
	foldApply(t, f, 5, 9, grant(3, 9, 2)) // epoch 1's span ends here
	at := func(seq uint64) obs1.ChainPos { return obs1.ChainPos{Seq: seq} }
	if !f.EpochCurrentAtOrAfter(3, 1, at(3)) {
		t.Fatal("epoch 1 was current at seq 3 and 4")
	}
	if f.EpochCurrentAtOrAfter(3, 1, at(5)) {
		t.Fatal("epoch 1 ended at seq 5")
	}
	if !f.EpochCurrentAtOrAfter(3, 2, at(90)) {
		t.Fatal("the current epoch is current at every later position")
	}
	if f.EpochCurrentAtOrAfter(3, 3, at(1)) || f.EpochCurrentAtOrAfter(3, 0, at(1)) {
		t.Fatal("a never-granted epoch is never current")
	}
	if f.EpochCurrentAtOrAfter(4, 1, at(1)) {
		t.Fatal("a never-leased group has no current epoch")
	}
}

func TestPrimeFromCheckpoint(t *testing.T) {
	f := obs1.NewLeaseFold()
	ck := obs1.Checkpoint{
		Through: obs1.ChainPos{Seq: 10},
		Members: []obs1.Member{{Node: 7, Incarnation: 1, Resp: "r:1", Mesh: "m:1", Weight: 1, Version: "v1"}},
		Leases: []obs1.LeaseEntry{
			{Group: 3, Node: 7, Epoch: 4},
			{Group: 5, Node: 0, Epoch: 2}, // released group travels as node 0
		},
	}
	if err := f.Prime(ck); err != nil {
		t.Fatal(err)
	}
	if err := f.Prime(ck); err == nil {
		t.Fatal("double prime accepted")
	}
	wantHolder(t, f, 3, 7, 4)
	if _, _, ok := f.Holder(5); ok {
		t.Fatal("released checkpoint row primed as held")
	}
	if err := foldApplyErr(f, 12, 7, obs1.HeartbeatRecord{}); err == nil {
		t.Fatal("gap after the checkpoint accepted")
	}
	foldApply(t, f, 11, 9, grant(5, 9, 3)) // released group frees at epoch+1 across the summary
	wantHolder(t, f, 5, 9, 3)
	foldApply(t, f, 12, 9, grant(3, 9, 5))
	if !f.EpochCurrentAtOrAfter(3, 4, obs1.ChainPos{Seq: 2}) {
		t.Fatal("the primed epoch ended at seq 12, so it was current at or after seq 2")
	}
	if f.EpochCurrentAtOrAfter(3, 3, obs1.ChainPos{Seq: 2}) {
		t.Fatal("an epoch whose span ended before the prime must answer false")
	}
	if !f.EpochCurrentAtOrAfter(3, 5, obs1.ChainPos{Seq: 12}) {
		t.Fatal("the post-prime current epoch must answer true")
	}
}

func TestDenseOrderGuard(t *testing.T) {
	f := obs1.NewLeaseFold()
	foldApply(t, f, 1, 7, obs1.HeartbeatRecord{})
	if err := foldApplyErr(f, 3, 7, obs1.HeartbeatRecord{}); err == nil {
		t.Fatal("gap accepted")
	}
	if err := foldApplyErr(f, 1, 7, obs1.HeartbeatRecord{}); err == nil {
		t.Fatal("replay accepted")
	}
	b, err := obs1.AppendChainBatch(nil, 7, obs1.ChainBatch{BatchID: 9, Incarnation: 1, Records: []obs1.ChainRecord{obs1.HeartbeatRecord{}}})
	if err != nil {
		t.Fatal(err)
	}
	batch, h, err := obs1.ParseChainBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.ApplyChain(obs1.ChainPos{DD: 1, Seq: 2}, h, batch); err == nil {
		t.Fatal("cross-domain batch accepted")
	}
}

func TestStateSumAgrees(t *testing.T) {
	feed := func(f *obs1.LeaseFold) {
		foldApply(t, f, 1, 7, grant(3, 7, 1), obs1.MemberRecord{Op: obs1.MemberJoin, Member: obs1.Member{Node: 7, Incarnation: 1, Resp: "r:1", Mesh: "m:1", Weight: 1, Version: "v1"}})
		foldApply(t, f, 2, 9, grant(5, 9, 1))
		foldApply(t, f, 3, 7, commit(7, 3, 1))
	}
	a, b := obs1.NewLeaseFold(), obs1.NewLeaseFold()
	feed(a)
	feed(b)
	if a.StateSum() != b.StateSum() {
		t.Fatal("two folders over the same chain disagree")
	}
	foldApply(t, b, 4, 9, obs1.HeartbeatRecord{})
	if a.StateSum() == b.StateSum() {
		t.Fatal("a renewal moved no state")
	}
}

func TestOnCommitErrorStopsFold(t *testing.T) {
	f := obs1.NewLeaseFold()
	sinkErr := errors.New("sink full")
	f.OnCommit = func(obs1.CommitVerdict) error { return sinkErr }
	foldApply(t, f, 1, 7, grant(3, 7, 1))
	if err := foldApplyErr(f, 2, 7, commit(7, 3, 1)); !errors.Is(err, sinkErr) {
		t.Fatalf("err = %v, want the sink's", err)
	}
	if got := f.Applied(); got.Seq != 1 {
		t.Fatalf("Applied = %d after a sink error, want 1", got.Seq)
	}
}

func TestLeaseGuard(t *testing.T) {
	g := obs1.NewLeaseGuard(0, 0) // doc 02 defaults: TTL 3000ms, skew 500ms
	t0 := time.Unix(1000, 0)
	if !g.Suspended(3, t0) {
		t.Fatal("a group never renewed must be suspended")
	}
	g.Renewed(3, t0)
	edge := t0.Add(obs1.DefaultLeaseTTL - obs1.DefaultSkewBound)
	if g.Suspended(3, edge.Add(-time.Nanosecond)) {
		t.Fatal("suspended before the believed deadline minus skew")
	}
	if !g.Suspended(3, edge) {
		t.Fatal("not suspended at the believed deadline minus skew")
	}
	g.Renewed(3, edge) // a late-landing retry un-suspends at the same epoch
	if g.Suspended(3, edge) {
		t.Fatal("still suspended after the retry landed")
	}
	g.Drop(3) // foreign grant observed: demote
	if !g.Suspended(3, edge) {
		t.Fatal("not suspended after demotion")
	}
}

// TestFoldThroughAppender is the fence smoke test end to end: two nodes
// share a simulated chain through real ChainAppenders with a LeaseFold
// each, node 9 takes group 3 over while node 7 still believes it holds
// it, and node 7's stale commit dies at fold on both folders, which
// finish bit-for-bit agreed.
func TestFoldThroughAppender(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Seed: 11})
	const prefix = "db/l/"
	node := func(id uint64) (*obs1.ChainAppender, *obs1.LeaseFold, *[]obs1.CommitVerdict) {
		f := obs1.NewLeaseFold()
		var vs []obs1.CommitVerdict
		f.OnCommit = func(v obs1.CommitVerdict) error {
			vs = append(vs, v)
			return nil
		}
		a, err := obs1.NewChainAppender(s, prefix, 0, id, 1, obs1.ChainPos{}, f)
		if err != nil {
			t.Fatal(err)
		}
		return a, f, &vs
	}
	a7, f7, _ := node(7)
	a9, f9, v9 := node(9)

	mustAppend := func(a *obs1.ChainAppender, recs ...obs1.ChainRecord) {
		t.Helper()
		if _, err := a.Append(ctx, recs); err != nil {
			t.Fatal(err)
		}
	}
	mustAppend(a7, grant(3, 7, 1))
	mustAppend(a7, commit(7, 3, 1))
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	mustAppend(a9, grant(3, 9, 2))  // takeover lands on the chain
	mustAppend(a7, commit(7, 3, 1)) // the zombie's append catches up first, then lands after the grant
	mustAppend(a9, commit(9, 3, 2))
	if err := a7.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a9.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	wantHolder(t, f7, 3, 9, 2)
	if f7.StateSum() != f9.StateSum() {
		t.Fatal("the two folders disagree")
	}
	got := *v9
	if len(got) != 3 {
		t.Fatalf("node 9 saw %d commits, want 3", len(got))
	}
	if got[0].Live[0] != true || got[1].Live[0] != false || got[2].Live[0] != true {
		t.Fatalf("verdicts = %v %v %v, want live, dead, live", got[0].Live, got[1].Live, got[2].Live)
	}
	if f9.Stats.SectionsDead != 1 {
		t.Fatalf("SectionsDead = %d, want 1", f9.Stats.SectionsDead)
	}
}
