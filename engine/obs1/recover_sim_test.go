package obs1_test

// Boot recovery over the sim store (spec 2064/obs1 doc 02 section 2.5,
// doc 04 section 2): a committed multi-group scenario with one group
// folded and published, then Recover reads root, chain, manifests, and
// the WAL tail back into per-group cursors, and a restarted write log
// continues from them, orphan object and all.

import (
	"context"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// recoverRig is the O1c cold pipeline over the write log rig: publisher
// on the verdict feed, folder on the keydel feed, explicit cuts only.
type recoverRig struct {
	wl     *obs1.WriteLog
	folder *obs1.Folder
	pub    *obs1.ManifestPublisher
}

func newRecoverRig(t *testing.T, store *sim.Sim, node uint64) *recoverRig {
	t.Helper()
	rig := newLogRig(t, store, node)
	rig.grant(t, node, 1, 0, 1, 2, 3)
	pub, err := obs1.NewManifestPublisher(obs1.ManPubConfig{Store: store, Prefix: "p", Node: node})
	if err != nil {
		t.Fatal(err)
	}
	wl := newTestLog(t, rig, node, obs1.WriteLogConfig{OnVerdict: pub.OnVerdict})
	folder, err := obs1.NewFolder(obs1.FoldConfig{
		Store: store, Prefix: "p", Node: node, MapKey: testMapKey,
		Mark: wl.GroupMark, Marks: wl.Marks(), OnPublish: pub.OnFolded,
		FoldAge: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wl.SetKeyDelFeed(folder.Delete)
	for g := uint16(0); g < 4; g++ {
		wl.SetGroup(g, 1, 1)
	}
	return &recoverRig{wl: wl, folder: folder, pub: pub}
}

func (r *recoverRig) close(t *testing.T) {
	t.Helper()
	if err := r.wl.Close(); err != nil {
		t.Fatal(err)
	}
	r.folder.Close()
	r.pub.Close()
}

func (r *recoverRig) waitPublished(t *testing.T, n uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for r.pub.Stats().Published < n {
		if time.Now().After(deadline) {
			t.Fatalf("publisher stats %+v, want %d published", r.pub.Stats(), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func waitAllCommitted(t *testing.T, wl *obs1.WriteLog) {
	t.Helper()
	done := make(chan struct{})
	wl.NotifyAllCommitted(func() { close(done) })
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commit barrier never fired")
	}
}

// TestRecoverColdBoot is the no-checkpoint boot: two flush rounds over
// four groups, group 0 folded through a delete mid-stream, an orphan WAL
// object from a crashed flush, then Recover and a same-node restart that
// continues seqs, skips the orphan's slot, and survives a second boot.
func TestRecoverColdBoot(t *testing.T) {
	store := sim.New(sim.Config{})
	ctx := context.Background()
	const nodeA = uint64(0xA1)
	if err := obs1.CreateRoot(ctx, store, "p", false, obs1.Root{CreatedMS: 1, G: 4, D: 1}); err != nil {
		t.Fatal(err)
	}
	rr := newRecoverRig(t, store, nodeA)

	cursor := map[uint16]uint64{}
	track := func(g uint16, seq uint64, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if seq > cursor[g] {
			cursor[g] = seq
		}
	}

	// Round 1: one frame on each group.
	g, s, err := rr.wl.StrSet([]byte("alpha"), []byte("one"), 0, false)
	track(g, s, err)
	g, s, err = rr.wl.StrSet([]byte("bravo"), []byte("b1"), 0, false)
	track(g, s, err)
	g, s, err = rr.wl.StrSet([]byte("charlie"), []byte("c1"), 0, false)
	track(g, s, err)
	g, s, err = rr.wl.StrSet([]byte("delta"), []byte("d1"), 0, false)
	track(g, s, err)
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)

	// Round 2: the fold cursor lands on the delete's seq, and one more
	// group 0 frame above it makes the round's section straddle.
	g, delSeq, err := rr.wl.KeyDel([]byte("alpha"))
	track(g, delSeq, err)
	if g != 0 {
		t.Fatalf("alpha maps to group %d, the test wants 0", g)
	}
	g, s, err = rr.wl.StrSet([]byte("alpha"), []byte("two"), 0, false)
	track(g, s, err)
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)
	rr.folder.Flush()
	rr.waitPublished(t, 1)
	rr.close(t)

	// The crashed-flush orphan: an object under our namespace one past
	// the last committed seq, named by no commit record.
	if _, err := store.Put(ctx, walKeyFor(nodeA, 3), []byte("orphan")); err != nil {
		t.Fatal(err)
	}

	applied := map[uint16][]uint64{}
	r, err := obs1.Recover(ctx, obs1.RecoverConfig{
		Store: store, Prefix: "p", DD: 0, Node: nodeA, Incarnation: 2,
		Apply: func(group uint16, f obs1.WALFrame) error {
			applied[group] = append(applied[group], f.Seq)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Root.G != 4 {
		t.Fatalf("root G = %d, want 4", r.Root.G)
	}
	if len(r.Winning) != 1 || r.Winning[0].FoldSeq != delSeq {
		t.Fatalf("winning = %+v, want group 0 folded through %d", r.Winning, delSeq)
	}
	for grp, want := range cursor {
		if r.Applied[grp] != want {
			t.Fatalf("group %d applied %d, emitted through %d", grp, r.Applied[grp], want)
		}
	}
	// Group 0 replays only the frame above the fold cursor; the other
	// groups replay everything.
	if len(applied[0]) != 1 || applied[0][0] != delSeq+1 {
		t.Fatalf("group 0 replayed %v, want exactly seq %d", applied[0], delSeq+1)
	}
	for grp := uint16(1); grp < 4; grp++ {
		if len(applied[grp]) != int(cursor[grp]) {
			t.Fatalf("group %d replayed %v, want %d frames", grp, applied[grp], cursor[grp])
		}
	}
	if r.Stats.SectionsSkipped == 0 || r.Stats.FramesSkipped == 0 {
		t.Fatalf("stats %+v, want the round 1 section skipped whole and the delete skipped in the straddle", r.Stats)
	}
	if r.NextWALSeq != 4 {
		t.Fatalf("next WAL seq %d, want 4: two committed objects then the orphan", r.NextWALSeq)
	}
	if node, epoch, ok := r.Fold.Holder(0); !ok || node != nodeA || epoch != 1 {
		t.Fatalf("holder(0) = %x epoch %d ok %v", node, epoch, ok)
	}

	// The restart: same node, next incarnation, write log composed from
	// the recovery. StartSeq keeps the flusher off the orphan's slot.
	wl2, err := obs1.NewWriteLog(obs1.WriteLogConfig{
		Store: store, Prefix: "p", Node: nodeA, Chain: r.Chain, Fold: r.Fold,
		Groups: 4, MapKey: testMapKey, FlushAge: time.Hour,
		StartSeq: r.NextWALSeq,
	})
	if err != nil {
		t.Fatal(err)
	}
	for grp := uint16(0); grp < 4; grp++ {
		_, epoch, ok := r.Fold.Holder(grp)
		if !ok {
			t.Fatalf("no holder for group %d", grp)
		}
		wl2.SetGroup(grp, epoch, r.Applied[grp]+1)
	}
	g, s, err = wl2.StrSet([]byte("alpha"), []byte("three"), 0, false)
	track(g, s, err)
	if s != r.Applied[0]+1 {
		t.Fatalf("restart emitted seq %d, want %d", s, r.Applied[0]+1)
	}
	wl2.Barrier()
	waitAllCommitted(t, wl2)
	if err := wl2.Close(); err != nil {
		t.Fatal(err)
	}
	// The new flush landed past the orphan, a parseable WAL object.
	if secs := walObject(t, store, nodeA, 4); len(secs) != 1 {
		t.Fatalf("restart WAL object sections = %d, want 1", len(secs))
	}

	r2, err := obs1.Recover(ctx, obs1.RecoverConfig{
		Store: store, Prefix: "p", DD: 0, Node: nodeA, Incarnation: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Applied[0] != cursor[0] {
		t.Fatalf("second boot group 0 applied %d, want %d", r2.Applied[0], cursor[0])
	}
	if r2.NextWALSeq != 5 {
		t.Fatalf("second boot next WAL seq %d, want 5", r2.NextWALSeq)
	}
}

// TestRecoverFromCheckpoint boots through a checkpoint: a fully folded
// group 0 scenario, an observer node checkpointing the chain and
// advancing the root, one more committed frame after it, then a fresh
// node recovers and sees only the post-checkpoint verdict replay.
func TestRecoverFromCheckpoint(t *testing.T) {
	store := sim.New(sim.Config{})
	ctx := context.Background()
	const nodeA = uint64(0xA1)
	if err := obs1.CreateRoot(ctx, store, "p", false, obs1.Root{CreatedMS: 1, G: 4, D: 1}); err != nil {
		t.Fatal(err)
	}
	rr := newRecoverRig(t, store, nodeA)

	// Group 0 only, delete last so the fold cursor covers every frame.
	if _, _, err := rr.wl.StrSet([]byte("aa"), []byte("v1"), 0, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rr.wl.StrSet([]byte("ab"), []byte("v2"), 0, false); err != nil {
		t.Fatal(err)
	}
	g, delSeq, err := rr.wl.KeyDel([]byte("aa"))
	if err != nil || g != 0 {
		t.Fatalf("del: group %d err %v", g, err)
	}
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)
	rr.folder.Flush()
	rr.waitPublished(t, 1)

	// The observer checkpoints the whole chain so far and advances the
	// root; any node may do this, lease or not.
	const observer = uint64(0xC1)
	fold2 := obs1.NewLeaseFold()
	ck, err := obs1.NewCheckpointer(fold2, observer, 3*time.Second, 0, 0, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := obs1.NewChainAppender(store, "p", 0, observer, 1, obs1.ChainPos{}, ck)
	if err != nil {
		t.Fatal(err)
	}
	if err := a2.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	ckPos, err := ck.WriteCheckpoint(ctx, store, "p", false, a2)
	if err != nil {
		t.Fatal(err)
	}

	// One committed frame after the checkpoint, the WAL tail a booter
	// still owes the store.
	_, tailSeq, err := rr.wl.StrSet([]byte("ab"), []byte("v3"), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	rr.wl.Barrier()
	waitAllCommitted(t, rr.wl)
	rr.close(t)

	var got []uint64
	r, err := obs1.Recover(ctx, obs1.RecoverConfig{
		Store: store, Prefix: "p", DD: 0, Node: 0xB2, Incarnation: 1,
		Apply: func(group uint16, f obs1.WALFrame) error {
			if group != 0 {
				t.Errorf("frame on group %d", group)
			}
			got = append(got, f.Seq)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Ckpt.Through != ckPos {
		t.Fatalf("booted through %+v, checkpoint wrote %+v", r.Ckpt.Through, ckPos)
	}
	if r.Winning[0].FoldSeq != delSeq {
		t.Fatalf("winning fold seq %d, want %d", r.Winning[0].FoldSeq, delSeq)
	}
	if r.Applied[0] != tailSeq {
		t.Fatalf("applied %d, want %d", r.Applied[0], tailSeq)
	}
	if len(got) != 1 || got[0] != tailSeq {
		t.Fatalf("replayed %v, want exactly the post-checkpoint frame %d", got, tailSeq)
	}
	if r.Stats.Verdicts != 1 {
		t.Fatalf("verdicts %d, want only the post-checkpoint commit", r.Stats.Verdicts)
	}
	if r.NextWALSeq != 1 {
		t.Fatalf("next WAL seq %d, want 1 under a fresh node id", r.NextWALSeq)
	}
	if node, epoch, ok := r.Fold.Holder(0); !ok || node != nodeA || epoch != 1 {
		t.Fatalf("holder(0) = %x epoch %d ok %v: the checkpoint's lease table did not prime", node, epoch, ok)
	}
}
