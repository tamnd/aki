package obs1_test

// The O1b crash suite (spec 2064/obs1 doc 10, suite crash, the two points
// this surface owns): kill -9 mid-flush and post-PUT-pre-commit, with
// replay idempotence checked by state hash (W-I3) and the committed
// stream's density checked on every walk (W-I2).
//
// The kill is modeled at the store boundary. A diskless node's only
// persistent state is the bucket, and every PUT is atomic, so a process
// dying at a labeled point leaves exactly the bucket that point had:
// mid-flush means the buffered frames never became an object, and
// post-PUT-pre-commit means the WAL object exists but no commit record
// names it. The suite freezes the bucket at each point by construction
// (abandoning a pipeline mid-buffer, failing the chain under a scripted
// fault) and then replays what the bucket really holds.
//
// The replayer here is the test's own seq-gated walk: chain batches in
// order through the production follower, one ranged GET per committed
// section planned from the commit record alone, frames gated by the
// per-group applied seq. It trusts every committed section because this
// scenario only commits from lease holders; the O1c replayer routes the
// same walk through the fold's fencing verdict like the watermarks do.
// The state hash is a running digest over the accepted frame stream,
// which determines the engine state because frames are deterministic
// post-decision effects (doc 04 section 2): equal streams, equal states.

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1"
	"github.com/tamnd/aki/engine/obs1/sim"
)

// walKeyFor renders the flusher's WAL key for one node and object seq.
func walKeyFor(node, walSeq uint64) string {
	return fmt.Sprintf("p/wal/%016x/%016d", node, walSeq)
}

// commitCollector is the walk's ChainApplier: it keeps every batch's
// commit records in chain order, one slice per chain object, so a prefix
// of the chain is a prefix of this slice.
type commitCollector struct {
	batches [][]obs1.CommitRecord
}

func (c *commitCollector) ApplyChain(pos obs1.ChainPos, h obs1.Header, b obs1.ChainBatch) error {
	var recs []obs1.CommitRecord
	for _, r := range b.Records {
		if cr, ok := r.(obs1.CommitRecord); ok {
			recs = append(recs, cr)
		}
	}
	c.batches = append(c.batches, recs)
	return nil
}

// chainBatches walks the whole chain through the production follower and
// returns its commit records grouped by chain object.
func chainBatches(t *testing.T, store obs1.Store) [][]obs1.CommitRecord {
	t.Helper()
	col := &commitCollector{}
	ap, err := obs1.NewChainAppender(store, "p", 0, 0xEE, 1, obs1.ChainPos{}, col)
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Follow(context.Background()); err != nil {
		t.Fatal(err)
	}
	return col.batches
}

// gatedReplay is the seq-gated frame walk. applied is the per-group
// cursor the gate reads; digests holds the running state hash after each
// accepted frame, so a prefix walk's digests must be a prefix of the
// full walk's.
type gatedReplay struct {
	store    obs1.Store
	applied  map[uint16]uint64
	h        hash.Hash64
	digests  []uint64
	accepted int
	skipped  int
}

func newGatedReplay(store obs1.Store) *gatedReplay {
	return &gatedReplay{store: store, applied: make(map[uint16]uint64), h: fnv.New64a()}
}

// apply replays batches in order through the gate. A frame at or below
// the group's cursor is skipped, the doc 04 section 2 idempotence rule;
// a frame further ahead than cursor plus one is a gap in the committed
// stream, the W-I2 violation this suite exists to catch.
func (r *gatedReplay) apply(batches [][]obs1.CommitRecord) error {
	for _, recs := range batches {
		for _, rec := range recs {
			for _, cs := range rec.Sections {
				if err := r.section(rec, cs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *gatedReplay) section(rec obs1.CommitRecord, cs obs1.CommitSection) error {
	// The commit record repeats the WAL footer's index, so the ranged GET
	// is planned from the chain alone; RawLen equals StoredLen at comp 0,
	// the only compression this build reads.
	e := obs1.WALIndexEntry{
		Group: cs.Group, Epoch: cs.Epoch, Offset: cs.Offset,
		StoredLen: cs.StoredLen, RawLen: cs.StoredLen, NFrames: cs.NFrames,
		FirstSeq: cs.FirstSeq, LastSeq: cs.LastSeq,
	}
	off, n := e.SectionSpan()
	key := walKeyFor(rec.WALNode, rec.WALSeq)
	b, _, err := r.store.GetRange(context.Background(), key, off, n)
	if err != nil {
		return fmt.Errorf("section GET %s: %w", key, err)
	}
	sec, err := obs1.ParseWALSection(b, e)
	if err != nil {
		return err
	}
	for _, f := range sec.Frames {
		cur := r.applied[sec.Group]
		if f.Seq <= cur {
			r.skipped++
			continue
		}
		if f.Seq != cur+1 {
			return fmt.Errorf("group %d frame seq %d after applied %d: the committed stream has a gap", sec.Group, f.Seq, cur)
		}
		if _, err := obs1.DecodeOp(f); err != nil {
			return fmt.Errorf("group %d seq %d: %w", sec.Group, f.Seq, err)
		}
		r.applied[sec.Group] = f.Seq
		var hdr [18]byte
		binary.LittleEndian.PutUint16(hdr[0:2], sec.Group)
		binary.LittleEndian.PutUint64(hdr[2:10], f.Seq)
		hdr[10] = f.Kind
		hdr[11] = f.Flags
		binary.LittleEndian.PutUint16(hdr[12:14], f.Slot)
		binary.LittleEndian.PutUint32(hdr[14:18], uint32(len(f.Key)))
		r.h.Write(hdr[:])
		r.h.Write(f.Key)
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(f.Payload)))
		r.h.Write(hdr[0:4])
		r.h.Write(f.Payload)
		r.digests = append(r.digests, r.h.Sum64())
		r.accepted++
	}
	return nil
}

// freshReplay walks the whole chain with a fresh gate and fails the test
// on any legality violation, including a skipped frame: on a fresh walk
// a skip means some committed seq was emitted twice.
func freshReplay(t *testing.T, store obs1.Store, batches [][]obs1.CommitRecord) *gatedReplay {
	t.Helper()
	r := newGatedReplay(store)
	if err := r.apply(batches); err != nil {
		t.Fatal(err)
	}
	if r.skipped != 0 {
		t.Fatalf("fresh replay skipped %d frames: a committed seq was re-emitted", r.skipped)
	}
	return r
}

func lastDigest(r *gatedReplay) uint64 {
	if len(r.digests) == 0 {
		return 0
	}
	return r.digests[len(r.digests)-1]
}

// TestCrashPointsReplay walks one bucket through the two O1b crash
// points and a takeover, replaying after each: a committed baseline, a
// mid-flush kill that loses buffered relaxed acks cleanly, a
// post-PUT-pre-commit kill that leaves an orphan WAL object the chain
// never names, and a new writer booting from the replay cursor. Each
// crashed incarnation is a fresh writer id, the doc 02 takeover slot:
// a same-id restart additionally needs the WAL open-sequence hand-off
// (FlusherConfig.StartSeq), which the recovery slice owns.
func TestCrashPointsReplay(t *testing.T) {
	var chainDown atomic.Bool
	store := sim.New(sim.Config{Fault: func(op sim.Op, key string) *sim.Fault {
		if chainDown.Load() && op == sim.OpPutIfAbsent && strings.Contains(key, "/chain/") {
			return &sim.Fault{Err: fmt.Errorf("sim: scripted chain outage")}
		}
		return nil
	}})
	ctx := context.Background()

	// cursor tracks the highest emitted seq per group, the test's own
	// copy of what the replay must reconstruct.
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
	waitCommitted := func(wl *obs1.WriteLog) {
		t.Helper()
		done := make(chan struct{})
		wl.NotifyAllCommitted(func() { close(done) })
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("commit barrier never fired")
		}
	}

	// Phase 1, the committed baseline: node A, epoch 1, a multi-type
	// scenario over all four groups, flushed in two rounds so the chain
	// carries more than one commit record.
	const nodeA = uint64(0xA1)
	rigA := newLogRig(t, store, nodeA)
	rigA.grant(t, nodeA, 1, 0, 1, 2, 3)
	wlA := newTestLog(t, rigA, nodeA, obs1.WriteLogConfig{})
	for g := uint16(0); g < 4; g++ {
		wlA.SetGroup(g, 1, 1)
	}
	g, s, err := wlA.StrSet([]byte("alpha"), []byte("one"), 0, false)
	track(g, s, err)
	g, s, err = wlA.HashSet([]byte("bravo"), true, [][]byte{[]byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}, 0)
	track(g, s, err)
	g, s, err = wlA.SetAdd([]byte("charlie"), true, [][]byte{[]byte("m1"), []byte("m2")})
	track(g, s, err)
	g, s, err = wlA.ListPush([]byte("delta"), true, false, [][]byte{[]byte("la"), []byte("lb"), []byte("lc")})
	track(g, s, err)
	wlA.Barrier()
	g, s, err = wlA.StreamAdd([]byte("echo"), true, 1, 1, [][]byte{[]byte("sf"), []byte("sv")}, 0)
	track(g, s, err)
	g, s, err = wlA.KeyDel([]byte("alpha"))
	track(g, s, err)
	g, s, err = wlA.StrSet([]byte("alpha"), []byte("two"), 5000, false)
	track(g, s, err)
	g, s, err = wlA.ZSetAdd([]byte("foxtrot"), true, []float64{1.5, -2.25}, [][]byte{[]byte("x"), []byte("y")})
	track(g, s, err)
	g, s, err = wlA.SetStore([]byte("golf"), false, false, [][]byte{[]byte("r"), []byte("s")})
	track(g, s, err)
	wlA.Barrier()
	waitCommitted(wlA)
	if err := wlA.Close(); err != nil {
		t.Fatal(err)
	}

	baseline := freshReplay(t, store, chainBatches(t, store))
	baseHash := lastDigest(baseline)
	for grp, want := range cursor {
		if baseline.applied[grp] != want {
			t.Fatalf("baseline replay group %d applied %d, emitted through %d", grp, baseline.applied[grp], want)
		}
	}

	// Crash point 1, mid-flush: node B takes over, acks writes into its
	// buffer, and dies before any flush. The frames never became an
	// object, so the bucket still replays to the baseline exactly: the
	// relaxed acks are lost whole, the documented window, never half
	// applied. The pipeline is abandoned, not closed, because Close
	// flushes and a kill does not.
	const nodeB = uint64(0xB2)
	rigB := newLogRig(t, store, nodeB)
	if err := rigB.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	rigB.grant(t, nodeB, 2, 0, 1, 2, 3)
	wlB := newTestLog(t, rigB, nodeB, obs1.WriteLogConfig{})
	for grp, cur := range cursor {
		wlB.SetGroup(grp, 2, cur+1)
	}
	gB, lostAlpha, err := wlB.StrSet([]byte("alpha"), []byte("ghost"), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if gB != 0 || lostAlpha != cursor[0]+1 {
		t.Fatalf("takeover mark = (%d, %d), want group 0 continuing at %d", gB, lostAlpha, cursor[0]+1)
	}
	if _, _, err := wlB.HashDel([]byte("bravo"), [][]byte{[]byte("f1")}, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wlB.ListPop([]byte("delta"), true, 1, false); err != nil {
		t.Fatal(err)
	}
	if wlB.FlushCount() != 0 {
		t.Fatal("mid-flush arm flushed; the crash point needs frames still in the buffer")
	}

	after := freshReplay(t, store, chainBatches(t, store))
	if lastDigest(after) != baseHash || after.accepted != baseline.accepted {
		t.Fatal("a mid-flush kill changed the committed stream")
	}

	// Crash point 2, post-PUT-pre-commit: node C takes over, its flush
	// PUTs the WAL object, and the chain append fails under the scripted
	// outage before a commit record lands. The object exists in the
	// bucket; the chain never names it; replay must not see it.
	const nodeC = uint64(0xC3)
	rigC := newLogRig(t, store, nodeC)
	if err := rigC.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	rigC.grant(t, nodeC, 3, 0, 1, 2, 3)
	wlC := newTestLog(t, rigC, nodeC, obs1.WriteLogConfig{})
	for grp, cur := range cursor {
		wlC.SetGroup(grp, 3, cur+1)
	}
	_, orphanAlpha, err := wlC.StrSet([]byte("alpha"), []byte("orphan"), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if orphanAlpha != lostAlpha {
		t.Fatalf("orphan mark %d, want %d: uncommitted seqs never existed", orphanAlpha, lostAlpha)
	}
	if _, _, err := wlC.SetRem([]byte("charlie"), [][]byte{[]byte("m1")}, false); err != nil {
		t.Fatal(err)
	}
	chainDown.Store(true)
	wlC.Barrier()
	deadline := time.Now().Add(10 * time.Second)
	for wlC.Err() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the chain outage never surfaced as a pipeline error")
		}
		time.Sleep(5 * time.Millisecond)
	}
	orphanKey := walKeyFor(nodeC, 1)
	if _, _, err := store.Get(ctx, orphanKey); err != nil {
		t.Fatalf("orphan WAL object %s: %v; the crash point needs the PUT to have landed", orphanKey, err)
	}
	// Closing the failed pipeline only reaps its goroutines; the bucket
	// froze at the kill.
	if err := wlC.Close(); err == nil {
		t.Fatal("closing the failed pipeline reported success")
	}
	chainDown.Store(false)

	after = freshReplay(t, store, chainBatches(t, store))
	if lastDigest(after) != baseHash || after.accepted != baseline.accepted {
		t.Fatal("an uncommitted WAL object changed the committed stream")
	}

	// Takeover: node D boots the way a real owner will, priming its seq
	// cursors from the replay, and commits fresh writes. The lost seqs
	// are reused with new content, which is exactly right: a seq the
	// chain never committed never existed.
	const nodeD = uint64(0xD4)
	rigD := newLogRig(t, store, nodeD)
	if err := rigD.ap.Follow(ctx); err != nil {
		t.Fatal(err)
	}
	rigD.grant(t, nodeD, 4, 0, 1, 2, 3)
	wlD := newTestLog(t, rigD, nodeD, obs1.WriteLogConfig{})
	for grp := uint16(0); grp < 4; grp++ {
		wlD.SetGroup(grp, 4, after.applied[grp]+1)
	}
	g, s, err = wlD.StrSet([]byte("alpha"), []byte("final"), 0, false)
	track(g, s, err)
	if s != lostAlpha {
		t.Fatalf("recovered mark %d, want %d: boot reads the chain, not a ghost", s, lostAlpha)
	}
	g, s, err = wlD.HashSet([]byte("bravo"), false, [][]byte{[]byte("f3"), []byte("v3")}, 0)
	track(g, s, err)
	g, s, err = wlD.ListPush([]byte("delta"), false, true, [][]byte{[]byte("ld")})
	track(g, s, err)
	g, s, err = wlD.StreamAdd([]byte("echo"), false, 2, 0, [][]byte{[]byte("sf2"), []byte("sv2")}, 0)
	track(g, s, err)
	wlD.Barrier()
	waitCommitted(wlD)
	if err := wlD.Close(); err != nil {
		t.Fatal(err)
	}

	// The final stream: deterministic, dense, exactly the emissions the
	// chain committed, orphan still present and still invisible.
	batches := chainBatches(t, store)
	final := freshReplay(t, store, batches)
	finalHash := lastDigest(final)
	if again := freshReplay(t, store, batches); lastDigest(again) != finalHash {
		t.Fatal("two walks of one bucket disagree")
	}
	var want int
	for grp, cur := range cursor {
		if final.applied[grp] != cur {
			t.Fatalf("final replay group %d applied %d, emitted through %d", grp, final.applied[grp], cur)
		}
		want += int(cur)
	}
	if final.accepted != want {
		t.Fatalf("final replay accepted %d frames, the dense streams hold %d", final.accepted, want)
	}
	if _, _, err := store.Get(ctx, orphanKey); err != nil {
		t.Fatalf("orphan WAL object gone: %v; nothing may delete it before the sweep milestone", err)
	}

	// W-I3, prefix-correct: every chain prefix's accepted stream is a
	// prefix of the full stream.
	for k := 0; k <= len(batches); k++ {
		pre := newGatedReplay(store)
		if err := pre.apply(batches[:k]); err != nil {
			t.Fatalf("prefix %d: %v", k, err)
		}
		if len(pre.digests) > len(final.digests) {
			t.Fatalf("prefix %d accepted more frames than the full walk", k)
		}
		for i, d := range pre.digests {
			if d != final.digests[i] {
				t.Fatalf("prefix %d digest %d diverges from the full walk", k, i)
			}
		}
		// W-I3, idempotent: a replayer that stopped at any prefix and
		// re-walks the whole chain lands on the full state, the
		// restart-from-checkpoint shape.
		if err := pre.apply(batches); err != nil {
			t.Fatalf("prefix %d re-walk: %v", k, err)
		}
		if lastDigest(pre) != finalHash {
			t.Fatalf("prefix %d then a full re-walk missed the full state", k)
		}
	}
}

// captureSink is a FlushSink that only records deliveries, the harness
// for hand-building chains whose commit records need real WAL objects.
type captureSink struct {
	mu  sync.Mutex
	got []obs1.CommitRecord
}

func (s *captureSink) WALFlushed(walSeq uint64, size int64, index []obs1.WALIndexEntry) error {
	rec := obs1.CommitRecord{WALNode: 0, WALSeq: walSeq, WALSize: uint64(size), Sections: make([]obs1.CommitSection, len(index))}
	for i, e := range index {
		rec.Sections[i] = e.CommitSection()
	}
	s.mu.Lock()
	s.got = append(s.got, rec)
	s.mu.Unlock()
	return nil
}

func (s *captureSink) wait(t *testing.T, n int) []obs1.CommitRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mu.Lock()
		got := len(s.got)
		s.mu.Unlock()
		if got >= n {
			s.mu.Lock()
			defer s.mu.Unlock()
			return append([]obs1.CommitRecord(nil), s.got...)
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink has %d deliveries, want %d", got, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// crashTeethRig flushes real WAL objects through a capturing sink and
// hands back commit records the test can put on the chain in any order,
// including wrong ones: the monitors must fail on a bad chain, or the
// suite is toothless.
func crashTeethRig(t *testing.T, store *sim.Sim, node uint64) (*obs1.ChainAppender, []obs1.CommitRecord) {
	t.Helper()
	sink := &captureSink{}
	fl, err := obs1.NewFlusher(obs1.FlusherConfig{
		Store: store, Sink: sink, Prefix: "p", Node: node, FlushAge: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	frame := func(seq uint64) obs1.WALFrame {
		f, err := obs1.EncodeOp(0, seq, []byte("alpha"), obs1.KeyDel{})
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	if err := fl.AppendOp(0, 1, frame(1)); err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendOp(0, 1, frame(2)); err != nil {
		t.Fatal(err)
	}
	fl.Barrier()
	sink.wait(t, 1)
	if err := fl.AppendOp(0, 1, frame(4)); err != nil {
		t.Fatal(err)
	}
	if err := fl.AppendOp(0, 1, frame(5)); err != nil {
		t.Fatal(err)
	}
	if err := fl.Close(); err != nil {
		t.Fatal(err)
	}
	recs := sink.wait(t, 2)
	for i := range recs {
		recs[i].WALNode = node
	}
	ap, err := obs1.NewChainAppender(store, "p", 0, node, 1, obs1.ChainPos{}, &commitCollector{})
	if err != nil {
		t.Fatal(err)
	}
	return ap, recs
}

// TestReplayGapDetected proves the density monitor has teeth: a chain
// that commits seqs 1..2 and then 4..5 must fail the walk, not replay
// around the hole.
func TestReplayGapDetected(t *testing.T) {
	store := sim.New(sim.Config{})
	ap, recs := crashTeethRig(t, store, 0xE1)
	for _, rec := range recs {
		if _, err := ap.Append(context.Background(), []obs1.ChainRecord{rec}); err != nil {
			t.Fatal(err)
		}
	}
	r := newGatedReplay(store)
	err := r.apply(chainBatches(t, store))
	if err == nil || !strings.Contains(err.Error(), "gap") {
		t.Fatalf("a gapped chain replayed: %v", err)
	}
}

// TestReplayDuplicateSkipped proves the other half of W-I2's monitor: a
// WAL object committed twice replays once, the gate skips the second
// pass, and the skip count is the re-emission signal a fresh walk
// asserts on.
func TestReplayDuplicateSkipped(t *testing.T) {
	store := sim.New(sim.Config{})
	ap, recs := crashTeethRig(t, store, 0xE2)
	first := recs[0]
	for _, rec := range []obs1.CommitRecord{first, first} {
		if _, err := ap.Append(context.Background(), []obs1.ChainRecord{rec}); err != nil {
			t.Fatal(err)
		}
	}
	r := newGatedReplay(store)
	if err := r.apply(chainBatches(t, store)); err != nil {
		t.Fatal(err)
	}
	if r.accepted != 2 || r.skipped != 2 {
		t.Fatalf("accepted %d skipped %d, want 2 and 2: the gate replays a duplicate commit exactly once", r.accepted, r.skipped)
	}
}
