package sqlo1b

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// Rule W3 at the store: delta root frames are elided from the WAL,
// structural roots frame a rootkey record, replay truncates the
// unacknowledged suffix and rebuilds elided roots from segment
// frames. These tests drive ApplyBatch with handcrafted segmented
// hash images; the end-to-end matrix over the real hash operators
// lives in engine/sqlo1's torn-tail test.

// w3Root builds a valid segmented hash root payload (doc 06 section
// 2.2) with a single fence entry per segid.
func w3Root(rooth uint64, count uint64, minExpMs int64, segids ...uint64) []byte {
	b := make([]byte, 44+16*len(segids))
	b[0] = 2 // hashSubSeg
	if minExpMs != 0 {
		b[1] = 1 << 1 // hflagAnyTTL
	}
	binary.LittleEndian.PutUint32(b[4:], 1) // rootgen
	binary.LittleEndian.PutUint64(b[8:], rooth)
	binary.LittleEndian.PutUint64(b[16:], count)
	next := uint64(0)
	for _, id := range segids {
		next = max(next, id+1)
	}
	binary.LittleEndian.PutUint64(b[24:], next)
	binary.LittleEndian.PutUint64(b[32:], uint64(minExpMs))
	binary.LittleEndian.PutUint32(b[40:], uint32(len(segids)))
	for i, id := range segids {
		off := 44 + 16*i
		// lo values only need to rise; the store never reads them.
		binary.LittleEndian.PutUint64(b[off:], uint64(i)<<32)
		binary.LittleEndian.PutUint64(b[off+8:], id)
	}
	if len(segids) > 0 {
		binary.LittleEndian.PutUint64(b[44:], 0) // first lo covers from 0
	}
	return b
}

// w3Seg builds a segment payload header (doc 06 section 2.4) with no
// entries; reconciliation reads only the header.
func w3Seg(n int, minExpMs int64) []byte {
	b := make([]byte, 12)
	binary.LittleEndian.PutUint16(b, uint16(n))
	binary.LittleEndian.PutUint64(b[4:], uint64(minExpMs))
	return b
}

func w3SegKey(t *testing.T, rooth, segid uint64) []byte {
	t.Helper()
	sk, err := sqlo1.NewSubkey(rooth, sqlo1.SubkindSeg, segid)
	if err != nil {
		t.Fatal(err)
	}
	return sk.Encode()
}

func segOp(key, val []byte) sqlo1.Op {
	return sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: val, Gen: 1}}
}

func rootOp(key string, val []byte, delta bool) sqlo1.Op {
	return sqlo1.Op{Rec: sqlo1.Record{Key: []byte(key), Value: val, Root: true, Delta: delta}}
}

// rootCount reads the count field out of a root record in the store.
func (r *storeRig) rootCount(t *testing.T, key string) uint64 {
	t.Helper()
	rec, err := r.s.lookup([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.RType != RecRoot {
		t.Fatalf("no root record under %q", key)
	}
	return binary.LittleEndian.Uint64(rec.Value[16:])
}

// walFrames replays the store's WAL after Close and returns the
// decoded PUT records plus the ops of every frame.
func walFrames(t *testing.T, r *storeRig) (recs []*Record, ops []uint8) {
	t.Helper()
	dbid := r.s.sb.WALDBID()
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := sqlo1.OpenWAL(sqlo1.WALPath(r.path), dbid, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	err = w.Replay(func(fr sqlo1.WALFrame) error {
		ops = append(ops, fr.Op)
		if fr.Op == sqlo1.WALOpPut {
			rec, err := DecodePutPayload(fr.Payload)
			if err != nil {
				return err
			}
			recs = append(recs, cloneRecord(rec))
		} else {
			recs = append(recs, nil)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return recs, ops
}

func TestApplyBatchElidesDeltaRootFrames(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0xabc123
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t,
		segOp(seg, w3Seg(13, 0)),
		rootOp("h", w3Root(rooth, 13, 0, 1), true),
	)
	recs, _ := walFrames(t, r)
	roots, rootkeys := 0, 0
	for _, rec := range recs {
		if rec == nil {
			continue
		}
		if rec.RType == RecRoot {
			roots++
			if got := binary.LittleEndian.Uint64(rec.Value[16:]); got != 10 {
				t.Fatalf("framed root has count %d, want the structural 10", got)
			}
		}
		if rooth2, ukey, ok := RootkeyRef(rec); ok {
			rootkeys++
			if rooth2 != rooth || !bytes.Equal(ukey, []byte("h")) {
				t.Fatalf("rootkey maps %x to %q", rooth2, ukey)
			}
		}
	}
	if roots != 1 {
		t.Fatalf("%d root frames in the WAL, want only the structural one", roots)
	}
	if rootkeys != 1 {
		t.Fatalf("%d rootkey frames, want 1", rootkeys)
	}
	// The elided image still recovers exactly.
	s, err := OpenStore(r.path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	r.s = s
	t.Cleanup(func() { s.Close() })
	if got := r.rootCount(t, "h"); got != 13 {
		t.Fatalf("recovered root count %d, want the reconciled 13", got)
	}
}

func TestReplayPatchesRootAcrossBatchSplit(t *testing.T) {
	// The W1 window: maxOps cuts a drain so the segments' batch lands
	// and the root's batch never does. The residual delta must patch
	// the last durable root, including the min_expire lowering.
	r := newStoreRig(t)
	const rooth = 0x77aa
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t, segOp(seg, w3Seg(16, 5000)))
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 16 {
		t.Fatalf("patched root count %d, want 16", got)
	}
	rec, err := r.s.lookup([]byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(binary.LittleEndian.Uint64(rec.Value[32:])); got != 5000 {
		t.Fatalf("patched min_expire %d, want 5000", got)
	}
	if rec.Value[1]&(1<<1) == 0 {
		t.Fatal("patched root did not gain the TTL flag")
	}
	// Repeat recoveries see the same tail and must patch to the same
	// image.
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 16 {
		t.Fatalf("second recovery drifted the count to %d", got)
	}
}

func TestReplayResolvesRootkeyFromCommittedState(t *testing.T) {
	// A checkpoint between the structural write and the count-only
	// window: the tail carries only segment frames, so the mapping and
	// the root image both resolve from the committed state.
	r := newStoreRig(t)
	const rooth = 0x9c9c
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r.apply(t,
		segOp(seg, w3Seg(25, 0)),
		rootOp("h", w3Root(rooth, 25, 0, 1), true),
	)
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 25 {
		t.Fatalf("patched root count %d, want 25", got)
	}
}

func TestReplayTruncatesUnmarkedSuffix(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0x5151
	r.apply(t, putOp("live", []byte("v1"), 0))
	dbid := r.s.sb.WALDBID()
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	// Hand-append a torn batch: data frames with no trailing mark,
	// then the standalone-synced frames a crash can legally leave
	// behind them.
	w, err := sqlo1.OpenWAL(sqlo1.WALPath(r.path), dbid, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	ghost, err := EncodePutPayload(&Record{RType: RecString, Key: []byte("ghost"), Value: []byte("torn")})
	if err != nil {
		t.Fatal(err)
	}
	del, err := EncodeDelPayload([]byte("live"))
	if err != nil {
		t.Fatal(err)
	}
	bump, err := EncodeGenbumpPayload(sqlo1.GenKey(rooth), 7)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := EncodeLeasePayload(3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, sqlo1.WALOpPut, 0, ghost); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, sqlo1.WALOpDel, 0, del); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, sqlo1.WALOpGenbump, 0, bump); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(0, sqlo1.WALOpPut, 0, lease); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(r.path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	r.s = s
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()
	got, err := s.Get(ctx, []byte("live"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Value, []byte("v1")) {
		t.Fatalf("live = %q, the unacknowledged DEL applied", got.Value)
	}
	if _, err := s.Get(ctx, []byte("ghost")); err != sqlo1.ErrNotFound {
		t.Fatalf("ghost survived truncation: %v", err)
	}
	// The trailing genbump and lease are durable acknowledged state
	// and must ride out the truncation.
	genRec, err := s.lookup(sqlo1.GenKey(rooth))
	if err != nil {
		t.Fatal(err)
	}
	if genRec == nil {
		t.Fatal("trailing GENBUMP was dropped")
	}
	if gen, err := genOf(genRec); err != nil || gen != 7 {
		t.Fatalf("replayed generation %d, %v, want 7", gen, err)
	}
	if mark, err := s.currentLease(); err != nil || mark != 3 {
		t.Fatalf("replayed lease mark %d, %v, want 3", mark, err)
	}
}

func TestGenbumpClearsResidualDelta(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0x3f3f
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t, segOp(seg, w3Seg(16, 0)))
	r.genbump(t, rooth, 2)
	r.reopen(t)
	// The plane retired after the count-only window; the stale root
	// image must not be patched, its count is moot behind the bump.
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("retired root was patched to %d", got)
	}
}

func TestStaleRootkeySkipsPatch(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0x6b6b
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	// The key moves on: deleted, recreated as a plain string. The
	// rootkey mapping for the old plane now points at a record that
	// fails the rooth check, so a residual delta must skip it.
	r.apply(t, delOp("h"))
	r.apply(t, putOp("h", []byte("plain again"), 0))
	r.apply(t, segOp(seg, w3Seg(16, 0)))
	r.reopen(t)
	got, err := r.s.Get(context.Background(), []byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Value, []byte("plain again")) {
		t.Fatalf("recreated key = %q", got.Value)
	}
}

func TestReplayDeletedRootSkipsPatch(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0x2d2d
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t, segOp(seg, w3Seg(16, 0)))
	r.apply(t, delOp("h"), delOp(string(seg)))
	r.reopen(t)
	if _, err := r.s.Get(context.Background(), []byte("h")); err != sqlo1.ErrNotFound {
		t.Fatalf("deleted root came back: %v", err)
	}
}

func TestReplayRollsBackTornSplit(t *testing.T) {
	// The structural window: a split's relaid segments land in an
	// acknowledged batch and the crash takes the batch with the new
	// root. No count patch can make the stale fence route reads into
	// the new segment, so the plane must roll back to its last rooted
	// batch, dropping the whole torn window.
	r := newStoreRig(t)
	const rooth = 0x8e8e
	seg1 := w3SegKey(t, rooth, 1)
	seg2 := w3SegKey(t, rooth, 2)
	r.apply(t,
		segOp(seg1, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	// The torn batch: segid 1 trimmed by the split, segid 2 brand new
	// and unfenced. The root frame for this split never arrived.
	r.apply(t,
		segOp(seg1, w3Seg(5, 0)),
		segOp(seg2, w3Seg(8, 0)),
	)
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("rolled-back root count %d, want the rooted 10", got)
	}
	rec, err := r.s.lookup(seg1)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("fenced segment vanished in the rollback")
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 10 {
		t.Fatalf("fenced segment count %d, want the pre-split 10", n)
	}
	orphan, err := r.s.lookup(seg2)
	if err != nil {
		t.Fatal(err)
	}
	if orphan != nil {
		t.Fatal("unfenced segment applied despite the rollback")
	}
	// Repeat recoveries roll back to the same image.
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("second recovery drifted the count to %d", got)
	}
}

func TestReplayRollsBackTornMergeDelete(t *testing.T) {
	// A lazy merge deletes a segment and rewrites its sibling; the
	// merged root's batch is lost. The segment delete is structural
	// evidence on its own and the plane rolls back, keeping both
	// segments as the surviving root fences them.
	r := newStoreRig(t)
	const rooth = 0x1c1c
	seg1 := w3SegKey(t, rooth, 1)
	seg2 := w3SegKey(t, rooth, 2)
	r.apply(t,
		segOp(seg1, w3Seg(6, 0)),
		segOp(seg2, w3Seg(4, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1, 2), false),
	)
	r.apply(t,
		segOp(seg1, w3Seg(10, 0)),
		delOp(string(seg2)),
	)
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("rolled-back root count %d, want 10", got)
	}
	rec, err := r.s.lookup(seg2)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("the torn merge's segment delete applied")
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 4 {
		t.Fatalf("merged-away segment count %d, want the fenced 4", n)
	}
	rec, err = r.s.lookup(seg1)
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 6 {
		t.Fatalf("merge survivor count %d, want the fenced 6", n)
	}
}

func TestReplayKeepsCountOnlyPrefixBeforeRollback(t *testing.T) {
	// A count-only batch and then a torn structural batch in the same
	// window: the prefix before the first structural evidence still
	// patches the root, only the evidence batch onward rolls back.
	r := newStoreRig(t)
	const rooth = 0xd0d0
	seg1 := w3SegKey(t, rooth, 1)
	seg3 := w3SegKey(t, rooth, 3)
	r.apply(t,
		segOp(seg1, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t, segOp(seg1, w3Seg(12, 0)))
	r.apply(t,
		segOp(seg1, w3Seg(5, 0)),
		segOp(seg3, w3Seg(8, 0)),
	)
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 12 {
		t.Fatalf("patched root count %d, want the count-only 12", got)
	}
	rec, err := r.s.lookup(seg1)
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 12 {
		t.Fatalf("segment count %d, want the count-only window's 12", n)
	}
	orphan, err := r.s.lookup(seg3)
	if err != nil {
		t.Fatal(err)
	}
	if orphan != nil {
		t.Fatal("torn batch's new segment applied")
	}
}

func TestReplayUnderflowFailsRecovery(t *testing.T) {
	// A count-only window can never legally empty a segmented root
	// (removing the last field deletes the collection through a root
	// DEL and a genbump). A tail that says otherwise means the data
	// file and the WAL disagree, and recovery must refuse to invent a
	// zero-field root.
	r := newStoreRig(t)
	const rooth = 0x4e4e
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3Root(rooth, 10, 0, 1), false),
	)
	r.apply(t, segOp(seg, w3Seg(0, 0)))
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := OpenStore(r.path, 1<<16)
	if err == nil {
		t.Fatal("recovery accepted a reconciled count of zero")
	}
	if !strings.Contains(err.Error(), "reconcil") {
		t.Fatalf("unexpected recovery error: %v", err)
	}
	// Hand the rig a fresh store so its cleanup has one to close.
	r.s, err = CreateStore(r.path+".fresh", 1<<16)
	if err != nil {
		t.Fatal(err)
	}
}

// The paged rungs: a paged root keeps its fence in rtype 5 page
// records, so the fenced-set classification resolves each page to the
// image that was current at the root frame, and a plane rollback
// takes the rolled-back batches' page frames with it.

// w3PagedRoot builds a paged segmented root (doc 06 section 2.3)
// whose index references pageids, weight 1 each. nextSegid clears the
// largest of pageids and segids so page payloads built with w3Page
// stay consistent with it.
func w3PagedRoot(rooth, count uint64, pageids []uint64, segids []uint64) []byte {
	b := make([]byte, 44+16*len(pageids))
	b[0] = 2                                // hashSubSeg
	b[1] = 1                                // hflagFencePaged
	binary.LittleEndian.PutUint32(b[4:], 1) // rootgen
	binary.LittleEndian.PutUint64(b[8:], rooth)
	binary.LittleEndian.PutUint64(b[16:], count)
	next := uint64(0)
	for _, id := range pageids {
		next = max(next, id+1)
	}
	for _, id := range segids {
		next = max(next, id+1)
	}
	binary.LittleEndian.PutUint64(b[24:], next)
	binary.LittleEndian.PutUint32(b[40:], uint32(len(pageids)))
	for i, id := range pageids {
		off := 44 + 16*i
		binary.LittleEndian.PutUint64(b[off:], uint64(i)<<32)
		binary.LittleEndian.PutUint64(b[off+8:], id|1<<48) // weight 1
	}
	if len(pageids) > 0 {
		binary.LittleEndian.PutUint64(b[44:], 0)
	}
	return b
}

// w3Page builds a fence page payload referencing segids, one entry
// per segid with rising lo values.
func w3Page(segids ...uint64) []byte {
	b := make([]byte, 4+16*len(segids))
	binary.LittleEndian.PutUint16(b, uint16(len(segids)))
	for i, id := range segids {
		off := 4 + 16*i
		binary.LittleEndian.PutUint64(b[off:], uint64(i)<<32)
		binary.LittleEndian.PutUint64(b[off+8:], id)
	}
	return b
}

func w3FenceKey(t *testing.T, rooth, pageid uint64) []byte {
	t.Helper()
	sk, err := sqlo1.NewSubkey(rooth, sqlo1.SubkindFence, pageid)
	if err != nil {
		t.Fatal(err)
	}
	return sk.Encode()
}

func fenceOp(key, val []byte) sqlo1.Op {
	return sqlo1.Op{Rec: sqlo1.Record{Key: key, Value: val, Gen: 1, Fence: true}}
}

func TestReplayPatchesPagedRootAcrossBatchSplit(t *testing.T) {
	// The W1 window over a paged root: the count-only segment frame
	// past the root patches through the page-resolved fenced set.
	r := newStoreRig(t)
	const rooth = 0x88bb
	seg := w3SegKey(t, rooth, 1)
	page := w3FenceKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		fenceOp(page, w3Page(1)),
		rootOp("h", w3PagedRoot(rooth, 10, []uint64{2}, []uint64{1}), false),
	)
	r.apply(t, segOp(seg, w3Seg(16, 5000)))
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 16 {
		t.Fatalf("patched paged root count %d, want 16", got)
	}
	// Repeat recoveries are idempotent here too.
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 16 {
		t.Fatalf("second recovery drifted the count to %d", got)
	}
}

func TestReplayRollsBackPagedUnfencedSegid(t *testing.T) {
	// A frame for a segid no resolved page references is structural
	// evidence: the plane rolls back instead of patching.
	r := newStoreRig(t)
	const rooth = 0x66dd
	seg1 := w3SegKey(t, rooth, 1)
	seg3 := w3SegKey(t, rooth, 3)
	page := w3FenceKey(t, rooth, 2)
	r.apply(t,
		segOp(seg1, w3Seg(10, 0)),
		fenceOp(page, w3Page(1)),
		rootOp("h", w3PagedRoot(rooth, 10, []uint64{2}, []uint64{1, 3}), false),
	)
	r.apply(t, segOp(seg3, w3Seg(4, 0)))
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("rolled-back paged root count %d, want the rooted 10", got)
	}
	orphan, err := r.s.lookup(seg3)
	if err != nil {
		t.Fatal(err)
	}
	if orphan != nil {
		t.Fatal("unfenced segment applied through the rollback")
	}
}

func TestReplayPagedResolvesPageAsOfRootFrame(t *testing.T) {
	// An in-page split cut between its page frame and its root frame:
	// the tail's newer page image lists the new segid, but the root
	// that survived is the committed one, so classification must use
	// the committed page. The new segid is unfenced, the plane rolls
	// back, and the rolled-back batch's page frame is dropped with it,
	// leaving the committed page image in place.
	r := newStoreRig(t)
	const rooth = 0x55ee
	seg1 := w3SegKey(t, rooth, 1)
	seg3 := w3SegKey(t, rooth, 3)
	page := w3FenceKey(t, rooth, 2)
	committed := w3Page(1)
	r.apply(t,
		segOp(seg1, w3Seg(10, 0)),
		fenceOp(page, committed),
		rootOp("h", w3PagedRoot(rooth, 10, []uint64{2}, []uint64{1, 3}), false),
	)
	if err := r.s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// The torn split: new segment and the page rewrite land, the root
	// frame does not.
	r.apply(t,
		segOp(seg3, w3Seg(4, 0)),
		fenceOp(page, w3Page(1, 3)),
	)
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("root count %d after the torn split, want 10", got)
	}
	orphan, err := r.s.lookup(seg3)
	if err != nil {
		t.Fatal(err)
	}
	if orphan != nil {
		t.Fatal("torn split's segment applied")
	}
	rec, err := r.s.lookup(page)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, committed) {
		t.Fatal("torn split's page frame overwrote the committed page")
	}
	// Repeat recovery settles to the same state.
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("second recovery drifted the count to %d", got)
	}
}

func TestReplayPagedMissingPageFails(t *testing.T) {
	// A paged root whose page neither the tail nor the data file holds
	// cannot classify its window; recovery must fail loudly.
	r := newStoreRig(t)
	const rooth = 0x33ff
	seg := w3SegKey(t, rooth, 1)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		rootOp("h", w3PagedRoot(rooth, 10, []uint64{2}, []uint64{1}), false),
	)
	r.apply(t, segOp(seg, w3Seg(16, 0)))
	if err := r.s.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := OpenStore(r.path, 1<<16)
	if err == nil {
		t.Fatal("recovery classified a window without the fence page")
	}
	if !strings.Contains(err.Error(), "fence page") {
		t.Fatalf("unexpected recovery error: %v", err)
	}
	r.s, err = CreateStore(r.path+".fresh", 1<<16)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReplayNeutralPageFrames(t *testing.T) {
	// Page frames are never evidence: a tail carrying only a page
	// rewrite (an advisory weight refresh) patches nothing and rolls
	// back nothing, and the frame itself applies.
	r := newStoreRig(t)
	const rooth = 0x2277
	seg := w3SegKey(t, rooth, 1)
	page := w3FenceKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		fenceOp(page, w3Page(1)),
		rootOp("h", w3PagedRoot(rooth, 10, []uint64{2}, []uint64{1}), false),
	)
	refreshed := w3Page(1)
	refreshed[4+8+6] = 0x08 // meta high bits: a different fill class
	r.apply(t, fenceOp(page, refreshed))
	r.reopen(t)
	if got := r.rootCount(t, "h"); got != 10 {
		t.Fatalf("neutral page frame moved the count to %d", got)
	}
	rec, err := r.s.lookup(page)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, refreshed) {
		t.Fatal("the advisory page frame did not apply")
	}
}

// The rollback discipline (doc 09 section 3): a zset root claims
// RollbackRef, never elides, frames a rootkey mapping ahead of
// itself, and at replay the plane's put frames past its last root
// frame drop whole while deletes still apply. Handcrafted plane
// images again; the end-to-end matrix over the real zset operators
// lands with the dual-side mutation slice.

// zRoot builds a segmented zset root payload: the doc 06 header under
// the doc 09 sub byte. RollbackRef reads only the sub byte and the
// rooth, so the score-run tail stays off and rootCount keeps working.
func zRoot(rooth uint64, count uint64, segids ...uint64) []byte {
	b := w3Root(rooth, count, 0, segids...)
	b[0] = 5 // zsetSubSeg, the TagZset slot
	return b
}

// zRunKey builds a score-run subkey, kind 2 under the zset plane's
// rooth (the kind byte is type-namespaced, doc 03 section 6.3).
func zRunKey(t *testing.T, rooth, segid uint64) []byte {
	t.Helper()
	sk, err := sqlo1.NewSubkey(rooth, 2, segid)
	if err != nil {
		t.Fatal(err)
	}
	return sk.Encode()
}

func TestZsetRootFramesRootkeyAndNeverElides(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0xe1e1
	seg := w3SegKey(t, rooth, 1)
	run := zRunKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(3, 0)),
		segOp(run, []byte("run-v1")),
		rootOp("z", zRoot(rooth, 3, 1), false),
	)
	// Even a Delta-flagged zset root frames in full: no
	// reconciliation exists to rebuild an elided frame from.
	r.apply(t,
		segOp(seg, w3Seg(4, 0)),
		rootOp("z", zRoot(rooth, 4, 1), true),
	)
	recs, _ := walFrames(t, r)
	roots, rootkeys := 0, 0
	for _, rec := range recs {
		if rec == nil {
			continue
		}
		if rec.RType == RecRoot {
			roots++
		}
		if rh, ukey, ok := RootkeyRef(rec); ok {
			rootkeys++
			if rh != rooth || !bytes.Equal(ukey, []byte("z")) {
				t.Fatalf("rootkey maps %x to %q", rh, ukey)
			}
		}
	}
	if roots != 2 {
		t.Fatalf("%d root frames in the WAL, want both zset roots framed", roots)
	}
	if rootkeys != 2 {
		t.Fatalf("%d rootkey frames, want one ahead of every zset root", rootkeys)
	}
}

func TestReplayRollsBackZsetPutsPastRoot(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0xa5a5
	seg := w3SegKey(t, rooth, 1)
	run := zRunKey(t, rooth, 2)
	orphanRun := zRunKey(t, rooth, 3)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		segOp(run, []byte("run-v1")),
		rootOp("z", zRoot(rooth, 10, 1), false),
	)
	// The torn window: both sides' post-images and a freshly minted
	// run land in acknowledged batches and the crash takes the batch
	// with the command's root frame.
	r.apply(t,
		segOp(seg, w3Seg(11, 0)),
		segOp(run, []byte("run-v2")),
		segOp(orphanRun, []byte("run-minted")),
	)
	r.reopen(t)
	if got := r.rootCount(t, "z"); got != 10 {
		t.Fatalf("rolled-back root count %d, want the rooted 10", got)
	}
	rec, err := r.s.lookup(seg)
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 10 {
		t.Fatalf("member segment count %d, want the rooted 10", n)
	}
	rec, err = r.s.lookup(run)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rec.Value, []byte("run-v1")) {
		t.Fatalf("score run = %q, want the rooted run-v1", rec.Value)
	}
	orphan, err := r.s.lookup(orphanRun)
	if err != nil {
		t.Fatal(err)
	}
	if orphan != nil {
		t.Fatal("the un-rooted minted run applied despite the rollback")
	}
	// Repeat recoveries roll back to the same image.
	r.reopen(t)
	rec, err = r.s.lookup(run)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rec.Value, []byte("run-v1")) {
		t.Fatalf("second recovery drifted the run to %q", rec.Value)
	}
}

func TestZsetDeletePastRootApplies(t *testing.T) {
	// Run deaths ride behind the root frame that stopped referencing
	// them (root first, then the record), so a delete past the last
	// root frame always had its commanding root land and must apply.
	r := newStoreRig(t)
	const rooth = 0xb7b7
	seg := w3SegKey(t, rooth, 1)
	run2 := zRunKey(t, rooth, 2)
	run3 := zRunKey(t, rooth, 3)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		segOp(run2, []byte("run2-v1")),
		segOp(run3, []byte("run3-dying")),
		rootOp("z", zRoot(rooth, 10, 1), false),
	)
	r.apply(t, delOp(string(run3)))
	r.reopen(t)
	dead, err := r.s.lookup(run3)
	if err != nil {
		t.Fatal(err)
	}
	if dead != nil {
		t.Fatal("the acknowledged run delete was dropped")
	}
	rec, err := r.s.lookup(run2)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("run2-v1")) {
		t.Fatal("the fenced run did not survive the delete's replay")
	}
	if got := r.rootCount(t, "z"); got != 10 {
		t.Fatalf("root count %d, want 10", got)
	}
}

func TestZsetFencePagePastRootRollsBack(t *testing.T) {
	r := newStoreRig(t)
	const rooth = 0xc9c9
	pkey := sqlo1.Subkey{Rooth: rooth, Kind: sqlo1.SubkindFence, Segid: 9}.Encode()
	pageOp := func(v []byte) sqlo1.Op {
		return sqlo1.Op{Rec: sqlo1.Record{Key: pkey, Value: v, Fence: true, Gen: 1}}
	}
	r.apply(t,
		pageOp([]byte("page-v1")),
		rootOp("z", zRoot(rooth, 10, 1), false),
	)
	r.apply(t, pageOp([]byte("page-v2")))
	r.reopen(t)
	rec, err := r.s.lookup(pkey)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("page-v1")) {
		t.Fatalf("fence page = %q, want the rooted page-v1", rec.Value)
	}
}

func TestZsetOrphanFramesWithoutMappingApply(t *testing.T) {
	// No zset root ever framed, so no rootkey mapping exists: the
	// plane's frames apply as harmless orphans, the same posture the
	// reconcilable planes take.
	r := newStoreRig(t)
	const rooth = 0xd4d4
	seg := w3SegKey(t, rooth, 1)
	run := zRunKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(5, 0)),
		segOp(run, []byte("run-orphan")),
	)
	r.reopen(t)
	rec, err := r.s.lookup(run)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("run-orphan")) {
		t.Fatal("orphan run frame did not apply")
	}
}

func TestZsetStaleMappingSkipsRollback(t *testing.T) {
	// The key moves on after the plane rooted: deleted, recreated as
	// a plain string. The mapping points at a record that is no zset
	// root, so the trailing plane frames apply as orphans instead of
	// rolling anything back.
	r := newStoreRig(t)
	const rooth = 0xf2f2
	seg := w3SegKey(t, rooth, 1)
	run := zRunKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		segOp(run, []byte("run-v1")),
		rootOp("z", zRoot(rooth, 10, 1), false),
	)
	r.apply(t, delOp("z"))
	r.apply(t, putOp("z", []byte("plain again"), 0))
	r.apply(t, segOp(run, []byte("run-v2")))
	r.reopen(t)
	got, err := r.s.Get(context.Background(), []byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Value, []byte("plain again")) {
		t.Fatalf("recreated key = %q", got.Value)
	}
	rec, err := r.s.lookup(run)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("run-v2")) {
		t.Fatalf("run = %q, the stale mapping rolled a dead plane back", rec.Value)
	}
}

func TestZsetGenbumpClearsRollbackWindow(t *testing.T) {
	// A genbump retires the plane mid-tail: the window collected so
	// far is moot and later frames belong to whatever comes next, so
	// nothing rolls back across the bump.
	r := newStoreRig(t)
	const rooth = 0x9a9a
	seg := w3SegKey(t, rooth, 1)
	run := zRunKey(t, rooth, 2)
	r.apply(t,
		segOp(seg, w3Seg(10, 0)),
		segOp(run, []byte("run-v1")),
		rootOp("z", zRoot(rooth, 10, 1), false),
	)
	r.apply(t,
		segOp(seg, w3Seg(12, 0)),
		segOp(run, []byte("run-v2")),
	)
	r.genbump(t, rooth, 2)
	r.reopen(t)
	rec, err := r.s.lookup(run)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || !bytes.Equal(rec.Value, []byte("run-v2")) {
		t.Fatalf("run = %q, the retired plane's frames were dropped", rec.Value)
	}
	rec, err = r.s.lookup(seg)
	if err != nil {
		t.Fatal(err)
	}
	if n, _, _ := sqlo1.SegCounts(rec.Value); n != 12 {
		t.Fatalf("segment count %d, want the applied 12", n)
	}
}
