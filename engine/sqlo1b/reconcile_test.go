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
