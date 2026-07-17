package akifile

import "testing"

// TestWriteBarrierRoundTrip cuts a snapshot over a running stream and reads the barrier
// back with FindBarrier: the emitted watermark, offset, and every shard tail match what
// the writer laid down.
func TestWriteBarrierRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("a")},
		{Shard: 1, Kind: KindLog, ShardSeq: 1, Payload: []byte("b")},
	}); err != nil {
		t.Fatalf("append log: %v", err)
	}

	shards := []BarrierShard{{TailSeg: 0x1000, TailSeq: 1}, {TailSeg: 0x2000, TailSeq: 2}}
	off, wbar, err := f.WriteBarrier(shards)
	if err != nil {
		t.Fatalf("write barrier: %v", err)
	}

	size, _ := dev.Size()
	h, rows, at, err := FindBarrier(dev, prefix, PageSize, uint64(size), wbar)
	if err != nil {
		t.Fatalf("find barrier: %v", err)
	}
	if at != off || h.Wbar != wbar || h.ShardCount != 2 {
		t.Fatalf("found barrier at %#x wbar %d count %d, want %#x/%d/2", at, h.Wbar, h.ShardCount, off, wbar)
	}
	for i := range shards {
		if rows[i] != shards[i] {
			t.Fatalf("shard %d = %+v, want %+v", i, rows[i], shards[i])
		}
	}
}

// TestWriteBarrierAssignsNextSeq confirms the barrier takes the next global_seq in the
// stream as its watermark: its segment header seq equals the returned Wbar, so a record
// appended after it sits strictly past the cut.
func TestWriteBarrierAssignsNextSeq(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("pre")}}); err != nil {
		t.Fatalf("append: %v", err)
	}

	off, wbar, err := f.WriteBarrier(nil)
	if err != nil {
		t.Fatalf("write barrier: %v", err)
	}
	if wbar != 2 || f.GlobalSeq() != 2 {
		t.Fatalf("wbar %d globalSeq %d, want 2/2", wbar, f.GlobalSeq())
	}

	hdr, _, err := f.ReadSegmentAt(off)
	if err != nil {
		t.Fatalf("read barrier segment: %v", err)
	}
	if hdr.Kind != KindBarrier || hdr.GlobalSeq != wbar {
		t.Fatalf("barrier segment kind %d seq %d, want KindBarrier/%d", hdr.Kind, hdr.GlobalSeq, wbar)
	}

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("post")}}); err != nil {
		t.Fatalf("append post: %v", err)
	}
	if f.GlobalSeq() <= wbar {
		t.Fatalf("post-barrier seq %d, want past the cut %d", f.GlobalSeq(), wbar)
	}
}

// TestWriteBarrierRefusesInconsistent refuses to emit a cut whose shard tail seq outruns
// the watermark: the single writer's total order cannot produce it, so WriteBarrier fails
// before writing and leaves the stream untouched.
func TestWriteBarrierRefusesInconsistent(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("x")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	seqBefore, curBefore := f.GlobalSeq(), f.Cursor()

	// Wbar would be 2; a shard tail seq of 5 cannot precede the cut.
	if _, _, err := f.WriteBarrier([]BarrierShard{{TailSeg: 0x1000, TailSeq: 5}}); err != ErrBarrier {
		t.Fatalf("err = %v, want ErrBarrier", err)
	}
	if f.GlobalSeq() != seqBefore || f.Cursor() != curBefore {
		t.Fatalf("refused barrier still advanced the stream: seq %d->%d cursor %d->%d",
			seqBefore, f.GlobalSeq(), curBefore, f.Cursor())
	}
}

// TestCommitSnapshotRootRoundTrips cuts a barrier, commits the SRT as that barrier's
// snapshot root, and recovers the file: the live root reads back flagged as a snapshot
// naming the same watermark, so the copy path finds the cut through the meta chain without
// scanning the log.
func TestCommitSnapshotRootRoundTrips(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("data")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_, wbar, err := f.WriteBarrier(nil)
	if err != nil {
		t.Fatalf("write barrier: %v", err)
	}

	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.CommitSnapshotRoot(&SRT{Gen: 5, Rows: rows}, nil, CheckpointStats{Clean: true}, CheckpointGlobals{}, wbar); err != nil {
		t.Fatalf("commit snapshot root: %v", err)
	}

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.SRT == nil || !rec.SRT.IsSnapshotRoot() {
		t.Fatalf("recovered root not flagged as a snapshot: %+v", rec.SRT)
	}
	if rec.SRT.SnapWbar != wbar {
		t.Fatalf("snapshot watermark = %d, want %d", rec.SRT.SnapWbar, wbar)
	}
}

// TestCommitSnapshotRootRejectsZeroWatermark refuses a snapshot commit at watermark zero,
// which a reader could not tell from an ordinary root, and leaves the stream untouched so
// the previous live root stays in force.
func TestCommitSnapshotRootRejectsZeroWatermark(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	seqBefore, curBefore := f.GlobalSeq(), f.Cursor()
	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.CommitSnapshotRoot(&SRT{Gen: 1, Rows: rows}, nil, CheckpointStats{}, CheckpointGlobals{}, 0); err != ErrSnapshotWatermark {
		t.Fatalf("err = %v, want ErrSnapshotWatermark", err)
	}
	if f.GlobalSeq() != seqBefore || f.Cursor() != curBefore {
		t.Fatalf("refused commit still advanced the stream: seq %d->%d cursor %d->%d",
			seqBefore, f.GlobalSeq(), curBefore, f.Cursor())
	}
}

// TestCommitSnapshotRootIsOrdinaryCheckpoint confirms a checkpoint committed the plain way
// stays an ordinary root: no snapshot flag, zero watermark, so only CommitSnapshotRoot ever
// marks a cut.
func TestCommitSnapshotRootIsOrdinaryCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.Checkpoint(&SRT{Gen: 2, Rows: rows}, nil, CheckpointStats{Clean: true}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.SRT == nil || rec.SRT.IsSnapshotRoot() || rec.SRT.SnapWbar != 0 {
		t.Fatalf("ordinary checkpoint reads as a snapshot root: %+v", rec.SRT)
	}
}

// TestWriteBarrierEmptyShards cuts a barrier over no shards: a valid watermark-only cut
// that reads back with an empty shard list.
func TestWriteBarrierEmptyShards(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	off, wbar, err := f.WriteBarrier(nil)
	if err != nil {
		t.Fatalf("write barrier: %v", err)
	}

	size, _ := dev.Size()
	h, rows, at, err := FindBarrier(dev, prefix, PageSize, uint64(size), wbar)
	if err != nil {
		t.Fatalf("find barrier: %v", err)
	}
	if at != off || h.ShardCount != 0 || len(rows) != 0 {
		t.Fatalf("empty barrier at %#x count %d rows %d, want %#x/0/0", at, h.ShardCount, len(rows), off)
	}
}
