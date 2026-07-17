package akifile

import (
	"errors"
	"testing"
)

// TestReadOpenStateClean opens a freshly created file: both slots are valid and
// carry clean_shutdown, so the state is OpenClean off slot A with the header-page
// file size as the durable tail.
func TestReadOpenStateClean(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("read open state: %v", err)
	}
	if st.Outcome != OpenClean {
		t.Fatalf("outcome = %d, want OpenClean", st.Outcome)
	}
	if st.Which != 0 {
		t.Fatalf("which = %d, want 0 (slot A)", st.Which)
	}
	if st.Meta == nil || st.Meta.FileSize != PageSize {
		t.Fatalf("meta file size = %v, want %d", st.Meta, PageSize)
	}
}

// TestReadOpenStateCrashed picks the higher-commit slot and reports OpenCrashed
// when it lacks clean_shutdown: the process died mid-run and the tail must be
// replayed.
func TestReadOpenStateCrashed(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// Overwrite slot B with a higher commit_seq that is not a clean shutdown, the
	// on-disk state a crash between checkpoint and clean close leaves.
	m := &MetaSlot{CommitSeq: 5, FileSize: PageSize, SRTShardCount: prefix.ShardCount}
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("read open state: %v", err)
	}
	if st.Outcome != OpenCrashed {
		t.Fatalf("outcome = %d, want OpenCrashed", st.Outcome)
	}
	if st.Which != 1 || st.Meta.CommitSeq != 5 {
		t.Fatalf("picked slot %d seq %d, want slot 1 seq 5", st.Which, st.Meta.CommitSeq)
	}
}

// TestReadOpenStateShardMismatch rejects a live root whose SRT shard count
// disagrees with the prefix: a torn SRT swap or a wrong-geometry open.
func TestReadOpenStateShardMismatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	m := &MetaSlot{CommitSeq: 2, FileSize: PageSize, SRTShardCount: prefix.ShardCount + 1}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	if _, err := ReadOpenState(dev); err != ErrShardCount {
		t.Fatalf("err = %v, want ErrShardCount", err)
	}
}

// TestReadOpenStateScanFallback tears both slots and confirms the state is a scan
// fallback with no root and no error: the full scan is a valid recovery path.
func TestReadOpenStateScanFallback(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	dev.buf[prefix.MetaSlotAOff+3] ^= 0xff
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("read open state: %v", err)
	}
	if st.Outcome != OpenScanFallback {
		t.Fatalf("outcome = %d, want OpenScanFallback", st.Outcome)
	}
	if st.Which != -1 || st.Meta != nil {
		t.Fatalf("scan fallback carries which=%d meta=%v, want -1/nil", st.Which, st.Meta)
	}
	if st.Prefix == nil {
		t.Fatalf("scan fallback dropped the prefix")
	}
}

// TestReadOpenStateBadPrefix stops at a torn prefix: recovery never guesses past
// a header it cannot trust.
func TestReadOpenStateBadPrefix(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)
	dev.buf[0] ^= 0xff // break the magic

	if _, err := ReadOpenState(dev); err != ErrMagic {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}

// TestReplayTailVisitsEverySegment appends three segments and confirms the replay
// walk hands each intact one to the visitor in file order, then stops at the
// durable tail (the append cursor).
func TestReplayTailVisitsEverySegment(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	group := []Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("one")},
		{Shard: 1, Kind: KindLog, ShardSeq: 1, Payload: []byte("two")},
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("three")},
	}
	offs, err := f.AppendGroup(group)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var seen []uint64
	var seqs []uint64
	size, _ := dev.Size()
	end, err := ReplayTail(dev, f.Prefix(), PageSize, uint64(size), func(off uint64, h *SegHeader, _ []byte) error {
		seen = append(seen, off)
		seqs = append(seqs, h.GlobalSeq)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(seen) != len(group) {
		t.Fatalf("visited %d segments, want %d", len(seen), len(group))
	}
	for i := range offs {
		if seen[i] != offs[i] {
			t.Fatalf("segment %d off = %d, want %d", i, seen[i], offs[i])
		}
		if seqs[i] != uint64(i+1) {
			t.Fatalf("segment %d seq = %d, want %d", i, seqs[i], i+1)
		}
	}
	if end != f.Cursor() {
		t.Fatalf("replay end %d, want cursor %d", end, f.Cursor())
	}
}

// TestReplayTailStopsAtTornSegment corrupts the second segment's payload and
// confirms the walk stops there: the first segment is replayed, the torn one and
// everything past it is the un-durable tail.
func TestReplayTailStopsAtTornSegment(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("durable")},
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("torn-tail")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[offs[1]+SegHeaderLen+1] ^= 0xff // corrupt the second payload

	count := 0
	size, _ := dev.Size()
	end, err := ReplayTail(dev, f.Prefix(), PageSize, uint64(size), func(_ uint64, _ *SegHeader, _ []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 1 {
		t.Fatalf("visited %d segments, want 1 (stop at the torn tail)", count)
	}
	if end != offs[1] {
		t.Fatalf("replay end %d, want the torn segment offset %d", end, offs[1])
	}
}

// TestReplayTailPropagatesVisitError stops the walk when a visitor cannot apply a
// segment and returns the error at the offending offset: recovery fails rather
// than dropping a durable segment it could not replay.
func TestReplayTailPropagatesVisitError(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("first")},
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("second")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	boom := errors.New("cannot apply")
	size, _ := dev.Size()
	end, err := ReplayTail(dev, f.Prefix(), PageSize, uint64(size), func(off uint64, _ *SegHeader, _ []byte) error {
		if off == offs[1] {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the visit error", err)
	}
	if end != offs[1] {
		t.Fatalf("replay stopped at %d, want the failing segment %d", end, offs[1])
	}
}

// TestReplayTailFromCheckpoint starts the walk past the first segment, the crashed
// open replaying only the tail appended since the last checkpoint.
func TestReplayTailFromCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("before-ckpt")},
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("after-ckpt")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var seen []uint64
	size, _ := dev.Size()
	if _, err := ReplayTail(dev, f.Prefix(), offs[1], uint64(size), func(off uint64, _ *SegHeader, _ []byte) error {
		seen = append(seen, off)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(seen) != 1 || seen[0] != offs[1] {
		t.Fatalf("replayed %v, want only the post-checkpoint segment at %d", seen, offs[1])
	}
}

func writeMeta(t *testing.T, dev *memDevice, prefix *Prefix, off uint64, m *MetaSlot) {
	t.Helper()
	b, err := m.Marshal(prefix.ChecksumKind)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if _, err := dev.WriteAt(b, int64(off)); err != nil {
		t.Fatalf("write meta: %v", err)
	}
}

// TestReadSRTFreshFileHasNoTable returns a nil table with no error for a freshly
// created file: no checkpoint has been taken, so there are no roots to read and
// the driver replays from the header page.
func TestReadSRTFreshFileHasNoTable(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	srt, err := ReadSRT(dev, f.Prefix(), st.Meta)
	if err != nil || srt != nil {
		t.Fatalf("fresh SRT = %v/%v, want nil/nil", srt, err)
	}
}

// TestReadSRTRoundTrip writes a shard root table into free space, points the live
// meta root at it, and reads back every shard's checkpoint roots.
func TestReadSRTRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	for i := range rows {
		rows[i] = SRTRow{
			IndexCkptOff: uint64(0x10000 * (i + 1)),
			CkptLogPos:   uint64(100 + i),
			FirstTailSeg: uint64(0x20000 * (i + 1)),
			LiveRecords:  uint64(i * 7),
		}
	}
	srtOff := writeSRT(t, dev, prefix, &SRT{Gen: 4, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 3, FileSize: PageSize, CleanShutdown: 1,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	srt, err := ReadSRT(dev, prefix, st.Meta)
	if err != nil {
		t.Fatalf("read srt: %v", err)
	}
	if srt.Gen != 4 || len(srt.Rows) != int(prefix.ShardCount) {
		t.Fatalf("srt gen %d rows %d, want 4/%d", srt.Gen, len(srt.Rows), prefix.ShardCount)
	}
	for i, r := range srt.Rows {
		if r.IndexCkptOff != rows[i].IndexCkptOff || r.FirstTailSeg != rows[i].FirstTailSeg {
			t.Fatalf("row %d = %+v, want %+v", i, r, rows[i])
		}
	}
}

// TestReadSRTRowCountMismatch refuses a table whose row count disagrees with the
// prefix shard count, the third leg of the three-way agreement.
func TestReadSRTRowCountMismatch(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// One row too few: a torn SRT swap or the wrong shard geometry.
	rows := make([]SRTRow, prefix.ShardCount-1)
	srtOff := writeSRT(t, dev, prefix, &SRT{Gen: 1, Rows: rows})

	meta := &MetaSlot{
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	if _, err := ReadSRT(dev, prefix, meta); err != ErrShardCount {
		t.Fatalf("err = %v, want ErrShardCount", err)
	}
}

// TestReadSRTCatchesTornTable returns ErrChecksum when the table bytes are torn:
// the SRT crc covers the header prefix and every row.
func TestReadSRTCatchesTornTable(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	srtOff := writeSRT(t, dev, prefix, &SRT{Gen: 1, Rows: rows})
	dev.buf[srtOff+SRTHeaderLen+3] ^= 0xff // corrupt a row byte

	meta := &MetaSlot{
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	if _, err := ReadSRT(dev, prefix, meta); err != ErrChecksum {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// writeSRT marshals a shard root table into the device just past the header page
// and returns the offset it landed at.
func writeSRT(t *testing.T, dev *memDevice, prefix *Prefix, srt *SRT) uint64 {
	t.Helper()
	b, err := srt.Marshal(prefix.ChecksumKind)
	if err != nil {
		t.Fatalf("marshal srt: %v", err)
	}
	off := uint64(PageSize)
	if _, err := dev.WriteAt(b, int64(off)); err != nil {
		t.Fatalf("write srt: %v", err)
	}
	return off
}

// TestRebuildShardIndexChain writes a full checkpoint segment and two delta
// segments chained by their base pointers, then rebuilds the shard index from the
// newest and confirms it is the live set with the newest addresses.
func TestRebuildShardIndexChain(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	fullOff := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 10}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100},
		{KeyHash: 2, RecordAddr: 0x200},
		{KeyHash: 3, RecordAddr: 0x300},
	})
	d1Off := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptDelta, CkptLogPos: 20, BaseCkptOff: fullOff}, []CkptEntry{
		{KeyHash: 2, RecordAddr: 0x2222},
		{KeyHash: 1, Flags: CkptTombstone},
	})
	d2Off := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptDelta, CkptLogPos: 30, BaseCkptOff: d1Off}, []CkptEntry{
		{KeyHash: 4, RecordAddr: 0x400},
	})

	r, err := RebuildShardIndex(dev, f.Prefix(), d2Off)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.LogPos != 30 {
		t.Fatalf("log pos %d, want 30", r.LogPos)
	}
	want := map[uint64]uint64{2: 0x2222, 3: 0x300, 4: 0x400} // 1 tombstoned
	if r.Len() != len(want) {
		t.Fatalf("index has %d entries, want %d", r.Len(), len(want))
	}
	for kh, addr := range want {
		if e, ok := r.Entries()[kh]; !ok || e.RecordAddr != addr {
			t.Fatalf("key %d = %v/%v, want addr %#x", kh, e, ok, addr)
		}
	}
}

// TestRebuildShardIndexNoCheckpoint returns an empty index for a shard that has
// never been checkpointed: the whole index comes from the tail replay.
func TestRebuildShardIndexNoCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	r, err := RebuildShardIndex(dev, f.Prefix(), 0)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("empty-checkpoint index has %d entries, want 0", r.Len())
	}
}

// TestRebuildShardIndexRejectsNonCheckpoint refuses a root that points at a
// segment that is not an index_ckpt.
func TestRebuildShardIndexRejectsNonCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("not a checkpoint")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := RebuildShardIndex(dev, f.Prefix(), offs[0]); err != ErrCheckpoint {
		t.Fatalf("err = %v, want ErrCheckpoint", err)
	}
}

// TestRebuildShardIndexRejectsCycle catches a back-pointer cycle: two delta
// segments whose bases point at each other never reach a base full.
func TestRebuildShardIndexRejectsCycle(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// Two checkpoint segments on the 4KiB grid, each naming the other as its base.
	offA := uint64(PageSize)
	offB := uint64(PageSize + SegmentAlign)
	writeCkptSegment(t, dev, prefix, offA, CkptHeader{FullOrDelta: CkptDelta, BaseCkptOff: offB}, nil)
	writeCkptSegment(t, dev, prefix, offB, CkptHeader{FullOrDelta: CkptDelta, BaseCkptOff: offA}, nil)

	if _, err := RebuildShardIndex(dev, prefix, offA); err != ErrCheckpoint {
		t.Fatalf("err = %v, want ErrCheckpoint", err)
	}
}

// TestRecoverFreshFile recovers a freshly created file: a clean open with no
// checkpoints, so no roots to read and the tail replay starts at the header page.
func TestRecoverFreshFile(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenClean {
		t.Fatalf("outcome = %d, want OpenClean", rec.State.Outcome)
	}
	if rec.SRT != nil || rec.Indexes != nil {
		t.Fatalf("fresh file carries srt=%v indexes=%v, want nil", rec.SRT, rec.Indexes)
	}
	if rec.TailFrom != PageSize {
		t.Fatalf("tail from %d, want the header page %d", rec.TailFrom, PageSize)
	}
}

// TestRecoverCrashedRebuildsShards recovers a crashed file with per-shard
// checkpoints: it rebuilds each shard's index and starts the tail replay at the
// earliest shard's first un-checkpointed segment.
func TestRecoverCrashedRebuildsShards(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	off0 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 5}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100}, {KeyHash: 2, RecordAddr: 0x200},
	})
	off1 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 6}, []CkptEntry{
		{KeyHash: 3, RecordAddr: 0x300}, {KeyHash: 4, RecordAddr: 0x400}, {KeyHash: 5, RecordAddr: 0x500},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{IndexCkptOff: off0, FirstTailSeg: off0, CkptLogPos: 5}
	rows[1] = SRTRow{IndexCkptOff: off1, FirstTailSeg: off1, CkptLogPos: 6}
	// shards 2 and 3 never checkpointed: zero rows.
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 9, FileSize: fileSize, CleanShutdown: 0,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenCrashed {
		t.Fatalf("outcome = %d, want OpenCrashed", rec.State.Outcome)
	}
	if len(rec.Indexes) != int(prefix.ShardCount) {
		t.Fatalf("rebuilt %d shard indexes, want %d", len(rec.Indexes), prefix.ShardCount)
	}
	if rec.Indexes[0].Len() != 2 || rec.Indexes[1].Len() != 3 {
		t.Fatalf("shard index sizes = %d/%d, want 2/3", rec.Indexes[0].Len(), rec.Indexes[1].Len())
	}
	if rec.Indexes[2].Len() != 0 || rec.Indexes[3].Len() != 0 {
		t.Fatalf("uncheckpointed shards carry %d/%d entries, want 0/0", rec.Indexes[2].Len(), rec.Indexes[3].Len())
	}
	// Replay starts at the earliest shard's first tail segment, off0 < off1.
	if rec.TailFrom != off0 {
		t.Fatalf("tail from %d, want the earliest first-tail-seg %d", rec.TailFrom, off0)
	}
}

// TestRecoverWiresAuxiliaryTables checks that Recover rebuilds a shard's dead-byte
// accounting and cold-chunk directory alongside its index, from the seg_stats and
// chunk_dir roots the SRT row names, and leaves an uncheckpointed shard with empty (not
// nil) tables so a consumer can always index the slice.
func TestRecoverWiresAuxiliaryTables(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	idxOff := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 5}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100}, {KeyHash: 2, RecordAddr: 0x200},
	})
	ssOff := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsFull, CkptLogPos: 5}, []SegStatsEntry{
		{SegOff: 0x1000, LiveBytes: 900, DeadBytes: 100},
		{SegOff: 0x2000, LiveBytes: 700, DeadBytes: 300},
	})
	cdOff := appendChunkDir(t, f, ChunkDirHeader{FullOrDelta: ChunkDirFull, CkptLogPos: 5}, []ChunkDirCollection{
		{KeyHash: 0xA1, Chunks: []ChunkDirRow{{FirstDisc: []byte("aaa"), ElementCount: 40, ChunkOff: 0x3000, ChunkLiveBytes: 500}}},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{IndexCkptOff: idxOff, SegstatsOff: ssOff, ChunkdirOff: cdOff, FirstTailSeg: idxOff, CkptLogPos: 5}
	// shards 1..3 never checkpointed any table: zero rows.
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 9, FileSize: fileSize, CleanShutdown: 0,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.SegStats) != int(prefix.ShardCount) || len(rec.ChunkDirs) != int(prefix.ShardCount) {
		t.Fatalf("aux slices sized %d/%d, want %d", len(rec.SegStats), len(rec.ChunkDirs), prefix.ShardCount)
	}
	if rec.Indexes[0].Len() != 2 {
		t.Fatalf("shard 0 index has %d entries, want 2", rec.Indexes[0].Len())
	}
	if rec.SegStats[0].Len() != 2 || rec.SegStats[0].TotalDeadBytes() != 400 {
		t.Fatalf("shard 0 seg-stats = %d entries / %d dead, want 2/400", rec.SegStats[0].Len(), rec.SegStats[0].TotalDeadBytes())
	}
	if rec.ChunkDirs[0].Len() != 1 || rec.ChunkDirs[0].TotalLiveBytes() != 500 {
		t.Fatalf("shard 0 chunk-dir = %d collections / %d live, want 1/500", rec.ChunkDirs[0].Len(), rec.ChunkDirs[0].TotalLiveBytes())
	}
	// An uncheckpointed shard carries empty rebuilders, never nil.
	if rec.SegStats[1] == nil || rec.SegStats[1].Len() != 0 || rec.ChunkDirs[1] == nil || rec.ChunkDirs[1].Len() != 0 {
		t.Fatalf("uncheckpointed shard 1 aux tables = %v/%v, want empty non-nil", rec.SegStats[1], rec.ChunkDirs[1])
	}
}

// TestDeadByteAuditRollsUpSegStats reopens a file whose shards checkpointed their
// accounting and rolls the seg_stats up into per-shard and file-wide live-and-dead totals,
// the durable garbage a reopened compaction trigger reads back instead of rediscovering.
func TestDeadByteAuditRollsUpSegStats(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	ss0 := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsFull, CkptLogPos: 5}, []SegStatsEntry{
		{SegOff: 0x1000, LiveBytes: 900, DeadBytes: 100},
		{SegOff: 0x2000, LiveBytes: 700, DeadBytes: 300},
	})
	ss1 := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsFull, CkptLogPos: 6}, []SegStatsEntry{
		{SegOff: 0x5000, LiveBytes: 500, DeadBytes: 500},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{SegstatsOff: ss0, CkptLogPos: 5}
	rows[1] = SRTRow{SegstatsOff: ss1, CkptLogPos: 6}
	// shards 2.. never checkpointed their accounting: zero rows.
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 3, FileSize: fileSize,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	audit := rec.DeadByteAudit()
	if audit.TotalLive != 900+700+500 || audit.TotalDead != 100+300+500 {
		t.Fatalf("totals = live %d / dead %d, want 2100/900", audit.TotalLive, audit.TotalDead)
	}
	if len(audit.PerShard) != int(prefix.ShardCount) {
		t.Fatalf("per-shard sized %d, want %d", len(audit.PerShard), prefix.ShardCount)
	}
	if audit.PerShard[0] != (ShardBytes{LiveBytes: 1600, DeadBytes: 400}) {
		t.Fatalf("shard 0 = %+v, want live 1600 dead 400", audit.PerShard[0])
	}
	if audit.PerShard[1] != (ShardBytes{LiveBytes: 500, DeadBytes: 500}) {
		t.Fatalf("shard 1 = %+v, want live 500 dead 500", audit.PerShard[1])
	}
	// An uncheckpointed shard contributes nothing.
	if audit.PerShard[2] != (ShardBytes{}) {
		t.Fatalf("uncheckpointed shard 2 = %+v, want zero", audit.PerShard[2])
	}
}

// TestDeadByteAuditFreshFileIsZero confirms a file with no seg_stats audits to a zero total
// over a nil breakdown: nothing has been accounted, the honest reading for a fresh open.
func TestDeadByteAuditFreshFileIsZero(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	audit := rec.DeadByteAudit()
	if audit.PerShard != nil || audit.TotalLive != 0 || audit.TotalDead != 0 {
		t.Fatalf("fresh audit = %+v, want zero over nil", audit)
	}
}

// TestRecoverWiresGlobalRoots checks that Recover surfaces the file-global TTL index,
// free map, and meta_kv the live meta slot points at, so a reopening writer resumes
// allocation, the expiry pass has its reclaim list, and the file's config and provenance
// read back without a second open.
func TestRecoverWiresGlobalRoots(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	fmOff := appendFreeMap(t, f, []FreeExtent{
		{StartOff: 0x40000, Length: 0x8000},
		{StartOff: 0x50000, Length: 0x1000, Flags: FreeMapPending},
	})
	kvOff := appendMetaKV(t, f, []MetaKVPair{
		{Key: []byte("import.source"), Value: []byte("RDB v12")},
		{Key: []byte("config.maxmemory"), Value: []byte("512mb")},
	})
	// Write the TTL index as a bare root past the segments, so the roots do not overlap
	// in the append space.
	ttlBytes := encodeTTLIndex([]TTLClass{
		{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000, 0x2000}},
	})
	ttlOff := f.Cursor()
	if _, err := dev.WriteAt(ttlBytes, int64(ttlOff)); err != nil {
		t.Fatalf("write ttl: %v", err)
	}

	m := &MetaSlot{
		CommitSeq: 3, FileSize: f.Cursor(), CleanShutdown: 1,
		TTLIndexOff: ttlOff, TTLIndexLen: uint32(len(ttlBytes)), FreeMapOff: fmOff, MetaKVOff: kvOff,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.TTLClasses) != 1 || len(rec.TTLClasses[0].Segments) != 2 {
		t.Fatalf("recovered TTL classes = %+v, want 1 class of 2 segments", rec.TTLClasses)
	}
	free, pending := FreeMapTotals(rec.FreeMap)
	if free != 0x8000 || pending != 0x1000 {
		t.Fatalf("recovered free map = free %d / pending %d, want %d/%d", free, pending, 0x8000, 0x1000)
	}
	if len(rec.MetaKV) != 2 {
		t.Fatalf("recovered meta kv = %+v, want 2 pairs", rec.MetaKV)
	}
	if v, ok := MetaKVLookup(rec.MetaKV, "import.source"); !ok || string(v) != "RDB v12" {
		t.Fatalf("recovered import.source = %q/%v, want RDB v12", v, ok)
	}
}

// TestRecoverCleanSkipsTail recovers a cleanly-closed file with checkpoints: the
// index rebuilds from the roots and the tail replay finds nothing, since a clean
// shutdown checkpointed everything.
func TestRecoverCleanSkipsTail(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	off0 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 5}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{IndexCkptOff: off0, CkptLogPos: 5}
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 4, FileSize: fileSize, CleanShutdown: 1,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenClean {
		t.Fatalf("outcome = %d, want OpenClean", rec.State.Outcome)
	}
	if rec.Indexes[0].Len() != 1 {
		t.Fatalf("shard 0 index has %d entries, want 1", rec.Indexes[0].Len())
	}
	if rec.TailFrom != fileSize {
		t.Fatalf("tail from %d, want the file size %d (nothing past the roots)", rec.TailFrom, fileSize)
	}
}

// TestRecoverScanFallback recovers a file whose meta slots are both torn: no root
// to trust, so it replays the whole append space from the header page.
func TestRecoverScanFallback(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	dev.buf[prefix.MetaSlotAOff+3] ^= 0xff
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenScanFallback {
		t.Fatalf("outcome = %d, want OpenScanFallback", rec.State.Outcome)
	}
	if rec.SRT != nil || rec.Indexes != nil {
		t.Fatalf("scan fallback carries srt=%v indexes=%v, want nil", rec.SRT, rec.Indexes)
	}
	if rec.TailFrom != PageSize {
		t.Fatalf("tail from %d, want the header page %d", rec.TailFrom, PageSize)
	}
}

// TestCrossCheckAgreesWithCheckpoints reconciles SRT rows whose live_records and
// shard_seq_high match the checkpoints they name, plus uncheckpointed zero rows over
// empty indexes, and finds no drift.
func TestCrossCheckAgreesWithCheckpoints(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	off0 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 5, SeqHigh: 42}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100}, {KeyHash: 2, RecordAddr: 0x200},
	})
	off1 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 6, SeqHigh: 57}, []CkptEntry{
		{KeyHash: 3, RecordAddr: 0x300}, {KeyHash: 4, RecordAddr: 0x400}, {KeyHash: 5, RecordAddr: 0x500},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{IndexCkptOff: off0, CkptLogPos: 5, LiveRecords: 2, ShardSeqHigh: 42}
	rows[1] = SRTRow{IndexCkptOff: off1, CkptLogPos: 6, LiveRecords: 3, ShardSeqHigh: 57}
	// shards 2 and 3 never checkpointed: zero rows over empty indexes.
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 9, FileSize: fileSize, CleanShutdown: 1,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if drift := rec.CrossCheck(); len(drift) != 0 {
		t.Fatalf("cross-check found drift over consistent rows: %+v", drift)
	}
}

// TestCrossCheckReportsDrift catches an SRT row whose live_records overcounts its
// checkpoint and another whose shard_seq_high disagrees, reporting one drift each and
// naming the claimed-versus-actual figures, without aborting recovery.
func TestCrossCheckReportsDrift(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	off0 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 5, SeqHigh: 42}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100}, {KeyHash: 2, RecordAddr: 0x200},
	})
	off1 := appendCkpt(t, f, CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 6, SeqHigh: 57}, []CkptEntry{
		{KeyHash: 3, RecordAddr: 0x300}, {KeyHash: 4, RecordAddr: 0x400}, {KeyHash: 5, RecordAddr: 0x500},
	})
	fileSize := f.Cursor()

	rows := make([]SRTRow, prefix.ShardCount)
	rows[0] = SRTRow{IndexCkptOff: off0, CkptLogPos: 5, LiveRecords: 5, ShardSeqHigh: 42} // overcounts entries
	rows[1] = SRTRow{IndexCkptOff: off1, CkptLogPos: 6, LiveRecords: 3, ShardSeqHigh: 99} // wrong seq_high
	srtOff := writeSRTAt(t, dev, prefix, fileSize, &SRT{Gen: 1, Rows: rows})

	m := &MetaSlot{
		CommitSeq: 9, FileSize: fileSize, CleanShutdown: 1,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	drift := rec.CrossCheck()
	if len(drift) != 2 {
		t.Fatalf("cross-check found %d drifts, want 2: %+v", len(drift), drift)
	}
	if drift[0] != (SRTRowDrift{Shard: 0, RowLiveRecords: 5, CkptEntries: 2, RowSeqHigh: 42, CkptSeqHigh: 42}) {
		t.Fatalf("shard 0 drift = %+v", drift[0])
	}
	if drift[1] != (SRTRowDrift{Shard: 1, RowLiveRecords: 3, CkptEntries: 3, RowSeqHigh: 99, CkptSeqHigh: 57}) {
		t.Fatalf("shard 1 drift = %+v", drift[1])
	}
}

// TestCrossCheckNoSRT reconciles a recovery with no shard root table, a fresh file or
// a scan fallback: there is nothing to reconcile, so it reports no drift.
func TestCrossCheckNoSRT(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)
	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if drift := rec.CrossCheck(); drift != nil {
		t.Fatalf("cross-check over a rootless recovery = %+v, want nil", drift)
	}
}

// TestReadExtentTableFresh returns a nil map for a fresh file: no extent table has
// been written, so there is no shape hint and a tool falls back to a scan.
func TestReadExtentTableFresh(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	es, err := ReadExtentTable(dev, st.Meta)
	if err != nil || es != nil {
		t.Fatalf("fresh extent table = %v/%v, want nil/nil", es, err)
	}
}

// TestReadExtentTableRoundTrip writes an extent map into free space, points the
// live meta root at it, and reads back every region.
func TestReadExtentTableRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	want := []Extent{
		{Kind: ExtentHeader, StartOff: 0, Length: PageSize},
		{Kind: ExtentAppend, StartOff: PageSize, Length: 0x40000},
		{Kind: ExtentFree, StartOff: 0x40000 + PageSize, Length: 0x10000},
	}
	b := MarshalExtents(want)
	extOff := uint64(PageSize)
	if _, err := dev.WriteAt(b, int64(extOff)); err != nil {
		t.Fatalf("write extents: %v", err)
	}

	m := &MetaSlot{
		CommitSeq: 1, FileSize: PageSize, CleanShutdown: 1,
		ExtentTableOff: extOff, ExtentTableLen: uint32(len(b)),
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	es, err := ReadExtentTable(dev, st.Meta)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}
	if len(es) != len(want) {
		t.Fatalf("read %d extents, want %d", len(es), len(want))
	}
	for i := range want {
		if es[i] != want[i] {
			t.Fatalf("extent %d = %+v, want %+v", i, es[i], want[i])
		}
	}
}

// TestReadExtentTableTornLength rejects a table whose byte length is not a whole
// number of extents: a torn or truncated write.
func TestReadExtentTableTornLength(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	b := MarshalExtents([]Extent{{Kind: ExtentAppend, StartOff: PageSize, Length: 0x1000}})
	extOff := uint64(PageSize)
	if _, err := dev.WriteAt(b, int64(extOff)); err != nil {
		t.Fatalf("write extents: %v", err)
	}

	// One byte short of a whole extent: ParseExtents refuses to decode a torn row.
	m := &MetaSlot{ExtentTableOff: extOff, ExtentTableLen: uint32(len(b)) - 1}
	if _, err := ReadExtentTable(dev, m); err != ErrLength {
		t.Fatalf("err = %v, want ErrLength", err)
	}
}

// writeSRTAt marshals a shard root table at a chosen offset and returns it.
func writeSRTAt(t *testing.T, dev *memDevice, prefix *Prefix, off uint64, srt *SRT) uint64 {
	t.Helper()
	b, err := srt.Marshal(prefix.ChecksumKind)
	if err != nil {
		t.Fatalf("marshal srt: %v", err)
	}
	if _, err := dev.WriteAt(b, int64(off)); err != nil {
		t.Fatalf("write srt: %v", err)
	}
	return off
}

// appendCkpt frames a checkpoint payload and appends it as an index_ckpt segment,
// returning the segment offset the base pointers reference.
func appendCkpt(t *testing.T, f *File, h CkptHeader, entries []CkptEntry) uint64 {
	t.Helper()
	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindIndexCkpt, ShardSeq: 1, Payload: ckptPayload(h, entries)}})
	if err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	return offs[0]
}

// writeCkptSegment lays a validly-framed index_ckpt segment at a chosen offset, so
// a test can hand-build a chain shape (like a cycle) the append writer cannot.
func writeCkptSegment(t *testing.T, dev *memDevice, prefix *Prefix, off uint64, h CkptHeader, entries []CkptEntry) {
	t.Helper()
	payload := ckptPayload(h, entries)
	sh := &SegHeader{Shard: 0, Kind: KindIndexCkpt, ShardSeq: 1}
	hb, err := sh.Marshal(prefix.ChecksumKind, payload)
	if err != nil {
		t.Fatalf("marshal segment: %v", err)
	}
	if _, err := dev.WriteAt(hb, int64(off)); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := dev.WriteAt(payload, int64(off)+SegHeaderLen); err != nil {
		t.Fatalf("write payload: %v", err)
	}
}

// ckptPayload frames a checkpoint payload from a header and its entries, the bytes
// an index_ckpt segment would carry.
func ckptPayload(h CkptHeader, entries []CkptEntry) []byte {
	h.EntryCount = uint64(len(entries))
	b := AppendCkptHeader(nil, h)
	for _, e := range entries {
		b = AppendCkptEntry(b, e)
	}
	return b
}

// TestIndexRebuilderFullThenDeltas loads a full dump, then two deltas that add,
// overwrite, and tombstone keys, and confirms the accumulated index is the live
// set with the newest addresses and the last checkpoint's log position.
func TestIndexRebuilderFullThenDeltas(t *testing.T) {
	full := ckptPayload(CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 10, SeqHigh: 3}, []CkptEntry{
		{KeyHash: 1, RecordAddr: 0x100, Slot: 1},
		{KeyHash: 2, RecordAddr: 0x200, Slot: 2},
		{KeyHash: 3, RecordAddr: 0x300, Slot: 3},
	})
	d1 := ckptPayload(CkptHeader{FullOrDelta: CkptDelta, CkptLogPos: 20, BaseCkptOff: 4096, SeqHigh: 5}, []CkptEntry{
		{KeyHash: 2, RecordAddr: 0x2222, Slot: 2}, // overwrite key 2
		{KeyHash: 4, RecordAddr: 0x400, Slot: 4},  // insert key 4
		{KeyHash: 1, Flags: CkptTombstone},        // delete key 1
	})
	d2 := ckptPayload(CkptHeader{FullOrDelta: CkptDelta, CkptLogPos: 30, BaseCkptOff: 8192, SeqHigh: 7}, []CkptEntry{
		{KeyHash: 4, Flags: CkptTombstone}, // delete key 4
	})

	r := NewIndexRebuilder()
	for i, p := range [][]byte{full, d1, d2} {
		if _, err := r.Apply(p); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}

	if r.LogPos != 30 || r.SeqHigh != 7 {
		t.Fatalf("log pos %d seq high %d, want 30/7", r.LogPos, r.SeqHigh)
	}
	// Live: key 2 (overwritten), key 3 (from full); 1 and 4 tombstoned.
	want := map[uint64]uint64{2: 0x2222, 3: 0x300}
	if r.Len() != len(want) {
		t.Fatalf("index has %d live entries, want %d", r.Len(), len(want))
	}
	for kh, addr := range want {
		if e, ok := r.Entries()[kh]; !ok || e.RecordAddr != addr {
			t.Fatalf("key %d = %v/%v, want addr %#x", kh, e, ok, addr)
		}
	}
	if _, ok := r.Entries()[1]; ok {
		t.Fatalf("tombstoned key 1 is still live")
	}
	if _, ok := r.Entries()[4]; ok {
		t.Fatalf("tombstoned key 4 is still live")
	}
}

// TestIndexRebuilderFullResetsAccumulator applies a full over an already-populated
// index and confirms the earlier entries are gone: a full is the whole live set,
// not an addition.
func TestIndexRebuilderFullResetsAccumulator(t *testing.T) {
	r := NewIndexRebuilder()
	if _, err := r.Apply(ckptPayload(CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 1}, []CkptEntry{
		{KeyHash: 9, RecordAddr: 0x900},
	})); err != nil {
		t.Fatalf("first full: %v", err)
	}
	if _, err := r.Apply(ckptPayload(CkptHeader{FullOrDelta: CkptFull, CkptLogPos: 2}, []CkptEntry{
		{KeyHash: 7, RecordAddr: 0x700},
	})); err != nil {
		t.Fatalf("second full: %v", err)
	}
	if r.Len() != 1 {
		t.Fatalf("index has %d entries, want 1 (the second full replaced the first)", r.Len())
	}
	if _, ok := r.Entries()[9]; ok {
		t.Fatalf("key 9 from the first full survived a replacing full")
	}
}

// TestIndexRebuilderRejectsCorruptCheckpoint stops on a malformed payload rather
// than folding garbage into the index.
func TestIndexRebuilderRejectsCorruptCheckpoint(t *testing.T) {
	r := NewIndexRebuilder()
	// A header claiming more entries than the payload carries: CkptEntries bounds it.
	bad := AppendCkptHeader(nil, CkptHeader{FullOrDelta: CkptFull, EntryCount: 5})
	if _, err := r.Apply(bad); err != ErrLength {
		t.Fatalf("err = %v, want ErrLength", err)
	}
	if r.Len() != 0 {
		t.Fatalf("corrupt checkpoint left %d entries", r.Len())
	}
}
