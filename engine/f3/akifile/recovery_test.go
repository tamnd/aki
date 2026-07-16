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
