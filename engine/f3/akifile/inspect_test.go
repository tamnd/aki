package akifile

import "testing"

// TestInspectFreshFile reports a fresh file: both slots valid and clean, slot A
// live, no roots yet, and an empty segment population.
func TestInspectFreshFile(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !rep.Slots[0].Valid || !rep.Slots[1].Valid {
		t.Fatalf("slots = %+v, want both valid", rep.Slots)
	}
	if !rep.Slots[0].CleanShutdown || rep.Live != 0 {
		t.Fatalf("live = %d clean = %v, want slot 0 clean", rep.Live, rep.Slots[0].CleanShutdown)
	}
	if rep.SRT != nil || rep.Extents != nil || rep.SRTErr != nil || rep.ExtErr != nil {
		t.Fatalf("fresh roots = srt %v/%v ext %v/%v, want all nil", rep.SRT, rep.SRTErr, rep.Extents, rep.ExtErr)
	}
	if rep.Segments.Total != 0 || rep.Segments.DurableTail != PageSize {
		t.Fatalf("segments = %+v, want empty with tail at the header page", rep.Segments)
	}
}

// TestInspectReportsTornSlot judges each slot on its own: a torn slot B is flagged
// while slot A stays the live root, and the segment population is still tallied.
func TestInspectReportsTornSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("live")},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff // tear slot B's body

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if !rep.Slots[0].Valid {
		t.Fatalf("slot A = %+v, want valid", rep.Slots[0])
	}
	if rep.Slots[1].Valid || rep.Slots[1].Err != ErrChecksum {
		t.Fatalf("slot B = %+v, want invalid with ErrChecksum", rep.Slots[1])
	}
	if rep.Live != 0 {
		t.Fatalf("live = %d, want slot 0 (the intact slot)", rep.Live)
	}
	if rep.Segments.Total != 1 || rep.Segments.ByKind[KindLog] != 1 {
		t.Fatalf("segments = %+v, want 1 log tallied past a torn slot", rep.Segments)
	}
}

// TestInspectRecordsTornRoot surfaces a torn SRT as a finding rather than failing:
// the report still carries the prefix, slots, and segment population.
func TestInspectRecordsTornRoot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	srtOff := writeSRT(t, dev, prefix, &SRT{Gen: 1, Rows: rows})
	dev.buf[srtOff+SRTHeaderLen+3] ^= 0xff // corrupt a row byte

	m := &MetaSlot{
		CommitSeq: 2, FileSize: PageSize, CleanShutdown: 1,
		SRTOff: srtOff, SRTLen: uint32(SRTHeaderLen + len(rows)*SRTRowSize), SRTShardCount: prefix.ShardCount,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect should not fail on a torn root: %v", err)
	}
	if rep.SRTErr != ErrChecksum || rep.SRT != nil {
		t.Fatalf("srt = %v/%v, want nil with ErrChecksum finding", rep.SRT, rep.SRTErr)
	}
	if rep.Prefix == nil || rep.Live != 0 {
		t.Fatalf("report dropped the prefix or live root on a torn SRT")
	}
}

// TestScanSegmentsTalliesByKind appends a mixed group and confirms the walk counts
// every intact segment by kind and reports the durable tail at the append cursor.
func TestScanSegmentsTalliesByKind(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	_, err := f.AppendGroup([]Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("log-a")},
		{Shard: 0, Kind: KindIndexCkpt, ShardSeq: 2, Payload: []byte("ckpt")},
		{Shard: 1, Kind: KindLog, ShardSeq: 1, Payload: []byte("log-b")},
		{Shard: 0, Kind: KindValueLog, ShardSeq: 3, Payload: []byte("value")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	size, _ := dev.Size()
	tally, err := ScanSegments(dev, f.Prefix(), PageSize, uint64(size))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if tally.Total != 4 {
		t.Fatalf("total = %d, want 4", tally.Total)
	}
	if tally.ByKind[KindLog] != 2 || tally.ByKind[KindIndexCkpt] != 1 || tally.ByKind[KindValueLog] != 1 {
		t.Fatalf("by kind = %v, want 2 log / 1 ckpt / 1 value", tally.ByKind)
	}
	if tally.DurableTail != f.Cursor() {
		t.Fatalf("durable tail = %d, want the append cursor %d", tally.DurableTail, f.Cursor())
	}
}

// TestScanSegmentsStopsAtTornTail counts only the segments before a torn one and
// reports the durable tail at the torn segment: what a tool shows as intact is
// exactly what recovery would keep.
func TestScanSegmentsStopsAtTornTail(t *testing.T) {
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

	size, _ := dev.Size()
	tally, err := ScanSegments(dev, f.Prefix(), PageSize, uint64(size))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if tally.Total != 1 || tally.ByKind[KindLog] != 1 {
		t.Fatalf("tally = %+v, want 1 log before the torn tail", tally)
	}
	if tally.DurableTail != offs[1] {
		t.Fatalf("durable tail = %d, want the torn segment offset %d", tally.DurableTail, offs[1])
	}
}

// TestScanSegmentsEmptyFile tallies nothing on a fresh file and reports the durable
// tail at the header page: the whole append space is empty.
func TestScanSegmentsEmptyFile(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	size, _ := dev.Size()
	tally, err := ScanSegments(dev, f.Prefix(), PageSize, uint64(size))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if tally.Total != 0 || len(tally.ByKind) != 0 {
		t.Fatalf("tally = %+v, want empty", tally)
	}
	if tally.DurableTail != PageSize {
		t.Fatalf("durable tail = %d, want the header page %d", tally.DurableTail, PageSize)
	}
}
