package akifile

import "testing"

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
