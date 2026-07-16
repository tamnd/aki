package akifile

import (
	"bytes"
	"strings"
	"testing"
)

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

// TestFindingsCleanFile reports no findings for a fresh file: both slots valid and
// no root torn, so a verify pass would exit clean.
func TestFindingsCleanFile(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if fs := rep.Findings(); len(fs) != 0 {
		t.Fatalf("findings = %v, want none on a clean file", fs)
	}
}

// TestFindingsTornSlot flags the torn slot by name while the file still has a live
// root: a verify pass reports the incomplete commit but the file is recoverable.
func TestFindingsTornSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff // tear slot B's body

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	fs := rep.Findings()
	if len(fs) != 1 || !strings.Contains(fs[0], "meta slot B") {
		t.Fatalf("findings = %v, want one naming slot B", fs)
	}
}

// TestFindingsBothSlotsTorn reports the no-trusted-root case: both slots tore, so a
// verify pass flags the fall back to a full segment scan on top of each slot.
func TestFindingsBothSlotsTorn(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()
	dev.buf[prefix.MetaSlotAOff+3] ^= 0xff
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	fs := rep.Findings()
	if len(fs) != 3 {
		t.Fatalf("findings = %v, want both slots plus the no-trusted-root note", fs)
	}
	if !strings.Contains(fs[2], "no trusted meta slot") {
		t.Fatalf("findings = %v, want the no-trusted-root finding last", fs)
	}
}

// TestFindingsTornRoot flags a torn shard root table: the slot is valid but the SRT
// it names did not read, so a verify pass reports the unreadable root.
func TestFindingsTornRoot(t *testing.T) {
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
		t.Fatalf("inspect: %v", err)
	}
	fs := rep.Findings()
	if len(fs) != 1 || !strings.Contains(fs[0], "shard root table") {
		t.Fatalf("findings = %v, want one for the torn root", fs)
	}
}

// TestWriteReportCleanFile prints a fresh file: the format header, both slots valid
// with slot A live, no roots yet, and no findings.
func TestWriteReportCleanFile(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	var buf bytes.Buffer
	WriteReport(&buf, rep)
	out := buf.String()

	for _, want := range []string{
		"format: aki store v3.0", "checksum crc32c",
		"meta slot A", "meta slot B", "(live)",
		"shard root table: none", "extent map: none", "findings: none",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q in:\n%s", want, out)
		}
	}
}

// TestWriteReportWithRoots prints a checkpointed file: the roots the live slot names
// and the segment population read back by name.
func TestWriteReportWithRoots(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("data")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	rows := make([]SRTRow, prefix.ShardCount)
	extents := []Extent{{Kind: ExtentHeader, StartOff: 0, Length: PageSize}}
	if err := f.Checkpoint(&SRT{Gen: 5, Rows: rows}, extents, CheckpointStats{Clean: true}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	var buf bytes.Buffer
	WriteReport(&buf, rep)
	out := buf.String()

	for _, want := range []string{
		"shard root table: gen 5,", "extent map: 1 extents",
		"log ", "srt ", "extent_table ", "findings: none",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report missing %q in:\n%s", want, out)
		}
	}
}

// TestWriteReportShowsFindings prints a torn slot as a finding: the slot line reads
// torn and the findings section counts it.
func TestWriteReportShowsFindings(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff // tear slot B

	rep, err := Inspect(dev)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	var buf bytes.Buffer
	WriteReport(&buf, rep)
	out := buf.String()

	if !strings.Contains(out, "meta slot B @") || !strings.Contains(out, "torn:") {
		t.Fatalf("report missing the torn slot line:\n%s", out)
	}
	if !strings.Contains(out, "findings: 1") || !strings.Contains(out, "meta slot B did not validate") {
		t.Fatalf("report missing the finding:\n%s", out)
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
