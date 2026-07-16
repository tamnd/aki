package akifile

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestCreateWritesValidHeaderPage(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 8, SepThreshold: 64})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if size, _ := dev.Size(); size != PageSize {
		t.Fatalf("fresh file is %d bytes, want the header page %d", size, PageSize)
	}
	if dev.syncs != 1 {
		t.Fatalf("create fsynced %d times, want 1", dev.syncs)
	}
	p, err := ParsePrefix(dev.buf[:PrefixSize])
	if err != nil {
		t.Fatalf("prefix does not parse: %v", err)
	}
	if p.ShardCount != 8 || p.SepThreshold != 64 {
		t.Fatalf("prefix identity mismatch: %+v", p)
	}
	a := dev.buf[p.MetaSlotAOff : p.MetaSlotAOff+MetaSlotSize]
	b := dev.buf[p.MetaSlotBOff : p.MetaSlotBOff+MetaSlotSize]
	live, which, err := MetaLive(a, b, p.ChecksumKind)
	if err != nil {
		t.Fatalf("neither meta slot is valid: %v", err)
	}
	if which != 0 {
		t.Fatalf("live slot = %d, want A (0) on a fresh file", which)
	}
	if live.FileSize != PageSize || live.CleanShutdown != 1 {
		t.Fatalf("fresh meta = %+v, want file_size=%d clean=1", live, PageSize)
	}
	if f.Cursor() != PageSize || f.GlobalSeq() != 0 {
		t.Fatalf("fresh file cursor=%d seq=%d, want %d and 0", f.Cursor(), f.GlobalSeq(), PageSize)
	}
}

func TestReopenResumesTailAndSeq(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 2, Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	payloads := [][]byte{[]byte("alpha"), bytes.Repeat([]byte("y"), 9000), []byte("gamma")}
	for _, p := range payloads {
		if _, err := f.AppendGroup([]Pending{{Kind: KindLog, Payload: p}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	wantCursor, wantSeq := f.Cursor(), f.GlobalSeq()

	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g.Cursor() != wantCursor {
		t.Fatalf("reopened cursor = %d, want %d", g.Cursor(), wantCursor)
	}
	if g.GlobalSeq() != wantSeq {
		t.Fatalf("reopened global_seq = %d, want %d", g.GlobalSeq(), wantSeq)
	}
	// The resumed writer appends past the recovered tail without clobbering.
	offs, err := g.AppendGroup([]Pending{{Kind: KindLog, ShardSeq: 4, Payload: []byte("delta")}})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if offs[0] != wantCursor {
		t.Fatalf("resumed append landed at %d, want the recovered tail %d", offs[0], wantCursor)
	}
	if g.GlobalSeq() != wantSeq+1 {
		t.Fatalf("global_seq did not resume: %d", g.GlobalSeq())
	}
	h, got, err := g.ReadSegmentAt(offs[0])
	if err != nil {
		t.Fatalf("read resumed segment: %v", err)
	}
	if h.GlobalSeq != wantSeq+1 || !bytes.Equal(got, []byte("delta")) {
		t.Fatalf("resumed segment wrong: seq=%d payload=%q", h.GlobalSeq, got)
	}
}

func TestScanTailStopsAtTornSegment(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	offs, err := f.AppendGroup([]Pending{
		{Kind: KindLog, Payload: []byte("one")},
		{Kind: KindLog, Payload: []byte("two")},
		{Kind: KindLog, Payload: []byte("three")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	// Tear the middle segment's payload. A forward scan must stop before it and
	// treat everything from there on as never durable.
	dev.buf[offs[1]+SegHeaderLen] ^= 0xFF

	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g.Cursor() != offs[1] {
		t.Fatalf("scan stopped at %d, want the torn segment offset %d", g.Cursor(), offs[1])
	}
	if g.GlobalSeq() != 1 {
		t.Fatalf("global_seq after torn scan = %d, want 1 (only the first segment survived)", g.GlobalSeq())
	}
}

func TestScanTailStopsAtZeroTail(t *testing.T) {
	dev := &memDevice{}
	f, err := CreateOnDevice(dev, CreateOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.AppendGroup([]Pending{{Kind: KindLog, Payload: []byte("only")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	tail := f.Cursor()
	// Extend the file with a zeroed region past the tail, as a preallocated file
	// would look. The zero header there is not a segment; the scan must stop.
	if err := dev.Truncate(int64(tail) + 3*SegmentAlign); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g.Cursor() != tail {
		t.Fatalf("scan ran into the zero tail: cursor %d, want %d", g.Cursor(), tail)
	}
}

// TestCreateOpenRoundTripOnDisk exercises the real *os.File path end to end so
// the osDevice adapter and O_EXCL create are covered, not just the memory device.
func TestCreateOpenRoundTripOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "round.aki")
	f, err := Create(path, CreateOptions{ShardCount: 3, Sync: SyncAlways})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.AppendGroup([]Pending{{Kind: KindLog, Payload: []byte("durable")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	cursor, seq := f.Cursor(), f.GlobalSeq()
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A second create on the same path must fail (O_EXCL).
	if _, err := Create(path, CreateOptions{}); err == nil {
		t.Fatal("create over an existing file succeeded, want O_EXCL failure")
	}

	g, err := Open(path, OpenOptions{Sync: SyncAlways})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer g.Close()
	if g.Cursor() != cursor || g.GlobalSeq() != seq {
		t.Fatalf("reopened cursor/seq = %d/%d, want %d/%d", g.Cursor(), g.GlobalSeq(), cursor, seq)
	}
	_, payload, err := g.ReadSegmentAt(PageSize)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(payload, []byte("durable")) {
		t.Fatalf("payload = %q, want durable", payload)
	}
}
