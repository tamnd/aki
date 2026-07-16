package akifile

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// memDevice is a sparse in-memory Device: it grows on WriteAt, reads back holes
// as zero, and counts Sync calls so a test can assert the fsync policy exactly.
type memDevice struct {
	buf   []byte
	syncs int
}

func (m *memDevice) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if off >= int64(len(m.buf)) {
		return 0, io.EOF
	}
	n := copy(p, m.buf[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memDevice) WriteAt(p []byte, off int64) (int, error) {
	if end := off + int64(len(p)); end > int64(len(m.buf)) {
		grown := make([]byte, end)
		copy(grown, m.buf)
		m.buf = grown
	}
	copy(m.buf[off:], p)
	return len(p), nil
}

func (m *memDevice) Sync() error { m.syncs++; return nil }

func (m *memDevice) Truncate(size int64) error {
	grown := make([]byte, size)
	copy(grown, m.buf)
	m.buf = grown
	return nil
}

func (m *memDevice) Size() (int64, error) { return int64(len(m.buf)), nil }

func (m *memDevice) Close() error { return nil }

// clock is an injectable monotonic time source for the everysec window test.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *clock                   { return &clock{t: time.Unix(1_700_000_000, 0)} }

func newTestFile(t *testing.T, dev *memDevice, sync SyncPolicy, now func() time.Time) *File {
	t.Helper()
	f, err := CreateOnDevice(dev, CreateOptions{ShardCount: 4, Sync: sync, Now: now})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return f
}

func TestAppendGroupAssignsSeqAndAlignedOffsets(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	group := []Pending{
		{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("first payload")},
		{Shard: 1, Kind: KindColdChunk, ShardSeq: 1, Payload: bytes.Repeat([]byte("x"), 5000)},
		{Shard: 0, Kind: KindLog, ShardSeq: 2, Payload: []byte("third")},
	}
	offs, err := f.AppendGroup(group)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(offs) != 3 {
		t.Fatalf("got %d offsets, want 3", len(offs))
	}
	if offs[0] != PageSize {
		t.Fatalf("first segment at %d, want past the header page %d", offs[0], PageSize)
	}
	for i, off := range offs {
		if off%SegmentAlign != 0 {
			t.Fatalf("segment %d offset %d not 4KiB aligned", i, off)
		}
	}
	// The 5000-byte payload spans two 4KiB units, so the third segment sits a
	// full SegmentSpan past the second, not one aligned unit.
	if want := offs[1] + SegmentSpan(5000); offs[2] != want {
		t.Fatalf("third segment at %d, want %d", offs[2], want)
	}
	if f.GlobalSeq() != 3 {
		t.Fatalf("global_seq = %d, want 3", f.GlobalSeq())
	}
	if f.Cursor() != offs[2]+SegmentSpan(5) {
		t.Fatalf("cursor = %d, want %d", f.Cursor(), offs[2]+SegmentSpan(5))
	}

	for i, off := range offs {
		h, payload, err := f.ReadSegmentAt(off)
		if err != nil {
			t.Fatalf("read segment %d: %v", i, err)
		}
		if h.GlobalSeq != uint64(i+1) {
			t.Fatalf("segment %d global_seq = %d, want %d", i, h.GlobalSeq, i+1)
		}
		if h.Shard != group[i].Shard || h.Kind != group[i].Kind || h.ShardSeq != group[i].ShardSeq {
			t.Fatalf("segment %d header mismatch: %+v", i, h)
		}
		if !bytes.Equal(payload, group[i].Payload) {
			t.Fatalf("segment %d payload mismatch", i)
		}
	}
}

func TestGroupCommitIssuesOneFsync(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncAlways, nil)
	base := dev.syncs // one fsync from create

	group := []Pending{
		{Kind: KindLog, Payload: []byte("a")},
		{Kind: KindLog, Payload: []byte("b")},
		{Kind: KindLog, Payload: []byte("c")},
		{Kind: KindLog, Payload: []byte("d")},
	}
	if _, err := f.AppendGroup(group); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 1 {
		t.Fatalf("a 4-segment group fsynced %d times, want 1", got)
	}
	if _, err := f.AppendGroup(group); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 2 {
		t.Fatalf("two groups fsynced %d times, want 2", got)
	}
}

func TestSyncNoNeverFlushesUntilExplicit(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	base := dev.syncs

	for i := 0; i < 5; i++ {
		if _, err := f.AppendGroup([]Pending{{Kind: KindLog, Payload: []byte("p")}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if got := dev.syncs - base; got != 0 {
		t.Fatalf("SyncNo flushed %d times on its own, want 0", got)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("explicit sync: %v", err)
	}
	if got := dev.syncs - base; got != 1 {
		t.Fatalf("explicit Sync gave %d flushes, want 1", got)
	}
}

func TestSyncEverySecFlushesOncePerWindow(t *testing.T) {
	dev := &memDevice{}
	c := newClock()
	f, err := CreateOnDevice(dev, CreateOptions{Sync: SyncEverySec, SyncInterval: time.Second, Now: c.now})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	base := dev.syncs
	one := []Pending{{Kind: KindLog, Payload: []byte("p")}}

	// Same window: no flush.
	if _, err := f.AppendGroup(one); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 0 {
		t.Fatalf("within the window flushed %d times, want 0", got)
	}
	// Window elapses: the next group flushes.
	c.advance(time.Second)
	if _, err := f.AppendGroup(one); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 1 {
		t.Fatalf("after one window flushed %d times, want 1", got)
	}
	// Back inside a fresh window: no flush.
	if _, err := f.AppendGroup(one); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 1 {
		t.Fatalf("second group in the window flushed %d times, want 1", got)
	}
	c.advance(time.Second)
	if _, err := f.AppendGroup(one); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := dev.syncs - base; got != 2 {
		t.Fatalf("after the second window flushed %d times, want 2", got)
	}
}

func TestAppendGroupEmptyIsNoOp(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncAlways, nil)
	before := f.Cursor()
	offs, err := f.AppendGroup(nil)
	if err != nil || offs != nil {
		t.Fatalf("empty append: offs=%v err=%v", offs, err)
	}
	if f.Cursor() != before {
		t.Fatal("empty append moved the cursor")
	}
}

func TestReadSegmentAtCatchesTornPayload(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	offs, err := f.AppendGroup([]Pending{{Kind: KindLog, Payload: []byte("the quick brown fox")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	dev.buf[offs[0]+SegHeaderLen+3] ^= 0x01 // flip a payload byte
	if _, _, err := f.ReadSegmentAt(offs[0]); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}
