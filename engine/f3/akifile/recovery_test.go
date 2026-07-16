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
