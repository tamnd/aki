package akifile

import "testing"

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
