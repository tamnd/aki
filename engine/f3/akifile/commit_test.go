package akifile

import "testing"

// TestCommitRootsFlipsSlot commits a new root and confirms the open sequence now
// picks the freshly written slot: the stale slot B takes the commit, its sequence
// advances, and it carries the roots and durable file size the writer stamped.
func TestCommitRootsFlipsSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if err := f.CommitRoots(MetaSlot{SRTOff: 0x40000, SRTLen: 80, SRTShardCount: prefix.ShardCount}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if f.CommitSeq() != 1 {
		t.Fatalf("commit seq = %d, want 1", f.CommitSeq())
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 1 {
		t.Fatalf("live slot = %d, want 1 (the flipped-to slot B)", st.Which)
	}
	if st.Meta.CommitSeq != 1 || st.Meta.SRTOff != 0x40000 {
		t.Fatalf("live root = seq %d srt %#x, want 1/0x40000", st.Meta.CommitSeq, st.Meta.SRTOff)
	}
	if st.Meta.FileSize != f.Cursor() {
		t.Fatalf("committed file size %d, want the durable cursor %d", st.Meta.FileSize, f.Cursor())
	}
	// No clean_shutdown was set, so a reopen would treat it as a crashed tail.
	if st.Outcome != OpenCrashed {
		t.Fatalf("outcome = %d, want OpenCrashed", st.Outcome)
	}
}

// TestCommitRootsAlternatesSlots confirms two commits take turns: the first flips
// to slot B, the second back to slot A, each with the next commit sequence.
func TestCommitRootsAlternatesSlots(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if f.CommitSeq() != 2 {
		t.Fatalf("commit seq = %d, want 2", f.CommitSeq())
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 2 {
		t.Fatalf("live = slot %d seq %d, want slot 0 seq 2", st.Which, st.Meta.CommitSeq)
	}
}

// TestCommitRootsSurvivesTornSlot tears the live slot after a commit and confirms
// the previous root, in the other slot, is still picked: the dual-slot flip keeps
// the last good root through a torn write.
func TestCommitRootsSurvivesTornSlot(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("data")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1, SRTOff: 0x50000}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The commit went to slot B; tearing it must not lose the file: slot A still
	// holds the create-time root at commit_seq 0.
	dev.buf[prefix.MetaSlotBOff+3] ^= 0xff

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 0 {
		t.Fatalf("fell back to slot %d seq %d, want slot 0 seq 0 (the surviving root)", st.Which, st.Meta.CommitSeq)
	}
}

// TestCheckpointRoundTrips writes roots to free space and commits them, then
// recovers the file and reads back the shard root table, the extent map, and the
// accounting: a clean checkpoint the open sequence trusts without a replay.
func TestCheckpointRoundTrips(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	if _, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("data")}}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// IndexCkptOff stays zero: these shards have no checkpoint chain, so recovery
	// leaves each index empty. The checkpoint's own round trip is what is under test.
	rows := make([]SRTRow, prefix.ShardCount)
	for i := range rows {
		rows[i] = SRTRow{FirstTailSeg: uint64(0x20000 * (i + 1))}
	}
	extents := []Extent{{Kind: ExtentHeader, StartOff: 0, Length: PageSize}}

	if err := f.Checkpoint(&SRT{Gen: 3, Rows: rows}, extents, CheckpointStats{RecordCount: 7, Clean: true}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rec.State.Outcome != OpenClean {
		t.Fatalf("outcome = %d, want OpenClean", rec.State.Outcome)
	}
	if rec.State.Meta.RecordCount != 7 {
		t.Fatalf("record count = %d, want 7", rec.State.Meta.RecordCount)
	}
	if rec.SRT == nil || rec.SRT.Gen != 3 || len(rec.SRT.Rows) != int(prefix.ShardCount) {
		t.Fatalf("recovered SRT = %+v, want gen 3 with %d rows", rec.SRT, prefix.ShardCount)
	}
	if rec.SRT.Rows[1].FirstTailSeg != rows[1].FirstTailSeg {
		t.Fatalf("SRT row 1 tail seg = %#x, want %#x", rec.SRT.Rows[1].FirstTailSeg, rows[1].FirstTailSeg)
	}

	es, err := ReadExtentTable(dev, rec.State.Meta)
	if err != nil {
		t.Fatalf("read extents: %v", err)
	}
	if len(es) != 1 || es[0] != extents[0] {
		t.Fatalf("recovered extents = %+v, want %+v", es, extents)
	}
}

// TestCheckpointGlobalsRoundTrip writes the file-global roots to free space, stamps their
// offsets into the commit, and recovers the file: the TTL index, free map, and meta_kv
// read back off the one live root alongside the SRT, the round trip a hand-built meta slot
// only stood in for until now.
func TestCheckpointGlobalsRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	// The allocator's free map lands as a self-describing segment; the meta slot names
	// its header offset.
	fmOff := appendFreeMap(t, f, []FreeExtent{
		{StartOff: 0x10000, Length: 0x4000},
		{StartOff: 0x20000, Length: 0x1000, Flags: FreeMapPending},
	})
	// The config and import path's meta_kv also lands as a self-describing segment.
	kvOff := appendMetaKV(t, f, []MetaKVPair{
		{Key: []byte("import.source"), Value: []byte("RDB v12")},
		{Key: []byte("config.maxmemory"), Value: []byte("512mb")},
	})
	// Active expiry's TTL index rides the grid like the SRT; the meta slot names its
	// payload offset and length for a bare read.
	ttlBytes := encodeTTLIndex([]TTLClass{
		{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000, 0x2000}},
		{Class: 2, ExpiryUpperUnix: 2000, Segments: []uint64{0x3000}},
	})
	ttlOffs, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindTTLIndex, Payload: ttlBytes}})
	if err != nil {
		t.Fatalf("append ttl index: %v", err)
	}

	rows := make([]SRTRow, prefix.ShardCount)
	globals := CheckpointGlobals{
		TTLIndexOff: ttlOffs[0] + SegHeaderLen,
		TTLIndexLen: uint32(len(ttlBytes)),
		FreeMapOff:  fmOff,
		MetaKVOff:   kvOff,
	}
	if err := f.CheckpointWithGlobals(&SRT{Gen: 4, Rows: rows}, nil, CheckpointStats{Clean: true}, globals); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rec, err := Recover(dev)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.TTLClasses) != 2 {
		t.Fatalf("recovered ttl = %+v, want 2 classes", rec.TTLClasses)
	}
	if len(rec.FreeMap) != 2 {
		t.Fatalf("recovered free map = %+v, want 2 runs", rec.FreeMap)
	}
	free, pending := FreeMapTotals(rec.FreeMap)
	if free != 0x4000 || pending != 0x1000 {
		t.Fatalf("free map totals = free %d / pending %d, want %d/%d", free, pending, 0x4000, 0x1000)
	}
	if len(rec.MetaKV) != 2 {
		t.Fatalf("recovered meta kv = %+v, want 2 pairs", rec.MetaKV)
	}
	if v, ok := MetaKVLookup(rec.MetaKV, "import.source"); !ok || string(v) != "RDB v12" {
		t.Fatalf("recovered import.source = %q/%v, want RDB v12", v, ok)
	}
}

// TestCheckpointLeavesGlobalsUnstamped confirms a plain Checkpoint names no global
// roots: a file with no TTL index, free map, or meta_kv commits with every pointer zero,
// so a reader reports none of them.
func TestCheckpointLeavesGlobalsUnstamped(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.Checkpoint(&SRT{Gen: 1, Rows: rows}, nil, CheckpointStats{Clean: true}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Meta.TTLIndexOff != 0 || st.Meta.TTLIndexLen != 0 || st.Meta.FreeMapOff != 0 || st.Meta.MetaKVOff != 0 {
		t.Fatalf("globals = ttl %#x/%d free %#x meta_kv %#x, want all zero",
			st.Meta.TTLIndexOff, st.Meta.TTLIndexLen, st.Meta.FreeMapOff, st.Meta.MetaKVOff)
	}
}

// TestCheckpointRootsRideTheGrid confirms the roots land as segments a grid walk
// counts and skips: the shard root table and extent map are owner-less segments,
// so the durable tail sits past them, not on them.
func TestCheckpointRootsRideTheGrid(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.Checkpoint(&SRT{Gen: 1, Rows: rows}, []Extent{{Kind: ExtentAppend, StartOff: PageSize, Length: 0x1000}}, CheckpointStats{}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	size, _ := dev.Size()
	tally, err := ScanSegments(dev, prefix, PageSize, uint64(size))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if tally.ByKind[KindSRT] != 1 || tally.ByKind[KindExtentTable] != 1 {
		t.Fatalf("tally = %v, want 1 SRT and 1 extent-table segment", tally.ByKind)
	}
	if tally.DurableTail != f.Cursor() {
		t.Fatalf("durable tail = %d, want past the root segments at %d", tally.DurableTail, f.Cursor())
	}
}

// TestCheckpointWithoutExtents omits the extent map and confirms the root commits
// with no extent pointer: a checkpoint does not have to carry a shape hint.
func TestCheckpointWithoutExtents(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	rows := make([]SRTRow, prefix.ShardCount)
	if err := f.Checkpoint(&SRT{Gen: 1, Rows: rows}, nil, CheckpointStats{Clean: true}); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Meta.ExtentTableLen != 0 {
		t.Fatalf("extent len = %d, want 0 (no extents committed)", st.Meta.ExtentTableLen)
	}
	es, err := ReadExtentTable(dev, st.Meta)
	if err != nil || es != nil {
		t.Fatalf("extents = %v/%v, want nil/nil", es, err)
	}
}

// TestOpenReseedsCommitSeq reopens a committed file and confirms the next commit
// continues the sequence: Open reads the live root's commit_seq so a flip does not
// resurrect an old slot.
func TestOpenReseedsCommitSeq(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	if err := f.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Reopen: the writer must resume with commit_seq 1 live in slot B, so the next
	// commit is seq 2 into slot A.
	g, err := OpenOnDevice(dev, OpenOptions{Sync: SyncNo})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g.CommitSeq() != 1 {
		t.Fatalf("reopened commit seq = %d, want 1", g.CommitSeq())
	}
	if err := g.CommitRoots(MetaSlot{CleanShutdown: 1}); err != nil {
		t.Fatalf("post-reopen commit: %v", err)
	}

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if st.Which != 0 || st.Meta.CommitSeq != 2 {
		t.Fatalf("live = slot %d seq %d, want slot 0 seq 2", st.Which, st.Meta.CommitSeq)
	}
}
