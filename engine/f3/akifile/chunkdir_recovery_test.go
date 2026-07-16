package akifile

import "testing"

// cdPayload frames a chunk-directory payload from a header and its collections,
// mirroring ssPayload for the seg-stats chain.
func cdPayload(h ChunkDirHeader, cols []ChunkDirCollection) []byte {
	h.CollectionCount = uint64(len(cols))
	payload := AppendChunkDirHeader(nil, h)
	for _, c := range cols {
		payload = AppendChunkDirCollectionHeader(payload, c.KeyHash, uint32(len(c.Chunks)), c.Flags)
		for _, row := range c.Chunks {
			payload = AppendChunkDirRow(payload, row)
		}
	}
	return payload
}

// appendChunkDir frames a chunk-directory payload and appends it as a chunk_dir
// segment, returning the segment offset the base pointers reference.
func appendChunkDir(t *testing.T, f *File, h ChunkDirHeader, cols []ChunkDirCollection) uint64 {
	t.Helper()
	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindChunkDir, ShardSeq: 1, Payload: cdPayload(h, cols)}})
	if err != nil {
		t.Fatalf("append chunk-dir: %v", err)
	}
	return offs[0]
}

// TestRebuildShardChunkDirChain folds a full directory plus two deltas: a moved
// collection whose chunk live bytes shrank, a fresh collection, and a collection the
// ChunkDirTombstone flag drops after it was promoted back to the hot tier.
func TestRebuildShardChunkDirChain(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	fullOff := appendChunkDir(t, f, ChunkDirHeader{FullOrDelta: ChunkDirFull, CkptLogPos: 10}, []ChunkDirCollection{
		{KeyHash: 0xA1, Chunks: []ChunkDirRow{
			{FirstDisc: []byte("aaa"), ElementCount: 100, ChunkOff: 0x1000, ChunkLiveBytes: 900},
		}},
		{KeyHash: 0xB2, Chunks: []ChunkDirRow{
			{FirstDisc: []byte("mmm"), ElementCount: 50, ChunkOff: 0x2000, ChunkLiveBytes: 400},
		}},
		{KeyHash: 0xC3, Chunks: []ChunkDirRow{
			{FirstDisc: []byte("zzz"), ElementCount: 20, ChunkOff: 0x3000, ChunkLiveBytes: 200},
		}},
	})
	d1Off := appendChunkDir(t, f, ChunkDirHeader{FullOrDelta: ChunkDirDelta, CkptLogPos: 20, BaseCkptOff: fullOff}, []ChunkDirCollection{
		{KeyHash: 0xB2, Chunks: []ChunkDirRow{ // cold deletes shed live bytes
			{FirstDisc: []byte("mmm"), ElementCount: 40, ChunkOff: 0x2000, ChunkLiveBytes: 250},
		}},
		{KeyHash: 0xA1, Flags: ChunkDirTombstone}, // promoted back to the hot tier
	})
	d2Off := appendChunkDir(t, f, ChunkDirHeader{FullOrDelta: ChunkDirDelta, CkptLogPos: 30, BaseCkptOff: d1Off}, []ChunkDirCollection{
		{KeyHash: 0xD4, Chunks: []ChunkDirRow{ // a fresh cold collection
			{FirstDisc: []byte("kkk"), ElementCount: 10, ChunkOff: 0x4000, ChunkLiveBytes: 100},
		}},
	})

	r, err := RebuildShardChunkDir(dev, f.Prefix(), d2Off)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.LogPos != 30 {
		t.Fatalf("log pos %d, want 30", r.LogPos)
	}
	wantLive := map[uint64]uint64{0xB2: 250, 0xC3: 200, 0xD4: 100} // 0xA1 tombstoned
	if r.Len() != len(wantLive) {
		t.Fatalf("directory has %d collections, want %d", r.Len(), len(wantLive))
	}
	for key, live := range wantLive {
		c, ok := r.Collections()[key]
		if !ok {
			t.Fatalf("collection %#x missing", key)
		}
		if len(c.Chunks) != 1 || c.Chunks[0].ChunkLiveBytes != live {
			t.Fatalf("collection %#x = %v, want one chunk with live %d", key, c.Chunks, live)
		}
	}
	if got := r.TotalLiveBytes(); got != 250+200+100 {
		t.Fatalf("total live = %d, want 550", got)
	}
}

// TestRebuildShardChunkDirNoCheckpoint returns an empty directory for a shard that has
// no cold chunks checkpointed: any cold directory comes from the tail replay.
func TestRebuildShardChunkDirNoCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	r, err := RebuildShardChunkDir(dev, f.Prefix(), 0)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("empty directory has %d collections, want 0", r.Len())
	}
	if r.TotalLiveBytes() != 0 {
		t.Fatalf("empty total live = %d, want 0", r.TotalLiveBytes())
	}
}

// TestRebuildShardChunkDirRejectsNonChunkDir refuses a root that points at a segment
// that is not a chunk_dir.
func TestRebuildShardChunkDirRejectsNonChunkDir(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("not chunk-dir")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := RebuildShardChunkDir(dev, f.Prefix(), offs[0]); err != ErrChunkDir {
		t.Fatalf("err = %v, want ErrChunkDir", err)
	}
}

// TestRebuildShardChunkDirRejectsCycle catches a back-pointer cycle: two delta segments
// whose bases point at each other never reach a base full.
func TestRebuildShardChunkDirRejectsCycle(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	offA := uint64(PageSize)
	offB := uint64(PageSize + SegmentAlign)
	writeChunkDirSegment(t, dev, prefix, offA, ChunkDirHeader{FullOrDelta: ChunkDirDelta, CkptLogPos: 20, BaseCkptOff: offB}, nil)
	writeChunkDirSegment(t, dev, prefix, offB, ChunkDirHeader{FullOrDelta: ChunkDirDelta, CkptLogPos: 10, BaseCkptOff: offA}, nil)

	if _, err := RebuildShardChunkDir(dev, prefix, offA); err != ErrChunkDir {
		t.Fatalf("err = %v, want ErrChunkDir", err)
	}
}

// writeChunkDirSegment lays a validly-framed chunk_dir segment at a chosen offset, so a
// test can hand-build a chain shape (like a cycle) the append writer cannot.
func writeChunkDirSegment(t *testing.T, dev *memDevice, prefix *Prefix, off uint64, h ChunkDirHeader, cols []ChunkDirCollection) {
	t.Helper()
	payload := cdPayload(h, cols)
	sh := &SegHeader{Shard: 0, Kind: KindChunkDir, ShardSeq: 1}
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
