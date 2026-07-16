package akifile

import "testing"

// ssPayload frames a seg-stats payload from a header and its entries, mirroring
// ckptPayload for the checkpoint chain.
func ssPayload(h SegStatsHeader, entries []SegStatsEntry) []byte {
	h.EntryCount = uint64(len(entries))
	payload := AppendSegStatsHeader(nil, h)
	for _, e := range entries {
		payload = AppendSegStatsEntry(payload, e)
	}
	return payload
}

// appendSegStats frames a seg-stats payload and appends it as a seg_stats segment,
// returning the segment offset the base pointers reference.
func appendSegStats(t *testing.T, f *File, h SegStatsHeader, entries []SegStatsEntry) uint64 {
	t.Helper()
	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindSegStats, ShardSeq: 1, Payload: ssPayload(h, entries)}})
	if err != nil {
		t.Fatalf("append seg-stats: %v", err)
	}
	return offs[0]
}

// TestRebuildShardSegStatsChain folds a full table plus two deltas: a moved count, an
// inserted segment, and a compacted-away segment that the SegStatsFreed flag drops.
func TestRebuildShardSegStatsChain(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	fullOff := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsFull, CkptLogPos: 10}, []SegStatsEntry{
		{SegOff: 0x1000, LiveBytes: 900, DeadBytes: 100},
		{SegOff: 0x2000, LiveBytes: 800, DeadBytes: 200},
		{SegOff: 0x3000, LiveBytes: 700, DeadBytes: 300},
	})
	d1Off := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsDelta, CkptLogPos: 20, BaseCkptOff: fullOff}, []SegStatsEntry{
		{SegOff: 0x2000, LiveBytes: 500, DeadBytes: 500}, // more garbage accrued
		{SegOff: 0x1000, Flags: SegStatsFreed},           // compacted away
	})
	d2Off := appendSegStats(t, f, SegStatsHeader{FullOrDelta: SegStatsDelta, CkptLogPos: 30, BaseCkptOff: d1Off}, []SegStatsEntry{
		{SegOff: 0x4000, LiveBytes: 1000, DeadBytes: 0}, // a fresh segment
	})

	r, err := RebuildShardSegStats(dev, f.Prefix(), d2Off)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.LogPos != 30 {
		t.Fatalf("log pos %d, want 30", r.LogPos)
	}
	wantDead := map[uint64]uint64{0x2000: 500, 0x3000: 300, 0x4000: 0} // 0x1000 freed
	if r.Len() != len(wantDead) {
		t.Fatalf("table has %d entries, want %d", r.Len(), len(wantDead))
	}
	for seg, dead := range wantDead {
		if e, ok := r.Entries()[seg]; !ok || e.DeadBytes != dead {
			t.Fatalf("seg %#x = %v/%v, want dead %d", seg, e, ok, dead)
		}
	}
	if got := r.TotalDeadBytes(); got != 500+300+0 {
		t.Fatalf("total dead = %d, want 800", got)
	}
}

// TestRebuildShardSegStatsNoCheckpoint returns an empty table for a shard that has
// never checkpointed its accounting: the whole table comes from the tail replay.
func TestRebuildShardSegStatsNoCheckpoint(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	r, err := RebuildShardSegStats(dev, f.Prefix(), 0)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("empty table has %d entries, want 0", r.Len())
	}
	if r.TotalDeadBytes() != 0 {
		t.Fatalf("empty total dead = %d, want 0", r.TotalDeadBytes())
	}
}

// TestRebuildShardSegStatsRejectsNonSegStats refuses a root that points at a segment
// that is not a seg_stats.
func TestRebuildShardSegStatsRejectsNonSegStats(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("not seg-stats")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := RebuildShardSegStats(dev, f.Prefix(), offs[0]); err != ErrSegStats {
		t.Fatalf("err = %v, want ErrSegStats", err)
	}
}

// TestRebuildShardSegStatsRejectsCycle catches a back-pointer cycle: two delta
// segments whose bases point at each other never reach a base full.
func TestRebuildShardSegStatsRejectsCycle(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	offA := uint64(PageSize)
	offB := uint64(PageSize + SegmentAlign)
	writeSegStatsSegment(t, dev, prefix, offA, SegStatsHeader{FullOrDelta: SegStatsDelta, CkptLogPos: 20, BaseCkptOff: offB}, nil)
	writeSegStatsSegment(t, dev, prefix, offB, SegStatsHeader{FullOrDelta: SegStatsDelta, CkptLogPos: 10, BaseCkptOff: offA}, nil)

	if _, err := RebuildShardSegStats(dev, prefix, offA); err != ErrSegStats {
		t.Fatalf("err = %v, want ErrSegStats", err)
	}
}

// writeSegStatsSegment lays a validly-framed seg_stats segment at a chosen offset, so
// a test can hand-build a chain shape (like a cycle) the append writer cannot.
func writeSegStatsSegment(t *testing.T, dev *memDevice, prefix *Prefix, off uint64, h SegStatsHeader, entries []SegStatsEntry) {
	t.Helper()
	payload := ssPayload(h, entries)
	sh := &SegHeader{Shard: 0, Kind: KindSegStats, ShardSeq: 1}
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
