package akifile

import "testing"

// freeMapPayload frames a free-map payload from its runs.
func freeMapPayload(runs []FreeExtent) []byte {
	payload := AppendFreeMapHeader(nil, FreeMapHeader{EntryCount: uint64(len(runs))})
	for _, r := range runs {
		payload = AppendFreeExtent(payload, r)
	}
	return payload
}

// appendFreeMap frames a free-map payload and appends it as a free_map segment,
// returning the segment offset the meta root points at.
func appendFreeMap(t *testing.T, f *File, runs []FreeExtent) uint64 {
	t.Helper()
	offs, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindFreeMap, ShardSeq: 1, Payload: freeMapPayload(runs)}})
	if err != nil {
		t.Fatalf("append free map: %v", err)
	}
	return offs[0]
}

// TestReadFreeMapFresh returns a nil map for a fresh file: no free map has been
// written, so the allocator has no reclaimed runs and only grows the file.
func TestReadFreeMapFresh(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	runs, err := ReadFreeMap(dev, f.Prefix(), st.Meta)
	if err != nil || runs != nil {
		t.Fatalf("fresh free map = %v/%v, want nil/nil", runs, err)
	}
}

// TestReadFreeMapRoundTrip appends a free_map segment, points the live meta root at it,
// and reads back every run, then confirms the free/pending split the allocator reads.
func TestReadFreeMapRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	want := []FreeExtent{
		{StartOff: 0x10000, Length: 0x2000},
		{StartOff: 0x20000, Length: 0x1000, Flags: FreeMapPending},
		{StartOff: 0x30000, Length: 0x8000},
	}
	fmOff := appendFreeMap(t, f, want)

	m := &MetaSlot{
		CommitSeq: 1, FileSize: f.Cursor(), CleanShutdown: 1,
		FreeMapOff: fmOff,
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	runs, err := ReadFreeMap(dev, prefix, st.Meta)
	if err != nil {
		t.Fatalf("read free map: %v", err)
	}
	if len(runs) != len(want) {
		t.Fatalf("read %d runs, want %d", len(runs), len(want))
	}
	for i := range want {
		if runs[i] != want[i] {
			t.Fatalf("run %d = %+v, want %+v", i, runs[i], want[i])
		}
	}
	free, pending := FreeMapTotals(runs)
	if free != 0x2000+0x8000 || pending != 0x1000 {
		t.Fatalf("totals = free %d / pending %d, want %d/%d", free, pending, 0x2000+0x8000, 0x1000)
	}
}

// TestReadFreeMapTornPayload catches a torn free-map segment through its payload CRC,
// the integrity the bare roots forgo.
func TestReadFreeMapTornPayload(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	fmOff := appendFreeMap(t, f, []FreeExtent{{StartOff: 0x10000, Length: 0x2000}})
	dev.buf[fmOff+SegHeaderLen+2] ^= 0xff // corrupt a payload byte past the header

	m := &MetaSlot{FreeMapOff: fmOff}
	if _, err := ReadFreeMap(dev, prefix, m); err != ErrChecksum {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// TestReadFreeMapWrongKind refuses a meta pointer that names a segment that is not a
// free_map: a misdirected or corrupt root.
func TestReadFreeMapWrongKind(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("not a free map")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	m := &MetaSlot{FreeMapOff: offs[0]}
	if _, err := ReadFreeMap(dev, prefix, m); err != ErrMagic {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}
