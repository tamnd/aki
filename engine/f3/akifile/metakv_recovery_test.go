package akifile

import (
	"bytes"
	"testing"
)

// appendMetaKV frames a meta_kv payload and appends it as a meta_kv segment, returning
// the segment offset the meta root points at.
func appendMetaKV(t *testing.T, f *File, pairs []MetaKVPair) uint64 {
	t.Helper()
	offs, err := f.AppendGroup([]Pending{{Shard: ShardOwnerless, Kind: KindMetaKV, ShardSeq: 1, Payload: encodeMetaKV(pairs)}})
	if err != nil {
		t.Fatalf("append meta kv: %v", err)
	}
	return offs[0]
}

// TestReadMetaKVFresh returns a nil map for a fresh file: no provenance has been
// stamped, so file-info has nothing to report and recovery reads no config.
func TestReadMetaKVFresh(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	pairs, err := ReadMetaKV(dev, f.Prefix(), st.Meta)
	if err != nil || pairs != nil {
		t.Fatalf("fresh meta kv = %v/%v, want nil/nil", pairs, err)
	}
}

// TestReadMetaKVRoundTrip appends a meta_kv segment, points the live meta root at it,
// and reads back every pair, then looks one up the way file-info does.
func TestReadMetaKVRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	want := []MetaKVPair{
		{Key: []byte("import.source"), Value: []byte("RDB v12")},
		{Key: []byte("config.maxmemory"), Value: []byte("512mb")},
		{Key: []byte("note"), Value: []byte("")},
	}
	kvOff := appendMetaKV(t, f, want)

	m := &MetaSlot{CommitSeq: 1, FileSize: f.Cursor(), CleanShutdown: 1, MetaKVOff: kvOff}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	pairs, err := ReadMetaKV(dev, prefix, st.Meta)
	if err != nil {
		t.Fatalf("read meta kv: %v", err)
	}
	if len(pairs) != len(want) {
		t.Fatalf("read %d pairs, want %d", len(pairs), len(want))
	}
	for i := range want {
		if !bytes.Equal(pairs[i].Key, want[i].Key) || !bytes.Equal(pairs[i].Value, want[i].Value) {
			t.Fatalf("pair %d = %q/%q, want %q/%q", i, pairs[i].Key, pairs[i].Value, want[i].Key, want[i].Value)
		}
	}
	if v, ok := MetaKVLookup(pairs, "config.maxmemory"); !ok || string(v) != "512mb" {
		t.Fatalf("lookup config.maxmemory = %q/%v, want 512mb", v, ok)
	}
}

// TestReadMetaKVTornPayload catches a torn meta_kv segment through its payload CRC, the
// integrity a self-describing root buys over a bare one.
func TestReadMetaKVTornPayload(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	kvOff := appendMetaKV(t, f, []MetaKVPair{{Key: []byte("k"), Value: []byte("v")}})
	dev.buf[kvOff+SegHeaderLen+2] ^= 0xff // corrupt a payload byte past the header

	m := &MetaSlot{MetaKVOff: kvOff}
	if _, err := ReadMetaKV(dev, prefix, m); err != ErrChecksum {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// TestReadMetaKVWrongKind refuses a meta pointer that names a segment that is not a
// meta_kv: a misdirected or corrupt root.
func TestReadMetaKVWrongKind(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	offs, err := f.AppendGroup([]Pending{{Shard: 0, Kind: KindLog, ShardSeq: 1, Payload: []byte("not a meta kv")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	m := &MetaSlot{MetaKVOff: offs[0]}
	if _, err := ReadMetaKV(dev, prefix, m); err != ErrMagic {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}
