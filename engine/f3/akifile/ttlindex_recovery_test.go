package akifile

import "testing"

// TestReadTTLIndexFresh returns a nil map for a fresh file: no TTL index has been
// written, so active expiry falls back to the per-segment scan.
func TestReadTTLIndexFresh(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	classes, err := ReadTTLIndex(dev, st.Meta)
	if err != nil || classes != nil {
		t.Fatalf("fresh TTL index = %v/%v, want nil/nil", classes, err)
	}
}

// TestReadTTLIndexRoundTrip writes a TTL index into free space, points the live meta
// root at it, and reads back every class, then confirms the expired-segment query the
// reclaim path runs against it.
func TestReadTTLIndexRoundTrip(t *testing.T) {
	dev := &memDevice{}
	f := newTestFile(t, dev, SyncNo, nil)
	prefix := f.Prefix()

	want := []TTLClass{
		{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000, 0x2000}},
		{Class: 2, ExpiryUpperUnix: 2000, Segments: []uint64{0x3000}},
		{Class: 3, ExpiryUpperUnix: 3000, Segments: []uint64{0x4000, 0x5000}},
	}
	b := encodeTTLIndex(want)
	ttlOff := uint64(PageSize)
	if _, err := dev.WriteAt(b, int64(ttlOff)); err != nil {
		t.Fatalf("write ttl index: %v", err)
	}

	m := &MetaSlot{
		CommitSeq: 1, FileSize: PageSize, CleanShutdown: 1,
		TTLIndexOff: ttlOff, TTLIndexLen: uint32(len(b)),
	}
	writeMeta(t, dev, prefix, prefix.MetaSlotAOff, m)
	writeMeta(t, dev, prefix, prefix.MetaSlotBOff, m)

	st, err := ReadOpenState(dev)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	classes, err := ReadTTLIndex(dev, st.Meta)
	if err != nil {
		t.Fatalf("read ttl index: %v", err)
	}
	if len(classes) != len(want) {
		t.Fatalf("read %d classes, want %d", len(classes), len(want))
	}
	for i := range want {
		if classes[i].Class != want[i].Class || classes[i].ExpiryUpperUnix != want[i].ExpiryUpperUnix || len(classes[i].Segments) != len(want[i].Segments) {
			t.Fatalf("class %d = %+v, want %+v", i, classes[i], want[i])
		}
	}
	// At clock 2000 the first two classes are wholly expired, the third is not.
	expired := ExpiredSegments(classes, 2000)
	if len(expired) != 3 {
		t.Fatalf("expired segments = %v, want the 3 segments of classes 1 and 2", expired)
	}
}

// TestReadTTLIndexTornMagic refuses an index whose payload no longer carries the TTL3
// magic: a torn write is caught rather than read as a bad reclaim list.
func TestReadTTLIndexTornMagic(t *testing.T) {
	dev := &memDevice{}
	newTestFile(t, dev, SyncNo, nil)

	b := encodeTTLIndex([]TTLClass{{Class: 1, ExpiryUpperUnix: 1000, Segments: []uint64{0x1000}}})
	ttlOff := uint64(PageSize)
	if _, err := dev.WriteAt(b, int64(ttlOff)); err != nil {
		t.Fatalf("write ttl index: %v", err)
	}
	dev.buf[ttlOff] ^= 0xff // corrupt the magic

	m := &MetaSlot{TTLIndexOff: ttlOff, TTLIndexLen: uint32(len(b))}
	if _, err := ReadTTLIndex(dev, m); err != ErrMagic {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}
