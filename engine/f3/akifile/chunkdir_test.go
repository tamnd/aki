package akifile

import (
	"bytes"
	"testing"
)

// appendCollection streams one collection the way the writer will: its header, then a
// descriptor per chunk.
func appendCollection(dst []byte, c ChunkDirCollection) []byte {
	dst = AppendChunkDirCollectionHeader(dst, c.KeyHash, uint32(len(c.Chunks)), c.Flags)
	for _, r := range c.Chunks {
		dst = AppendChunkDirRow(dst, r)
	}
	return dst
}

// sampleDir is a full directory over two cold collections, one with two chunks and
// one with a single chunk.
func sampleDir() []ChunkDirCollection {
	return []ChunkDirCollection{
		{
			KeyHash: 0x1111,
			Chunks: []ChunkDirRow{
				{FirstDisc: []byte("aaaa"), ElementCount: 100, ChunkOff: 0x4000, ChunkLiveBytes: 900_000},
				{FirstDisc: []byte("mmmm"), ElementCount: 80, ChunkOff: 0x14F000, ChunkLiveBytes: 720_000},
			},
		},
		{
			KeyHash: 0x2222,
			Chunks: []ChunkDirRow{
				{FirstDisc: []byte{0x00, 0x01, 0x02}, ElementCount: 50, ChunkOff: 0x300000, ChunkLiveBytes: 400_000},
			},
		},
	}
}

func encodeDir(h ChunkDirHeader, cols []ChunkDirCollection) []byte {
	payload := AppendChunkDirHeader(nil, h)
	for _, c := range cols {
		payload = appendCollection(payload, c)
	}
	return payload
}

func eqRow(a, b ChunkDirRow) bool {
	return bytes.Equal(a.FirstDisc, b.FirstDisc) &&
		a.ElementCount == b.ElementCount &&
		a.ChunkOff == b.ChunkOff &&
		a.ChunkLiveBytes == b.ChunkLiveBytes
}

// TestChunkDirRoundTripFull builds a full directory, encodes it, and reads back the
// nested collections and their descriptors unchanged.
func TestChunkDirRoundTripFull(t *testing.T) {
	cols := sampleDir()
	h := ChunkDirHeader{FullOrDelta: ChunkDirFull, CkptLogPos: 1012, CollectionCount: uint64(len(cols))}
	payload := encodeDir(h, cols)

	got, err := ParseChunkDirHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got != h {
		t.Fatalf("header = %+v, want %+v", got, h)
	}

	decoded, err := ChunkDirCollections(payload, got)
	if err != nil {
		t.Fatalf("collections: %v", err)
	}
	if len(decoded) != len(cols) {
		t.Fatalf("got %d collections, want %d", len(decoded), len(cols))
	}
	for i := range cols {
		if decoded[i].KeyHash != cols[i].KeyHash || decoded[i].Flags != cols[i].Flags {
			t.Fatalf("collection %d = %+v, want key/flags of %+v", i, decoded[i], cols[i])
		}
		if len(decoded[i].Chunks) != len(cols[i].Chunks) {
			t.Fatalf("collection %d has %d chunks, want %d", i, len(decoded[i].Chunks), len(cols[i].Chunks))
		}
		for j := range cols[i].Chunks {
			if !eqRow(decoded[i].Chunks[j], cols[i].Chunks[j]) {
				t.Fatalf("collection %d chunk %d = %+v, want %+v", i, j, decoded[i].Chunks[j], cols[i].Chunks[j])
			}
		}
	}
}

// TestChunkDirDeltaCarriesBaseAndTombstone confirms a delta names its base and drops a
// promoted-or-deleted collection with a tombstone that carries no descriptors.
func TestChunkDirDeltaCarriesBaseAndTombstone(t *testing.T) {
	cols := []ChunkDirCollection{
		{KeyHash: 0x1111, Flags: ChunkDirTombstone}, // promoted back to hot, no chunks
		{
			KeyHash: 0x3333,
			Chunks:  []ChunkDirRow{{FirstDisc: []byte("zz"), ElementCount: 10, ChunkOff: 0x9000, ChunkLiveBytes: 90_000}},
		},
	}
	h := ChunkDirHeader{FullOrDelta: ChunkDirDelta, CkptLogPos: 2000, CollectionCount: 2, BaseCkptOff: 0x24F000}
	payload := encodeDir(h, cols)

	got, err := ParseChunkDirHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	if got.BaseCkptOff != 0x24F000 || got.FullOrDelta != ChunkDirDelta {
		t.Fatalf("delta header = %+v, want base 0x24F000 delta", got)
	}
	decoded, err := ChunkDirCollections(payload, got)
	if err != nil {
		t.Fatalf("collections: %v", err)
	}
	if decoded[0].Flags&ChunkDirTombstone == 0 {
		t.Fatalf("collection 0 lost its tombstone: %+v", decoded[0])
	}
	if len(decoded[0].Chunks) != 0 {
		t.Fatalf("tombstoned collection carries %d chunks, want 0", len(decoded[0].Chunks))
	}
	if len(decoded[1].Chunks) != 1 {
		t.Fatalf("collection 1 has %d chunks, want 1", len(decoded[1].Chunks))
	}
}

// TestChunkDirRowTruncatesLongDisc holds the resident directory's rule: a
// discriminator longer than the inline bound is stored as its ordering prefix.
func TestChunkDirRowTruncatesLongDisc(t *testing.T) {
	long := bytes.Repeat([]byte("a"), ChunkDirMaxDisc+8)
	payload := AppendChunkDirRow(nil, ChunkDirRow{FirstDisc: long, ElementCount: 1, ChunkOff: 0x4000})
	got, err := parseChunkDirRow(payload)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}
	if len(got.FirstDisc) != ChunkDirMaxDisc {
		t.Fatalf("stored disc len %d, want %d", len(got.FirstDisc), ChunkDirMaxDisc)
	}
	if !bytes.Equal(got.FirstDisc, long[:ChunkDirMaxDisc]) {
		t.Fatalf("stored disc = %x, want the leading %d bytes", got.FirstDisc, ChunkDirMaxDisc)
	}
}

// TestParseChunkDirHeaderShort refuses a header buffer below the fixed size.
func TestParseChunkDirHeaderShort(t *testing.T) {
	if _, err := ParseChunkDirHeader(make([]byte, ChunkDirHeaderLen-1)); err != ErrShort {
		t.Fatalf("short err = %v, want ErrShort", err)
	}
}

// TestParseChunkDirHeaderBadMagic refuses a payload that is not a chunk directory.
func TestParseChunkDirHeaderBadMagic(t *testing.T) {
	b := make([]byte, ChunkDirHeaderLen)
	copy(b[0:4], "XXXX")
	if _, err := ParseChunkDirHeader(b); err != ErrMagic {
		t.Fatalf("bad magic err = %v, want ErrMagic", err)
	}
}

// TestParseChunkDirHeaderFullWithBase refuses a full directory that names a base, the
// same invariant the checkpoint and seg-stats headers hold.
func TestParseChunkDirHeaderFullWithBase(t *testing.T) {
	payload := AppendChunkDirHeader(nil, ChunkDirHeader{FullOrDelta: ChunkDirFull, BaseCkptOff: 0x1000})
	if _, err := ParseChunkDirHeader(payload); err != ErrChunkDir {
		t.Fatalf("full-with-base err = %v, want ErrChunkDir", err)
	}
}

// TestParseChunkDirHeaderUnknownKind refuses a full-or-delta byte that is neither.
func TestParseChunkDirHeaderUnknownKind(t *testing.T) {
	payload := AppendChunkDirHeader(nil, ChunkDirHeader{FullOrDelta: 9})
	if _, err := ParseChunkDirHeader(payload); err != ErrChunkDir {
		t.Fatalf("unknown kind err = %v, want ErrChunkDir", err)
	}
}

// TestChunkDirCollectionsRejectsOverrunCollectionCount catches a collection_count that
// claims more collections than the payload can hold.
func TestChunkDirCollectionsRejectsOverrunCollectionCount(t *testing.T) {
	cols := sampleDir()
	h := ChunkDirHeader{FullOrDelta: ChunkDirFull, CollectionCount: uint64(len(cols))}
	payload := encodeDir(h, cols)
	le.PutUint64(payload[16:24], 1000) // claim far more collections than are present
	bad, err := ParseChunkDirHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := ChunkDirCollections(payload, bad); err != ErrLength {
		t.Fatalf("overrun collection count err = %v, want ErrLength", err)
	}
}

// TestChunkDirCollectionsRejectsOverrunChunkCount catches a chunk_count that claims
// more descriptors than the remaining bytes hold, a torn directory that must not
// over-read.
func TestChunkDirCollectionsRejectsOverrunChunkCount(t *testing.T) {
	cols := []ChunkDirCollection{{
		KeyHash: 0x1111,
		Chunks:  []ChunkDirRow{{FirstDisc: []byte("aa"), ElementCount: 1, ChunkOff: 0x4000}},
	}}
	h := ChunkDirHeader{FullOrDelta: ChunkDirFull, CollectionCount: 1}
	payload := encodeDir(h, cols)
	// The collection header's chunk_count sits at the first collection's offset+8.
	le.PutUint32(payload[ChunkDirHeaderLen+8:ChunkDirHeaderLen+12], 50)
	if _, err := ChunkDirCollections(payload, h); err != ErrLength {
		t.Fatalf("overrun chunk count err = %v, want ErrLength", err)
	}
}

// TestChunkDirCollectionsEmpty decodes a directory with no cold collections: a header
// and nothing after it.
func TestChunkDirCollectionsEmpty(t *testing.T) {
	payload := AppendChunkDirHeader(nil, ChunkDirHeader{FullOrDelta: ChunkDirFull})
	h, err := ParseChunkDirHeader(payload)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cols, err := ChunkDirCollections(payload, h)
	if err != nil {
		t.Fatalf("collections: %v", err)
	}
	if len(cols) != 0 {
		t.Fatalf("empty directory decoded %d collections", len(cols))
	}
}
