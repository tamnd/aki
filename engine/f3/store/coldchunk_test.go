package store

import (
	"bytes"
	"testing"
)

// The store cold chunk plane (spec 2064/f3/06 sections 6.2 and 6.5): AppendChunk
// frames a packed chunk into the cold region and ReadChunk reads it back. These
// tests hold the round trip (kind, count, discriminator, and payload survive a
// write and a pread), the frameChunk bit is set on the wire but masked off the
// returned kind, several chunks stay independently addressable at their offsets,
// and the primitives refuse cleanly on a store with no cold region.

// kindPlainChunk is a stand-in collection kind for the store-level test, a plain
// value below frameChunk; AppendChunk sets frameChunk itself, and the real
// per-type kind is the set package's to own.
const kindPlainChunk = 0x02

func TestAppendChunkRoundTrips(t *testing.T) {
	s := coldStore(t)

	key := []byte("myset")
	disc := []byte("00000001")
	payload := []byte("alpha\x00beta\x00gamma")
	off, ok := s.AppendChunk(kindPlainChunk, 0, 3, key, disc, payload)
	if !ok {
		t.Fatal("AppendChunk on a cold store reported not ok")
	}
	if off == 0 {
		t.Fatal("chunk landed at offset 0, the reserved null")
	}

	c, _, ok := s.ReadChunk(off, nil)
	if !ok {
		t.Fatal("ReadChunk reported not ok for a just-appended chunk")
	}
	if c.Kind != kindPlainChunk {
		t.Fatalf("kind %#x, want %#x with frameChunk masked off", c.Kind, kindPlainChunk)
	}
	if c.Count != 3 {
		t.Fatalf("count %d, want 3", c.Count)
	}
	if !bytes.Equal(c.Disc, disc) {
		t.Fatalf("disc %q, want %q", c.Disc, disc)
	}
	if !bytes.Equal(c.Payload, payload) {
		t.Fatalf("payload %q, want %q", c.Payload, payload)
	}
}

// The wire kind carries the frameChunk high bit (so an M8 recovery walk tells a
// chunk from a whole record), but ReadChunk hands back the plain collection kind.
func TestAppendChunkSetsFrameChunkBitOnTheWire(t *testing.T) {
	s := coldStore(t)

	off, ok := s.AppendChunk(kindPlainChunk, 0, 1, []byte("k"), []byte("d"), []byte("m"))
	if !ok {
		t.Fatal("AppendChunk not ok")
	}
	// Read the raw kind byte straight out of the region: it is at the frame's
	// fifth byte (after the u32 total), and it must have frameChunk set.
	var raw [chunkHdr]byte
	if err := s.cold.readFill(off, raw[:]); err != nil {
		t.Fatalf("read raw header: %v", err)
	}
	if raw[4]&frameChunk == 0 {
		t.Fatalf("wire kind %#x missing the frameChunk bit", raw[4])
	}
	if raw[4]&^frameChunk != kindPlainChunk {
		t.Fatalf("wire kind low bits %#x, want %#x", raw[4]&^frameChunk, kindPlainChunk)
	}
}

// Several chunks written back to back stay independently addressable: each offset
// reads back its own frame, so a directory of offsets resolves without a walk.
func TestAppendChunkManyStayAddressable(t *testing.T) {
	s := coldStore(t)

	type want struct {
		off     uint64
		disc    []byte
		payload []byte
		count   uint16
	}
	var chunks []want
	for i := 0; i < 8; i++ {
		disc := []byte{byte('a' + i)}
		payload := bytes.Repeat([]byte{byte('0' + i)}, 100+i)
		off, ok := s.AppendChunk(kindPlainChunk, 0, uint16(i+1), []byte("set"), disc, payload)
		if !ok {
			t.Fatalf("AppendChunk %d not ok", i)
		}
		chunks = append(chunks, want{off, disc, payload, uint16(i + 1)})
	}

	var buf []byte
	for i, w := range chunks {
		var c Chunk
		var ok bool
		c, buf, ok = s.ReadChunk(w.off, buf)
		if !ok {
			t.Fatalf("ReadChunk %d not ok", i)
		}
		if c.Count != int(w.count) || !bytes.Equal(c.Disc, w.disc) || !bytes.Equal(c.Payload, w.payload) {
			t.Fatalf("chunk %d read back wrong: count %d disc %q", i, c.Count, c.Disc)
		}
	}
}

// A store with no cold region refuses both primitives cleanly rather than
// panicking, the same nil-region gate the whole-record demote path holds.
func TestColdChunkNoRegion(t *testing.T) {
	s := New(16<<20, 256<<10)
	if _, ok := s.AppendChunk(kindPlainChunk, 0, 1, []byte("k"), []byte("d"), []byte("m")); ok {
		t.Fatal("AppendChunk reported ok with no cold region")
	}
	if _, _, ok := s.ReadChunk(64, nil); ok {
		t.Fatal("ReadChunk reported ok with no cold region")
	}
}
