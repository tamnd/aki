package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// kindSetChunk is a stand-in collection chunk kind for the codec tests: the set
// type's real kind byte lands with the set cold chunk form, but the codec is
// type-agnostic, so any value with frameChunk set exercises it.
const kindSetChunk = frameChunk | 0x01

// TestChunkFrameRoundTrip frames a packed chunk and decodes it: the header fields,
// the collection key, the first discriminator, and the payload all come back
// byte-identical, and the decode consumes exactly the frame.
func TestChunkFrameRoundTrip(t *testing.T) {
	key := []byte("myset")
	disc := []byte("aardvark")
	payload := []byte("packed listpack-class member blob, opaque to the codec")
	frame := appendChunkFrame(nil, kindSetChunk, 0, 42, key, disc, payload)

	f, n, err := decodeChunkFrame(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != len(frame) {
		t.Fatalf("decode consumed %d, frame is %d", n, len(frame))
	}
	if f.kind != kindSetChunk {
		t.Fatalf("kind %#x, want %#x", f.kind, kindSetChunk)
	}
	if f.kind&frameChunk == 0 {
		t.Fatal("frameChunk bit lost")
	}
	if f.count != 42 {
		t.Fatalf("count %d, want 42", f.count)
	}
	if !bytes.Equal(f.key, key) {
		t.Fatalf("key %q, want %q", f.key, key)
	}
	if !bytes.Equal(f.disc, disc) {
		t.Fatalf("disc %q, want %q", f.disc, disc)
	}
	if !bytes.Equal(f.payload, payload) {
		t.Fatalf("payload %q, want %q", f.payload, payload)
	}
}

// TestChunkFrameEmptyDisc covers the boundary where a chunk has no discriminator
// bytes (an empty first member): the empty run still self-delimits because dlen is
// zero and the payload starts right after the key.
func TestChunkFrameEmptyDisc(t *testing.T) {
	frame := appendChunkFrame(nil, kindSetChunk, 0, 1, []byte("k"), nil, []byte("p"))
	f, n, err := decodeChunkFrame(frame)
	if err != nil || n != len(frame) {
		t.Fatalf("decode: %v n=%d", err, n)
	}
	if len(f.disc) != 0 {
		t.Fatalf("disc %q, want empty", f.disc)
	}
	if string(f.key) != "k" || string(f.payload) != "p" {
		t.Fatalf("key %q payload %q", f.key, f.payload)
	}
}

// TestChunkFrameWalk lays several chunks end to end and walks them by the consumed
// count alone, the self-delimiting linear scan recovery and compaction run: each
// frame's total re-derives the next boundary with no index.
func TestChunkFrameWalk(t *testing.T) {
	type chunk struct {
		key, disc, payload []byte
		count              uint16
	}
	chunks := []chunk{
		{[]byte("s1"), []byte("alpha"), []byte("aaa"), 3},
		{[]byte("s1"), []byte("mike"), []byte("bbbbb"), 5},
		{[]byte("s2"), nil, []byte("c"), 1},
		{[]byte("longer-set-key"), []byte("zebra-first-member"), bytes.Repeat([]byte("z"), 200), 170},
	}
	var buf []byte
	for _, c := range chunks {
		buf = appendChunkFrame(buf, kindSetChunk, 0, c.count, c.key, c.disc, c.payload)
	}
	for i := 0; len(buf) > 0; i++ {
		f, n, err := decodeChunkFrame(buf)
		if err != nil {
			t.Fatalf("chunk %d: decode: %v", i, err)
		}
		want := chunks[i]
		if f.count != want.count || !bytes.Equal(f.key, want.key) ||
			!bytes.Equal(f.disc, want.disc) || !bytes.Equal(f.payload, want.payload) {
			t.Fatalf("chunk %d mismatch: got count=%d key=%q disc=%q", i, f.count, f.key, f.disc)
		}
		buf = buf[n:]
	}
}

// TestChunkFrameShort covers the torn-tail and corrupt-length guards: a buffer
// shorter than the header, a total past the buffer, and a klen+dlen+payloadLen
// that disagrees with total all error rather than aliasing past the buffer.
func TestChunkFrameShort(t *testing.T) {
	frame := appendChunkFrame(nil, kindSetChunk, 0, 1, []byte("kk"), []byte("dd"), []byte("pppp"))

	if _, _, err := decodeChunkFrame(frame[:chunkHdr-1]); err != errChunkShort {
		t.Fatalf("short header: err %v, want errChunkShort", err)
	}
	if _, _, err := decodeChunkFrame(frame[:len(frame)-1]); err != errChunkShort {
		t.Fatalf("truncated tail: err %v, want errChunkShort", err)
	}
	// A total that claims more than the header holds but the field sum disagrees.
	bad := append([]byte(nil), frame...)
	binary.LittleEndian.PutUint16(bad[6:], uint16(len(frame))) // klen overruns total
	if _, _, err := decodeChunkFrame(bad); err != errChunkShort {
		t.Fatalf("klen overrun: err %v, want errChunkShort", err)
	}
	// A total shorter than the header errors before any field read.
	stub := make([]byte, chunkHdr)
	binary.LittleEndian.PutUint32(stub, chunkHdr-1)
	if _, _, err := decodeChunkFrame(stub); err != errChunkShort {
		t.Fatalf("total below header: err %v, want errChunkShort", err)
	}
}
