package sqlo1

import (
	"bytes"
	"testing"
)

func TestRopeRootCodec(t *testing.T) {
	r := ropeRoot{
		log2chunk:  13,
		rootgen:    7,
		rooth:      0xdeadbeefcafef00d,
		totalLen:   3<<13 + 17,
		chunkCount: 4,
		pcSegCount: 0,
	}
	enc := appendRopeRoot(nil, r)
	if len(enc) != ropeRootLen {
		t.Fatalf("encoded %d bytes, want %d", len(enc), ropeRootLen)
	}
	got, err := decodeRopeRoot(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != r {
		t.Fatalf("roundtrip: got %+v want %+v", got, r)
	}

	corrupt := func(name string, mutate func([]byte)) {
		t.Helper()
		b := append([]byte(nil), enc...)
		mutate(b)
		if _, err := decodeRopeRoot(b); err == nil {
			t.Errorf("%s: decode accepted a bad payload", name)
		}
	}
	if _, err := decodeRopeRoot(enc[:ropeRootLen-1]); err == nil {
		t.Error("short payload accepted")
	}
	corrupt("wrong sub", func(b []byte) { b[0] = 2 })
	corrupt("reserved set", func(b []byte) { b[2] = 1 })
	corrupt("log2chunk under floor", func(b []byte) { b[1] = minLog2Chunk - 1 })
	corrupt("log2chunk over ceiling", func(b []byte) { b[1] = maxLog2Chunk + 1 })
	corrupt("zero generation", func(b []byte) { b[4], b[5], b[6], b[7] = 0, 0, 0, 0 })
	corrupt("zero length", func(b []byte) {
		for i := 16; i < 24; i++ {
			b[i] = 0
		}
	})
	corrupt("chunk count mismatch", func(b []byte) { b[24]++ })
}

// putChunkKey must produce exactly what the canonical subkey codec
// produces; a shared scratch buffer is the only difference.
func TestPutChunkKeyMatchesSubkeyEncode(t *testing.T) {
	var buf [SubkeySize]byte
	for _, tc := range []struct{ rooth, segid uint64 }{
		{0, 0},
		{1, 1},
		{0xdeadbeefcafef00d, 12345},
		{^uint64(0), maxSegid},
	} {
		sk, err := NewSubkey(tc.rooth, chunkKind, tc.segid)
		if err != nil {
			t.Fatalf("NewSubkey(%#x, %d): %v", tc.rooth, tc.segid, err)
		}
		putChunkKey(buf[:], tc.rooth, tc.segid)
		if want := sk.Encode(); !bytes.Equal(buf[:], want) {
			t.Errorf("putChunkKey(%#x, %d) = %x, Encode = %x", tc.rooth, tc.segid, buf, want)
		}
	}
}
