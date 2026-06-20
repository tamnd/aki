package encoding

import (
	"bytes"
	"testing"
)

func TestUvarintRoundTrip(t *testing.T) {
	cases := []uint64{0, 1, 127, 128, 300, 16383, 16384, 1 << 20, 1<<63 - 1, 1 << 63, ^uint64(0)}
	for _, want := range cases {
		buf := AppendUvarint(nil, want)
		got, n, err := Uvarint(buf)
		if err != nil {
			t.Fatalf("Uvarint(%d): %v", want, err)
		}
		if got != want {
			t.Errorf("Uvarint round-trip: got %d want %d", got, want)
		}
		if n != len(buf) {
			t.Errorf("Uvarint(%d) consumed %d of %d bytes", want, n, len(buf))
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	cases := []int64{0, -1, 1, -2, 2, 63, -64, 1 << 40, -(1 << 40), 1<<63 - 1, -1 << 63}
	for _, want := range cases {
		buf := AppendVarint(nil, want)
		got, n, err := Varint(buf)
		if err != nil {
			t.Fatalf("Varint(%d): %v", want, err)
		}
		if got != want {
			t.Errorf("Varint round-trip: got %d want %d", got, want)
		}
		if n != len(buf) {
			t.Errorf("Varint(%d) consumed %d of %d bytes", want, n, len(buf))
		}
	}
}

func TestZigzagSmallMagnitudesStayShort(t *testing.T) {
	// 0,-1,1,-2,2 must map to 0,1,2,3,4 and encode in one byte.
	want := []uint64{0, 1, 2, 3, 4}
	in := []int64{0, -1, 1, -2, 2}
	for i, v := range in {
		if got := Zigzag(v); got != want[i] {
			t.Errorf("Zigzag(%d)=%d want %d", v, got, want[i])
		}
		if buf := AppendVarint(nil, v); len(buf) != 1 {
			t.Errorf("AppendVarint(%d) used %d bytes, want 1", v, len(buf))
		}
	}
}

func TestUvarintTruncated(t *testing.T) {
	// A buffer of only continuation bytes is truncated.
	if _, _, err := Uvarint([]byte{0x80, 0x80}); err != ErrTruncated {
		t.Errorf("got %v want ErrTruncated", err)
	}
}

func TestUvarintOverflow(t *testing.T) {
	bad := bytes.Repeat([]byte{0xFF}, 11)
	if _, _, err := Uvarint(bad); err != ErrOverflow {
		t.Errorf("got %v want ErrOverflow", err)
	}
}

func TestFixedWidth(t *testing.T) {
	b := make([]byte, 8)
	PutU16(b, 0x0102)
	if got := U16(b); got != 0x0102 {
		t.Errorf("U16=%#x", got)
	}
	if b[0] != 0x02 || b[1] != 0x01 {
		t.Errorf("U16 not little-endian: %#x %#x", b[0], b[1])
	}
	PutU32(b, 0x01020304)
	if got := U32(b); got != 0x01020304 {
		t.Errorf("U32=%#x", got)
	}
	PutU64(b, 0x0102030405060708)
	if got := U64(b); got != 0x0102030405060708 {
		t.Errorf("U64=%#x", got)
	}
}

func TestFloat64RoundTrip(t *testing.T) {
	for _, v := range []float64{0, 1, -1, 3.14159, 1e308, -1e-308} {
		b := AppendF64(nil, v)
		if got := F64(b); got != v {
			t.Errorf("F64 round-trip: got %v want %v", got, v)
		}
	}
}

func TestAppendHelpersMatchPut(t *testing.T) {
	a := AppendU32(nil, 0xDEADBEEF)
	b := make([]byte, 4)
	PutU32(b, 0xDEADBEEF)
	if !bytes.Equal(a, b) {
		t.Errorf("AppendU32 %x != PutU32 %x", a, b)
	}
}
