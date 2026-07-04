package f1srv

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// The header-P codec carries the partition count in the set header row (spec 2064/19 section 5).
// Two things must hold before slice 6 ever writes a P>1 header: a P=1 header is byte-identical to
// the 9-byte header a stock reader already understands, and every header round-trips its count,
// encoding, and P. These tests pin both so turning partitioning on cannot orphan an existing set
// or misread a partition count on recovery.

// TestSetHeaderEncodeP1Identical checks a P=1 header is the exact 9 bytes the existing header
// writer produces: 8 LittleEndian count then the encoding byte, no partition byte. A drift here
// would rewrite every unpartitioned set's header the first time this codec touched it.
func TestSetHeaderEncodeP1Identical(t *testing.T) {
	for _, count := range []uint64{1, 2, 512, 1 << 40} {
		for _, enc := range []byte{encNone, encIntset, encListpack, encHashtable} {
			got := setHeaderEncodeP(nil, count, enc, 1)
			var want [9]byte
			binary.LittleEndian.PutUint64(want[:8], count)
			want[8] = enc
			if !bytes.Equal(got, want[:]) {
				t.Fatalf("P=1 header count=%d enc=%d: got %x want %x", count, enc, got, want[:])
			}
		}
	}
}

// TestSetHeaderEncodePByte checks a P>1 header appends exactly one partition byte after the
// encoding, and that byte is P's base-2 exponent (P=2 stores 1, P=256 stores 8), so the whole
// range fits a byte and a recovering reader learns how many partitions to derive.
func TestSetHeaderEncodePByte(t *testing.T) {
	for _, tc := range []struct {
		p, exp int
	}{{2, 1}, {4, 2}, {8, 3}, {16, 4}, {64, 6}, {256, 8}} {
		got := setHeaderEncodeP(nil, 100, encHashtable, tc.p)
		if len(got) != 10 {
			t.Fatalf("p=%d: header len %d, want 10", tc.p, len(got))
		}
		if int(got[9]) != tc.exp {
			t.Fatalf("p=%d: partition byte %d, want exponent %d", tc.p, got[9], tc.exp)
		}
	}
}

// TestSetHeaderRoundTrip checks encode then decode returns the same count, encoding, and P across
// every legal partition count.
func TestSetHeaderRoundTrip(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		for _, count := range []uint64{1, 999, 1 << 32} {
			enc := encListpack
			v := setHeaderEncodeP(nil, count, enc, p)
			gotCount, gotEnc, gotP, ok := setHeaderDecodeP(v)
			if !ok {
				t.Fatalf("p=%d count=%d: decode not ok", p, count)
			}
			if gotCount != count || gotEnc != enc || gotP != p {
				t.Fatalf("p=%d count=%d: round-trip got (%d,%d,%d)", p, count, gotCount, gotEnc, gotP)
			}
		}
	}
}

// TestSetHeaderDecodeLegacy checks a pre-partitioning header reads back as P=1: a bare 8-byte
// count decodes with encNone and P=1, and a 9-byte count+encoding decodes with P=1. This is the
// compatibility promise for headers written before this field existed.
func TestSetHeaderDecodeLegacy(t *testing.T) {
	var eight [8]byte
	binary.LittleEndian.PutUint64(eight[:], 42)
	count, enc, p, ok := setHeaderDecodeP(eight[:])
	if !ok || count != 42 || enc != encNone || p != 1 {
		t.Fatalf("8-byte header: got (%v,%d,%d,%d)", ok, count, enc, p)
	}

	var nine [9]byte
	binary.LittleEndian.PutUint64(nine[:8], 42)
	nine[8] = encHashtable
	count, enc, p, ok = setHeaderDecodeP(nine[:])
	if !ok || count != 42 || enc != encHashtable || p != 1 {
		t.Fatalf("9-byte header: got (%v,%d,%d,%d)", ok, count, enc, p)
	}
}

// TestSetHeaderDecodeShort checks a value too short to be a header reports not ok, and a corrupt
// tenth byte whose exponent is out of range (0 means unpartitioned, above 8 would exceed 256
// partitions) degrades to P=1 rather than mis-routing a scan.
func TestSetHeaderDecodeShort(t *testing.T) {
	if _, _, _, ok := setHeaderDecodeP([]byte{1, 2, 3}); ok {
		t.Fatalf("3-byte value: decode should not be ok")
	}
	for _, bad := range []byte{0, 9, 100, 255} {
		var v [10]byte
		binary.LittleEndian.PutUint64(v[:8], 7)
		v[8] = encListpack
		v[9] = bad
		_, _, p, ok := setHeaderDecodeP(v[:])
		if !ok || p != 1 {
			t.Fatalf("corrupt partition byte %d: got p=%d ok=%v, want p=1 ok=true", bad, p, ok)
		}
	}
}
