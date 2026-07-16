package store

import (
	"bytes"
	"testing"
)

// getBit is the bit-to-byte contract restated for the test's own reads, so a
// failure blames the store and not a shared helper.
func bitOf(b []byte, off int64) int {
	i := off >> 3
	if i >= int64(len(b)) {
		return 0
	}
	return int(b[i]>>(7-uint(off&7))) & 1
}

func TestSetBitBasic(t *testing.T) {
	s := newTestStore()
	// A fresh key: setting bit 7 returns the old 0 and leaves a one-byte value.
	if old, err := s.SetBit([]byte("k"), 7, 1, 1); err != nil || old != 0 {
		t.Fatalf("SetBit new = %d, %v; want 0, nil", old, err)
	}
	if n, _ := s.StrLen([]byte("k"), 1); n != 1 {
		t.Fatalf("StrLen = %d; want 1", n)
	}
	if got := s.GetBit([]byte("k"), 7, 1); got != 1 {
		t.Fatalf("GetBit 7 = %d; want 1", got)
	}
	// The unset bits in the same byte read 0.
	for _, off := range []int64{0, 1, 6} {
		if got := s.GetBit([]byte("k"), off, 1); got != 0 {
			t.Fatalf("GetBit %d = %d; want 0", off, got)
		}
	}
	// Overwriting the same bit returns the old 1; clearing returns 1 then reads 0.
	if old, _ := s.SetBit([]byte("k"), 7, 1, 1); old != 1 {
		t.Fatalf("SetBit over set = %d; want 1", old)
	}
	if old, _ := s.SetBit([]byte("k"), 7, 0, 1); old != 1 {
		t.Fatalf("SetBit clear = %d; want 1", old)
	}
	if got := s.GetBit([]byte("k"), 7, 1); got != 0 {
		t.Fatalf("GetBit after clear = %d; want 0", got)
	}
}

func TestSetBitByteContract(t *testing.T) {
	s := newTestStore()
	// Bit 0 is the MSB of byte 0, so SETBIT k 0 1 must leave byte 0 == 0x80,
	// the cross-tool interop contract a GET has to see.
	if _, err := s.SetBit([]byte("k"), 0, 1, 1); err != nil {
		t.Fatal(err)
	}
	v, _ := s.GetString([]byte("k"), 1, nil)
	if len(v) != 1 || v[0] != 0x80 {
		t.Fatalf("byte 0 = %#v; want [0x80]", v)
	}
	// Bit 8 is the MSB of byte 1.
	if _, err := s.SetBit([]byte("k"), 8, 1, 1); err != nil {
		t.Fatal(err)
	}
	v, _ = s.GetString([]byte("k"), 1, nil)
	if len(v) != 2 || v[1] != 0x80 {
		t.Fatalf("byte 1 = %#v; want ...0x80", v)
	}
}

func TestGetBitAbsentAndPastEnd(t *testing.T) {
	s := newTestStore()
	// Missing key reads 0 at any offset and never creates the key.
	if got := s.GetBit([]byte("missing"), 12345, 1); got != 0 {
		t.Fatalf("GetBit missing = %d; want 0", got)
	}
	if s.Exists([]byte("missing"), 1) {
		t.Fatal("GetBit created the key")
	}
	s.SetBit([]byte("k"), 3, 1, 1) // one byte
	// An offset past the value length reads 0 from metadata.
	if got := s.GetBit([]byte("k"), 100, 1); got != 0 {
		t.Fatalf("GetBit past end = %d; want 0", got)
	}
}

func TestSetBitGrowth(t *testing.T) {
	s := newTestStore()
	// A SETBIT far past the end zero-extends and only the addressed bit is set.
	if old, _ := s.SetBit([]byte("k"), 1000, 1, 1); old != 0 {
		t.Fatalf("SetBit grow = %d; want 0", old)
	}
	if n, _ := s.StrLen([]byte("k"), 1); n != 126 { // byte 125 is the highest, len 126
		t.Fatalf("StrLen after grow = %d; want 126", n)
	}
	if got := s.GetBit([]byte("k"), 1000, 1); got != 1 {
		t.Fatalf("GetBit 1000 = %d; want 1", got)
	}
	// Every other bit in the extent is 0.
	v, _ := s.GetString([]byte("k"), 1, nil)
	for off := int64(0); off < 1008; off++ {
		if want := 0; off == 1000 {
		} else if bitOf(v, off) != want {
			t.Fatalf("bit %d = %d; want 0", off, bitOf(v, off))
		}
	}
	// SETBIT val 0 past the end still extends the value, matching Redis.
	if old, _ := s.SetBit([]byte("k"), 2000, 0, 1); old != 0 {
		t.Fatalf("SetBit grow zero = %d; want 0", old)
	}
	if n, _ := s.StrLen([]byte("k"), 1); n != 251 {
		t.Fatalf("StrLen after zero-grow = %d; want 251", n)
	}
}

func TestBitIntCell(t *testing.T) {
	s := newTestStore()
	// SET k 255 stores an int cell; GETBIT reads the raw decimal text, so bit 0
	// is the MSB of '2' (0x32) and reads 0, the documented int-cell wrinkle.
	if err := s.SetString([]byte("k"), []byte("255"), 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if got := s.GetBit([]byte("k"), 0, 1); got != 0 {
		t.Fatalf("GetBit int-cell bit 0 = %d; want 0", got)
	}
	// '2' is 0x32 = 0b00110010, so bit 2 is the first set bit.
	if got := s.GetBit([]byte("k"), 2, 1); got != 1 {
		t.Fatalf("GetBit int-cell bit 2 = %d; want 1", got)
	}
	// A SETBIT materializes the int cell to its text and pokes one byte; the
	// value becomes the raw form and the other bytes stay '5','5'.
	if _, err := s.SetBit([]byte("k"), 0, 1, 1); err != nil {
		t.Fatal(err)
	}
	v, _ := s.GetString([]byte("k"), 1, nil)
	if !bytes.Equal(v, []byte{0x32 | 0x80, '5', '5'}) {
		t.Fatalf("int-cell after SETBIT = %#v", v)
	}
}

func TestBitChunkedBand(t *testing.T) {
	s := newTestStore()
	// A value at two chunks (>= 64KiB) lives in the chunked band; SETBIT and
	// GETBIT must touch only the covering chunk and stay byte-consistent with a
	// whole-value read.
	base := bytes.Repeat([]byte{0x00}, 2*ChunkSize)
	if err := s.SetString([]byte("k"), base, 1, 0, false); err != nil {
		t.Fatal(err)
	}
	// A bit in the first chunk and a bit deep in the second chunk.
	offs := []int64{5, int64(ChunkSize)*8 + 17}
	for _, off := range offs {
		if old, err := s.SetBit([]byte("k"), off, 1, 1); err != nil || old != 0 {
			t.Fatalf("SetBit chunked %d = %d, %v", off, old, err)
		}
	}
	for _, off := range offs {
		if got := s.GetBit([]byte("k"), off, 1); got != 1 {
			t.Fatalf("GetBit chunked %d = %d; want 1", off, got)
		}
	}
	// The whole value read agrees bit for bit, and the length is unchanged.
	if n, _ := s.StrLen([]byte("k"), 1); n != int64(2*ChunkSize) {
		t.Fatalf("StrLen chunked = %d; want %d", n, 2*ChunkSize)
	}
	v, _ := s.GetString([]byte("k"), 1, nil)
	set := map[int64]bool{offs[0]: true, offs[1]: true}
	for off := int64(0); off < int64(len(v))*8; off++ {
		want := 0
		if set[off] {
			want = 1
		}
		if bitOf(v, off) != want {
			t.Fatalf("chunked bit %d = %d; want %d", off, bitOf(v, off), want)
		}
	}
}

func TestBitSepBand(t *testing.T) {
	s := newTestStore()
	// The separated band sits between the embedded cap and the chunk threshold
	// (1024 < len < 64KiB); a 4KiB value lands there and the bit pair must work
	// in place.
	base := bytes.Repeat([]byte{0x00}, 4096)
	if err := s.SetString([]byte("k"), base, 1, 0, false); err != nil {
		t.Fatal(err)
	}
	if old, err := s.SetBit([]byte("k"), 4096*8-1, 1, 1); err != nil || old != 0 {
		t.Fatalf("SetBit sep = %d, %v", old, err)
	}
	if got := s.GetBit([]byte("k"), 4096*8-1, 1); got != 1 {
		t.Fatalf("GetBit sep = %d; want 1", got)
	}
	if got := s.GetBit([]byte("k"), 0, 1); got != 0 {
		t.Fatalf("GetBit sep bit 0 = %d; want 0", got)
	}
}
