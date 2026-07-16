package main

import (
	"encoding/binary"
	"testing"
)

// The lab's candidate encoder must match the wal.go frame layout the
// parser demands, or slice 1 would bake a number measured on different
// bytes: flen covers everything including itself, and the fixed part
// is flen u32, kind u8, flags u8, slot u16, seq u64, klen u16.
func TestAppendFrameLayout(t *testing.T) {
	key, val := []byte("k:12345678"), []byte("valuevalue")
	b := appendFrame(nil, 0x01, 0x02, 0x1234, 99, key, val, 5000)
	wantLen := 4 + 1 + 1 + 2 + 8 + 2 + len(key) + len(val) + 8 + 1
	if len(b) != wantLen {
		t.Fatalf("frame is %d bytes, want %d", len(b), wantLen)
	}
	if got := binary.LittleEndian.Uint32(b[0:4]); got != uint32(wantLen) {
		t.Fatalf("flen = %d, want %d", got, wantLen)
	}
	if b[4] != 0x01 || b[5] != 0x02 {
		t.Fatalf("kind/flags = %x/%x", b[4], b[5])
	}
	if got := binary.LittleEndian.Uint16(b[6:8]); got != 0x1234 {
		t.Fatalf("slot = %x", got)
	}
	if got := binary.LittleEndian.Uint64(b[8:16]); got != 99 {
		t.Fatalf("seq = %d", got)
	}
	if got := binary.LittleEndian.Uint16(b[16:18]); got != uint16(len(key)) {
		t.Fatalf("klen = %d", got)
	}
	if string(b[18:18+len(key)]) != string(key) {
		t.Fatalf("key bytes wrong")
	}
}

func TestMedian(t *testing.T) {
	if m := median([]float64{3, 1, 2}); m != 2 {
		t.Fatalf("median = %v, want 2", m)
	}
	if m := median([]float64{4, 1, 3, 2}); m != 2.5 {
		t.Fatalf("median = %v, want 2.5", m)
	}
}
