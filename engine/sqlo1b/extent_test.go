package sqlo1b

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func hdrFixture() *ExtentHeader {
	return &ExtentHeader{
		Kind:        KindVlog,
		EFlags:      EFlagSealed | EFlagCompressed,
		Shard:       3,
		SealSeq:     9001,
		PayloadLen:  1044480,
		GroupCount:  255,
		FirstWALSeq: 8000,
	}
}

func TestExtentHeaderRoundtrip(t *testing.T) {
	want := hdrFixture()
	got, err := DecodeExtentHeader(want.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip mismatch:\ngot  %+v\nwant %+v", got, want)
	}
}

// TestExtentHeaderOffsets pins the doc 03 section 4.2 table bytes.
func TestExtentHeaderOffsets(t *testing.T) {
	b := hdrFixture().Encode()
	if string(b[0:4]) != "AKXT" {
		t.Fatalf("emagic %q", b[0:4])
	}
	if b[4] != KindVlog || b[5] != (EFlagSealed|EFlagCompressed) {
		t.Fatalf("kind/eflags bytes %d/%d", b[4], b[5])
	}
	if got := binary.LittleEndian.Uint16(b[6:]); got != 3 {
		t.Fatalf("shard at 6 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[8:]); got != 9001 {
		t.Fatalf("seal_seq at 8 = %d", got)
	}
	if got := binary.LittleEndian.Uint32(b[16:]); got != 1044480 {
		t.Fatalf("payload_len at 16 = %d", got)
	}
	if got := binary.LittleEndian.Uint16(b[20:]); got != 255 {
		t.Fatalf("group_count at 20 = %d", got)
	}
	if got := binary.LittleEndian.Uint64(b[24:]); got != 8000 {
		t.Fatalf("first_wal_seq at 24 = %d", got)
	}
	for _, i := range []int{22, 23, 32, 40, 55, 56, 59} {
		if b[i] != 0 {
			t.Fatalf("reserved byte %d not zero", i)
		}
	}
	if got := binary.LittleEndian.Uint32(b[60:]); got != crc32.Checksum(b[:60], crcTable) {
		t.Fatal("header_crc at 60 does not seal bytes 0..59")
	}
}

// TestExtentHeaderCorruption flips every byte the crc covers, one at
// a time, plus the crc bytes themselves; decode must refuse each.
func TestExtentHeaderCorruption(t *testing.T) {
	clean := hdrFixture().Encode()
	for i := range ExtentHeaderSize {
		b := bytes.Clone(clean)
		b[i] ^= 0x40
		if _, err := DecodeExtentHeader(b); err == nil {
			t.Fatalf("flipped byte %d decoded without error", i)
		}
	}
}

func TestExtentHeaderKindRange(t *testing.T) {
	for _, kind := range []uint8{0, 7, 200} {
		h := hdrFixture()
		h.Kind = kind
		if _, err := DecodeExtentHeader(h.Encode()); err == nil {
			t.Fatalf("kind %d decoded without error", kind)
		}
	}
}

// TestExtentChecksum pins that the hash covers the whole extent,
// header included, and moves when any byte moves.
func TestExtentChecksum(t *testing.T) {
	const extentSize = 4096
	path := filepath.Join(t.TempDir(), "b.aki")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ext1 := make([]byte, extentSize)
	copy(ext1, hdrFixture().Encode())
	for i := ExtentHeaderSize; i < extentSize; i++ {
		ext1[i] = byte(i)
	}
	if _, err := f.WriteAt(ext1, extentSize); err != nil {
		t.Fatal(err)
	}
	sum1, err := ExtentChecksum(f, extentSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ExtentChecksum(f, extentSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	if sum1 != again {
		t.Fatal("checksum not deterministic")
	}
	for _, off := range []int64{0, ExtentHeaderSize, extentSize - 1} {
		if _, err := f.WriteAt([]byte{0xEE}, extentSize+off); err != nil {
			t.Fatal(err)
		}
		moved, err := ExtentChecksum(f, extentSize, 1)
		if err != nil {
			t.Fatal(err)
		}
		if moved == sum1 {
			t.Fatalf("byte at extent offset %d not covered by the checksum", off)
		}
		if _, err := f.WriteAt(ext1, extentSize); err != nil {
			t.Fatal(err)
		}
	}
}
