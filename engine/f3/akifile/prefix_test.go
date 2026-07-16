package akifile

import (
	"bytes"
	"errors"
	"testing"
)

func samplePrefix() *Prefix {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	p := NewPrefix(12, 128, uuid, 1_700_000_000_000_000_000)
	p.Flags = 0b101
	return p
}

func TestPrefixRoundTrip(t *testing.T) {
	p := samplePrefix()
	b := p.Marshal()
	if len(b) != PrefixSize {
		t.Fatalf("marshalled %d bytes, want %d", len(b), PrefixSize)
	}
	if string(b[0:16]) != Magic {
		t.Fatal("magic not written at offset 0")
	}
	got, err := ParsePrefix(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *got != *p {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", *got, *p)
	}
	if got.ChecksumKind != ChecksumCRC32C || got.PageSize != PageSize || got.SegmentAlign != SegmentAlign {
		t.Fatalf("defaults not preserved: %+v", *got)
	}
}

func TestPrefixRejectsBadMagic(t *testing.T) {
	b := samplePrefix().Marshal()
	b[3] = 'X'
	if _, err := ParsePrefix(b); !errors.Is(err, ErrMagic) {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}

// TestPrefixHeaderCRCGuardsEarlyFields tampers with a field covered by
// header_crc (bytes 0..72) and expects the first CRC to catch it.
func TestPrefixHeaderCRCGuardsEarlyFields(t *testing.T) {
	b := samplePrefix().Marshal()
	b[36] ^= 0xFF // shard_count, inside 0..72
	if _, err := ParsePrefix(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// TestPrefixCRCGuardsSlotPointers tampers with a meta-slot offset, which
// header_crc does not cover but prefix_crc (bytes 0..96) does.
func TestPrefixCRCGuardsSlotPointers(t *testing.T) {
	b := samplePrefix().Marshal()
	b[76] ^= 0xFF // meta_slot_a_off, outside header_crc, inside prefix_crc
	if _, err := ParsePrefix(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestPrefixRejectsUnknownMajor(t *testing.T) {
	p := samplePrefix()
	p.FormatMajor = 4
	b := p.Marshal()
	if _, err := ParsePrefix(b); !errors.Is(err, ErrMajor) {
		t.Fatalf("err = %v, want ErrMajor", err)
	}
}

func TestPrefixShortBuffer(t *testing.T) {
	if _, err := ParsePrefix(make([]byte, PrefixSize-1)); !errors.Is(err, ErrShort) {
		t.Fatalf("err = %v, want ErrShort", err)
	}
}

// TestPrefixReservedTailIsZero confirms the marshalled prefix leaves no stray
// bytes between prefix_crc and the buffer end (there are none at PrefixSize, but
// the fields must exactly fill the region).
func TestPrefixReservedTailIsZero(t *testing.T) {
	b := samplePrefix().Marshal()
	if !bytes.Equal(b[100:], []byte{}) {
		t.Fatalf("prefix buffer longer than the fixed region: %d bytes", len(b))
	}
}
