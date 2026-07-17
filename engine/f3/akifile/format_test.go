package akifile

import "testing"

// TestFixedSizesStayFrozen pins the on-disk sizes the format froze per major
// version. A drift here is a format break, not a refactor.
func TestFixedSizesStayFrozen(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"magic", len(Magic), 16},
		{"prefix", PrefixSize, 100},
		{"meta slot", MetaSlotSize, 128},
		{"srt header", SRTHeaderLen, 40},
		{"srt row", SRTRowSize, 80},
		{"seg header", SegHeaderLen, 64},
		{"extent", ExtentSize, 24},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s size = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// TestMetaSlotsSitInSeparateSectors is the torn-safety invariant: the two slots
// occupy distinct 4KiB sectors of page zero, so a torn write to one cannot reach
// the other, and both stay clear of the immutable prefix.
func TestMetaSlotsSitInSeparateSectors(t *testing.T) {
	if MetaSlotAOff/SegmentAlign == MetaSlotBOff/SegmentAlign {
		t.Fatalf("slots share a %d sector: A=%d B=%d", SegmentAlign, MetaSlotAOff, MetaSlotBOff)
	}
	if MetaSlotAOff < PrefixSize {
		t.Fatalf("slot A at %d overlaps the prefix (%d bytes)", MetaSlotAOff, PrefixSize)
	}
	if MetaSlotBOff+MetaSlotSize > PageSize {
		t.Fatalf("slot B [%d,%d) runs off page zero (%d)", MetaSlotBOff, MetaSlotBOff+MetaSlotSize, PageSize)
	}
}

func TestAlignUpAndSegmentSpan(t *testing.T) {
	if got := AlignUp(1, 4096); got != 4096 {
		t.Errorf("AlignUp(1,4096) = %d, want 4096", got)
	}
	if got := AlignUp(4096, 4096); got != 4096 {
		t.Errorf("AlignUp(4096,4096) = %d, want 4096", got)
	}
	if got := AlignUp(4097, 4096); got != 8192 {
		t.Errorf("AlignUp(4097,4096) = %d, want 8192", got)
	}
	// a header plus a one-byte payload still spans exactly one 4KiB segment.
	if got := SegmentSpan(1); got != 4096 {
		t.Errorf("SegmentSpan(1) = %d, want 4096", got)
	}
	// header plus a payload that reaches into the second boundary spans two.
	if got := SegmentSpan(4096); got != 8192 {
		t.Errorf("SegmentSpan(4096) = %d, want 8192", got)
	}
}

// TestChecksumUnknownKind confirms a kind this build cannot compute is refused
// rather than silently mis-summed.
func TestChecksumUnknownKind(t *testing.T) {
	if _, ok := checksum(ChecksumXXH3, []byte("x")); ok {
		t.Fatal("xxh3 reported ok but is not implemented")
	}
	if _, ok := checksum(ChecksumCRC32C, []byte("x")); !ok {
		t.Fatal("crc32c reported not ok")
	}
}
