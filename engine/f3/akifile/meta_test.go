package akifile

import (
	"errors"
	"testing"
)

func sampleMeta(commitSeq uint64) *MetaSlot {
	return &MetaSlot{
		CommitSeq:      commitSeq,
		GlobalSeq:      9000 + commitSeq,
		SRTOff:         16384,
		SRTLen:         2600,
		SRTShardCount:  12,
		ExtentTableOff: 20480,
		ExtentTableLen: 96,
		TTLIndexLen:    48,
		TTLIndexOff:    24576,
		FreeMapOff:     28672,
		FileSize:       1 << 20,
		LiveBytes:      700000,
		DeadBytes:      12345,
		RecordCount:    4096,
		LastCkptUnix:   1_700_000_123,
		CleanShutdown:  1,
		MetaKVOff:      32768,
	}
}

func TestMetaSlotRoundTrip(t *testing.T) {
	m := sampleMeta(7)
	b, err := m.Marshal(ChecksumCRC32C)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(b) != MetaSlotSize {
		t.Fatalf("marshalled %d bytes, want %d", len(b), MetaSlotSize)
	}
	got, err := ParseMetaSlot(b, ChecksumCRC32C)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *got != *m {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", *got, *m)
	}
}

func TestMetaSlotChecksumCatchesTamper(t *testing.T) {
	b, _ := sampleMeta(7).Marshal(ChecksumCRC32C)
	b[80] ^= 0xFF // dead_bytes, inside the checksummed region
	if _, err := ParseMetaSlot(b, ChecksumCRC32C); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestMetaSlotUnknownKind(t *testing.T) {
	if _, err := sampleMeta(1).Marshal(ChecksumXXH3); !errors.Is(err, ErrChecksumKind) {
		t.Fatalf("marshal err = %v, want ErrChecksumKind", err)
	}
}

func TestMetaSlotShortBuffer(t *testing.T) {
	if _, err := ParseMetaSlot(make([]byte, MetaSlotSize-1), ChecksumCRC32C); !errors.Is(err, ErrShort) {
		t.Fatalf("err = %v, want ErrShort", err)
	}
}

// TestMetaLivePicksHigherCommitSeq is the open-time root selection: the valid
// slot with the higher commit_seq wins, regardless of which physical slot holds
// it.
func TestMetaLivePicksHigherCommitSeq(t *testing.T) {
	a, _ := sampleMeta(4).Marshal(ChecksumCRC32C)
	b, _ := sampleMeta(5).Marshal(ChecksumCRC32C)

	live, which, err := MetaLive(a, b, ChecksumCRC32C)
	if err != nil || which != 1 || live.CommitSeq != 5 {
		t.Fatalf("B newer: got which=%d seq=%d err=%v", which, live.CommitSeq, err)
	}

	live, which, err = MetaLive(b, a, ChecksumCRC32C) // swap physical slots
	if err != nil || which != 0 || live.CommitSeq != 5 {
		t.Fatalf("A newer: got which=%d seq=%d err=%v", which, live.CommitSeq, err)
	}
}

// TestMetaLiveIgnoresTornSlot models a crash that tore the slot mid-commit: the
// higher commit_seq is present but its checksum is broken, so the previous root
// is chosen and no data is lost.
func TestMetaLiveIgnoresTornSlot(t *testing.T) {
	old, _ := sampleMeta(4).Marshal(ChecksumCRC32C)
	torn, _ := sampleMeta(5).Marshal(ChecksumCRC32C)
	torn[0] ^= 0xFF // break the newer slot's checksum

	live, which, err := MetaLive(old, torn, ChecksumCRC32C)
	if err != nil || which != 0 || live.CommitSeq != 4 {
		t.Fatalf("got which=%d seq=%d err=%v, want the intact seq=4", which, live.CommitSeq, err)
	}
}

// TestMetaLiveBothTorn falls back to a full scan (an error) only when neither
// slot validates.
func TestMetaLiveBothTorn(t *testing.T) {
	a, _ := sampleMeta(4).Marshal(ChecksumCRC32C)
	b, _ := sampleMeta(5).Marshal(ChecksumCRC32C)
	a[0] ^= 0xFF
	b[0] ^= 0xFF
	if _, _, err := MetaLive(a, b, ChecksumCRC32C); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}
