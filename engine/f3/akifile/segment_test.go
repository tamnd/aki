package akifile

import (
	"bytes"
	"errors"
	"testing"
)

func sampleSegHeader() *SegHeader {
	return &SegHeader{
		Shard:        3,
		Kind:         KindLog,
		GlobalSeq:    128,
		ShardSeq:     44,
		PrevShardSeg: 4096,
		TTLClass:     0,
		Flags:        SegSealed,
	}
}

func TestSegHeaderRoundTrip(t *testing.T) {
	h := sampleSegHeader()
	payload := bytes.Repeat([]byte("aki payload "), 40)
	b, err := h.Marshal(ChecksumCRC32C, payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(b) != SegHeaderLen {
		t.Fatalf("header %d bytes, want %d", len(b), SegHeaderLen)
	}
	if string(b[0:4]) != segMagic {
		t.Fatal("seg magic not at offset 0")
	}
	got, err := ParseSegHeader(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Shard != h.Shard || got.Kind != h.Kind || got.GlobalSeq != h.GlobalSeq ||
		got.ShardSeq != h.ShardSeq || got.PrevShardSeg != h.PrevShardSeg ||
		got.Flags != h.Flags || got.TTLClass != h.TTLClass {
		t.Fatalf("field mismatch:\n got %+v\nwant %+v", got, h)
	}
	if got.PayloadLen != uint64(len(payload)) {
		t.Fatalf("payload_len = %d, want %d", got.PayloadLen, len(payload))
	}
	if !got.Sealed() {
		t.Fatal("sealed flag lost")
	}
	if err := got.VerifyPayload(payload, ChecksumCRC32C); err != nil {
		t.Fatalf("verify good payload: %v", err)
	}
}

// TestSegHeaderCRCCatchesHeaderTamper flips a header field and expects the
// always-CRC32C header_crc to catch it before any payload is trusted.
func TestSegHeaderCRCCatchesHeaderTamper(t *testing.T) {
	h := sampleSegHeader()
	b, _ := h.Marshal(ChecksumCRC32C, []byte("body"))
	b[24] ^= 0xFF // payload_len, inside header_crc's 0..48
	if _, err := ParseSegHeader(b); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestSegHeaderRejectsBadMagic(t *testing.T) {
	b, _ := sampleSegHeader().Marshal(ChecksumCRC32C, []byte("body"))
	b[0] = 'x'
	if _, err := ParseSegHeader(b); !errors.Is(err, ErrMagic) {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}

// TestSegVerifyPayloadCatchesTornBody proves payload_crc is the torn-body gate:
// a bitflip in the payload fails verification even though the header is intact.
func TestSegVerifyPayloadCatchesTornBody(t *testing.T) {
	h := sampleSegHeader()
	payload := []byte("the quick brown fox")
	b, _ := h.Marshal(ChecksumCRC32C, payload)
	got, err := ParseSegHeader(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	torn := append([]byte(nil), payload...)
	torn[5] ^= 0x01
	if err := got.VerifyPayload(torn, ChecksumCRC32C); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestSegVerifyPayloadLengthMismatch(t *testing.T) {
	h := sampleSegHeader()
	payload := []byte("exactly-this-long")
	b, _ := h.Marshal(ChecksumCRC32C, payload)
	got, _ := ParseSegHeader(b)
	if err := got.VerifyPayload(payload[:len(payload)-1], ChecksumCRC32C); !errors.Is(err, ErrLength) {
		t.Fatalf("err = %v, want ErrLength", err)
	}
}

func TestSegHeaderUnknownKind(t *testing.T) {
	if _, err := sampleSegHeader().Marshal(ChecksumXXH3, []byte("body")); !errors.Is(err, ErrChecksumKind) {
		t.Fatalf("err = %v, want ErrChecksumKind", err)
	}
}

func TestSegHeaderReservedBytesZero(t *testing.T) {
	b, _ := sampleSegHeader().Marshal(ChecksumCRC32C, []byte("body"))
	if b[52] != 0 || b[53] != 0 || b[54] != 0 || b[55] != 0 {
		t.Fatalf("reserved bytes 52..56 not zero: %v", b[52:56])
	}
}
