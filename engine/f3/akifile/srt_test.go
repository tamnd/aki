package akifile

import (
	"errors"
	"reflect"
	"testing"
)

func sampleSRT(n int) *SRT {
	s := &SRT{Gen: 42, Rows: make([]SRTRow, n)}
	for i := range s.Rows {
		k := uint64(i + 1)
		s.Rows[i] = SRTRow{
			IndexCkptOff: 4096 * k,
			IndexCkptLen: 512 * k,
			ChunkdirOff:  8192 * k,
			ChunkdirLen:  256 * k,
			SegstatsOff:  12288 * k,
			SegstatsLen:  128 * k,
			CkptLogPos:   1000 * k,
			ShardSeqHigh: 2000 * k,
			FirstTailSeg: 16384 * k,
			LiveRecords:  100 * k,
		}
	}
	return s
}

func TestSRTRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 12, 256} {
		s := sampleSRT(n)
		b, err := s.Marshal(ChecksumCRC32C)
		if err != nil {
			t.Fatalf("n=%d marshal: %v", n, err)
		}
		if len(b) != SRTHeaderLen+n*SRTRowSize {
			t.Fatalf("n=%d marshalled %d bytes", n, len(b))
		}
		got, err := ParseSRT(b, ChecksumCRC32C)
		if err != nil {
			t.Fatalf("n=%d parse: %v", n, err)
		}
		if got.Gen != s.Gen || !reflect.DeepEqual(got.Rows, s.Rows) {
			t.Fatalf("n=%d round trip mismatch:\n got %+v\nwant %+v", n, got, s)
		}
	}
}

func TestSRTRejectsBadMagic(t *testing.T) {
	b, _ := sampleSRT(4).Marshal(ChecksumCRC32C)
	b[1] = 'x'
	if _, err := ParseSRT(b, ChecksumCRC32C); !errors.Is(err, ErrMagic) {
		t.Fatalf("err = %v, want ErrMagic", err)
	}
}

// TestSRTChecksumCoversRows tampers with a row body, which lives after the crc
// field, to prove the checksum spans the rows and not just the header.
func TestSRTChecksumCoversRows(t *testing.T) {
	b, _ := sampleSRT(4).Marshal(ChecksumCRC32C)
	b[SRTHeaderLen+8] ^= 0xFF // second field of row 0
	if _, err := ParseSRT(b, ChecksumCRC32C); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestSRTChecksumCoversHeader(t *testing.T) {
	b, _ := sampleSRT(4).Marshal(ChecksumCRC32C)
	b[8] ^= 0xFF // gen, before the crc field
	if _, err := ParseSRT(b, ChecksumCRC32C); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// TestSRTTruncatedRows models a table whose declared row count runs past the
// buffer.
func TestSRTTruncatedRows(t *testing.T) {
	b, _ := sampleSRT(4).Marshal(ChecksumCRC32C)
	if _, err := ParseSRT(b[:len(b)-1], ChecksumCRC32C); !errors.Is(err, ErrShort) {
		t.Fatalf("err = %v, want ErrShort", err)
	}
}

func TestSRTUnknownKind(t *testing.T) {
	if _, err := sampleSRT(2).Marshal(ChecksumXXH3); !errors.Is(err, ErrChecksumKind) {
		t.Fatalf("err = %v, want ErrChecksumKind", err)
	}
}
