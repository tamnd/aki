package sqlo1b

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/cespare/xxhash/v2"
)

// Extents are the allocation grain of the file: 1 MiB in v0, first 64
// bytes a header, immutable after seal (doc 03 section 4). The extent
// checksum lives in whatever references the extent, never inside it;
// header_crc exists only so the scrubber and compaction can sanity
// check an extent without consulting a referencer.
const ExtentHeaderSize = 64

// Extent kinds (doc 03 section 4.2).
const (
	KindVlog      uint8 = 1
	KindIndex     uint8 = 2
	KindDirectory uint8 = 3
	KindAllocmap  uint8 = 4
	KindDict      uint8 = 5
	KindStats     uint8 = 6
)

// eflags bits.
const (
	EFlagSealed     uint8 = 1 << 0
	EFlagCompressed uint8 = 1 << 1
	EFlagBlob       uint8 = 1 << 2
)

var extentMagic = [4]byte{'A', 'K', 'X', 'T'}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ExtentHeader is the first 64 bytes of every extent.
type ExtentHeader struct {
	Kind        uint8
	EFlags      uint8
	Shard       uint16
	SealSeq     uint64
	PayloadLen  uint32
	GroupCount  uint16
	FirstWALSeq uint64
}

// Sealed reports the eflags seal bit.
func (h *ExtentHeader) Sealed() bool { return h.EFlags&EFlagSealed != 0 }

// Encode lays the header out per the doc 03 section 4.2 table and
// seals bytes 0..59 with crc32c at offset 60.
func (h *ExtentHeader) Encode() []byte {
	b := make([]byte, ExtentHeaderSize)
	copy(b[0:4], extentMagic[:])
	b[4] = h.Kind
	b[5] = h.EFlags
	binary.LittleEndian.PutUint16(b[6:], h.Shard)
	binary.LittleEndian.PutUint64(b[8:], h.SealSeq)
	binary.LittleEndian.PutUint32(b[16:], h.PayloadLen)
	binary.LittleEndian.PutUint16(b[20:], h.GroupCount)
	binary.LittleEndian.PutUint64(b[24:], h.FirstWALSeq)
	binary.LittleEndian.PutUint32(b[60:], crc32.Checksum(b[:60], crcTable))
	return b
}

// DecodeExtentHeader verifies emagic, header_crc, and the kind range.
func DecodeExtentHeader(b []byte) (*ExtentHeader, error) {
	if len(b) < ExtentHeaderSize {
		return nil, fmt.Errorf("sqlo1b: extent header is %d bytes, want %d", len(b), ExtentHeaderSize)
	}
	if [4]byte(b[0:4]) != extentMagic {
		return nil, errors.New("sqlo1b: extent magic mismatch")
	}
	if got, want := crc32.Checksum(b[:60], crcTable), binary.LittleEndian.Uint32(b[60:]); got != want {
		return nil, fmt.Errorf("sqlo1b: extent header_crc %#x, stored %#x", got, want)
	}
	h := &ExtentHeader{
		Kind:        b[4],
		EFlags:      b[5],
		Shard:       binary.LittleEndian.Uint16(b[6:]),
		SealSeq:     binary.LittleEndian.Uint64(b[8:]),
		PayloadLen:  binary.LittleEndian.Uint32(b[16:]),
		GroupCount:  binary.LittleEndian.Uint16(b[20:]),
		FirstWALSeq: binary.LittleEndian.Uint64(b[24:]),
	}
	if h.Kind < KindVlog || h.Kind > KindStats {
		return nil, fmt.Errorf("sqlo1b: extent kind %d out of range", h.Kind)
	}
	return h, nil
}

// ExtentChecksum hashes a full extent (header included) with
// xxhash64; sealing hands this to whatever will reference the
// extent, per the checksums-held-by-the-referencer principle.
func ExtentChecksum(r io.ReaderAt, extentSize uint32, ext uint64) (uint64, error) {
	buf := make([]byte, extentSize)
	if _, err := r.ReadAt(buf, int64(ext)*int64(extentSize)); err != nil {
		return 0, fmt.Errorf("sqlo1b: checksum extent %d: %w", ext, err)
	}
	return xxhash.Sum64(buf), nil
}
