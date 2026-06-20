// Package encoding holds the byte-level integer codecs aki uses on disk and on
// the WAL (spec 2064 doc 02 §5, doc 04 §3). Everything multi-byte in the file
// format is little-endian so that the common amd64/arm64 case is a plain load;
// variable-length integers use unsigned LEB128, and signed values are mapped to
// unsigned with zigzag so small magnitudes of either sign stay short.
//
// The functions here are intentionally allocation-free and append-based so the
// pager and WAL writer can build records into a reused buffer.
package encoding

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrTruncated is returned by the decoders when the input ends mid-value.
var ErrTruncated = errors.New("aki/encoding: truncated input")

// ErrOverflow is returned when a varint is longer than 10 bytes, which cannot
// fit a 64-bit value and signals corruption.
var ErrOverflow = errors.New("aki/encoding: varint overflows 64 bits")

// AppendUvarint appends the unsigned LEB128 encoding of v to dst.
func AppendUvarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// Uvarint decodes an unsigned LEB128 value from the front of src and returns it
// with the number of bytes consumed.
func Uvarint(src []byte) (uint64, int, error) {
	var v uint64
	var shift uint
	for i, b := range src {
		if i == 10 {
			return 0, 0, ErrOverflow
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, 0, ErrOverflow
			}
			return v | uint64(b)<<shift, i + 1, nil
		}
		v |= uint64(b&0x7f) << shift
		shift += 7
	}
	return 0, 0, ErrTruncated
}

// AppendVarint appends the zigzag+LEB128 encoding of a signed v to dst.
func AppendVarint(dst []byte, v int64) []byte {
	return AppendUvarint(dst, Zigzag(v))
}

// Varint decodes a zigzag+LEB128 signed value from the front of src.
func Varint(src []byte) (int64, int, error) {
	u, n, err := Uvarint(src)
	if err != nil {
		return 0, 0, err
	}
	return Unzigzag(u), n, nil
}

// Zigzag maps a signed integer to an unsigned one so that small magnitudes of
// either sign encode to short varints: 0,-1,1,-2,2 -> 0,1,2,3,4.
func Zigzag(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }

// Unzigzag is the inverse of Zigzag.
func Unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// PutU16, PutU32, PutU64 write fixed-width little-endian integers into b, which
// must be large enough. They wrap binary.LittleEndian so call sites read in
// aki's vocabulary rather than the standard library's.
func PutU16(b []byte, v uint16) { binary.LittleEndian.PutUint16(b, v) }
func PutU32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }
func PutU64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }

// U16, U32, U64 read fixed-width little-endian integers from b.
func U16(b []byte) uint16 { return binary.LittleEndian.Uint16(b) }
func U32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }
func U64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

// AppendU16, AppendU32, AppendU64 append fixed-width little-endian integers.
func AppendU16(dst []byte, v uint16) []byte { return binary.LittleEndian.AppendUint16(dst, v) }
func AppendU32(dst []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(dst, v) }
func AppendU64(dst []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(dst, v) }

// AppendF64 appends a float64 as little-endian IEEE-754 bits, used by sorted-set
// scores and the geo commands (spec 2064 doc 11, doc 13).
func AppendF64(dst []byte, v float64) []byte { return AppendU64(dst, math.Float64bits(v)) }

// F64 reads a little-endian IEEE-754 float64 from b.
func F64(b []byte) float64 { return math.Float64frombits(U64(b)) }
