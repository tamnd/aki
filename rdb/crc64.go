// Package rdb serializes and deserializes values in Redis RDB wire form. It backs
// DUMP and RESTORE, whose payloads are RDB value bytes plus a short version and
// checksum footer, and it is the same codec a full snapshot file would reuse.
package rdb

import "github.com/tamnd/aki/encoding"

// crc64Poly is the Jones polynomial Redis uses for its RDB checksum: the ECMA-182
// polynomial reflected, 64 bits.
const crc64Poly = uint64(0xad93d23594c935a9)

// crc64Table is the reflected lookup table built once for the Jones polynomial.
var crc64Table = makeCRC64Table()

// makeCRC64Table builds the 256-entry table by running each byte value through the
// polynomial eight times, shifting toward the low bit since the table is reflected.
func makeCRC64Table() [256]uint64 {
	var table [256]uint64
	for i := range 256 {
		crc := uint64(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ crc64Poly
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

// crc64 folds data into the running checksum. The caller starts at zero and passes
// the whole payload up to the checksum field.
func crc64(crc uint64, data []byte) uint64 {
	for _, b := range data {
		crc = crc64Table[(crc^uint64(b))&0xFF] ^ (crc >> 8)
	}
	return crc
}

// appendCRC64 writes the 8-byte little-endian checksum of data after dst.
func appendCRC64(dst, data []byte) []byte {
	return encoding.AppendU64(dst, crc64(0, data))
}
