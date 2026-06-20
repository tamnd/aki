// Package checksum provides the CRC-32C (Castagnoli) checksum used throughout
// aki's on-disk format: the file header, every page header, the double-buffered
// meta pages, and every WAL frame (spec 2064 doc 02 §4, doc 03 §5, doc 04 §3).
//
// CRC-32C is chosen over CRC-32 (IEEE) because the Castagnoli polynomial has
// better error-detection properties for short records and, more importantly,
// because it is hardware-accelerated on amd64 (SSE4.2) and arm64, which the Go
// standard library's crc32 package uses automatically. A page checksum is on
// the hot read path, so the difference is measurable.
package checksum

import "hash/crc32"

// table is the Castagnoli polynomial table, built once at init.
var table = crc32.MakeTable(crc32.Castagnoli)

// Sum returns the CRC-32C of p.
func Sum(p []byte) uint32 { return crc32.Checksum(p, table) }

// Verify reports whether the CRC-32C of p equals want.
func Verify(p []byte, want uint32) bool { return Sum(p) == want }

// New returns a hash.Hash32 computing CRC-32C, for streaming a checksum over
// several writes without first concatenating them. Callers that have the whole
// buffer should prefer Sum.
func New() interface {
	Write([]byte) (int, error)
	Sum32() uint32
} {
	return crc32.New(table)
}
