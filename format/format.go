// Package format is the byte-level on-disk layout of the .aki file (spec 2064
// doc 02). It defines the magic, the file header on page 0, the double-buffered
// meta pages on pages 1 and 2, the common page header that prefixes every other
// page, and the page-type constants. A second engineer reading doc 02 plus this
// package can produce a byte-compatible reader and writer.
//
// Everything multi-byte is little-endian, unconditionally, on every platform
// (doc 02 §6). Structures marshal into and unmarshal out of fixed byte ranges
// at the offsets the spec fixes; the checksum fields are CRC-32C over the bytes
// that precede them.
package format

import (
	"errors"

	"github.com/tamnd/aki/checksum"
	"github.com/tamnd/aki/encoding"
)

// Magic is the first 16 bytes of every .aki file: the ASCII string
// "tamndaki fmt001" (15 bytes) followed by a 0x0A newline (doc 02 §3). The
// newline keeps file(1) and head -c 16 showing human-readable text.
var Magic = [16]byte{
	't', 'a', 'm', 'n', 'd', 'a', 'k', 'i', ' ', 'f', 'm', 't', '0', '0', '1', '\n',
}

// Format and size constants (doc 02 §1, §4, §6).
const (
	// FormatVersion is the current file format version, carried in the header.
	// Version 2 introduced per-shard B-tree roots in the catalog record.
	FormatVersion uint16 = 2

	// DefaultPageSize is the page size used when a file is created without an
	// explicit override.
	DefaultPageSize uint32 = 16384

	// MinPageSize and MaxPageSize bound the legal page sizes; a page size must
	// be a power of two in this inclusive range.
	MinPageSize uint32 = 4096
	MaxPageSize uint32 = 65536

	// HeaderSize is the size in bytes of the file header on page 0. The rest of
	// page 0 is zero padding.
	HeaderSize = 128

	// CommonHeaderSize is the size of the header prefixing every page except
	// page 0.
	CommonHeaderSize = 16

	// LeafHeaderSize is the effective header size of a B-tree leaf page, which
	// adds a 4-byte right-sibling pointer after the common header.
	LeafHeaderSize = 20

	// MetaHeaderSize is the size of the meta-page header (doc 02 §9).
	MetaHeaderSize = 124

	// DefaultDBCount is the default number of logical databases.
	DefaultDBCount uint32 = 16

	// MaxDBCount is the maximum number of logical databases.
	MaxDBCount uint32 = 256

	// MetaPageA and MetaPageB are the fixed page numbers of the two meta pages.
	MetaPageA uint32 = 1
	MetaPageB uint32 = 2
)

// NullPage is the all-ones sentinel used in pointer fields to mean "no page"
// (doc 02 §6).
const NullPage uint32 = 0xFFFFFFFF

// Page-type values stored at byte 0 of the common header (doc 02 §7).
const (
	PageTypeHeader    uint8 = 0x00
	PageTypeMeta      uint8 = 0x01
	PageTypeBTreeInt  uint8 = 0x02
	PageTypeBTreeLeaf uint8 = 0x03
	PageTypeOverflow  uint8 = 0x04
	PageTypeFLTrunk   uint8 = 0x05
	PageTypeFLLeaf    uint8 = 0x06
	PageTypeVExtent   uint8 = 0x07
	PageTypeWALIdx    uint8 = 0x08
	PageTypeStreamSeg uint8 = 0x09
	PageTypeHLLBlob   uint8 = 0x0A
	PageTypeCatalog   uint8 = 0x0B
	PageTypeFree      uint8 = 0xFF
)

// Codec and encryption identifiers (doc 02 §4).
const (
	CodecNone uint8 = 0x00
	CodecLZ4  uint8 = 0x01
	CodecZstd uint8 = 0x02

	EncryptionNone   uint8 = 0x00
	EncryptionAESGCM uint8 = 0x01
)

// Page-flag bits in the common header's Flags byte. Only B-tree pages use it so
// far. FlagBTreeOrderStat marks an interior page as order-statistic augmented:
// each child pointer carries a 4-byte subtree row count, so a rank or select is
// one root-to-leaf descent. Plain interior pages leave the byte zero and read
// exactly as before, so the bit is backward compatible with files written before
// it existed (spec 2064 doc 04, implementation note 345).
const (
	FlagBTreeOrderStat uint8 = 1 << 0
)

// File-flag bits in the header's file_flags field (doc 02 §4).
const (
	FileFlagInMemory          uint32 = 1 << 0
	FileFlagWALMode           uint32 = 1 << 1
	FileFlagEncrypted         uint32 = 1 << 2
	FileFlagCompressedDefault uint32 = 1 << 3
)

// Errors returned by the format package.
var (
	// ErrBadMagic means the first 16 bytes are not the aki magic.
	ErrBadMagic = errors.New("aki/format: bad magic")
	// ErrBadChecksum means a stored CRC-32C does not match the bytes it covers.
	ErrBadChecksum = errors.New("aki/format: checksum mismatch")
	// ErrShortBuffer means a marshal/unmarshal target is too small.
	ErrShortBuffer = errors.New("aki/format: buffer too small")
	// ErrBadPageSize means a page size is out of range or not a power of two.
	ErrBadPageSize = errors.New("aki/format: invalid page size")
	// ErrUnknownPageType is returned when traversal hits an undefined page type.
	ErrUnknownPageType = errors.New("aki/format: unknown page type")
)

// ValidPageSize reports whether n is a legal page size: a power of two in
// [MinPageSize, MaxPageSize].
func ValidPageSize(n uint32) bool {
	if n < MinPageSize || n > MaxPageSize {
		return false
	}
	return n&(n-1) == 0
}

// PageHeader is the 16-byte common header at the front of every page except
// page 0 (doc 02 §8). RightSibling is only meaningful for B-tree leaf pages and
// lives just past this header; it is carried here for convenience but marshaled
// separately by the page-specific code.
type PageHeader struct {
	Type      uint8
	Flags     uint8
	CellCount uint16
	FreeStart uint16
	FreeEnd   uint16
	PageLSN   uint64
}

// MarshalTo writes the common header into the first CommonHeaderSize bytes of b.
func (h PageHeader) MarshalTo(b []byte) error {
	if len(b) < CommonHeaderSize {
		return ErrShortBuffer
	}
	b[0] = h.Type
	b[1] = h.Flags
	encoding.PutU16(b[2:], h.CellCount)
	encoding.PutU16(b[4:], h.FreeStart)
	encoding.PutU16(b[6:], h.FreeEnd)
	encoding.PutU64(b[8:], h.PageLSN)
	return nil
}

// ParsePageHeader reads a common header from the front of b.
func ParsePageHeader(b []byte) (PageHeader, error) {
	if len(b) < CommonHeaderSize {
		return PageHeader{}, ErrShortBuffer
	}
	return PageHeader{
		Type:      b[0],
		Flags:     b[1],
		CellCount: encoding.U16(b[2:]),
		FreeStart: encoding.U16(b[4:]),
		FreeEnd:   encoding.U16(b[6:]),
		PageLSN:   encoding.U64(b[8:]),
	}, nil
}

// FreeSpace returns the number of free bytes between FreeStart and FreeEnd.
func (h PageHeader) FreeSpace() int {
	if h.FreeStart >= h.FreeEnd {
		return 0
	}
	return int(h.FreeEnd - h.FreeStart)
}

// crc32c is the package-local alias for the on-disk checksum.
func crc32c(b []byte) uint32 { return checksum.Sum(b) }
