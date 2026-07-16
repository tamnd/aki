// The common object header (spec 2064/obs1 doc 03 section 2). Every obs1
// object begins with the same 32 bytes: the 2064 family magic, the format
// code, the format version, a crc32c over those twenty bytes, and the
// writer's node id. A reader rejects anything whose magic, format, or
// hcrc does not match; the doc 10 fuzz suite holds every parser to clean
// errors on truncated, bit-flipped, and cross-typed input.
package obs1

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// HeaderSize is the fixed length of the common header.
const HeaderSize = 32

// headerMagic is the 2064 family magic: 15 characters and one NUL.
var headerMagic = [16]byte{'t', 'a', 'm', 'n', 'd', 'a', 'k', 'i', ' ', 'f', 'm', 't', '0', '0', '1', 0}

// Format is the object format code (doc 03 section 2's table).
type Format uint16

const (
	FormatRoot       Format = 0x6F01
	FormatChain      Format = 0x6F02 // chain record batch
	FormatCheckpoint Format = 0x6F03
	FormatWAL        Format = 0x6F04
	FormatSegment    Format = 0x6F05
	FormatManifest   Format = 0x6F06
	FormatTombstone  Format = 0x6F07 // GC tombstone page
)

func (f Format) String() string {
	switch f {
	case FormatRoot:
		return "root"
	case FormatChain:
		return "chain"
	case FormatCheckpoint:
		return "checkpoint"
	case FormatWAL:
		return "wal"
	case FormatSegment:
		return "segment"
	case FormatManifest:
		return "manifest"
	case FormatTombstone:
		return "tombstone"
	}
	return fmt.Sprintf("format(0x%04x)", uint16(f))
}

func (f Format) known() bool {
	return f >= FormatRoot && f <= FormatTombstone
}

// Header is the parsed common header. FVersion starts at 1 and bumps on
// layout change; Writer is the writing node's id, 0 for the create-time
// root.
type Header struct {
	Format   Format
	FVersion uint16
	Writer   uint64
}

// crcTable is the Castagnoli table every obs1 integrity check uses
// (doc 03: crc32c everywhere).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// crc32c is the family checksum.
func crc32c(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}

// AppendHeader appends the 32-byte encoding of h to b.
func AppendHeader(b []byte, h Header) []byte {
	var buf [HeaderSize]byte
	copy(buf[0:16], headerMagic[:])
	binary.LittleEndian.PutUint16(buf[16:18], uint16(h.Format))
	binary.LittleEndian.PutUint16(buf[18:20], h.FVersion)
	binary.LittleEndian.PutUint32(buf[20:24], crc32c(buf[0:20]))
	binary.LittleEndian.PutUint64(buf[24:32], h.Writer)
	return append(b, buf[:]...)
}

// ParseHeader reads the common header off the front of an object. It
// rejects short input, a wrong magic, a corrupt hcrc, and a format code
// outside the doc 03 table; callers that know which object they are
// reading use ParseHeaderAs.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("obs1: header is %d bytes, want %d", len(b), HeaderSize)
	}
	if [16]byte(b[0:16]) != headerMagic {
		return Header{}, fmt.Errorf("obs1: header magic mismatch")
	}
	if got, want := binary.LittleEndian.Uint32(b[20:24]), crc32c(b[0:20]); got != want {
		return Header{}, fmt.Errorf("obs1: header crc 0x%08x, computed 0x%08x", got, want)
	}
	h := Header{
		Format:   Format(binary.LittleEndian.Uint16(b[16:18])),
		FVersion: binary.LittleEndian.Uint16(b[18:20]),
		Writer:   binary.LittleEndian.Uint64(b[24:32]),
	}
	if !h.Format.known() {
		return Header{}, fmt.Errorf("obs1: unknown object format 0x%04x", uint16(h.Format))
	}
	return h, nil
}

// ParseHeaderAs is ParseHeader plus the cross-type check: a WAL object
// handed to the root parser is an error here, never a silent read.
func ParseHeaderAs(b []byte, want Format) (Header, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return Header{}, err
	}
	if h.Format != want {
		return Header{}, fmt.Errorf("obs1: object is %v, want %v", h.Format, want)
	}
	return h, nil
}
