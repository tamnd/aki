package akifile

import "encoding/binary"

// The value-log frame codec (spec 2064/f3/07 section 4). A value_log segment
// payload is a run of framed values, each `uvarint len, bytes, crc32c`, and a
// record that separated its value (F5) keeps a 16-byte pointer in its place.
//
// The two shapes serve two readers. The varint-framed run is what a recovery
// walk steps through, one frame to the next, validating framing without an
// index. The pointer is what the hot path dereferences: value_off jumps past
// the varint straight to the bytes, so a point read is one slice, no parse. The
// pointer's CRC must equal the frame's own trailing CRC, which is the torn-blob
// guard: a pointer that outlived a crash whose value tore fails on read instead
// of handing back rot. Both the frame CRC and the pointer CRC are CRC32C (a u32
// checksum field, always CRC32C regardless of the file's checksum kind).
//
// This is codec only, like coldframe.go and chunkframe.go before their
// consumers: it frames into and reads out of a caller-owned payload buffer and
// never touches a File. The slice that redirects the store's per-shard value
// log into value_log segments in the .aki append space is the consumer.

// PointerSize is the on-disk size of a value pointer: value_off u64,
// value_len u32, value_crc u32.
const PointerSize = 16

// ValuePointer is the 16-byte reference a record keeps in place of a separated
// value: where the value bytes are in the file, how many, and their CRC32C. The
// CRC ties the pointer to the exact blob it was published for.
type ValuePointer struct {
	ValueOff uint64 // absolute file offset of the value bytes, not the frame
	ValueLen uint32 // value byte count
	ValueCRC uint32 // CRC32C of the value bytes, equal to the frame's trailing CRC
}

// Marshal writes the pointer as 16 little-endian bytes.
func (p ValuePointer) Marshal() []byte {
	b := make([]byte, PointerSize)
	le.PutUint64(b[0:8], p.ValueOff)
	le.PutUint32(b[8:12], p.ValueLen)
	le.PutUint32(b[12:16], p.ValueCRC)
	return b
}

// ParseValuePointer decodes a 16-byte pointer.
func ParseValuePointer(b []byte) (ValuePointer, error) {
	if len(b) < PointerSize {
		return ValuePointer{}, ErrShort
	}
	return ValuePointer{
		ValueOff: le.Uint64(b[0:8]),
		ValueLen: le.Uint32(b[8:12]),
		ValueCRC: le.Uint32(b[12:16]),
	}, nil
}

// ValueFrame locates one framed value inside a value_log payload: the frame
// start, where the varint length sits and where a recovery walk anchors, and
// the value bytes inside it, where a pointer points so a point read skips the
// varint. Offsets are relative to the payload the frame lives in; the wiring
// slice adds the segment's payload base to ValueOff to form the file offset a
// ValuePointer carries.
type ValueFrame struct {
	FrameOff uint64 // start of the varint length, the walk's step anchor
	ValueOff uint64 // start of the value bytes, what a pointer references
	ValueLen uint32
	CRC      uint32 // CRC32C of the value bytes, trailing the bytes in the frame
}

// AppendValueFrame frames val onto a value_log payload: the uvarint length, the
// bytes, then the CRC32C of the bytes. It returns the grown payload and the
// frame's location; the caller builds the ValuePointer by adding the segment's
// payload base to ValueOff and copying ValueLen and CRC. Consecutive appends are
// contiguous, so one payload packs many values and the next frame begins where
// this one ends.
func AppendValueFrame(dst []byte, val []byte) ([]byte, ValueFrame) {
	frameOff := uint64(len(dst))
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(val)))
	dst = append(dst, hdr[:n]...)
	valueOff := uint64(len(dst))
	dst = append(dst, val...)
	crc := crc32c(val)
	var cb [4]byte
	le.PutUint32(cb[:], crc)
	dst = append(dst, cb[:]...)
	return dst, ValueFrame{
		FrameOff: frameOff,
		ValueOff: valueOff,
		ValueLen: uint32(len(val)),
		CRC:      crc,
	}
}

// ReadValue returns the value bytes a pointer references from a value_log
// payload whose first byte is at base in the file. It slices the bytes directly,
// the pointer's value_off having skipped the varint, and verifies CRC32C against
// the pointer's own CRC. A mismatch is a torn or superseded blob and returns
// ErrChecksum rather than the bytes. The returned slice aliases payload.
func ReadValue(payload []byte, base uint64, p ValuePointer) ([]byte, error) {
	if p.ValueOff < base {
		return nil, ErrShort
	}
	start := p.ValueOff - base
	if start > uint64(len(payload)) || uint64(p.ValueLen) > uint64(len(payload))-start {
		return nil, ErrShort
	}
	val := payload[start : start+uint64(p.ValueLen)]
	if crc32c(val) != p.ValueCRC {
		return nil, ErrChecksum
	}
	return val, nil
}

// NextValueFrame parses the frame at off in a value_log payload for the recovery
// linear walk: it decodes the varint length, bounds-checks the bytes and the
// trailing CRC, verifies the CRC, and returns the frame, its value bytes, and
// the offset of the following frame. A torn tail, a partial varint, bytes past
// the payload, or a CRC mismatch, stops the walk with an error, which is the
// durable-tail cut a recovering reader wants. The returned slice aliases payload.
func NextValueFrame(payload []byte, off uint64) (ValueFrame, []byte, uint64, error) {
	if off >= uint64(len(payload)) {
		return ValueFrame{}, nil, off, ErrShort
	}
	n, adv := binary.Uvarint(payload[off:])
	if adv <= 0 {
		return ValueFrame{}, nil, off, ErrLength
	}
	valueOff := off + uint64(adv)
	// Guard n before any offset arithmetic so a corrupt varint cannot overflow.
	if n > uint64(len(payload)) || valueOff+n+4 > uint64(len(payload)) {
		return ValueFrame{}, nil, off, ErrShort
	}
	end := valueOff + n
	val := payload[valueOff:end]
	crc := le.Uint32(payload[end : end+4])
	if crc32c(val) != crc {
		return ValueFrame{}, nil, off, ErrChecksum
	}
	return ValueFrame{
		FrameOff: off,
		ValueOff: valueOff,
		ValueLen: uint32(n),
		CRC:      crc,
	}, val, end + 4, nil
}
