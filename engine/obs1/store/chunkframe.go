package store

import (
	"encoding/binary"
	"errors"
)

// The packed cold chunk (spec 2064/f3/06 section 6.2): a cold-record variant that
// carries many ordered collection elements per frame instead of one whole record,
// so the frame overhead and the cold-index entry amortize by the packing factor
// (about 150x at the default payload) and a resident directory over chunks answers
// ranked and ranged queries. It lives in the same cold region as the whole-record
// frame, under the same compactor and the same self-delimiting liveness rule.
// Layout:
//
//	[ u32 total | u8 kind | u8 flags | u16 klen | u32 payloadLen | u16 count
//	  | u16 dlen | collection key | first discriminator | packed blob ]
//
// total is the whole frame's byte length, so a linear recovery scan re-derives
// every boundary with no index and detects a torn tail exactly as it does for a
// whole record (07-aki-single-file.md). kind names the collection type with the
// frameChunk bit set, which is how that recovery walk tells a chunk from a whole
// record before decoding the rest; flags is reserved for per-type chunk variants.
// klen and the collection key ride along whole so Bitcask-style liveness (the
// index names me or I am dead) works from the frame alone. count is the packed
// element count a rank descent accumulates; dlen bounds the first discriminator,
// the inline ordering key (member hash for a set, (score, member) for a zset)
// the resident directory keys chunks by. The packed blob is the listpack-class
// payload of elements in the collection's logical order, opaque to this codec: a
// promotion unpacks it into the native structure, and a range read scans it.
const chunkHdr = 16 // u32 total + u8 kind + u8 flags + u16 klen + u32 payloadLen + u16 count + u16 dlen

// frameChunk is the high bit of a cold frame's kind byte, set on a packed chunk
// and clear on a whole record. A whole-record frame's kind is a plain record kind
// (kindString and the collection point kinds, all below 0x80), so the two frame
// variants never collide in the shared cold region and a recovery walk dispatches
// on this bit alone.
const frameChunk = 0x80

// errChunkShort marks a chunk frame whose total, key, discriminator, or payload
// runs past the buffer that holds it: a torn tail on recovery or a corrupt length
// on a linear walk.
var errChunkShort = errors.New("store: cold chunk frame runs past its buffer")

// chunkFrame is one decoded packed chunk. key, disc, and payload alias the source
// buffer and stay valid only as long as it does; the migrator copies out before
// the buffer recycles, exactly as coldFrame does.
type chunkFrame struct {
	kind    byte
	flags   byte
	count   uint16
	key     []byte
	disc    []byte
	payload []byte
}

// appendChunkFrame writes one packed chunk frame onto dst and returns the extended
// slice. kind carries the collection type with frameChunk already set (the caller
// owns the per-type kind byte). Field widths match the resident header so a
// collection the store already admitted cannot overflow klen, and count and the
// discriminator are bounded by the packing factor and maxDisc.
func appendChunkFrame(dst []byte, kind, flags byte, count uint16, key, disc, payload []byte) []byte {
	total := chunkHdr + len(key) + len(disc) + len(payload)
	var h [chunkHdr]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(total))
	h[4] = kind
	h[5] = flags
	binary.LittleEndian.PutUint16(h[6:], uint16(len(key)))
	binary.LittleEndian.PutUint32(h[8:], uint32(len(payload)))
	binary.LittleEndian.PutUint16(h[12:], count)
	binary.LittleEndian.PutUint16(h[14:], uint16(len(disc)))
	dst = append(dst, h[:]...)
	dst = append(dst, key...)
	dst = append(dst, disc...)
	dst = append(dst, payload...)
	return dst
}

// decodeChunkFrame parses the chunk frame at the front of buf, returning it and
// the byte count consumed so a linear scan advances by the return value. A total
// shorter than the header, a klen or dlen or payloadLen that overruns total, or a
// total longer than buf is a torn or corrupt frame and errors without aliasing
// past the buffer.
func decodeChunkFrame(buf []byte) (chunkFrame, int, error) {
	if len(buf) < chunkHdr {
		return chunkFrame{}, 0, errChunkShort
	}
	total := int(binary.LittleEndian.Uint32(buf[0:]))
	if total < chunkHdr || total > len(buf) {
		return chunkFrame{}, 0, errChunkShort
	}
	klen := int(binary.LittleEndian.Uint16(buf[6:]))
	payloadLen := int(binary.LittleEndian.Uint32(buf[8:]))
	dlen := int(binary.LittleEndian.Uint16(buf[14:]))
	if chunkHdr+klen+dlen+payloadLen != total {
		return chunkFrame{}, 0, errChunkShort
	}
	kstart := chunkHdr
	dstart := kstart + klen
	pstart := dstart + dlen
	f := chunkFrame{
		kind:    buf[4],
		flags:   buf[5],
		count:   binary.LittleEndian.Uint16(buf[12:]),
		key:     buf[kstart:dstart],
		disc:    buf[dstart:pstart],
		payload: buf[pstart:total],
	}
	return f, total, nil
}

// ChunkFramePayloadLen reads the payload length the chunk frame at the
// front of data records, or -1 when data is too short to hold the header.
// The segment builder uses it to hold a zero-count chunk, a trim's
// manifest drop marker, to its payload-free shape.
func ChunkFramePayloadLen(data []byte) int {
	if len(data) < chunkHdr {
		return -1
	}
	return int(binary.LittleEndian.Uint32(data[8:]))
}
