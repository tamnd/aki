package stream

import (
	"encoding/binary"
	"strconv"
)

// A stream entry ID is a 128-bit (ms, seq) pair (spec 2064/f3/14 section 3.6):
// ms is a unix-millisecond time, seq disambiguates entries within one
// millisecond. IDs are strictly increasing over a stream, so a block holds a
// monotone run and the first entry's ID is the block's smallest.
type streamID struct {
	ms  uint64
	seq uint64
}

// cmp orders two IDs, ms first then seq, matching Redis's stream ID order.
func (a streamID) cmp(b streamID) int {
	switch {
	case a.ms != b.ms:
		if a.ms < b.ms {
			return -1
		}
		return 1
	case a.seq != b.seq:
		if a.seq < b.seq {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func (a streamID) less(b streamID) bool { return a.cmp(b) < 0 }

// String formats the ID as Redis prints it, "ms-seq".
func (id streamID) String() string {
	return strconv.FormatUint(id.ms, 10) + "-" + strconv.FormatUint(id.seq, 10)
}

// The ID codec, doc 14 section 3.3: an entry stores its ID as a delta against
// its block's firstID, not against its predecessor, so every entry is
// independently decodable given the block header and a mid-block seek never
// decodes the entries before it. Dense auto-IDs carry ~4 bytes of real entropy
// per 16-byte ID, and delta coding recovers the 3x-4x reduction Redis's rax
// gets from prefix compression at 1-3 varint bytes per entry (section 3.3).
//
// The ms delta is an unsigned varint: within a block every entry's ms is at
// least the firstID ms, because IDs increase and firstID is the smallest. The
// seq delta is a signed varint (binary.PutVarint is the zigzag encoding, the
// signed listpack integer Redis stores): when the millisecond rolls forward
// mid-block the seq resets to 0, below the block's first seq, so the delta goes
// negative. labs/f3/m5/01_block_capacity pinned this: a plain unsigned varint
// underflows the negative delta to a 10-byte value and reports the sparse-ID
// overhead falsely high, so the seq delta must be signed.

// putIDDelta appends id's delta against base to dst.
func putIDDelta(dst []byte, base, id streamID) []byte {
	dst = binary.AppendUvarint(dst, id.ms-base.ms)
	dst = binary.AppendVarint(dst, int64(id.seq)-int64(base.seq))
	return dst
}

// readIDDelta reads a delta written by putIDDelta and returns the ID it encodes
// against base plus the bytes consumed.
func readIDDelta(src []byte, base streamID) (streamID, int) {
	md, n1 := binary.Uvarint(src)
	sd, n2 := binary.Varint(src[n1:])
	return streamID{ms: base.ms + md, seq: uint64(int64(base.seq) + sd)}, n1 + n2
}

// idDeltaLen reports the bytes putIDDelta would write for id against base,
// without encoding, so the block can price a frame before committing it.
func idDeltaLen(base, id streamID) int {
	return uvlen(id.ms-base.ms) + vlen(int64(id.seq)-int64(base.seq))
}

// uvlen is the encoded length of an unsigned varint.
func uvlen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// vlen is the encoded length of a signed (zigzag) varint, matching
// binary.AppendVarint.
func vlen(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return uvlen(ux)
}
