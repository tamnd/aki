package sqlo1b

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/tamnd/aki/engine/sqlo1"
)

// Value-log records (doc 03 section 6). The envelope is fixed here;
// the value payload encodings belong to the per-type docs 05..10 and
// stay opaque bytes at this layer. Every record self-verifies (rcrc
// over everything before it) and self-describes (the full user key or
// 16-byte subkey is always stored), which is invariant F6: correctness
// never rests on a hash, a fingerprint false hit resolves by comparing
// the stored key bytes.

// Record types (doc 03 section 6.2).
const (
	RecString uint8 = 1 // raw bytes value
	RecRoot   uint8 = 2 // collection root, per-type header plus inline elements
	RecSeg    uint8 = 3 // collection segment, 16-byte subkey, carries rootgen
	RecTomb   uint8 = 4 // tombstone, vlen 0
	RecFence  uint8 = 5 // paged fence-table node, 16-byte subkey
	RecMeta   uint8 = 6 // reserved for format-internal records
)

// keyRType reports whether a record type names an addressable key
// (a plain value or a collection root). Segments, fences, tombstones,
// and meta records share the index but are not keys, and the DBSIZE
// counter must not see them.
func keyRType(rtype uint8) bool {
	return rtype == RecString || rtype == RecRoot
}

// rflags bits (doc 03 section 6.1). Reserved bits must be zero.
const (
	RFlagExpiry  uint8 = 1 << 0 // expire_ms field present
	RFlagRootgen uint8 = 1 << 1 // rootgen field present, segment and fence records only
	RFlagDict    uint8 = 1 << 2 // value compressed against a catalog dictionary

	rflagsKnown = RFlagExpiry | RFlagRootgen | RFlagDict
)

// SubkeySize is the synthetic key length for segment and fence
// records: u64 rooth, u8 kind, 7-byte segid (doc 03 section 6.3).
const SubkeySize = sqlo1.SubkeySize

const (
	recHdrSize  = 12 // rlen, rtype, rflags, klen, vlen
	recTailSize = 4  // rcrc
)

// Record is one decoded value-log record. RFlags is authoritative for
// which optional fields are live; ExpireMS and Rootgen are read only
// when their bit is set.
type Record struct {
	RType    uint8
	RFlags   uint8
	Key      []byte
	Value    []byte
	ExpireMS uint64 // absolute unix ms, live iff RFlagExpiry
	Rootgen  uint32 // root generation echo, live iff RFlagRootgen
}

// HasExpiry reports the expire_ms presence bit.
func (r *Record) HasExpiry() bool { return r.RFlags&RFlagExpiry != 0 }

// HasRootgen reports the rootgen presence bit.
func (r *Record) HasRootgen() bool { return r.RFlags&RFlagRootgen != 0 }

// optLen is the byte cost of the optional fields the flags declare.
func optLen(rflags uint8) int {
	n := 0
	if rflags&RFlagExpiry != 0 {
		n += 8
	}
	if rflags&RFlagRootgen != 0 {
		n += 4
	}
	return n
}

// validateEnvelope holds the structural rules shared by encode and
// decode, so a Record that encodes is exactly a Record that decodes.
func validateEnvelope(rtype, rflags uint8, klen, vlen uint64) error {
	if rtype < RecString || rtype > RecMeta {
		return fmt.Errorf("sqlo1b: record rtype %d out of range", rtype)
	}
	if rflags&^rflagsKnown != 0 {
		return fmt.Errorf("sqlo1b: record rflags %#x sets reserved bits", rflags)
	}
	if klen == 0 || klen > math.MaxUint16 {
		return fmt.Errorf("sqlo1b: record key of %d bytes, want 1..%d", klen, math.MaxUint16)
	}
	switch rtype {
	case RecSeg:
		if klen != SubkeySize {
			return fmt.Errorf("sqlo1b: seg record subkey is %d bytes, want %d", klen, SubkeySize)
		}
		if rflags&RFlagRootgen == 0 {
			return fmt.Errorf("sqlo1b: seg record without rootgen")
		}
	case RecFence:
		if klen != SubkeySize {
			return fmt.Errorf("sqlo1b: fence record subkey is %d bytes, want %d", klen, SubkeySize)
		}
		if rflags&RFlagRootgen == 0 {
			return fmt.Errorf("sqlo1b: fence record without rootgen")
		}
	case RecTomb:
		if vlen != 0 {
			return fmt.Errorf("sqlo1b: tomb record with %d value bytes", vlen)
		}
		if rflags&RFlagDict != 0 {
			return fmt.Errorf("sqlo1b: tomb record with the dict bit set")
		}
	}
	if rtype != RecSeg && rtype != RecFence && rflags&RFlagRootgen != 0 {
		return fmt.Errorf("sqlo1b: rootgen on rtype %d, segment and fence records only", rtype)
	}
	total := uint64(recHdrSize+optLen(rflags)+recTailSize) + klen + vlen
	if total > math.MaxUint32 {
		return fmt.Errorf("sqlo1b: record of %d bytes overflows rlen", total)
	}
	return nil
}

// EncodedLen is the exact rlen Encode will produce.
func (r *Record) EncodedLen() int {
	return recHdrSize + optLen(r.RFlags) + len(r.Key) + len(r.Value) + recTailSize
}

// Encode lays the record out per the doc 03 section 6.1 table and
// seals it with crc32c over every preceding byte. Encoding is
// canonical: decode of the output yields a record that encodes back
// to the same bytes.
func (r *Record) Encode() ([]byte, error) {
	if err := validateEnvelope(r.RType, r.RFlags, uint64(len(r.Key)), uint64(len(r.Value))); err != nil {
		return nil, err
	}
	b := make([]byte, r.EncodedLen())
	binary.LittleEndian.PutUint32(b[0:], uint32(len(b)))
	b[4] = r.RType
	b[5] = r.RFlags
	binary.LittleEndian.PutUint16(b[6:], uint16(len(r.Key)))
	binary.LittleEndian.PutUint32(b[8:], uint32(len(r.Value)))
	off := recHdrSize
	if r.HasExpiry() {
		binary.LittleEndian.PutUint64(b[off:], r.ExpireMS)
		off += 8
	}
	if r.HasRootgen() {
		binary.LittleEndian.PutUint32(b[off:], r.Rootgen)
		off += 4
	}
	off += copy(b[off:], r.Key)
	off += copy(b[off:], r.Value)
	binary.LittleEndian.PutUint32(b[off:], crc32.Checksum(b[:off], crcTable))
	return b, nil
}

// DecodeRecord parses and verifies one record from the start of b.
// Trailing bytes past rlen are ignored: a group's last record slice
// runs to the slot table and may carry the pad marker, the envelope's
// length prefix is the trim. Key and Value alias b.
func DecodeRecord(b []byte) (*Record, error) {
	if len(b) < recHdrSize+recTailSize {
		return nil, fmt.Errorf("sqlo1b: record of %d bytes has no room for the envelope", len(b))
	}
	rlen := binary.LittleEndian.Uint32(b)
	if rlen < recHdrSize+recTailSize || uint64(rlen) > uint64(len(b)) {
		return nil, fmt.Errorf("sqlo1b: record rlen %d against %d available bytes", rlen, len(b))
	}
	body := b[:rlen-recTailSize]
	if got, want := crc32.Checksum(body, crcTable), binary.LittleEndian.Uint32(b[rlen-recTailSize:]); got != want {
		return nil, fmt.Errorf("sqlo1b: record rcrc %#x, stored %#x", got, want)
	}
	rec := &Record{RType: b[4], RFlags: b[5]}
	klen := uint64(binary.LittleEndian.Uint16(b[6:]))
	vlen := uint64(binary.LittleEndian.Uint32(b[8:]))
	if err := validateEnvelope(rec.RType, rec.RFlags, klen, vlen); err != nil {
		return nil, err
	}
	if want := uint64(recHdrSize+optLen(rec.RFlags)+recTailSize) + klen + vlen; want != uint64(rlen) {
		return nil, fmt.Errorf("sqlo1b: record rlen %d, fields add to %d", rlen, want)
	}
	off := uint64(recHdrSize)
	if rec.HasExpiry() {
		rec.ExpireMS = binary.LittleEndian.Uint64(b[off:])
		off += 8
	}
	if rec.HasRootgen() {
		rec.Rootgen = binary.LittleEndian.Uint32(b[off:])
		off += 4
	}
	rec.Key = b[off : off+klen]
	rec.Value = b[off+klen : off+klen+vlen]
	return rec, nil
}
