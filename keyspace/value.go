package keyspace

import "github.com/tamnd/aki/encoding"

// Type codes for the value stored under a key (doc 05 §3.2). TYPE reports the
// name in the comment; bitmaps, bitfields and HLL all live under TypeString.
const (
	TypeString uint8 = 0 // "string"
	TypeList   uint8 = 1 // "list"
	TypeHash   uint8 = 2 // "hash"
	TypeSet    uint8 = 3 // "set"
	TypeZSet   uint8 = 4 // "zset"
	TypeStream uint8 = 5 // "stream"
)

// Encoding codes reported by OBJECT ENCODING (doc 05 §3.3). They label the
// logical Redis structure, not aki's physical paging.
const (
	EncInt       uint8 = 0
	EncEmbStr    uint8 = 1
	EncRaw       uint8 = 2
	EncListpack  uint8 = 3
	EncQuicklist uint8 = 4
	EncHashtable uint8 = 6
	EncIntset    uint8 = 7
	EncSkiplist  uint8 = 8
	EncStream    uint8 = 9
)

// Flag bits in ValueHeader.Flags (doc 05 §3.1).
const (
	FlagHasTTL     uint8 = 1 << 0
	FlagInlineBody uint8 = 1 << 1
	FlagLFUMode    uint8 = 1 << 2
	FlagNoEvict    uint8 = 1 << 3
	FlagNoTouch    uint8 = 1 << 4
)

// HeaderSize is the on-disk size of a serialized ValueHeader (doc 05 §3.1).
const HeaderSize = 40

// ValueHeader is the envelope written as the value side of every key in the
// keyspace B-tree. It carries type and encoding metadata, the absolute TTL, the
// write version used by WATCH and MVCC, and a reference to the value body. In
// this slice the body is always stored inline right after the header in the same
// B-tree leaf cell, so BodyRef is zero and FlagInlineBody is set.
type ValueHeader struct {
	Type     uint8
	Encoding uint8
	Flags    uint8
	TTLms    int64  // absolute Unix epoch ms; -1 means no expiry
	Version  uint64 // monotonic write version
	LRULFU   uint32 // LRU clock or LFU counter
	BodyRef  uint64 // sub-tree root page when not inline; 0 when inline
	BodyLen  uint32 // serialized body size
	RefCount uint32 // always 1 for now
}

// HasTTL reports whether the header carries an expiry.
func (h ValueHeader) HasTTL() bool { return h.Flags&FlagHasTTL != 0 }

// AppendTo appends the 40-byte little-endian encoding of h to dst.
func (h ValueHeader) AppendTo(dst []byte) []byte {
	dst = append(dst, h.Type, h.Encoding, h.Flags, 0) // byte 3 reserved
	dst = encoding.AppendU64(dst, uint64(h.TTLms))
	dst = encoding.AppendU64(dst, h.Version)
	dst = encoding.AppendU32(dst, h.LRULFU)
	dst = encoding.AppendU64(dst, h.BodyRef)
	dst = encoding.AppendU32(dst, h.BodyLen)
	dst = encoding.AppendU32(dst, h.RefCount)
	return dst
}

// parseHeader decodes a ValueHeader from the first 40 bytes of b. It returns the
// header and the number of bytes consumed.
func parseHeader(b []byte) (ValueHeader, int, bool) {
	if len(b) < HeaderSize {
		return ValueHeader{}, 0, false
	}
	h := ValueHeader{
		Type:     b[0],
		Encoding: b[1],
		Flags:    b[2],
		TTLms:    int64(encoding.U64(b[4:])),
		Version:  encoding.U64(b[12:]),
		LRULFU:   encoding.U32(b[20:]),
		BodyRef:  encoding.U64(b[24:]),
		BodyLen:  encoding.U32(b[32:]),
		RefCount: encoding.U32(b[36:]),
	}
	return h, HeaderSize, true
}

// compositeKey builds the B-tree key for a raw key: the 2-byte little-endian
// hash slot, then the 4-byte little-endian key length, then the key bytes (doc
// 05 §4.1). The slot prefix groups a slot's keys contiguously; the length prefix
// removes the prefix ambiguity of a bare concatenation.
func compositeKey(key []byte) []byte {
	out := make([]byte, 0, 6+len(key))
	out = encoding.AppendU16(out, HashSlot(key))
	out = encoding.AppendU32(out, uint32(len(key)))
	out = append(out, key...)
	return out
}

// rawKey extracts the original key bytes from a composite key.
func rawKey(composite []byte) []byte {
	if len(composite) < 6 {
		return nil
	}
	return composite[6:]
}
