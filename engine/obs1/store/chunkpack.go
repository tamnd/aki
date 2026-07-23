package store

import "encoding/binary"

// The packed-pair codec for collection chunk payloads (spec 2064/obs1 doc
// 08 section 3): the encoding a hash demoter packs and every consumer of a
// folded hash chunk decodes, so the local cold tier, the segment folder,
// and a cold planner on another node all read the same bytes. An entry is
// an unsigned-varint field length, the field bytes, an optional inline
// eight-byte little-endian absolute unix-ms expiry, an unsigned-varint
// value length, and the value bytes. Whether an entry carries the expiry
// is decided by the chunk-level ChunkFlagTTLBitmap flag and its presence
// bitmap: a flagged chunk's payload leads with ceil(count/8) bytes, one
// bit per entry in pack order, and only entries with their bit set carry
// the expiry word. A chunk with no expiry-bearing entry leaves the flag
// clear and packs plain, byte-identical to a codec with no TTL support at
// all, which is the pay-only-if-used rule the fieldttl lab (#1294)
// measured this encoding against its rivals on.

// PackedPair is one decoded entry: the field name, the value bytes, and
// the absolute unix-ms expiry, zero when the entry carries none. Field
// and Value alias the payload they were decoded from.
type PackedPair struct {
	Field []byte
	Value []byte
	Exp   uint64
}

// ChunkPacker accumulates entries for one chunk payload. Add in
// discriminator order, watch Bytes against the chunk target, then Finish
// and Reset for the next chunk. The zero value is ready to use.
type ChunkPacker struct {
	body    []byte
	bits    []byte
	n       int
	bearers int
	out     []byte
}

// Reset clears the packer for the next chunk, keeping its buffers.
func (p *ChunkPacker) Reset() {
	p.body = p.body[:0]
	p.bits = p.bits[:0]
	p.n = 0
	p.bearers = 0
}

// Add packs one entry. exp is the field's absolute unix-ms expiry, zero
// for none; a non-zero expiry marks the entry in the presence bitmap and
// rides inline between the field and the value length.
func (p *ChunkPacker) Add(field, value []byte, exp uint64) {
	if p.n%8 == 0 {
		p.bits = append(p.bits, 0)
	}
	p.body = binary.AppendUvarint(p.body, uint64(len(field)))
	p.body = append(p.body, field...)
	if exp != 0 {
		p.bits[p.n/8] |= 1 << (p.n % 8)
		p.body = binary.LittleEndian.AppendUint64(p.body, exp)
		p.bearers++
	}
	p.body = binary.AppendUvarint(p.body, uint64(len(value)))
	p.body = append(p.body, value...)
	p.n++
}

// Count reports the packed entry count, the chunk frame's count word.
func (p *ChunkPacker) Count() int { return p.n }

// Bytes reports the packed entry bytes so far, the figure the demoter
// holds against the chunk byte target. The bitmap a Finish may prepend is
// not counted; it is at most count/8 bytes and charging it here would make
// the flush decision depend on whether a TTL bearer happened to land yet.
func (p *ChunkPacker) Bytes() int { return len(p.body) }

// Finish returns the chunk payload and its flags: the plain body with no
// flags when no entry carries an expiry, or the presence bitmap followed
// by the body under ChunkFlagTTLBitmap. The payload aliases the packer's
// buffers and is valid until the next Add or Reset.
func (p *ChunkPacker) Finish() ([]byte, byte) {
	if p.bearers == 0 {
		return p.body, 0
	}
	p.out = append(p.out[:0], p.bits...)
	p.out = append(p.out, p.body...)
	return p.out, ChunkFlagTTLBitmap
}

// WalkPackedPairs decodes a chunk payload under its frame flags and count,
// calling fn with each entry in pack order until fn returns false. It
// reports false when the payload is torn: a bitmap shorter than the count
// demands, a varint that does not parse, or a length that runs past the
// payload.
func WalkPackedPairs(payload []byte, flags byte, count int, fn func(i int, p PackedPair) bool) bool {
	var bits []byte
	if flags&ChunkFlagTTLBitmap != 0 {
		bl := (count + 7) / 8
		if len(payload) < bl {
			return false
		}
		bits = payload[:bl]
		payload = payload[bl:]
	}
	for i := 0; i < count; i++ {
		fn0, w := binary.Uvarint(payload)
		if w <= 0 || uint64(len(payload)-w) < fn0 {
			return false
		}
		payload = payload[w:]
		field := payload[:fn0]
		payload = payload[fn0:]
		var exp uint64
		if bits != nil && bits[i/8]&(1<<(i%8)) != 0 {
			if len(payload) < 8 {
				return false
			}
			exp = binary.LittleEndian.Uint64(payload)
			payload = payload[8:]
		}
		vn, w := binary.Uvarint(payload)
		if w <= 0 || uint64(len(payload)-w) < vn {
			return false
		}
		payload = payload[w:]
		value := payload[:vn]
		payload = payload[vn:]
		if !fn(i, PackedPair{Field: field, Value: value, Exp: exp}) {
			return true
		}
	}
	return true
}

// PackedPairAt decodes the idx-th entry of a chunk payload, the locator
// read: a cold record's entry index resolves to its exact packed pair. It
// reports false on a torn payload or an index past the count.
func PackedPairAt(payload []byte, flags byte, count, idx int) (PackedPair, bool) {
	var out PackedPair
	found := false
	if idx < 0 || idx >= count {
		return out, false
	}
	ok := WalkPackedPairs(payload, flags, count, func(i int, p PackedPair) bool {
		if i == idx {
			out = p
			found = true
			return false
		}
		return true
	})
	return out, ok && found
}
