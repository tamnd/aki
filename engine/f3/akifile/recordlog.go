package akifile

import "encoding/binary"

// The record-log frame codec (spec 2064/f3/07 sections 2 and 6). A `log`
// segment payload is a run of framed records, each `uvarint body_len, body,
// crc32c(body)`, and the body is the persisted form of one store record: the
// fields recovery's replay reads to set or clear an index entry (section 6 step
// 7). Until this codec, the store persisted only value bytes and cold frames;
// the index, the record rows, and the per-shard sequence lived in memory and
// rebuilt from nothing because there was nothing to rebuild from. This is the
// first byte of the record log itself.
//
// The frame mirrors the value frame's shape (valuelog.go) so the same two
// readers work the same way. A recovery walk steps one frame to the next by the
// varint length, validating framing without an index (WalkRecords). A point read
// jumps to a record by its absolute frame offset, the address the index entry
// and the checkpoint keep (the record_addr in section 5's checkpoint entry). The
// CRC32C spans the whole body, so a torn write to any field, the value word, the
// expiry, or the key, fails the frame on read instead of applying rot; it is
// CRC32C regardless of the file's checksum kind, like every frame CRC in the
// format.
//
// This is codec only, like valuelog.go before its consumers. It frames into and
// reads out of a caller-owned payload buffer and never touches a File beyond the
// writer below, which cuts the segment. The store-side adapter that stages a
// command's record row here is a later slice.

// recRowHdr is the fixed header before a record's key: flags u32, value_word
// u64, value_len u32, expire_at u64. The key follows and its length is the
// frame body length minus this header.
const recRowHdr = 24

// RecordRow is the decoded form of one persisted record. The value word is the
// store's tier-tagged run word, opaque to akifile: an embedded blob, a value-log
// pointer, or a cold address, decided by the store's band layout. value_len is
// the length carried beside the word (the store's in-record pointer has no room
// for it inline). expire_at is the inline TTL, zero for none. A delete is a
// tombstone row, RecFlagTombstone set, so replay clears the index entry the
// same way it sets one.
type RecordRow struct {
	Flags     uint32
	ValueWord uint64
	ValueLen  uint32
	ExpireAt  uint64
	Key       []byte // aliases the payload it was parsed from
}

// RecFlagTombstone marks a row that clears its key rather than setting it, so a
// DEL or an expiry replays as an index clear. Bit zero of the row flags; the
// higher bits are reserved for the store's band and tier tags.
const RecFlagTombstone uint32 = 1 << 0

// RecordFrame locates one framed record inside a `log` payload: the frame start
// where the varint length sits and where a walk anchors, the body start where a
// point read decodes from, the body length, and the body CRC. Offsets are
// relative to the payload the frame lives in; the writer adds the segment's
// payload base to FrameOff to form the absolute record address an index entry
// keeps.
type RecordFrame struct {
	FrameOff uint64 // start of the varint length, the walk's step anchor
	BodyOff  uint64 // start of the body, what a record address references
	BodyLen  uint32 // flags+word+len+expiry+key, the CRC's span
	CRC      uint32 // CRC32C of the body
}

// AppendRecordFrame frames row onto a `log` payload: the uvarint body length,
// the fixed header, the key, then the CRC32C of the body. It returns the grown
// payload and the frame's location; the caller builds the absolute address by
// adding the segment's payload base to FrameOff. Consecutive appends are
// contiguous, so one payload packs many records and the next frame begins where
// this one ends.
func AppendRecordFrame(dst []byte, row RecordRow) ([]byte, RecordFrame) {
	frameOff := uint64(len(dst))
	bodyLen := recRowHdr + len(row.Key)
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(bodyLen))
	dst = append(dst, hdr[:n]...)
	bodyOff := uint64(len(dst))
	var fixed [recRowHdr]byte
	le.PutUint32(fixed[0:4], row.Flags)
	le.PutUint64(fixed[4:12], row.ValueWord)
	le.PutUint32(fixed[12:16], row.ValueLen)
	le.PutUint64(fixed[16:24], row.ExpireAt)
	dst = append(dst, fixed[:]...)
	dst = append(dst, row.Key...)
	body := dst[bodyOff : bodyOff+uint64(bodyLen)]
	crc := crc32c(body)
	var cb [4]byte
	le.PutUint32(cb[:], crc)
	dst = append(dst, cb[:]...)
	return dst, RecordFrame{
		FrameOff: frameOff,
		BodyOff:  bodyOff,
		BodyLen:  uint32(bodyLen),
		CRC:      crc,
	}
}

// ParseRecordBody decodes a record body: the fixed header then the key, which is
// the remaining bytes. It does not verify a CRC; the frame walk and the point
// read own that, exactly where they read the trailing CRC bytes. The returned
// Key aliases body.
func ParseRecordBody(body []byte) (RecordRow, error) {
	if len(body) < recRowHdr {
		return RecordRow{}, ErrShort
	}
	return RecordRow{
		Flags:     le.Uint32(body[0:4]),
		ValueWord: le.Uint64(body[4:12]),
		ValueLen:  le.Uint32(body[12:16]),
		ExpireAt:  le.Uint64(body[16:24]),
		Key:       body[recRowHdr:],
	}, nil
}

// NextRecordFrame parses the frame at off in a `log` payload for the recovery
// linear walk: it decodes the varint body length, bounds-checks the body and the
// trailing CRC, verifies the CRC, decodes the row, and returns the offset of the
// following frame. A torn tail, a partial varint, a body past the payload, or a
// CRC mismatch stops the walk with an error, the durable-tail cut a recovering
// reader wants. The returned row's Key aliases payload.
func NextRecordFrame(payload []byte, off uint64) (RecordFrame, RecordRow, uint64, error) {
	if off >= uint64(len(payload)) {
		return RecordFrame{}, RecordRow{}, off, ErrShort
	}
	bl, adv := binary.Uvarint(payload[off:])
	if adv <= 0 {
		return RecordFrame{}, RecordRow{}, off, ErrLength
	}
	// Guard bl before any offset arithmetic so a corrupt varint cannot overflow,
	// and reject a body too small to hold the fixed header.
	if bl < recRowHdr || bl > uint64(len(payload)) {
		return RecordFrame{}, RecordRow{}, off, ErrShort
	}
	bodyOff := off + uint64(adv)
	if bodyOff+bl+4 > uint64(len(payload)) {
		return RecordFrame{}, RecordRow{}, off, ErrShort
	}
	body := payload[bodyOff : bodyOff+bl]
	crc := le.Uint32(payload[bodyOff+bl : bodyOff+bl+4])
	if crc32c(body) != crc {
		return RecordFrame{}, RecordRow{}, off, ErrChecksum
	}
	row, _ := ParseRecordBody(body)
	return RecordFrame{
		FrameOff: off,
		BodyOff:  bodyOff,
		BodyLen:  uint32(bl),
		CRC:      crc,
	}, row, bodyOff + bl + 4, nil
}
