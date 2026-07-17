package akifile

import "encoding/binary"

// The record-log frame codec (spec 2064/f3/07 sections 2 and 6). A `log`
// segment payload is a run of framed records, each `uvarint body_len, body,
// crc32c(body)`, and the body is the persisted form of one store record: the
// fields recovery's replay reads to set or clear an index entry (section 6 step
// 7). The body is the fixed header, then the value-or-pointer doc 07 section 6
// frames: an inline row (RecFlagInline) carries its value bytes so a small
// arena-resident string survives a crash the volatile word could not, and a
// pointer row carries none because its word already names the durable value log.
// Until this codec, the store persisted only value bytes and cold frames;
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

// recRowHdr is the fixed header before a record's variable tail: flags u32,
// value_word u64, value_len u32, expire_at u64. The tail is the value bytes
// (only when RecFlagInline is set, value_len of them) followed by the key, so
// the key length is the frame body length minus this header and any inline
// value.
const recRowHdr = 24

// RecordRow is the decoded form of one persisted record. The value word is the
// store's tier-tagged run word, opaque to akifile: an embedded blob, a value-log
// pointer, or a cold address, decided by the store's band layout. value_len is
// the length carried beside the word (the store's in-record pointer has no room
// for it inline). expire_at is the inline TTL, zero for none. A delete is a
// tombstone row, RecFlagTombstone set, so replay clears the index entry the
// same way it sets one.
//
// Value carries the record's bytes when RecFlagInline is set, the value-or-pointer
// choice doc 07 section 6 frames: a small embedded string lives in the arena, so
// its word is a volatile arena offset a crash invalidates, and the only durable
// copy of its bytes is the one the frame carries here. A separated or chunked
// record leaves Value nil and its word is a durable value-log pointer replay
// derefs instead. value_len is the inline length, so a walk slices Value without a
// second length field.
type RecordRow struct {
	Flags     uint32
	ValueWord uint64
	ValueLen  uint32
	ExpireAt  uint64
	Value     []byte // inline value bytes when RecFlagInline; aliases the payload
	Key       []byte // aliases the payload it was parsed from
}

// RecFlagTombstone marks a row that clears its key rather than setting it, so a
// DEL or an expiry replays as an index clear. Bit zero of the row flags; the
// higher bits are reserved for the store's band and tier tags.
const RecFlagTombstone uint32 = 1 << 0

// RecFlagInline marks a row whose value bytes ride the frame, value_len of them
// between the header and the key, because the record's word is a volatile arena
// offset no crash survives. Replay reinserts those bytes rather than dereferencing
// the word. Bit one of the row flags.
const RecFlagInline uint32 = 1 << 1

// RecFlagChunked marks a row whose value is a multi-chunk run, so its word names
// a chunk-extent table rather than a single value-log run a one-shot read
// resolves. Replay follows the chunk directory to reassemble the value; a reader
// that only handles single-run values refuses a chunked row rather than misreading
// its word as a run offset. Bit two of the row flags. A separated value carries
// neither this nor RecFlagInline: its word is a lone value-log run.
const RecFlagChunked uint32 = 1 << 2

// RecFlagCollectionOp marks a row whose payload is one collection mutation, the
// effect-log frame a set, list, hash, zset, or stream cuts per mutating command
// (spec 2064/f3/M8-collection-durability-plan). The key is the collection key and
// the payload rides the frame's value slot, an opaque collection op the store's
// codec frames (collectionlog.go): a kind byte, an op byte, and the sub-key and
// sub-value the op carries. akifile leaves the payload opaque the way it leaves a
// value word opaque; the flag is here so a walk and a checkpoint tell a collection
// frame from a string record without decoding it. Bit three of the row flags.
const RecFlagCollectionOp uint32 = 1 << 3

// RecFlagCollectionSnap marks a row whose payload is a whole-collection snapshot,
// the base a reopen rebuilds a collection from before it replays the effect tail
// past it. The key is the collection key and the payload rides the value slot: a
// kind byte, a key-level header, and the element run the type's cold encoder
// frames. A snapshot supersedes every collection-op frame for its key that
// precedes it, the way a string record supersedes an older one, so replay resets
// the key at a snapshot and applies only the later ops. Bit four of the row flags.
const RecFlagCollectionSnap uint32 = 1 << 4

// framePayload reports whether a row carries payload bytes between the header and
// the key, value_len of them: an inline string value (RecFlagInline), or a
// collection op or snapshot payload. A pointer row, separated or chunked, carries
// none because its word is the durable locator.
func framePayload(flags uint32) bool {
	return flags&(RecFlagInline|RecFlagCollectionOp|RecFlagCollectionSnap) != 0
}

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
// the fixed header, the inline value bytes when RecFlagInline is set, the key,
// then the CRC32C of the body. It returns the grown payload and the frame's
// location; the caller builds the absolute address by adding the segment's
// payload base to FrameOff. Consecutive appends are contiguous, so one payload
// packs many records and the next frame begins where this one ends.
//
// An inline row writes value_len value bytes between the header and the key, so
// the value's durable copy rides the frame; a pointer row writes no value bytes
// and its word is the durable locator. The header's value_len is authoritative
// for the inline slice, so the caller must set it to len(Value) on an inline row.
func AppendRecordFrame(dst []byte, row RecordRow) ([]byte, RecordFrame) {
	frameOff := uint64(len(dst))
	inline := framePayload(row.Flags)
	valLen := 0
	if inline {
		valLen = len(row.Value)
	}
	bodyLen := recRowHdr + valLen + len(row.Key)
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
	if inline {
		dst = append(dst, row.Value...)
	}
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

// ParseRecordBody decodes a record body: the fixed header, then the inline value
// bytes when RecFlagInline is set (value_len of them), then the key as the
// remaining bytes. It does not verify a CRC; the frame walk and the point read
// own that, exactly where they read the trailing CRC bytes. The returned Value
// and Key alias body. A value_len that outruns the body is a torn or corrupt
// frame, returned as ErrLength.
func ParseRecordBody(body []byte) (RecordRow, error) {
	if len(body) < recRowHdr {
		return RecordRow{}, ErrShort
	}
	row := RecordRow{
		Flags:     le.Uint32(body[0:4]),
		ValueWord: le.Uint64(body[4:12]),
		ValueLen:  le.Uint32(body[12:16]),
		ExpireAt:  le.Uint64(body[16:24]),
	}
	tail := body[recRowHdr:]
	if framePayload(row.Flags) {
		if uint64(row.ValueLen) > uint64(len(tail)) {
			return RecordRow{}, ErrLength
		}
		row.Value = tail[:row.ValueLen]
		row.Key = tail[row.ValueLen:]
	} else {
		row.Key = tail
	}
	return row, nil
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
