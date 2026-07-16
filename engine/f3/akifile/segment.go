package akifile

// segMagic frames every segment and lets a repair scan resync on the 4KiB grid.
const segMagic = "SEG3"

// SegHeader is the 64-byte header at the front of every segment (spec 2064/f3/07
// section 3). It always lands on a 4KiB boundary, so it sits in one atomic write
// sector and never tears internally; a torn payload is caught by PayloadCRC.
type SegHeader struct {
	Shard        uint16
	Kind         uint16
	GlobalSeq    uint64 // writer-assigned, strictly increasing across shards
	ShardSeq     uint64 // strictly increasing within the owning shard
	PayloadLen   uint64
	PrevShardSeg uint64 // offset of this shard's previous segment, the back-chain
	TTLClass     uint32
	Flags        uint32
	PayloadCRC   uint64
}

// MarshalSegment encodes the header for the given payload and returns both the
// 64-byte header and the payload's checksum stamped into it. header_crc covers
// bytes 0..48 and is always CRC32C so recovery can trust the length and kind
// before it touches the payload; PayloadCRC honors the file's checksum kind.
// PayloadLen is taken from the payload, so the framed length can never disagree
// with the bytes.
func (h *SegHeader) Marshal(kind uint32, payload []byte) ([]byte, error) {
	pc, ok := checksum(kind, payload)
	if !ok {
		return nil, ErrChecksumKind
	}
	b := make([]byte, SegHeaderLen)
	copy(b[0:4], segMagic)
	le.PutUint16(b[4:], h.Shard)
	le.PutUint16(b[6:], h.Kind)
	le.PutUint64(b[8:], h.GlobalSeq)
	le.PutUint64(b[16:], h.ShardSeq)
	le.PutUint64(b[24:], uint64(len(payload)))
	le.PutUint64(b[32:], h.PrevShardSeg)
	le.PutUint32(b[40:], h.TTLClass)
	le.PutUint32(b[44:], h.Flags)
	le.PutUint32(b[48:], crc32c(b[0:48])) // header_crc
	// bytes 52..56 reserved, left zero
	le.PutUint64(b[56:], pc)
	h.PayloadLen = uint64(len(payload))
	h.PayloadCRC = pc
	return b, nil
}

// ParseSegHeader validates the segment magic and header_crc and decodes the
// header. It does not need the checksum kind: the header CRC is always CRC32C.
// Verify the payload separately with VerifyPayload once it has been read.
func ParseSegHeader(b []byte) (*SegHeader, error) {
	if len(b) < SegHeaderLen {
		return nil, ErrShort
	}
	if string(b[0:4]) != segMagic {
		return nil, ErrMagic
	}
	if crc32c(b[0:48]) != le.Uint32(b[48:]) {
		return nil, ErrChecksum
	}
	return &SegHeader{
		Shard:        le.Uint16(b[4:]),
		Kind:         le.Uint16(b[6:]),
		GlobalSeq:    le.Uint64(b[8:]),
		ShardSeq:     le.Uint64(b[16:]),
		PayloadLen:   le.Uint64(b[24:]),
		PrevShardSeg: le.Uint64(b[32:]),
		TTLClass:     le.Uint32(b[40:]),
		Flags:        le.Uint32(b[44:]),
		PayloadCRC:   le.Uint64(b[56:]),
	}, nil
}

// VerifyPayload checks a payload against the header's framed length and checksum:
// the torn-write gate for the segment body. A length mismatch is ErrLength, a
// checksum mismatch ErrChecksum, and the caller truncates the shard's tail at the
// last good segment.
func (h *SegHeader) VerifyPayload(payload []byte, kind uint32) error {
	if uint64(len(payload)) != h.PayloadLen {
		return ErrLength
	}
	pc, ok := checksum(kind, payload)
	if !ok {
		return ErrChecksumKind
	}
	if pc != h.PayloadCRC {
		return ErrChecksum
	}
	return nil
}

// Sealed reports whether the segment is sealed (no more appends).
func (h *SegHeader) Sealed() bool { return h.Flags&SegSealed != 0 }
