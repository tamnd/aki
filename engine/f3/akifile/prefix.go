package akifile

// Prefix is the immutable header written once at create and never rewritten in
// normal operation (spec 2064/f3/07 section 3). It is self-describing: a reader
// that knows the format finds every region from page zero, and validates the
// two prefix CRCs before it trusts a single other byte.
type Prefix struct {
	FormatMajor      uint16
	FormatMinor      uint16
	PageSize         uint32
	SegmentAlign     uint32
	Flags            uint32
	ChecksumKind     uint32
	ShardCount       uint32
	CreatedUnixNanos uint64
	StoreUUID        [16]byte
	SlotCount        uint32
	SepThreshold     uint32
	MetaSlotAOff     uint64
	MetaSlotBOff     uint64
	MetaSlotSize     uint32
}

// NewPrefix builds a create-time prefix with the fixed defaults filled in: the
// caller supplies only what identifies this file. The meta slots land in their
// default torn-safe sectors and the checksum kind is the shipped CRC32C.
func NewPrefix(shardCount, sepThreshold uint32, uuid [16]byte, createdUnixNanos uint64) *Prefix {
	return &Prefix{
		FormatMajor:      FormatMajor,
		FormatMinor:      FormatMinor,
		PageSize:         PageSize,
		SegmentAlign:     SegmentAlign,
		ChecksumKind:     ChecksumCRC32C,
		ShardCount:       shardCount,
		CreatedUnixNanos: createdUnixNanos,
		StoreUUID:        uuid,
		SlotCount:        SlotCount,
		SepThreshold:     sepThreshold,
		MetaSlotAOff:     MetaSlotAOff,
		MetaSlotBOff:     MetaSlotBOff,
		MetaSlotSize:     MetaSlotSize,
	}
}

// Marshal encodes the prefix into its fixed 100-byte region. Both prefix CRCs
// are CRC32C regardless of ChecksumKind, so a reader can gate on them before it
// knows whether it can even compute the file's body checksum. header_crc covers
// bytes 0..72 (the fields a reader needs to size the file); prefix_crc covers
// bytes 0..96 (through the meta-slot pointers), the final gate on the header.
func (p *Prefix) Marshal() []byte {
	b := make([]byte, PrefixSize)
	copy(b[0:16], Magic)
	le.PutUint16(b[16:], p.FormatMajor)
	le.PutUint16(b[18:], p.FormatMinor)
	le.PutUint32(b[20:], p.PageSize)
	le.PutUint32(b[24:], p.SegmentAlign)
	le.PutUint32(b[28:], p.Flags)
	le.PutUint32(b[32:], p.ChecksumKind)
	le.PutUint32(b[36:], p.ShardCount)
	le.PutUint64(b[40:], p.CreatedUnixNanos)
	copy(b[48:64], p.StoreUUID[:])
	le.PutUint32(b[64:], p.SlotCount)
	le.PutUint32(b[68:], p.SepThreshold)
	le.PutUint32(b[72:], crc32c(b[0:72])) // header_crc

	le.PutUint64(b[76:], p.MetaSlotAOff)
	le.PutUint64(b[84:], p.MetaSlotBOff)
	le.PutUint32(b[92:], p.MetaSlotSize)
	le.PutUint32(b[96:], crc32c(b[0:96])) // prefix_crc
	return b
}

// ParsePrefix validates and decodes the immutable prefix: magic first (never
// guess past a bad magic), then both CRCs, then the format major. A higher minor
// is left for the caller to gate on read-only vs writer (section 10).
func ParsePrefix(b []byte) (*Prefix, error) {
	if len(b) < PrefixSize {
		return nil, ErrShort
	}
	if string(b[0:16]) != Magic {
		return nil, ErrMagic
	}
	if crc32c(b[0:72]) != le.Uint32(b[72:]) {
		return nil, ErrChecksum
	}
	if crc32c(b[0:96]) != le.Uint32(b[96:]) {
		return nil, ErrChecksum
	}
	if major := le.Uint16(b[16:]); major != FormatMajor {
		return nil, ErrMajor
	}
	p := &Prefix{
		FormatMajor:      le.Uint16(b[16:]),
		FormatMinor:      le.Uint16(b[18:]),
		PageSize:         le.Uint32(b[20:]),
		SegmentAlign:     le.Uint32(b[24:]),
		Flags:            le.Uint32(b[28:]),
		ChecksumKind:     le.Uint32(b[32:]),
		ShardCount:       le.Uint32(b[36:]),
		CreatedUnixNanos: le.Uint64(b[40:]),
		SlotCount:        le.Uint32(b[64:]),
		SepThreshold:     le.Uint32(b[68:]),
		MetaSlotAOff:     le.Uint64(b[76:]),
		MetaSlotBOff:     le.Uint64(b[84:]),
		MetaSlotSize:     le.Uint32(b[92:]),
	}
	copy(p.StoreUUID[:], b[48:64])
	return p, nil
}
