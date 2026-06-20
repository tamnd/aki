package format

import "github.com/tamnd/aki/encoding"

// FileHeader is the 128-byte header on page 0 of a .aki file (doc 02 §4). It is
// written atomically at create time; only the fields the spec marks mutable
// change afterward, and any change recomputes HeaderChecksum.
type FileHeader struct {
	// Magic is always format.Magic; ParseFileHeader rejects anything else.
	Magic [16]byte

	FormatVersion   uint16 // offset 16
	MinReadVersion  uint16 // offset 18
	MinWriteVersion uint16 // offset 20
	PageSize        uint32 // offset 22
	PageCount       uint32 // offset 26 (mutable)
	FreelistHead    uint32 // offset 30 (mutable)
	FreelistCount   uint32 // offset 34 (mutable)
	CatalogRoot     uint32 // offset 38 (mutable)
	DBCount         uint32 // offset 42
	ChangeCounter   uint64 // offset 46 (mutable)
	FileCreateTime  uint64 // offset 54 (microseconds)
	WALCheckpoint   uint64 // offset 62 (mutable)
	DefaultCodec    uint8  // offset 70
	EncryptionID    uint8  // offset 71
	KDFSalt         [16]byte
	KDFParams       uint32 // offset 88
	FileFlags       uint32 // offset 92
	MetaPageA       uint32 // offset 96
	MetaPageB       uint32 // offset 100
	SchemaVersion   uint32 // offset 104 (mutable)
	UserVersion     uint32 // offset 108 (mutable)
	AutovacuumMode  uint32 // offset 112
	// HeaderChecksum at offset 124 is CRC-32C of bytes 0..123; it is computed
	// by MarshalTo and verified by ParseFileHeader, not set by callers.
	HeaderChecksum uint32
}

// NewFileHeader returns a header for a freshly created file with the given page
// size, database count, and creation timestamp (in microseconds). Pointer
// fields start at their empty sentinels and the meta pages are fixed at 1 and 2.
func NewFileHeader(pageSize, dbCount uint32, createTimeUS uint64) FileHeader {
	return FileHeader{
		Magic:           Magic,
		FormatVersion:   FormatVersion,
		MinReadVersion:  FormatVersion,
		MinWriteVersion: FormatVersion,
		PageSize:        pageSize,
		PageCount:       3, // page 0 header + meta pages 1 and 2
		FreelistHead:    NullPage,
		FreelistCount:   0,
		CatalogRoot:     NullPage,
		DBCount:         dbCount,
		ChangeCounter:   0,
		FileCreateTime:  createTimeUS,
		WALCheckpoint:   0,
		DefaultCodec:    CodecNone,
		EncryptionID:    EncryptionNone,
		KDFParams:       0,
		FileFlags:       0,
		MetaPageA:       MetaPageA,
		MetaPageB:       MetaPageB,
		SchemaVersion:   0,
		UserVersion:     0,
		AutovacuumMode:  0,
	}
}

// MarshalTo writes the header into the first HeaderSize bytes of b and fills in
// the trailing CRC-32C over bytes 0..123. The remainder of the page (b past
// byte 128) is left untouched; callers zero-pad the page.
func (h FileHeader) MarshalTo(b []byte) error {
	if len(b) < HeaderSize {
		return ErrShortBuffer
	}
	// Zero the header region so reserved bytes are deterministic.
	for i := range HeaderSize {
		b[i] = 0
	}
	copy(b[0:16], h.Magic[:])
	encoding.PutU16(b[16:], h.FormatVersion)
	encoding.PutU16(b[18:], h.MinReadVersion)
	encoding.PutU16(b[20:], h.MinWriteVersion)
	encoding.PutU32(b[22:], h.PageSize)
	encoding.PutU32(b[26:], h.PageCount)
	encoding.PutU32(b[30:], h.FreelistHead)
	encoding.PutU32(b[34:], h.FreelistCount)
	encoding.PutU32(b[38:], h.CatalogRoot)
	encoding.PutU32(b[42:], h.DBCount)
	encoding.PutU64(b[46:], h.ChangeCounter)
	encoding.PutU64(b[54:], h.FileCreateTime)
	encoding.PutU64(b[62:], h.WALCheckpoint)
	b[70] = h.DefaultCodec
	b[71] = h.EncryptionID
	copy(b[72:88], h.KDFSalt[:])
	encoding.PutU32(b[88:], h.KDFParams)
	encoding.PutU32(b[92:], h.FileFlags)
	encoding.PutU32(b[96:], h.MetaPageA)
	encoding.PutU32(b[100:], h.MetaPageB)
	encoding.PutU32(b[104:], h.SchemaVersion)
	encoding.PutU32(b[108:], h.UserVersion)
	encoding.PutU32(b[112:], h.AutovacuumMode)
	// bytes 116..123 are reserved and already zeroed.
	sum := crc32c(b[0:124])
	encoding.PutU32(b[124:], sum)
	return nil
}

// ParseFileHeader reads and validates a header from the front of b. It checks
// the magic and the CRC-32C; a mismatch returns ErrBadMagic or ErrBadChecksum.
func ParseFileHeader(b []byte) (FileHeader, error) {
	if len(b) < HeaderSize {
		return FileHeader{}, ErrShortBuffer
	}
	var h FileHeader
	copy(h.Magic[:], b[0:16])
	if h.Magic != Magic {
		return FileHeader{}, ErrBadMagic
	}
	stored := encoding.U32(b[124:])
	if crc32c(b[0:124]) != stored {
		return FileHeader{}, ErrBadChecksum
	}
	h.FormatVersion = encoding.U16(b[16:])
	h.MinReadVersion = encoding.U16(b[18:])
	h.MinWriteVersion = encoding.U16(b[20:])
	h.PageSize = encoding.U32(b[22:])
	h.PageCount = encoding.U32(b[26:])
	h.FreelistHead = encoding.U32(b[30:])
	h.FreelistCount = encoding.U32(b[34:])
	h.CatalogRoot = encoding.U32(b[38:])
	h.DBCount = encoding.U32(b[42:])
	h.ChangeCounter = encoding.U64(b[46:])
	h.FileCreateTime = encoding.U64(b[54:])
	h.WALCheckpoint = encoding.U64(b[62:])
	h.DefaultCodec = b[70]
	h.EncryptionID = b[71]
	copy(h.KDFSalt[:], b[72:88])
	h.KDFParams = encoding.U32(b[88:])
	h.FileFlags = encoding.U32(b[92:])
	h.MetaPageA = encoding.U32(b[96:])
	h.MetaPageB = encoding.U32(b[100:])
	h.SchemaVersion = encoding.U32(b[104:])
	h.UserVersion = encoding.U32(b[108:])
	h.AutovacuumMode = encoding.U32(b[112:])
	h.HeaderChecksum = stored
	return h, nil
}
