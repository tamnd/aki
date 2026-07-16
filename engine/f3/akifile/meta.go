package akifile

// MetaSlot is one of the two 128-byte roots that take turns being live (LMDB
// lineage, spec 2064/f3/07 section 3). The higher valid commit_seq wins on open;
// because the two slots sit in separate sectors and a commit writes only the
// stale one, a torn slot write damages at most one and the previous root stays
// intact by construction.
type MetaSlot struct {
	CommitSeq      uint64
	GlobalSeq      uint64
	SRTOff         uint64
	SRTLen         uint32
	SRTShardCount  uint32
	ExtentTableOff uint64
	ExtentTableLen uint32
	TTLIndexLen    uint32
	TTLIndexOff    uint64
	FreeMapOff     uint64
	FileSize       uint64
	LiveBytes      uint64
	DeadBytes      uint64
	RecordCount    uint64
	LastCkptUnix   uint64
	CleanShutdown  uint8
}

// Marshal encodes the slot and stamps its checksum over bytes 0..120 under the
// file's checksum kind. It errors only if the kind is one this build cannot
// compute, so a caller never writes a slot with a silently-wrong sum.
func (m *MetaSlot) Marshal(kind uint32) ([]byte, error) {
	b := make([]byte, MetaSlotSize)
	le.PutUint64(b[0:], m.CommitSeq)
	le.PutUint64(b[8:], m.GlobalSeq)
	le.PutUint64(b[16:], m.SRTOff)
	le.PutUint32(b[24:], m.SRTLen)
	le.PutUint32(b[28:], m.SRTShardCount)
	le.PutUint64(b[32:], m.ExtentTableOff)
	le.PutUint32(b[40:], m.ExtentTableLen)
	le.PutUint32(b[44:], m.TTLIndexLen)
	le.PutUint64(b[48:], m.TTLIndexOff)
	le.PutUint64(b[56:], m.FreeMapOff)
	le.PutUint64(b[64:], m.FileSize)
	le.PutUint64(b[72:], m.LiveBytes)
	le.PutUint64(b[80:], m.DeadBytes)
	le.PutUint64(b[88:], m.RecordCount)
	le.PutUint64(b[96:], m.LastCkptUnix)
	b[104] = m.CleanShutdown
	sum, ok := checksum(kind, b[0:120])
	if !ok {
		return nil, ErrChecksumKind
	}
	le.PutUint64(b[120:], sum)
	return b, nil
}

// ParseMetaSlot validates the slot checksum and decodes it. A mismatch is
// ErrChecksum, which the caller reads as "this slot is invalid, use the other".
func ParseMetaSlot(b []byte, kind uint32) (*MetaSlot, error) {
	if len(b) < MetaSlotSize {
		return nil, ErrShort
	}
	sum, ok := checksum(kind, b[0:120])
	if !ok {
		return nil, ErrChecksumKind
	}
	if sum != le.Uint64(b[120:]) {
		return nil, ErrChecksum
	}
	return &MetaSlot{
		CommitSeq:      le.Uint64(b[0:]),
		GlobalSeq:      le.Uint64(b[8:]),
		SRTOff:         le.Uint64(b[16:]),
		SRTLen:         le.Uint32(b[24:]),
		SRTShardCount:  le.Uint32(b[28:]),
		ExtentTableOff: le.Uint64(b[32:]),
		ExtentTableLen: le.Uint32(b[40:]),
		TTLIndexLen:    le.Uint32(b[44:]),
		TTLIndexOff:    le.Uint64(b[48:]),
		FreeMapOff:     le.Uint64(b[56:]),
		FileSize:       le.Uint64(b[64:]),
		LiveBytes:      le.Uint64(b[72:]),
		DeadBytes:      le.Uint64(b[80:]),
		RecordCount:    le.Uint64(b[88:]),
		LastCkptUnix:   le.Uint64(b[96:]),
		CleanShutdown:  b[104],
	}, nil
}

// MetaLive picks the live root from the two raw slot buffers: the valid slot
// with the higher commit_seq. which is 0 for slot A, 1 for slot B. If neither
// slot validates it returns ErrChecksum and the caller falls back to a full
// scan; the returned error is slot A's so the caller can log the first fault.
func MetaLive(a, b []byte, kind uint32) (live *MetaSlot, which int, err error) {
	ma, ea := ParseMetaSlot(a, kind)
	mb, eb := ParseMetaSlot(b, kind)
	switch {
	case ea == nil && eb == nil:
		if mb.CommitSeq > ma.CommitSeq {
			return mb, 1, nil
		}
		return ma, 0, nil
	case ea == nil:
		return ma, 0, nil
	case eb == nil:
		return mb, 1, nil
	default:
		return nil, -1, ea
	}
}
