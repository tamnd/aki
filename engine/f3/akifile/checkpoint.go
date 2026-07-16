package akifile

// The index checkpoint codec (spec 2064/f3/07 section 5). An index_ckpt segment
// payload is a compact dump of one shard's index bound to a log position: a
// 48-byte header, then a run of 20-byte entries. A checkpoint bounds recovery,
// which without one replays every segment ever written and with one loads the
// dump and replays only the tail past ckpt_log_pos.
//
// The payload carries no checksum of its own; the index_ckpt segment header's
// payload CRC already covers it, so the CKP3 magic here is a kind cross-check,
// not an integrity field. Keys are not stored: an entry points at a record that
// carries its own key, and verify-on-read catches a hash collision.
//
// Codec only, like the value-log and chunk codecs before their consumers: it
// frames into and reads out of a caller-owned payload and never touches a File.
// The forkless-checkpoint slice that dumps a live shard index into these
// segments is the consumer.

// CkptMagic is the index_ckpt payload sentinel.
const CkptMagic = "CKP3"

const (
	// CkptHeaderLen is the fixed checkpoint header size.
	CkptHeaderLen = 48
	// CkptEntrySize is one index entry: key_hash u64, record_addr u64, slot u16,
	// flags u16.
	CkptEntrySize = 20
)

// Checkpoint kinds (the full_or_delta byte).
const (
	CkptFull  uint8 = 1 // a full dump: every live entry, base_ckpt_off zero
	CkptDelta uint8 = 2 // a delta over a base: only entries written since it
)

// CkptTombstone marks a delta entry as a delete: the key's index entry is
// removed when the delta is applied over its base.
const CkptTombstone uint16 = 1 << 0

// CkptHeader binds a dump to the log position it is consistent up to. A delta
// names the base it extends; a full leaves base zero.
type CkptHeader struct {
	FullOrDelta uint8  // CkptFull or CkptDelta
	CkptLogPos  uint64 // the global_seq replay resumes from
	EntryCount  uint64 // entries that follow the header
	BucketCount uint64 // index capacity at dump time, so open sizes without a rehash storm
	BaseCkptOff uint64 // for a delta, the checkpoint it extends; 0 for a full
	SeqHigh     uint64 // highest shard record sequence reflected, a cross-check
}

// CkptEntry is one index slot: where a key's record lives and which bucket held
// it. The address is tier-tagged by the store (a bit selects hot arena versus
// file offset); the codec carries the word verbatim.
type CkptEntry struct {
	KeyHash    uint64
	RecordAddr uint64
	Slot       uint16
	Flags      uint16
}

// AppendCkptHeader frames a checkpoint header onto dst. Entries follow with
// AppendCkptEntry, so a large dump streams out in bounded slices without ever
// holding every entry in memory at once.
func AppendCkptHeader(dst []byte, h CkptHeader) []byte {
	var b [CkptHeaderLen]byte
	copy(b[0:4], CkptMagic)
	b[4] = h.FullOrDelta
	// b[5:8] reserved, left zero.
	le.PutUint64(b[8:16], h.CkptLogPos)
	le.PutUint64(b[16:24], h.EntryCount)
	le.PutUint64(b[24:32], h.BucketCount)
	le.PutUint64(b[32:40], h.BaseCkptOff)
	le.PutUint64(b[40:48], h.SeqHigh)
	return append(dst, b[:]...)
}

// AppendCkptEntry frames one 20-byte index entry onto dst.
func AppendCkptEntry(dst []byte, e CkptEntry) []byte {
	var b [CkptEntrySize]byte
	le.PutUint64(b[0:8], e.KeyHash)
	le.PutUint64(b[8:16], e.RecordAddr)
	le.PutUint16(b[16:18], e.Slot)
	le.PutUint16(b[18:20], e.Flags)
	return append(dst, b[:]...)
}

// ParseCkptHeader decodes and validates a checkpoint header: the magic, a known
// full-or-delta kind, and the full-dump invariant that a full carries no base.
func ParseCkptHeader(b []byte) (CkptHeader, error) {
	if len(b) < CkptHeaderLen {
		return CkptHeader{}, ErrShort
	}
	if string(b[0:4]) != CkptMagic {
		return CkptHeader{}, ErrMagic
	}
	h := CkptHeader{
		FullOrDelta: b[4],
		CkptLogPos:  le.Uint64(b[8:16]),
		EntryCount:  le.Uint64(b[16:24]),
		BucketCount: le.Uint64(b[24:32]),
		BaseCkptOff: le.Uint64(b[32:40]),
		SeqHigh:     le.Uint64(b[40:48]),
	}
	switch h.FullOrDelta {
	case CkptFull:
		if h.BaseCkptOff != 0 {
			return CkptHeader{}, ErrCheckpoint
		}
	case CkptDelta:
		// a delta may name any base, including 0 for a delta over the empty index
	default:
		return CkptHeader{}, ErrCheckpoint
	}
	return h, nil
}

// ParseCkptEntry decodes one 20-byte index entry.
func ParseCkptEntry(b []byte) (CkptEntry, error) {
	if len(b) < CkptEntrySize {
		return CkptEntry{}, ErrShort
	}
	return CkptEntry{
		KeyHash:    le.Uint64(b[0:8]),
		RecordAddr: le.Uint64(b[8:16]),
		Slot:       le.Uint16(b[16:18]),
		Flags:      le.Uint16(b[18:20]),
	}, nil
}

// CkptEntries decodes every entry in a checkpoint payload after its header, the
// load path that restores an index from a full or delta. It bounds entry_count
// against the payload so a corrupt count cannot over-read; a count that outruns
// the bytes is ErrLength.
func CkptEntries(payload []byte, h CkptHeader) ([]CkptEntry, error) {
	if uint64(len(payload)) < CkptHeaderLen {
		return nil, ErrShort
	}
	avail := (uint64(len(payload)) - CkptHeaderLen) / CkptEntrySize
	if h.EntryCount > avail {
		return nil, ErrLength
	}
	entries := make([]CkptEntry, h.EntryCount)
	off := uint64(CkptHeaderLen)
	for i := range entries {
		e, err := ParseCkptEntry(payload[off : off+CkptEntrySize])
		if err != nil {
			return nil, err
		}
		entries[i] = e
		off += CkptEntrySize
	}
	return entries, nil
}
