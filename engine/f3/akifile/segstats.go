package akifile

// The seg-stats codec (spec 2064/f3/07 section 6, "Dead-byte accounting that
// survives restart"). A seg_stats segment payload is one shard's per-segment
// (live_bytes, dead_bytes) table bound to a log position: a 32-byte header, then a
// run of 32-byte entries. It is the durable half of the O10 fix: f1 kept these
// counters in memory, so a restart zeroed them and the store under-triggered
// compaction until organic churn rediscovered the garbage. f3 checkpoints the table
// full-or-delta, pinned to ckpt_log_pos like every other root, and tail replay
// re-derives the deltas past the cut, so a reopened store knows exactly how much
// dead weight each segment carries.
//
// The payload carries no checksum of its own; the seg_stats segment header's
// payload CRC already covers it, so the SST3 magic here is a kind cross-check, not
// an integrity field. Like the checkpoint and value-log codecs, this is codec only:
// it frames into and reads out of a caller-owned payload and never touches a File.
// The consumer that dumps a live shard's segment table into these segments and the
// replay path that re-derives the tail deltas are separate slices.

// SegStatsMagic is the seg_stats payload sentinel.
const SegStatsMagic = "SST3"

const (
	// SegStatsHeaderLen is the fixed seg-stats header size.
	SegStatsHeaderLen = 32
	// SegStatsEntrySize is one segment's accounting: seg_off u64, live_bytes u64,
	// dead_bytes u64, flags u32, and 4 reserved bytes.
	SegStatsEntrySize = 32
)

// Seg-stats kinds (the full_or_delta byte), the same full-or-delta shape the index
// checkpoint uses.
const (
	SegStatsFull  uint8 = 1 // a full table: every tracked segment, base_ckpt_off zero
	SegStatsDelta uint8 = 2 // a delta over a base: only segments whose accounting moved
)

// SegStatsFreed marks a delta entry as a reclaim: the segment was compacted away, so
// its row is dropped from the table when the delta is applied over its base.
const SegStatsFreed uint32 = 1 << 0

// SegStatsHeader binds a table to the log position it is consistent up to. A delta
// names the base it extends; a full leaves base zero.
type SegStatsHeader struct {
	FullOrDelta uint8  // SegStatsFull or SegStatsDelta
	CkptLogPos  uint64 // the global_seq this table is consistent up to; replay re-derives past it
	EntryCount  uint64 // entries that follow the header
	BaseCkptOff uint64 // for a delta, the seg-stats segment it extends; 0 for a full
}

// SegStatsEntry is one segment's live-and-dead accounting. A freed segment appears
// in a delta with the SegStatsFreed flag and is removed from the table on apply.
type SegStatsEntry struct {
	SegOff    uint64 // the segment this row accounts for
	LiveBytes uint64 // bytes still referenced by a live index entry
	DeadBytes uint64 // bytes superseded, the compaction trigger's fuel
	Flags     uint32 // SegStatsFreed or zero
}

// AppendSegStatsHeader frames a seg-stats header onto dst. Entries follow with
// AppendSegStatsEntry, so a large table streams out in bounded slices without ever
// holding every row in memory at once.
func AppendSegStatsHeader(dst []byte, h SegStatsHeader) []byte {
	var b [SegStatsHeaderLen]byte
	copy(b[0:4], SegStatsMagic)
	b[4] = h.FullOrDelta
	// b[5:8] reserved, left zero.
	le.PutUint64(b[8:16], h.CkptLogPos)
	le.PutUint64(b[16:24], h.EntryCount)
	le.PutUint64(b[24:32], h.BaseCkptOff)
	return append(dst, b[:]...)
}

// AppendSegStatsEntry frames one 32-byte accounting row onto dst.
func AppendSegStatsEntry(dst []byte, e SegStatsEntry) []byte {
	var b [SegStatsEntrySize]byte
	le.PutUint64(b[0:8], e.SegOff)
	le.PutUint64(b[8:16], e.LiveBytes)
	le.PutUint64(b[16:24], e.DeadBytes)
	le.PutUint32(b[24:28], e.Flags)
	// b[28:32] reserved, left zero.
	return append(dst, b[:]...)
}

// ParseSegStatsHeader decodes and validates a seg-stats header: the magic, a known
// full-or-delta kind, and the full-table invariant that a full carries no base.
func ParseSegStatsHeader(b []byte) (SegStatsHeader, error) {
	if len(b) < SegStatsHeaderLen {
		return SegStatsHeader{}, ErrShort
	}
	if string(b[0:4]) != SegStatsMagic {
		return SegStatsHeader{}, ErrMagic
	}
	h := SegStatsHeader{
		FullOrDelta: b[4],
		CkptLogPos:  le.Uint64(b[8:16]),
		EntryCount:  le.Uint64(b[16:24]),
		BaseCkptOff: le.Uint64(b[24:32]),
	}
	switch h.FullOrDelta {
	case SegStatsFull:
		if h.BaseCkptOff != 0 {
			return SegStatsHeader{}, ErrSegStats
		}
	case SegStatsDelta:
		// a delta may name any base, including 0 for a delta over the empty table
	default:
		return SegStatsHeader{}, ErrSegStats
	}
	return h, nil
}

// ParseSegStatsEntry decodes one 32-byte accounting row.
func ParseSegStatsEntry(b []byte) (SegStatsEntry, error) {
	if len(b) < SegStatsEntrySize {
		return SegStatsEntry{}, ErrShort
	}
	return SegStatsEntry{
		SegOff:    le.Uint64(b[0:8]),
		LiveBytes: le.Uint64(b[8:16]),
		DeadBytes: le.Uint64(b[16:24]),
		Flags:     le.Uint32(b[24:28]),
	}, nil
}

// SegStatsEntries decodes every row in a seg-stats payload after its header, the
// load path that restores the accounting table from a full or delta. It bounds
// entry_count against the payload so a corrupt count cannot over-read; a count that
// outruns the bytes is ErrLength.
func SegStatsEntries(payload []byte, h SegStatsHeader) ([]SegStatsEntry, error) {
	if uint64(len(payload)) < SegStatsHeaderLen {
		return nil, ErrShort
	}
	avail := (uint64(len(payload)) - SegStatsHeaderLen) / SegStatsEntrySize
	if h.EntryCount > avail {
		return nil, ErrLength
	}
	entries := make([]SegStatsEntry, h.EntryCount)
	off := uint64(SegStatsHeaderLen)
	for i := range entries {
		e, err := ParseSegStatsEntry(payload[off : off+SegStatsEntrySize])
		if err != nil {
			return nil, err
		}
		entries[i] = e
		off += SegStatsEntrySize
	}
	return entries, nil
}
