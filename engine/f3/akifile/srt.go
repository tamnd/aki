package akifile

// srtMagic frames a shard root table so a repair scan recognizes it.
const srtMagic = "SRT3"

// SRTSnapshotRoot is the SRT header flag marking a table committed as the root of a
// point-in-time snapshot (spec 2064/f3/07 section 5, snapshot protocol step 3). When
// it is set the table's SnapWbar names the barrier watermark the snapshot cuts at, so
// the copy path finds the cut through the live meta -> SRT chain without scanning, and
// the 128-byte meta slot stays untouched.
const SRTSnapshotRoot = 1 << 0

// SRTRow is shard k's checkpoint roots and replay entry point (spec 2064/f3/07
// section 3). The writer mutates the table only at checkpoint commits, never on
// the data path.
type SRTRow struct {
	IndexCkptOff uint64
	IndexCkptLen uint64
	ChunkdirOff  uint64
	ChunkdirLen  uint64
	SegstatsOff  uint64
	SegstatsLen  uint64
	CkptLogPos   uint64 // global_seq the checkpoints are consistent up to; replay starts here
	ShardSeqHigh uint64
	FirstTailSeg uint64 // first segment past CkptLogPos, the forward replay entry point
	LiveRecords  uint64
}

// SRT is the shard root table: a small header plus one row per shard, written to
// free space and swapped in by a meta flip. It carries the N checkpoint roots one
// 128-byte meta slot cannot. SnapWbar and Flags carry the snapshot cut: a table
// committed as a snapshot root sets SRTSnapshotRoot and names the barrier watermark
// in SnapWbar, so the meta slot needs no snapshot field of its own.
type SRT struct {
	Gen      uint64
	SnapWbar uint64 // barrier watermark when Flags has SRTSnapshotRoot, else zero
	Flags    uint32
	Rows     []SRTRow
}

// IsSnapshotRoot reports whether the table was committed as the root of a snapshot,
// in which case SnapWbar is the barrier watermark the snapshot cuts at.
func (s *SRT) IsSnapshotRoot() bool { return s.Flags&SRTSnapshotRoot != 0 }

// Marshal encodes the table. The crc word sits at the end of the header (bytes
// 32..40), so it covers the header prefix (bytes 0..32) and every row (bytes 40..end),
// the whole table with its own crc word excluded.
func (s *SRT) Marshal(kind uint32) ([]byte, error) {
	b := make([]byte, SRTHeaderLen+len(s.Rows)*SRTRowSize)
	copy(b[0:4], srtMagic)
	le.PutUint32(b[4:], uint32(len(s.Rows)))
	le.PutUint64(b[8:], s.Gen)
	le.PutUint64(b[16:], s.SnapWbar)
	le.PutUint32(b[24:], s.Flags)
	// b[28:32] reserved, left zero.
	off := SRTHeaderLen
	for i := range s.Rows {
		putSRTRow(b[off:], &s.Rows[i])
		off += SRTRowSize
	}
	sum, ok := checksum(kind, b[0:32], b[SRTHeaderLen:])
	if !ok {
		return nil, ErrChecksumKind
	}
	le.PutUint64(b[32:], sum)
	return b, nil
}

// ParseSRT validates the magic and checksum and decodes the rows.
func ParseSRT(b []byte, kind uint32) (*SRT, error) {
	if len(b) < SRTHeaderLen {
		return nil, ErrShort
	}
	if string(b[0:4]) != srtMagic {
		return nil, ErrMagic
	}
	n := int(le.Uint32(b[4:]))
	if len(b) < SRTHeaderLen+n*SRTRowSize {
		return nil, ErrShort
	}
	sum, ok := checksum(kind, b[0:32], b[SRTHeaderLen:SRTHeaderLen+n*SRTRowSize])
	if !ok {
		return nil, ErrChecksumKind
	}
	if sum != le.Uint64(b[32:]) {
		return nil, ErrChecksum
	}
	s := &SRT{
		Gen:      le.Uint64(b[8:]),
		SnapWbar: le.Uint64(b[16:]),
		Flags:    le.Uint32(b[24:]),
		Rows:     make([]SRTRow, n),
	}
	off := SRTHeaderLen
	for i := 0; i < n; i++ {
		getSRTRow(b[off:], &s.Rows[i])
		off += SRTRowSize
	}
	return s, nil
}

func putSRTRow(b []byte, r *SRTRow) {
	le.PutUint64(b[0:], r.IndexCkptOff)
	le.PutUint64(b[8:], r.IndexCkptLen)
	le.PutUint64(b[16:], r.ChunkdirOff)
	le.PutUint64(b[24:], r.ChunkdirLen)
	le.PutUint64(b[32:], r.SegstatsOff)
	le.PutUint64(b[40:], r.SegstatsLen)
	le.PutUint64(b[48:], r.CkptLogPos)
	le.PutUint64(b[56:], r.ShardSeqHigh)
	le.PutUint64(b[64:], r.FirstTailSeg)
	le.PutUint64(b[72:], r.LiveRecords)
}

func getSRTRow(b []byte, r *SRTRow) {
	r.IndexCkptOff = le.Uint64(b[0:])
	r.IndexCkptLen = le.Uint64(b[8:])
	r.ChunkdirOff = le.Uint64(b[16:])
	r.ChunkdirLen = le.Uint64(b[24:])
	r.SegstatsOff = le.Uint64(b[32:])
	r.SegstatsLen = le.Uint64(b[40:])
	r.CkptLogPos = le.Uint64(b[48:])
	r.ShardSeqHigh = le.Uint64(b[56:])
	r.FirstTailSeg = le.Uint64(b[64:])
	r.LiveRecords = le.Uint64(b[72:])
}
