package akifile

import "io"

// Pending is one segment staged for the next group append. The caller owns every
// field except global_seq, which the writer assigns in group order so the file's
// one piece of cross-shard state stays monotonic (spec 2064/f3/07 section 2).
type Pending struct {
	Shard        uint16
	Kind         uint16
	ShardSeq     uint64
	PrevShardSeg uint64
	TTLClass     uint32
	Flags        uint32
	Payload      []byte
}

// AppendGroup lays every segment in the group down as one 4KiB-aligned run,
// assigns each a strictly increasing global_seq, and issues a single fsync for
// the whole group under the file's policy: the group-commit amplification win
// (one flush for B records, section 2). It returns the assigned offsets in group
// order. A write error stops the group; the cursor is left past the last segment
// that landed whole, and a later scan truncates any torn tail.
func (f *File) AppendGroup(group []Pending) ([]uint64, error) {
	if len(group) == 0 {
		return nil, nil
	}
	offs := make([]uint64, len(group))
	for i := range group {
		p := &group[i]
		f.globalSeq++
		h := &SegHeader{
			Shard:        p.Shard,
			Kind:         p.Kind,
			GlobalSeq:    f.globalSeq,
			ShardSeq:     p.ShardSeq,
			PrevShardSeg: p.PrevShardSeg,
			TTLClass:     p.TTLClass,
			Flags:        p.Flags,
		}
		hb, err := h.Marshal(f.prefix.ChecksumKind, p.Payload)
		if err != nil {
			f.globalSeq--
			return offs[:i], err
		}
		off := f.cursor
		if err := f.writeAt(hb, off); err != nil {
			return offs[:i], err
		}
		if err := f.writeAt(p.Payload, off+SegHeaderLen); err != nil {
			return offs[:i], err
		}
		offs[i] = off
		// The gap from the end of the payload to the next 4KiB boundary is left
		// as a hole: a scan skips it by SegmentSpan, and it reads back as zero.
		f.cursor = off + SegmentSpan(uint64(len(p.Payload)))
	}
	if err := f.maybeSync(); err != nil {
		return offs, err
	}
	return offs, nil
}

// ReadSegmentAt reads and fully validates the segment at off: the 64-byte header
// (magic and header_crc), then its payload against the framed length and the
// payload CRC. A torn segment returns ErrChecksum with the parsed header so a
// caller can see how far it got; a clean read returns a copy of the payload.
func (f *File) ReadSegmentAt(off uint64) (*SegHeader, []byte, error) {
	hb := make([]byte, SegHeaderLen)
	if _, err := f.dev.ReadAt(hb, int64(off)); err != nil {
		return nil, nil, err
	}
	h, err := ParseSegHeader(hb)
	if err != nil {
		return nil, nil, err
	}
	payload := make([]byte, h.PayloadLen)
	if _, err := f.dev.ReadAt(payload, int64(off)+SegHeaderLen); err != nil {
		return h, nil, err
	}
	if err := h.VerifyPayload(payload, f.prefix.ChecksumKind); err != nil {
		return h, nil, err
	}
	return h, payload, nil
}

func (f *File) writeAt(b []byte, off uint64) error {
	n, err := f.dev.WriteAt(b, int64(off))
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}

func (f *File) maybeSync() error {
	switch f.sync {
	case SyncAlways:
		return f.doSync()
	case SyncEverySec:
		if f.now().Sub(f.lastSync) >= f.interval {
			return f.doSync()
		}
		return nil
	default: // SyncNo
		return nil
	}
}

func (f *File) doSync() error {
	if err := f.dev.Sync(); err != nil {
		return err
	}
	f.lastSync = f.now()
	return nil
}

// scanTail walks the append space from the header page forward on the 4KiB grid
// and returns the offset just past the last intact segment plus the highest
// global_seq seen. It stops at the first header that fails to parse or a payload
// that does not verify: the durable tail, past which lies a torn or never-synced
// write. Full per-shard parallel recovery (slice 5) generalizes this; here it
// bootstraps the writer's cursor and seq on open.
func scanTail(dev Device, prefix *Prefix, size uint64) (cursor, seq uint64) {
	cursor = uint64(prefix.PageSize)
	for cursor+SegHeaderLen <= size {
		hb := make([]byte, SegHeaderLen)
		if _, err := dev.ReadAt(hb, int64(cursor)); err != nil {
			break
		}
		h, err := ParseSegHeader(hb)
		if err != nil {
			break
		}
		if cursor+SegHeaderLen+h.PayloadLen > size {
			break
		}
		payload := make([]byte, h.PayloadLen)
		if _, err := dev.ReadAt(payload, int64(cursor)+SegHeaderLen); err != nil {
			break
		}
		if h.VerifyPayload(payload, prefix.ChecksumKind) != nil {
			break
		}
		if h.GlobalSeq > seq {
			seq = h.GlobalSeq
		}
		cursor += SegmentSpan(h.PayloadLen)
	}
	return cursor, seq
}
