package akifile

// The value-log region on the File: the bridge between the frame codec
// (valuelog.go) and the append writer. A batch of separated values packs into
// one value_log segment through the group-commit path, and each value comes back
// as a ValuePointer carrying an absolute file offset a record can keep in place
// of its bytes (F5, spec 2064/f3/07 section 4).
//
// This is the codec's first consumer but still store-agnostic: it turns values
// into pointers and pointers back into values against the File, and the slice
// that redirects the store's per-shard vlog appends here is the next consumer up.

// AppendValues frames every value in vals into one value_log segment owned by
// shard, appends the segment through the group-commit writer (one header, one
// fsync for the batch), and returns a pointer per value with an absolute file
// offset. The pointer's value_off is the segment's payload base plus the frame's
// value offset, so a later ReadValueAt is one pread with no header or varint to
// parse. An empty batch writes no segment.
func (f *File) AppendValues(shard uint16, shardSeq uint64, vals [][]byte) ([]ValuePointer, error) {
	if len(vals) == 0 {
		return nil, nil
	}
	var payload []byte
	frames := make([]ValueFrame, len(vals))
	for i, v := range vals {
		payload, frames[i] = AppendValueFrame(payload, v)
	}
	offs, err := f.AppendGroup([]Pending{{
		Shard:    shard,
		Kind:     KindValueLog,
		ShardSeq: shardSeq,
		Payload:  payload,
	}})
	if err != nil {
		return nil, err
	}
	// The payload lands at the segment offset plus the 64-byte header; a frame's
	// value offset is relative to the payload start, so the file offset is the sum.
	base := offs[0] + SegHeaderLen
	ptrs := make([]ValuePointer, len(vals))
	for i, fr := range frames {
		ptrs[i] = ValuePointer{
			ValueOff: base + fr.ValueOff,
			ValueLen: fr.ValueLen,
			ValueCRC: fr.CRC,
		}
	}
	return ptrs, nil
}

// ReadValueAt reads the value a pointer references straight from the file: one
// pread of value_len bytes at value_off, the pointer having already skipped the
// segment header and the frame's varint, verified against the pointer's CRC. dst
// is reused when it has the room, mirroring the per-shard log's read buffer. A
// torn or superseded blob returns ErrChecksum instead of the bytes.
func (f *File) ReadValueAt(p ValuePointer, dst []byte) ([]byte, error) {
	if cap(dst) < int(p.ValueLen) {
		dst = make([]byte, p.ValueLen)
	}
	dst = dst[:p.ValueLen]
	if _, err := f.dev.ReadAt(dst, int64(p.ValueOff)); err != nil {
		return nil, err
	}
	if crc32c(dst) != p.ValueCRC {
		return nil, ErrChecksum
	}
	return dst, nil
}
