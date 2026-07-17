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

// CompactValues re-homes a set of still-live values into one fresh value_log
// segment, the reclaim step the store's value-log compaction drives: a segment
// whose dead fraction is high is rewritten by handing its live pointers here,
// and the returned pointers replace the old ones in their records so the old
// segment's whole span, live bytes and dead alike, becomes free. It reads each
// live value through its pointer (so a torn or superseded blob fails the copy
// instead of migrating rot forward), frames the run into one payload, and cuts a
// single value_log segment owned by shard. The returned pointers are in the same
// order as live, so the caller maps old to new by index. An empty set writes no
// segment. The read scratch is reused across values, so the whole batch costs one
// growing buffer, not one per value.
func (f *File) CompactValues(shard uint16, shardSeq uint64, live []ValuePointer) ([]ValuePointer, error) {
	if len(live) == 0 {
		return nil, nil
	}
	var payload, scratch []byte
	frames := make([]ValueFrame, len(live))
	for i, p := range live {
		val, err := f.ReadValueAt(p, scratch)
		if err != nil {
			return nil, err
		}
		payload, frames[i] = AppendValueFrame(payload, val)
		// AppendValueFrame copied val into payload, so the scratch is free to hold
		// the next read.
		scratch = val[:0]
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
	base := offs[0] + SegHeaderLen
	ptrs := make([]ValuePointer, len(live))
	for i, fr := range frames {
		ptrs[i] = ValuePointer{
			ValueOff: base + fr.ValueOff,
			ValueLen: fr.ValueLen,
			ValueCRC: fr.CRC,
		}
	}
	return ptrs, nil
}

// ReadValueFrameAt reads a value the caller located by only its offset and length,
// verifying the torn-blob guard off the frame's own trailing CRC32C instead of a
// stored one. It reads value_len bytes plus the four CRC bytes the frame trails
// them with, checks the sum, and returns just the value. This is the read the value
// log's re-home leans on: the store's in-record pointer is a 48-bit offset with the
// length carried beside it and no room for a CRC, so it cannot form a full
// ValuePointer; the frame carrying its own CRC is what lets a bare (off, len) still
// fail closed on a value that tore or was superseded rather than hand back rot. dst
// is reused when it has the room. A mismatch is ErrChecksum.
func (f *File) ReadValueFrameAt(off uint64, valLen uint32, dst []byte) ([]byte, error) {
	need := int(valLen) + 4
	if cap(dst) < need {
		dst = make([]byte, need)
	}
	dst = dst[:need]
	if _, err := f.dev.ReadAt(dst, int64(off)); err != nil {
		return nil, err
	}
	val := dst[:valLen]
	if crc32c(val) != le.Uint32(dst[valLen:need]) {
		return nil, ErrChecksum
	}
	return val, nil
}
