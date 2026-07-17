package akifile

// The record-log accumulator: the batching the store's durable append needs when
// a command's record row re-homes into the .aki `log` region. It mirrors
// ValueLogWriter (valuewriter.go) exactly, because the record log has the same
// tension the value log did. A shard executes one command at a time and cannot
// afford a 4KiB-aligned segment per record, so RecordLogWriter stages framed
// records across many Stage calls and cuts a single `log` segment per Flush; a
// run of commands coalesces into one segment and one group commit (section 8's
// amplification win, one fsync for B records).
//
// It keeps the scratch log's read-before-flush property: a staged record is
// readable from the pending buffer the moment Stage returns, before the segment
// is cut, so an in-batch read of a just-written key resolves without waiting on
// the flush. The absolute file address is not known until Flush assigns the
// segment, so Stage returns only a batch index; Flush returns the resolved
// addresses in stage order. This defers the record's published address to the
// group boundary, which is where the command's ack already waits on the group
// fsync, so the record log's cut lands on the same seam its durability does. The
// two-phase publish the store builds on top (stage a provisional index entry,
// patch it to the Flush address at the boundary) is the next slice up.
type RecordLogWriter struct {
	f       *File
	shard   uint16
	pending []byte        // framed records awaiting a segment
	frames  []RecordFrame // one per staged record, offsets relative to pending
}

// NewRecordLogWriter builds an accumulator that cuts `log` segments owned by
// shard through f's group-commit writer.
func NewRecordLogWriter(f *File, shard uint16) *RecordLogWriter {
	return &RecordLogWriter{f: f, shard: shard}
}

// Stage frames row into the pending batch and returns its index in stage order.
// The row is readable through ReadStaged straight away; the absolute address
// comes back from Flush at the same index. Consecutive stages pack contiguously,
// so one Flush emits them all in one segment. Stage copies row.Key into the
// pending buffer, so the caller may reuse its key slice across stages.
func (w *RecordLogWriter) Stage(row RecordRow) int {
	var fr RecordFrame
	w.pending, fr = AppendRecordFrame(w.pending, row)
	i := len(w.frames)
	w.frames = append(w.frames, fr)
	return i
}

// Staged reports how many records are staged for the next Flush.
func (w *RecordLogWriter) Staged() int { return len(w.frames) }

// PendingBytes reports the staged payload size, the signal a caller uses to
// decide when the batch is worth a segment.
func (w *RecordLogWriter) PendingBytes() int { return len(w.pending) }

// ReadStaged returns the record staged at idx, decoded from the pending buffer
// before the segment is cut, the read-before-flush the scratch log served from
// its own pending buffer. The returned row's Key aliases the pending buffer,
// valid until the next Stage or Flush. An out-of-range index is ErrShort.
func (w *RecordLogWriter) ReadStaged(idx int) (RecordRow, error) {
	if idx < 0 || idx >= len(w.frames) {
		return RecordRow{}, ErrShort
	}
	fr := w.frames[idx]
	return ParseRecordBody(w.pending[fr.BodyOff : fr.BodyOff+uint64(fr.BodyLen)])
}

// Flush cuts one `log` segment for the whole staged batch, resolves each staged
// record to its absolute frame address, resets the accumulator, and returns the
// addresses in stage order. An empty batch writes no segment and returns nil, so
// the shard's sequence advances only on a real cut. shardSeq stamps the segment;
// the caller advances it per flush like the scratch log advanced its tail. The
// returned address points at the frame start, so a later point read decodes the
// varint length before the body.
func (w *RecordLogWriter) Flush(shardSeq uint64) ([]uint64, error) {
	if len(w.frames) == 0 {
		return nil, nil
	}
	offs, err := w.f.AppendGroup([]Pending{{
		Shard:    w.shard,
		Kind:     KindLog,
		ShardSeq: shardSeq,
		Payload:  w.pending,
	}})
	if err != nil {
		return nil, err
	}
	base := offs[0] + SegHeaderLen
	addrs := make([]uint64, len(w.frames))
	for i, fr := range w.frames {
		addrs[i] = base + fr.FrameOff
	}
	w.pending = w.pending[:0]
	w.frames = w.frames[:0]
	return addrs, nil
}
