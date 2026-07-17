package akifile

// The value-log accumulator: the batching the store's spill path needs when its
// per-shard scratch value log re-homes into the .aki value region. AppendValues
// cuts one segment per call, but the store spills one value at a time and cannot
// afford a 4KiB-aligned segment per value. ValueLogWriter stages framed values
// across many Stage calls and cuts a single value_log segment per Flush, so a run
// of spills coalesces into one segment and one group commit.
//
// It mirrors the scratch log's key property: a staged value is readable from the
// pending buffer the moment Stage returns, before the segment is cut, so the store
// can serve a read of a just-spilled value without waiting on the flush. The
// absolute file offset is not known until Flush assigns the segment, so Stage
// returns only a batch index; Flush returns the resolved pointers in stage order.
// This defers the record's published offset to the group boundary, which is where
// the command's ack already waits on the group fsync, so the value log's cut lands
// on the same seam the log's durability does.
type ValueLogWriter struct {
	f       *File
	shard   uint16
	pending []byte       // framed values awaiting a segment
	frames  []ValueFrame // one per staged value, offsets relative to pending
}

// NewValueLogWriter builds an accumulator that cuts value_log segments owned by
// shard through f's group-commit writer.
func NewValueLogWriter(f *File, shard uint16) *ValueLogWriter {
	return &ValueLogWriter{f: f, shard: shard}
}

// Stage frames val into the pending batch and returns its index in stage order.
// The bytes are readable through ReadStaged straight away; the absolute pointer
// comes back from Flush at the same index. Consecutive stages pack contiguously,
// so one Flush emits them all in one segment.
func (w *ValueLogWriter) Stage(val []byte) int {
	var fr ValueFrame
	w.pending, fr = AppendValueFrame(w.pending, val)
	i := len(w.frames)
	w.frames = append(w.frames, fr)
	return i
}

// Staged reports how many values are staged for the next Flush.
func (w *ValueLogWriter) Staged() int { return len(w.frames) }

// PendingBytes reports the staged payload size, the signal a caller uses to decide
// when the batch is worth a segment.
func (w *ValueLogWriter) PendingBytes() int { return len(w.pending) }

// ReadStaged returns the bytes of the value staged at idx, read from the pending
// buffer before the segment is cut. It is the read-before-flush the scratch log
// served from its own pending buffer. The returned slice aliases the pending
// buffer, valid until the next Stage or Flush. An out-of-range index is ErrShort.
func (w *ValueLogWriter) ReadStaged(idx int) ([]byte, error) {
	if idx < 0 || idx >= len(w.frames) {
		return nil, ErrShort
	}
	fr := w.frames[idx]
	return w.pending[fr.ValueOff : fr.ValueOff+uint64(fr.ValueLen)], nil
}

// Flush cuts one value_log segment for the whole staged batch, resolves each
// staged value to an absolute ValuePointer, resets the accumulator, and returns
// the pointers in stage order. An empty batch writes no segment and returns nil.
// shardSeq stamps the segment; the caller advances it per flush like the scratch
// log advanced its tail.
func (w *ValueLogWriter) Flush(shardSeq uint64) ([]ValuePointer, error) {
	if len(w.frames) == 0 {
		return nil, nil
	}
	offs, err := w.f.AppendGroup([]Pending{{
		Shard:    w.shard,
		Kind:     KindValueLog,
		ShardSeq: shardSeq,
		Payload:  w.pending,
	}})
	if err != nil {
		return nil, err
	}
	base := offs[0] + SegHeaderLen
	ptrs := make([]ValuePointer, len(w.frames))
	for i, fr := range w.frames {
		ptrs[i] = ValuePointer{
			ValueOff: base + fr.ValueOff,
			ValueLen: fr.ValueLen,
			ValueCRC: fr.CRC,
		}
	}
	w.pending = w.pending[:0]
	w.frames = w.frames[:0]
	return ptrs, nil
}
