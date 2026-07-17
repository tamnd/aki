package akifile

// The record-log region on the File: the enumerate side of the record log, the
// counterpart to WalkValues (valueregion.go). RecordLogWriter cuts `log`
// segments; WalkRecords walks them back. Recovery is the first consumer, the
// tail replay of section 6 step 7 that re-derives the index from the record rows,
// and it needs exactly this walk: from a checkpoint's log position up to the
// durable tail, every framed record in `global_seq` order, applied idempotently.
//
// It reuses ScanSegments' tail walk, so it stops exactly where recovery would
// resume, and it descends only into `log` segments, skipping the value-log,
// cold-chunk, checkpoint, SRT, barrier, and free-map segments interleaved in the
// same append space. The payload a segment hands back is the exact framed run
// (no padding), so the frame walk consumes it end to end. A torn frame stops the
// walk with an error, the same durable cut a recovering reader takes; a visit
// that returns an error stops it too and the error propagates, so a store-side
// apply failure fails the restore rather than dropping a committed record.

// WalkRecords walks every `log` segment in the append space from `from` up to
// the durable tail and calls visit for each framed record with its absolute
// frame address and decoded row, in append order. The address is the frame
// start, the same address RecordLogWriter.Flush returned and a checkpoint entry
// keeps, so a caller can tie a walked record to its index entry. The row's Key
// aliases the segment payload for the duration of the visit call.
func (f *File) WalkRecords(from uint64, visit func(addr uint64, row RecordRow) error) error {
	_, err := ReplayTail(f.dev, f.prefix, from, f.cursor, func(off uint64, h *SegHeader, payload []byte) error {
		if h.Kind != KindLog {
			return nil
		}
		base := off + SegHeaderLen
		for cur := uint64(0); cur < uint64(len(payload)); {
			fr, row, next, err := NextRecordFrame(payload, cur)
			if err != nil {
				return err
			}
			if err := visit(base+fr.FrameOff, row); err != nil {
				return err
			}
			cur = next
		}
		return nil
	})
	return err
}
