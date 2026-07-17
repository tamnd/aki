package akifile

import (
	"encoding/binary"
	"io"
)

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

// ReadRecordAt decodes the record at an absolute frame address, the random-access
// counterpart to WalkRecords' sequential walk. A checkpoint entry keeps a record's
// address (section 5's record_addr), and a recovery cross-check or a verify pass
// reads the key back from that address to catch a hash collision; this is that
// deref. The address points at the frame start, so it reads the varint body length
// first, then the body and its trailing CRC32C, and verifies the CRC before
// returning the row. A torn or superseded record fails ErrChecksum rather than
// handing back rot, the same guard the walk applies. The returned row's Key is a
// fresh copy, not an alias into a shared buffer, because a point read owns its
// bytes.
func (f *File) ReadRecordAt(addr uint64) (RecordRow, error) {
	// Read a bounded window to decode the varint length. A short read at the file
	// tail returns io.EOF with the bytes that were there (the io.ReaderAt
	// contract), which still holds the whole varint of any real frame, so only a
	// zero-byte read is fatal.
	var hdr [binary.MaxVarintLen64]byte
	n, err := f.dev.ReadAt(hdr[:], int64(addr))
	if n == 0 {
		if err == nil || err == io.EOF {
			return RecordRow{}, ErrShort
		}
		return RecordRow{}, err
	}
	bl, adv := binary.Uvarint(hdr[:n])
	if adv <= 0 || bl < recRowHdr {
		return RecordRow{}, ErrLength
	}
	buf := make([]byte, bl+4)
	if _, err := f.dev.ReadAt(buf, int64(addr)+int64(adv)); err != nil {
		return RecordRow{}, err
	}
	body := buf[:bl]
	if crc32c(body) != le.Uint32(buf[bl:bl+4]) {
		return RecordRow{}, ErrChecksum
	}
	row, err := ParseRecordBody(body)
	if err != nil {
		return RecordRow{}, err
	}
	// The body buffer is this call's own, but the key and any inline value alias
	// it; hand back copies so the caller may hold the row past the buffer.
	row.Key = append([]byte(nil), row.Key...)
	if row.Value != nil {
		row.Value = append([]byte(nil), row.Value...)
	}
	return row, nil
}
