package wal

import "github.com/tamnd/aki/vfs"

// Checkpoint folds every committed frame back into the main file and then resets
// the WAL to a fresh generation (doc 04 §8). For each main-file page it writes
// the newest committed frame image, fsyncs the main file so the images are
// durable, then truncates the main file to the committed db size, rewrites the
// WAL header with a bumped checkpoint sequence and new salts (invalidating all
// old frames), and truncates the WAL to just its header.
//
// main must be the .aki file's VFS handle, opened for writing. Checkpoint holds
// the WAL lock for its duration; in M0 it is a serial, blocking operation.
func (w *WAL) Checkpoint(main vfs.File) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrShortFrame
	}
	if len(w.index) == 0 {
		return nil
	}

	page := make([]byte, w.pageSize)
	for pageNo, idx := range w.index {
		off := frameOffset(idx, w.pageSize) + FrameHeaderSize
		if _, err := w.file.ReadAt(page, off); err != nil {
			return err
		}
		// Page numbers in frames are 1-based logical pages mapping directly to
		// the same page number in the main file (page 0 is the header).
		if _, err := main.WriteAt(page, int64(pageNo)*int64(w.pageSize)); err != nil {
			return err
		}
	}
	if err := main.Sync(); err != nil {
		return err
	}
	if w.dbSizeAfter != 0 {
		if err := main.Truncate(int64(w.dbSizeAfter) * int64(w.pageSize)); err != nil {
			return err
		}
		if err := main.Sync(); err != nil {
			return err
		}
	}

	// Reset the WAL: new generation, new salts, header-only file.
	w.hdr.checkpointSeq++
	w.hdr.salt1 = randUint32()
	w.hdr.salt2 = randUint32()
	hbuf := make([]byte, HeaderSize)
	marshalHeader(hbuf, w.hdr)
	if _, err := w.file.WriteAt(hbuf, 0); err != nil {
		return err
	}
	if err := w.file.Truncate(int64(HeaderSize)); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.nFrames = 0
	w.index = make(map[uint32]int)
	w.cksum = [2]uint32{w.hdr.salt1, w.hdr.salt2}
	w.dbSizeAfter = 0
	return nil
}

// CheckpointSeq returns the WAL's current checkpoint generation.
func (w *WAL) CheckpointSeq() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hdr.checkpointSeq
}
