package wal

import (
	"encoding/binary"
	"sync"

	"github.com/tamnd/aki/vfs"
)

// WAL is an open write-ahead log over a single .aki-wal file.
type WAL struct {
	mu       sync.Mutex
	file     vfs.File
	pageSize uint32
	hdr      header

	// nFrames is the number of valid frames currently in the file.
	nFrames int
	// cksum is the running cumulative checksum after the last valid frame; the
	// next frame chains from it. Before any frame it is (salt1, salt2).
	cksum [2]uint32
	// index maps a main-file page number to the index of its newest committed
	// frame, for serving reads from the WAL (doc 04 §3). Entries are added only
	// after a transaction's commit frame is fsynced, so every indexed frame is
	// durable.
	index map[uint32]int
	// dbSizeAfter is the main-file page count recorded by the most recent commit
	// frame, used by checkpoint to truncate or extend the main file (doc 04 §8).
	dbSizeAfter uint32
	closed      bool
}

// DBSizeAfter returns the main-file page count from the latest commit frame.
func (w *WAL) DBSizeAfter() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dbSizeAfter
}

// Frame is one page change to append within a transaction.
type Frame struct {
	PageNo uint32
	Data   []byte
}

// Create initialises a new .aki-wal with a fresh header and salts. If
// opts.Salt1/Salt2 are zero a CSPRNG seeds them.
func Create(fsys vfs.VFS, name string, pageSize uint32, opts Options) (*WAL, error) {
	f, err := fsys.Open(name, true)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil, err
	}
	s1, s2 := opts.Salt1, opts.Salt2
	if s1 == 0 {
		s1 = randUint32()
	}
	if s2 == 0 {
		s2 = randUint32()
	}
	h := header{pageSize: pageSize, checkpointSeq: opts.CheckpointSeq, salt1: s1, salt2: s2}
	buf := make([]byte, HeaderSize)
	marshalHeader(buf, h)
	if _, err := f.WriteAt(buf, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{
		file:     f,
		pageSize: pageSize,
		hdr:      h,
		cksum:    [2]uint32{s1, s2},
		index:    make(map[uint32]int),
	}, nil
}

// PageSize returns the WAL's page size.
func (w *WAL) PageSize() uint32 { return w.pageSize }

// FrameCount returns the number of valid frames in the WAL.
func (w *WAL) FrameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nFrames
}

// CommitTxn appends frames as one transaction and makes it durable. The last
// frame is the commit frame, stamped with dbSizeAfter (the main file's new page
// count); preceding frames carry db_size_after = 0. The whole batch is fsynced
// once, after the commit frame, which is the linearization point (doc 04 §4).
func (w *WAL) CommitTxn(frames []Frame, dbSizeAfter uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrShortFrame
	}
	if len(frames) == 0 {
		return nil
	}
	s1, s2 := w.cksum[0], w.cksum[1]
	base := w.nFrames
	fhdr := make([]byte, FrameHeaderSize)
	for i, fr := range frames {
		if uint32(len(fr.Data)) != w.pageSize {
			return ErrShortFrame
		}
		dbSize := uint32(0)
		if i == len(frames)-1 {
			dbSize = dbSizeAfter
		}
		binary.LittleEndian.PutUint32(fhdr[0:], fr.PageNo)
		binary.LittleEndian.PutUint32(fhdr[4:], dbSize)
		binary.LittleEndian.PutUint32(fhdr[8:], w.hdr.salt1)
		binary.LittleEndian.PutUint32(fhdr[12:], w.hdr.salt2)
		// Cumulative checksum over the first 16 header bytes then the payload.
		s1, s2 = walChecksum(s1, s2, fhdr[0:16])
		s1, s2 = walChecksum(s1, s2, fr.Data)
		binary.LittleEndian.PutUint32(fhdr[16:], s1)
		binary.LittleEndian.PutUint32(fhdr[20:], s2)

		off := frameOffset(base+i, w.pageSize)
		if _, err := w.file.WriteAt(fhdr, off); err != nil {
			return err
		}
		if _, err := w.file.WriteAt(fr.Data, off+FrameHeaderSize); err != nil {
			return err
		}
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	// Commit succeeded: publish the new frames to the index.
	for i, fr := range frames {
		w.index[fr.PageNo] = base + i
	}
	w.nFrames = base + len(frames)
	w.cksum = [2]uint32{s1, s2}
	if dbSizeAfter != 0 {
		w.dbSizeAfter = dbSizeAfter
	}
	return nil
}

// Read returns the newest committed page image for pageNo from the WAL and true,
// or nil and false if the WAL holds no committed copy.
func (w *WAL) Read(pageNo uint32) ([]byte, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx, ok := w.index[pageNo]
	if !ok {
		return nil, false, nil
	}
	buf := make([]byte, w.pageSize)
	off := frameOffset(idx, w.pageSize) + FrameHeaderSize
	if _, err := w.file.ReadAt(buf, off); err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

// Close releases the WAL file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}
