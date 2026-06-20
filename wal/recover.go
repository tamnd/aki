package wal

import (
	"encoding/binary"
	"io"
	"maps"

	"github.com/tamnd/aki/vfs"
)

// Open opens an existing .aki-wal and runs recovery: it validates the header,
// scans frames in order, recomputes the cumulative checksum chain, and accepts
// frames up to the last valid commit frame. A torn or partial tail fails the
// checksum and is discarded, so the recovered state is the last durable commit
// (doc 04 §7). pageSize must match the main file; a mismatch is corruption.
func Open(fsys vfs.VFS, name string, pageSize uint32) (*WAL, error) {
	f, err := fsys.Open(name, false)
	if err != nil {
		return nil, err
	}
	hbuf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(hbuf, 0); err != nil {
		f.Close()
		return nil, err
	}
	h, err := parseHeader(hbuf)
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.pageSize != pageSize {
		f.Close()
		return nil, ErrPageSize
	}

	w := &WAL{
		file:     f,
		pageSize: pageSize,
		hdr:      h,
		cksum:    [2]uint32{h.salt1, h.salt2},
		index:    make(map[uint32]int),
	}

	// Scan frames, chaining the checksum. Track the last committed frame so we
	// can roll an uncommitted tail back to the previous commit.
	s1, s2 := h.salt1, h.salt2
	fhdr := make([]byte, FrameHeaderSize)
	page := make([]byte, pageSize)
	lastCommit := -1
	var committedIndex map[uint32]int
	pending := make(map[uint32]int)
	frameDBSize := uint32(0)

	for i := 0; ; i++ {
		off := frameOffset(i, pageSize)
		if _, err := f.ReadAt(fhdr, off); err != nil {
			if err == io.EOF {
				break
			}
			// A short read at the tail means a torn frame; stop scanning.
			break
		}
		if _, err := f.ReadAt(page, off+FrameHeaderSize); err != nil {
			break // torn payload
		}
		fSalt1 := binary.LittleEndian.Uint32(fhdr[8:])
		fSalt2 := binary.LittleEndian.Uint32(fhdr[12:])
		if fSalt1 != h.salt1 || fSalt2 != h.salt2 {
			break // frame from a prior WAL generation
		}
		n1, n2 := walChecksum(s1, s2, fhdr[0:16])
		n1, n2 = walChecksum(n1, n2, page)
		if n1 != binary.LittleEndian.Uint32(fhdr[16:]) || n2 != binary.LittleEndian.Uint32(fhdr[20:]) {
			break // checksum mismatch: torn write, stop here
		}
		s1, s2 = n1, n2
		pageNo := binary.LittleEndian.Uint32(fhdr[0:])
		dbSize := binary.LittleEndian.Uint32(fhdr[4:])
		pending[pageNo] = i
		if dbSize != 0 {
			// Commit frame: snapshot the pending index as committed.
			lastCommit = i
			frameDBSize = dbSize
			committedIndex = make(map[uint32]int, len(pending))
			maps.Copy(committedIndex, pending)
		}
	}

	if lastCommit < 0 {
		// No complete transaction; WAL is logically empty.
		w.nFrames = 0
		w.cksum = [2]uint32{h.salt1, h.salt2}
		return w, nil
	}

	// Recompute the running checksum up to and including lastCommit so future
	// appends chain correctly, and expose only committed frames.
	w.index = committedIndex
	w.nFrames = lastCommit + 1
	w.cksum = recomputeChecksum(f, h, lastCommit, pageSize)
	w.dbSizeAfter = frameDBSize
	return w, nil
}

// recomputeChecksum replays the checksum chain through frame index last
// (inclusive) and returns the running pair, so appended frames continue it.
func recomputeChecksum(f vfs.File, h header, last int, pageSize uint32) [2]uint32 {
	s1, s2 := h.salt1, h.salt2
	fhdr := make([]byte, FrameHeaderSize)
	page := make([]byte, pageSize)
	for i := 0; i <= last; i++ {
		off := frameOffset(i, pageSize)
		if _, err := f.ReadAt(fhdr, off); err != nil {
			break
		}
		if _, err := f.ReadAt(page, off+FrameHeaderSize); err != nil {
			break
		}
		s1, s2 = walChecksum(s1, s2, fhdr[0:16])
		s1, s2 = walChecksum(s1, s2, page)
	}
	return [2]uint32{s1, s2}
}
