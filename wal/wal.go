// Package wal is aki's write-ahead log (spec 2064 doc 04). New and changed
// pages are appended to a .aki-wal sidecar as frames before the main file is
// touched; a transaction becomes durable when its commit frame is fsynced. On
// restart, recovery scans the WAL, validates the cumulative salted checksum
// chain, and replays every frame up to the last valid commit, discarding any
// torn tail. A checkpoint folds committed frames back into the main file and
// resets the WAL under a fresh salt generation.
//
// The frame and header layout and the two-word cumulative checksum follow the
// SQLite WAL design (walChecksumBytes), adapted to aki's magic and page sizes.
package wal

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/tamnd/aki/format"
)

// On-disk constants (doc 04 §2).
const (
	// Magic is "AWL1" — aki WAL version 1.
	Magic uint32 = 0x41574C31
	// FormatVersion is the WAL format version.
	FormatVersion uint16 = 1
	// HeaderSize is the fixed WAL header size in bytes.
	HeaderSize = 32
	// FrameHeaderSize is the per-frame header size in bytes; the page payload
	// follows immediately.
	FrameHeaderSize = 24
)

// Errors returned by the WAL.
var (
	ErrBadMagic    = errors.New("aki/wal: bad WAL magic")
	ErrBadVersion  = errors.New("aki/wal: unsupported WAL version")
	ErrPageSize    = errors.New("aki/wal: page size mismatch")
	ErrBadChecksum = errors.New("aki/wal: header checksum mismatch")
	ErrShortFrame  = errors.New("aki/wal: short frame")
)

// Options configure WAL creation. Salt1/Salt2 are exposed so tests can pin the
// salts for deterministic checksums; production leaves them zero and a CSPRNG
// seeds them.
type Options struct {
	Salt1, Salt2  uint32
	CheckpointSeq uint32
}

// header is the parsed WAL header.
type header struct {
	pageSize      uint32
	checkpointSeq uint32
	salt1, salt2  uint32
}

// walChecksum is the two-word cumulative checksum from SQLite's
// walChecksumBytes. It chains from a prior (s1, s2) over b, which must be a
// multiple of 8 bytes, in native little-endian word order (doc 04 §2.3).
func walChecksum(s1, s2 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+7 < len(b); i += 8 {
		s1 += binary.LittleEndian.Uint32(b[i:]) + s2
		s2 += binary.LittleEndian.Uint32(b[i+4:]) + s1
	}
	return s1, s2
}

// randUint32 returns a cryptographically random non-zero uint32 for a salt.
func randUint32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	v := binary.LittleEndian.Uint32(b[:])
	if v == 0 {
		v = 1
	}
	return v
}

// marshalHeader writes the 32-byte WAL header into b and fills its two-word
// checksum over bytes 0..23.
func marshalHeader(b []byte, h header) {
	binary.LittleEndian.PutUint32(b[0:], Magic)
	binary.LittleEndian.PutUint16(b[4:], FormatVersion)
	binary.LittleEndian.PutUint16(b[6:], 0)
	binary.LittleEndian.PutUint32(b[8:], h.pageSize)
	binary.LittleEndian.PutUint32(b[12:], h.checkpointSeq)
	binary.LittleEndian.PutUint32(b[16:], h.salt1)
	binary.LittleEndian.PutUint32(b[20:], h.salt2)
	c1, c2 := walChecksum(0, 0, b[0:24])
	binary.LittleEndian.PutUint32(b[24:], c1)
	binary.LittleEndian.PutUint32(b[28:], c2)
}

// parseHeader validates and decodes the WAL header from b.
func parseHeader(b []byte) (header, error) {
	if len(b) < HeaderSize {
		return header{}, ErrShortFrame
	}
	if binary.LittleEndian.Uint32(b[0:]) != Magic {
		return header{}, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[4:]) != FormatVersion {
		return header{}, ErrBadVersion
	}
	c1, c2 := walChecksum(0, 0, b[0:24])
	if c1 != binary.LittleEndian.Uint32(b[24:]) || c2 != binary.LittleEndian.Uint32(b[28:]) {
		return header{}, ErrBadChecksum
	}
	h := header{
		pageSize:      binary.LittleEndian.Uint32(b[8:]),
		checkpointSeq: binary.LittleEndian.Uint32(b[12:]),
		salt1:         binary.LittleEndian.Uint32(b[16:]),
		salt2:         binary.LittleEndian.Uint32(b[20:]),
	}
	if !format.ValidPageSize(h.pageSize) {
		return header{}, ErrPageSize
	}
	return h, nil
}

// frameSize returns the on-disk size of one frame for the given page size.
func frameSize(pageSize uint32) int64 { return int64(FrameHeaderSize) + int64(pageSize) }

// frameOffset returns the byte offset of frame index i (0-based) in the WAL.
func frameOffset(i int, pageSize uint32) int64 {
	return int64(HeaderSize) + int64(i)*frameSize(pageSize)
}
