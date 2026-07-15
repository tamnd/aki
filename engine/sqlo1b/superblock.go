package sqlo1b

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

// The superblock is the root of the file: two 4 KiB copies at offsets
// 0 and 4096, committed alternately so a torn write can only destroy
// the copy being written (spec 2064/sqlo1 doc 03 section 3). All
// fields are little-endian.
const (
	SuperblockSize = 4096
	slotAOff       = 0
	slotBOff       = SuperblockSize

	// FormatVersion is the format major version this code writes and
	// the only one it reads.
	FormatVersion = 1

	// DefaultIOUnit is the v0 group size. It is superblock data, not
	// a code constant: readers use what the superblock says, so the
	// unitsize lab verdict can amend this default without a version
	// bump.
	DefaultIOUnit = 4096

	// DefaultExtentSize is the v0 extent size, 1 MiB.
	DefaultExtentSize = 1 << 20
)

// superMagic is the 16-byte file signature, 15 characters and one NUL.
var superMagic = [16]byte{'t', 'a', 'm', 'n', 'd', 'a', 'k', 'i', ' ', 's', 'q', 'l', 'o', 'b', '1', 0}

// ErrNoSuperblock reports that neither superblock copy verified.
var ErrNoSuperblock = errors.New("sqlo1b: no valid superblock")

// FullPtr is the 16-byte verifiable reference form: a packed position
// and the checksum of what it points at (doc 03 section 5.2). A zero
// FullPtr means the structure does not exist yet.
type FullPtr struct {
	Pos uint64
	Sum uint64
}

// Superblock is the decoded root. The advisory fields (RecordCount,
// GarbageBytes) carry no correctness weight; everything else does,
// including HighWater: the store seam's exactly-once mark, which must
// survive the WAL trim that discards the frames that carried it.
type Superblock struct {
	Version      uint32
	IOUnit       uint32
	ExtentSize   uint32
	Flags        uint32
	Seq          uint64
	ExtentCount  uint64
	DBID         [16]byte
	WALTrimSeq   uint64
	HashEpoch    uint64
	DirRoot      FullPtr
	AllocmapRoot FullPtr
	DictRoot     FullPtr
	StatsRoot    FullPtr
	RecordCount  uint64
	GarbageBytes uint64
	HighWater    int64
}

// NewSuperblock returns the creation-time root: seq 1, v0 geometry,
// a random db_id, and every pointer zero.
func NewSuperblock() (*Superblock, error) {
	sb := &Superblock{
		Version:    FormatVersion,
		IOUnit:     DefaultIOUnit,
		ExtentSize: DefaultExtentSize,
		Seq:        1,
	}
	if _, err := rand.Read(sb.DBID[:]); err != nil {
		return nil, fmt.Errorf("sqlo1b: db_id: %w", err)
	}
	return sb, nil
}

// PackHashEpoch packs the linear-hashing state: split pointer in bits
// 63..8, level in bits 7..0.
func PackHashEpoch(split uint64, level uint8) uint64 {
	return split<<8 | uint64(level)
}

// UnpackHashEpoch is the inverse of PackHashEpoch.
func UnpackHashEpoch(e uint64) (split uint64, level uint8) {
	return e >> 8, uint8(e)
}

func putPtr(b []byte, p FullPtr) {
	binary.LittleEndian.PutUint64(b[0:], p.Pos)
	binary.LittleEndian.PutUint64(b[8:], p.Sum)
}

func getPtr(b []byte) FullPtr {
	return FullPtr{
		Pos: binary.LittleEndian.Uint64(b[0:]),
		Sum: binary.LittleEndian.Uint64(b[8:]),
	}
}

// Encode lays the superblock out per the doc 03 section 3 table,
// echoes seq at 4080, and seals bytes 0..4087 with xxhash64 at 4088.
func (sb *Superblock) Encode() []byte {
	b := make([]byte, SuperblockSize)
	copy(b[0:16], superMagic[:])
	binary.LittleEndian.PutUint32(b[16:], sb.Version)
	binary.LittleEndian.PutUint32(b[20:], sb.IOUnit)
	binary.LittleEndian.PutUint32(b[24:], sb.ExtentSize)
	binary.LittleEndian.PutUint32(b[28:], sb.Flags)
	binary.LittleEndian.PutUint64(b[32:], sb.Seq)
	binary.LittleEndian.PutUint64(b[40:], sb.ExtentCount)
	copy(b[48:64], sb.DBID[:])
	binary.LittleEndian.PutUint64(b[64:], sb.WALTrimSeq)
	binary.LittleEndian.PutUint64(b[72:], sb.HashEpoch)
	putPtr(b[80:], sb.DirRoot)
	putPtr(b[96:], sb.AllocmapRoot)
	putPtr(b[112:], sb.DictRoot)
	putPtr(b[128:], sb.StatsRoot)
	binary.LittleEndian.PutUint64(b[144:], sb.RecordCount)
	binary.LittleEndian.PutUint64(b[152:], sb.GarbageBytes)
	binary.LittleEndian.PutUint64(b[160:], uint64(sb.HighWater))
	binary.LittleEndian.PutUint64(b[4080:], sb.Seq)
	binary.LittleEndian.PutUint64(b[4088:], xxhash.Sum64(b[:4088]))
	return b
}

// DecodeSuperblock verifies magic, checksum, the seq echo, and the
// format version, in that order, and returns the decoded root.
func DecodeSuperblock(b []byte) (*Superblock, error) {
	if len(b) != SuperblockSize {
		return nil, fmt.Errorf("sqlo1b: superblock is %d bytes, want %d", len(b), SuperblockSize)
	}
	if [16]byte(b[0:16]) != superMagic {
		return nil, errors.New("sqlo1b: superblock magic mismatch")
	}
	if got, want := xxhash.Sum64(b[:4088]), binary.LittleEndian.Uint64(b[4088:]); got != want {
		return nil, fmt.Errorf("sqlo1b: superblock checksum %#x, stored %#x", got, want)
	}
	seq := binary.LittleEndian.Uint64(b[32:])
	if echo := binary.LittleEndian.Uint64(b[4080:]); echo != seq {
		return nil, fmt.Errorf("sqlo1b: superblock seq %d but echo %d, partial write", seq, echo)
	}
	sb := &Superblock{
		Version:      binary.LittleEndian.Uint32(b[16:]),
		IOUnit:       binary.LittleEndian.Uint32(b[20:]),
		ExtentSize:   binary.LittleEndian.Uint32(b[24:]),
		Flags:        binary.LittleEndian.Uint32(b[28:]),
		Seq:          seq,
		ExtentCount:  binary.LittleEndian.Uint64(b[40:]),
		DBID:         [16]byte(b[48:64]),
		WALTrimSeq:   binary.LittleEndian.Uint64(b[64:]),
		HashEpoch:    binary.LittleEndian.Uint64(b[72:]),
		DirRoot:      getPtr(b[80:]),
		AllocmapRoot: getPtr(b[96:]),
		DictRoot:     getPtr(b[112:]),
		StatsRoot:    getPtr(b[128:]),
		RecordCount:  binary.LittleEndian.Uint64(b[144:]),
		GarbageBytes: binary.LittleEndian.Uint64(b[152:]),
		HighWater:    int64(binary.LittleEndian.Uint64(b[160:])),
	}
	if sb.Version != FormatVersion {
		return nil, fmt.Errorf("sqlo1b: format version %d, this build reads %d", sb.Version, FormatVersion)
	}
	return sb, nil
}

// syncWriterAt is the file shape superblock commits need. os.File
// satisfies it; Sync is a full fsync, which subsumes the fdatasync
// the doc asks for (the weaker call is an iopool-slice optimization).
type syncWriterAt interface {
	io.WriterAt
	Sync() error
}

// slotOff returns the file offset a given seq commits to. Seq is
// monotonic and increments by one per commit, so seq%2 alternates
// slots and the copy being overwritten is always the older one.
func slotOff(seq uint64) int64 {
	if seq%2 == 0 {
		return slotAOff
	}
	return slotBOff
}

// InitSuperblocks writes the same creation root to both slots and
// syncs, so a fresh file satisfies pick-highest-valid immediately.
func InitSuperblocks(w syncWriterAt, sb *Superblock) error {
	b := sb.Encode()
	if _, err := w.WriteAt(b, slotAOff); err != nil {
		return fmt.Errorf("sqlo1b: init superblock A: %w", err)
	}
	if _, err := w.WriteAt(b, slotBOff); err != nil {
		return fmt.Errorf("sqlo1b: init superblock B: %w", err)
	}
	if err := w.Sync(); err != nil {
		return fmt.Errorf("sqlo1b: init superblock sync: %w", err)
	}
	return nil
}

// CommitSuperblock writes sb to the slot its seq alternates onto and
// syncs. The caller owns seq monotonicity; the other slot keeps the
// previous durable root until this write is fully down.
func CommitSuperblock(w syncWriterAt, sb *Superblock) error {
	if _, err := w.WriteAt(sb.Encode(), slotOff(sb.Seq)); err != nil {
		return fmt.Errorf("sqlo1b: commit superblock seq %d: %w", sb.Seq, err)
	}
	if err := w.Sync(); err != nil {
		return fmt.Errorf("sqlo1b: commit superblock sync: %w", err)
	}
	return nil
}

// ReadSuperblock reads both copies, keeps the ones that verify, and
// returns the one with the higher seq plus the slot it came from
// (0 or 1). Neither verifying is ErrNoSuperblock; the per-copy
// failures ride along for diagnosis.
func ReadSuperblock(r io.ReaderAt) (*Superblock, int, error) {
	var picked *Superblock
	slot := -1
	var fails []error
	for i, off := range [2]int64{slotAOff, slotBOff} {
		b := make([]byte, SuperblockSize)
		if _, err := r.ReadAt(b, off); err != nil {
			fails = append(fails, fmt.Errorf("slot %d: %w", i, err))
			continue
		}
		sb, err := DecodeSuperblock(b)
		if err != nil {
			fails = append(fails, fmt.Errorf("slot %d: %w", i, err))
			continue
		}
		if picked == nil || sb.Seq > picked.Seq {
			picked, slot = sb, i
		}
	}
	if picked == nil {
		return nil, -1, fmt.Errorf("%w: %w", ErrNoSuperblock, errors.Join(fails...))
	}
	return picked, slot, nil
}
