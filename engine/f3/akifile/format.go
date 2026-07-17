package akifile

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Fixed layout constants for the on-disk format (spec 2064/f3/07 section 3). All
// multi-byte integers are little-endian; magics are raw ASCII bytes.
const (
	// Magic is the immutable-prefix sentinel: "AKI store v3" padded to 16 bytes.
	// A v1 or v2 file is refused here, never misread.
	Magic = "AKI store v3\x00\x00\x00\x00"

	FormatMajor = 3 // a reader refuses a major it does not implement
	FormatMinor = 0 // higher minor may open read-only; a writer must match

	PageSize     = 16384 // header-page and checkpoint padding unit
	SegmentAlign = 4096  // every segment starts on this boundary
	SlotCount    = 16384 // CRC16 slot space, frozen so a cluster spec cannot drift it

	PrefixSize   = 100 // fixed prefix fields; the reserved tail runs on to slot A
	MetaSlotSize = 128
	SRTHeaderLen = 40 // magic(4) shard_count(4) gen(8) snap_wbar(8) flags(4) reserved(4) crc(8)
	SRTRowSize   = 80
	SegHeaderLen = 64
	ExtentSize   = 24 // kind(4) flags(4) start(8) length(8)

	// Default meta-slot placement inside page zero: slot A and slot B sit in
	// distinct 4KiB sectors so a torn write to one sector can never reach the
	// other, which is what makes the dual-slot commit torn-safe (section 6).
	MetaSlotAOff = 4096
	MetaSlotBOff = 8192
)

// Checksum kinds. One kind covers a whole file, recorded in the prefix. u32
// checksum fields (the prefix CRCs and the segment header CRC) are always CRC32C
// so a reader can validate the header before it trusts the kind field; u64
// checksum fields (meta slot, SRT, segment payload) honor this kind.
const (
	ChecksumCRC32C uint32 = 1 // default and shipped
	ChecksumXXH3   uint32 = 2 // reserved, not yet implemented
)

// Segment kinds (spec 2064/f3/07 section 3, segment header table).
const (
	KindLog uint16 = iota
	KindColdChunk
	KindValueLog
	KindIndexCkpt
	KindChunkDir
	KindSegStats
	KindSRT
	KindExtentTable
	KindFreeMap
	KindBarrier
	KindTTLIndex
	KindMetaKV
	KindFree
)

// ShardOwnerless tags a segment that belongs to no single shard (barrier, free
// map, SRT): the shard field carries 0xFFFF.
const ShardOwnerless uint16 = 0xFFFF

// Segment header flag bits.
const (
	SegSealed uint32 = 1 << iota
	SegCompactionOutput
	SegBarrierFollows
	SegDeltaCheckpoint
)

// Extent kinds (the coarse map; segments are found by SRT roots, never here).
const (
	ExtentHeader uint32 = iota
	ExtentAppend
	ExtentFree
	ExtentPendingFree
)

var (
	// ErrMagic is a bad or absent format sentinel; recovery never guesses past it.
	ErrMagic = errors.New("akifile: bad magic")
	// ErrMajor is a format major this build does not implement.
	ErrMajor = errors.New("akifile: unsupported format major")
	// ErrChecksumKind is a checksum kind this build cannot compute (xxh3 today).
	ErrChecksumKind = errors.New("akifile: unsupported checksum kind")
	// ErrChecksum is a checksum mismatch: media rot or a torn write.
	ErrChecksum = errors.New("akifile: checksum mismatch")
	// ErrShort is a buffer too small to hold the fixed region it must decode.
	ErrShort = errors.New("akifile: buffer too short")
	// ErrLength is a payload length that disagrees with its framed length.
	ErrLength = errors.New("akifile: length mismatch")
	// ErrCheckpoint is a checkpoint header that is malformed: an unknown
	// full-or-delta kind, or a full dump carrying a nonzero base offset.
	ErrCheckpoint = errors.New("akifile: malformed checkpoint header")
	// ErrShardCount is a live root whose SRT shard count disagrees with the
	// prefix: a torn SRT swap or a file opened under the wrong shard geometry.
	ErrShardCount = errors.New("akifile: shard count disagreement")
	// ErrReadOnly is a write through a read-only device: an inspect tool opens the
	// file for reading only, so any attempt to mutate it is a bug, not I/O.
	ErrReadOnly = errors.New("akifile: read-only device")
	// ErrSegStats is a seg-stats header that is malformed: an unknown full-or-delta
	// kind, or a full table carrying a nonzero base offset.
	ErrSegStats = errors.New("akifile: malformed seg-stats header")
	// ErrChunkDir is a chunk-directory header or descriptor that is malformed: an
	// unknown full-or-delta kind, a full directory carrying a nonzero base offset, or
	// a descriptor whose discriminator length exceeds the inline bound.
	ErrChunkDir = errors.New("akifile: malformed chunk-directory")
	// ErrBarrier is a barrier that is not a genuine cut: its segment global_seq
	// disagrees with the recorded Wbar, or a shard seq outruns the watermark, which
	// the single writer's total order cannot produce. A snapshot restore refuses such
	// a barrier rather than cutting a torn image.
	ErrBarrier = errors.New("akifile: inconsistent barrier")
	// ErrNoBarrier is a scan that reached the durable tail without finding a barrier at
	// the requested watermark: the snapshot was never written, or the file was
	// truncated before it.
	ErrNoBarrier = errors.New("akifile: no barrier at watermark")
)

var (
	le         = binary.LittleEndian
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// crc32c is the always-on hardware CRC used for the u32 header checksums.
func crc32c(spans ...[]byte) uint32 {
	var c uint32
	for _, s := range spans {
		c = crc32.Update(c, castagnoli, s)
	}
	return c
}

// checksum computes a u64 body checksum under the file's checksum kind over the
// concatenation of spans. It reports ok=false for a kind this build cannot
// compute, so a caller turns that into ErrChecksumKind rather than a wrong sum.
func checksum(kind uint32, spans ...[]byte) (sum uint64, ok bool) {
	switch kind {
	case ChecksumCRC32C:
		return uint64(crc32c(spans...)), true
	default:
		return 0, false
	}
}

// AlignUp rounds n up to the next multiple of a, which must be a power of two.
func AlignUp(n, a uint64) uint64 { return (n + a - 1) &^ (a - 1) }

// SegmentSpan is the on-disk footprint of a segment carrying payloadLen bytes:
// the 64-byte header plus the payload, zero-padded up to the 4KiB boundary the
// next segment starts on.
func SegmentSpan(payloadLen uint64) uint64 {
	return AlignUp(SegHeaderLen+payloadLen, SegmentAlign)
}
