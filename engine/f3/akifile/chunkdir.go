package akifile

// The chunk-directory codec (spec 2064/f3/07 section 4, "Cold chunks and the value
// log"). A chunk_dir payload is the durable form of the resident chunk directory
// (engine/f3/tier): per cold collection, a key hash and an ordered run of chunk
// descriptors. Each descriptor carries the chunk's first discriminator, its element
// count, its file offset, and its live-byte count. The directory rebuilds from these
// checkpoints plus tail replay, never from chunk scans, so a reopened store answers
// a cold membership or rank query without reading a single cold byte.
//
// Per-chunk live_bytes rides in the descriptor on purpose: a cold element deleted or
// overwritten marks its chunk's dead bytes in the directory without touching the
// chunk, and compaction picks victims from the directory alone. The first
// discriminator is stored inline up to 32 bytes, the same bound the resident
// directory keeps (its maxDisc); a longer discriminator is truncated to its ordering
// prefix, which is all the binary search needs since the confirming pread reads the
// real element bytes anyway.
//
// The directory checkpoints full-or-delta, pinned to ckpt_log_pos like every other
// root: a full replaces the table, a delta applies over a named base and a
// tombstoned collection is dropped (a collection promoted back to the hot tier or
// deleted outright). The payload carries no checksum of its own; the chunk_dir
// segment header's payload CRC covers it, so the CHD3 magic here is a kind
// cross-check, not an integrity field. Codec only: it frames into and reads out of a
// caller-owned payload and never touches a File. The consumer that dumps a live
// shard's directory into these segments and the replay path that folds the tail
// deltas are separate slices.

// ChunkDirMagic is the chunk_dir payload sentinel.
const ChunkDirMagic = "CHD3"

const (
	// ChunkDirHeaderLen is the fixed chunk-directory header size.
	ChunkDirHeaderLen = 32
	// ChunkDirCollectionLen is one collection's fixed header: key_hash u64, then
	// chunk_count u32 and flags u32. Chunk descriptors follow it.
	ChunkDirCollectionLen = 16
	// ChunkDirRowSize is one chunk descriptor: the 32-byte inline discriminator, a
	// one-byte discriminator length, 3 reserved bytes, element_count u32, chunk_off
	// u64, and chunk_live_bytes u64.
	ChunkDirRowSize = 56
	// ChunkDirMaxDisc is the inline first-discriminator width, matching the resident
	// directory's bound so the durable form round-trips the resident descriptor.
	ChunkDirMaxDisc = 32
)

// Chunk-directory kinds (the full_or_delta byte), the same full-or-delta shape the
// index checkpoint and seg-stats table use.
const (
	ChunkDirFull  uint8 = 1 // a full directory: every cold collection, base_ckpt_off zero
	ChunkDirDelta uint8 = 2 // a delta over a base: only collections whose directory moved
)

// ChunkDirTombstone marks a collection dropped from the table when a delta is applied
// over its base: it was promoted back to the hot tier or deleted, so its descriptors
// go away. A tombstoned collection carries no chunk descriptors.
const ChunkDirTombstone uint32 = 1 << 0

// ChunkDirHeader binds a directory to the log position it is consistent up to. A
// delta names the base it extends; a full leaves base zero.
type ChunkDirHeader struct {
	FullOrDelta     uint8  // ChunkDirFull or ChunkDirDelta
	CkptLogPos      uint64 // the global_seq this directory is consistent up to; replay re-derives past it
	CollectionCount uint64 // cold collections that follow the header
	BaseCkptOff     uint64 // for a delta, the chunk_dir segment it extends; 0 for a full
}

// ChunkDirRow is one chunk's descriptor: the first discriminator that orders the
// chunk, its element count, its file offset, and its live-byte count. FirstDisc is at
// most ChunkDirMaxDisc bytes.
type ChunkDirRow struct {
	FirstDisc      []byte // the chunk's first discriminator, up to 32 bytes
	ElementCount   uint32 // elements packed in the chunk
	ChunkOff       uint64 // the chunk segment's file offset
	ChunkLiveBytes uint64 // bytes still referenced; the compaction victim signal
}

// ChunkDirCollection is one cold collection's directory: its key hash, its flags, and
// its ordered chunk descriptors. A tombstoned collection (ChunkDirTombstone in a
// delta) carries no chunks.
type ChunkDirCollection struct {
	KeyHash uint64        // the collection key's hash, the directory's lookup key
	Flags   uint32        // ChunkDirTombstone or zero
	Chunks  []ChunkDirRow // chunk descriptors in discriminator order
}

// AppendChunkDirHeader frames a chunk-directory header onto dst. Collections follow
// with AppendChunkDirCollectionHeader and AppendChunkDirRow, so a large directory
// streams out in bounded slices without holding every collection in memory at once.
func AppendChunkDirHeader(dst []byte, h ChunkDirHeader) []byte {
	var b [ChunkDirHeaderLen]byte
	copy(b[0:4], ChunkDirMagic)
	b[4] = h.FullOrDelta
	// b[5:8] reserved, left zero.
	le.PutUint64(b[8:16], h.CkptLogPos)
	le.PutUint64(b[16:24], h.CollectionCount)
	le.PutUint64(b[24:32], h.BaseCkptOff)
	return append(dst, b[:]...)
}

// AppendChunkDirCollectionHeader frames one collection's 16-byte header onto dst.
// chunkCount descriptors follow with AppendChunkDirRow, except for a tombstoned
// collection, which carries none.
func AppendChunkDirCollectionHeader(dst []byte, keyHash uint64, chunkCount, flags uint32) []byte {
	var b [ChunkDirCollectionLen]byte
	le.PutUint64(b[0:8], keyHash)
	le.PutUint32(b[8:12], chunkCount)
	le.PutUint32(b[12:16], flags)
	return append(dst, b[:]...)
}

// AppendChunkDirRow frames one 56-byte chunk descriptor onto dst. A discriminator
// longer than ChunkDirMaxDisc is truncated to its ordering prefix, matching the
// resident directory.
func AppendChunkDirRow(dst []byte, r ChunkDirRow) []byte {
	var b [ChunkDirRowSize]byte
	dlen := copy(b[0:ChunkDirMaxDisc], r.FirstDisc)
	b[ChunkDirMaxDisc] = uint8(dlen)
	// b[33:36] reserved, left zero.
	le.PutUint32(b[36:40], r.ElementCount)
	le.PutUint64(b[40:48], r.ChunkOff)
	le.PutUint64(b[48:56], r.ChunkLiveBytes)
	return append(dst, b[:]...)
}

// ParseChunkDirHeader decodes and validates a chunk-directory header: the magic, a
// known full-or-delta kind, and the full-table invariant that a full carries no base.
func ParseChunkDirHeader(b []byte) (ChunkDirHeader, error) {
	if len(b) < ChunkDirHeaderLen {
		return ChunkDirHeader{}, ErrShort
	}
	if string(b[0:4]) != ChunkDirMagic {
		return ChunkDirHeader{}, ErrMagic
	}
	h := ChunkDirHeader{
		FullOrDelta:     b[4],
		CkptLogPos:      le.Uint64(b[8:16]),
		CollectionCount: le.Uint64(b[16:24]),
		BaseCkptOff:     le.Uint64(b[24:32]),
	}
	switch h.FullOrDelta {
	case ChunkDirFull:
		if h.BaseCkptOff != 0 {
			return ChunkDirHeader{}, ErrChunkDir
		}
	case ChunkDirDelta:
		// a delta may name any base, including 0 for a delta over the empty table
	default:
		return ChunkDirHeader{}, ErrChunkDir
	}
	return h, nil
}

// parseChunkDirRow decodes one 56-byte chunk descriptor, returning the inline
// discriminator as a fresh slice so it does not alias the payload.
func parseChunkDirRow(b []byte) (ChunkDirRow, error) {
	if len(b) < ChunkDirRowSize {
		return ChunkDirRow{}, ErrShort
	}
	dlen := int(b[ChunkDirMaxDisc])
	if dlen > ChunkDirMaxDisc {
		return ChunkDirRow{}, ErrChunkDir
	}
	return ChunkDirRow{
		FirstDisc:      append([]byte(nil), b[0:dlen]...),
		ElementCount:   le.Uint32(b[36:40]),
		ChunkOff:       le.Uint64(b[40:48]),
		ChunkLiveBytes: le.Uint64(b[48:56]),
	}, nil
}

// ChunkDirCollections decodes every collection in a chunk-directory payload after its
// header, the load path that restores the resident directory from a full or delta. It
// walks the nested structure with a bounds check at every step so a corrupt count
// (collections or chunks) cannot over-read: a count that outruns the remaining bytes
// is ErrLength.
func ChunkDirCollections(payload []byte, h ChunkDirHeader) ([]ChunkDirCollection, error) {
	if uint64(len(payload)) < ChunkDirHeaderLen {
		return nil, ErrShort
	}
	// Every collection costs at least its fixed header, so a count that exceeds the
	// remaining bytes divided by that floor is corrupt before we walk a single row.
	rest := uint64(len(payload)) - ChunkDirHeaderLen
	if h.CollectionCount > rest/ChunkDirCollectionLen {
		return nil, ErrLength
	}
	cols := make([]ChunkDirCollection, h.CollectionCount)
	off := uint64(ChunkDirHeaderLen)
	for i := range cols {
		if off+ChunkDirCollectionLen > uint64(len(payload)) {
			return nil, ErrLength
		}
		keyHash := le.Uint64(payload[off : off+8])
		chunkCount := le.Uint32(payload[off+8 : off+12])
		flags := le.Uint32(payload[off+12 : off+16])
		off += ChunkDirCollectionLen

		need := uint64(chunkCount) * ChunkDirRowSize
		if need > uint64(len(payload))-off {
			return nil, ErrLength
		}
		rows := make([]ChunkDirRow, chunkCount)
		for j := range rows {
			r, err := parseChunkDirRow(payload[off : off+ChunkDirRowSize])
			if err != nil {
				return nil, err
			}
			rows[j] = r
			off += ChunkDirRowSize
		}
		cols[i] = ChunkDirCollection{KeyHash: keyHash, Flags: flags, Chunks: rows}
	}
	return cols, nil
}
