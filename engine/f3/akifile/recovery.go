package akifile

// The recovery open sequence (spec 2064/f3/07 section 6, steps 3-4): read the
// immutable prefix, pick the live meta root from the two slots, cross-check the
// shard geometry, and classify the open into one of three outcomes. This is the
// pure decision the recovery driver runs before it touches the append space; the
// tail replay and native rebuild (slice 5) build on the outcome it returns.

// OpenOutcome is how recovery must treat the file after it has picked a root.
type OpenOutcome uint8

const (
	// OpenClean is a file whose live root carries clean_shutdown: the roots are
	// trustworthy and the durable tail is the root's file_size, no replay needed.
	OpenClean OpenOutcome = iota
	// OpenCrashed is a valid root without clean_shutdown: the process died mid-run,
	// so recovery replays the append tail past the root's ckpt_log_pos to catch any
	// segments that landed after the last checkpoint.
	OpenCrashed
	// OpenScanFallback is both meta slots torn: no root to trust, so recovery falls
	// back to a full 4KiB-grid scan of the whole file. A valid recovery path, not an
	// error.
	OpenScanFallback
)

// OpenState is the result of the open sequence: the immutable prefix, the live
// root (nil in scan fallback), which slot it came from (0=A, 1=B, -1=neither),
// and the outcome that decides what recovery does next.
type OpenState struct {
	Prefix  *Prefix
	Meta    *MetaSlot
	Which   int
	Outcome OpenOutcome
}

// ReadOpenState runs the open decision against a device. It reads and validates
// the prefix (a bad magic or major stops here, recovery never guesses past it),
// reads both 128-byte meta slots from their separate sectors, and picks the live
// root. If neither slot validates, the state is a scan fallback with a nil root
// and no error, because the full scan is a legitimate recovery path. Otherwise it
// cross-checks the root's SRT shard count against the prefix (a disagreement is a
// torn SRT swap or the wrong-geometry open, ErrShardCount) and classifies the
// outcome by the root's clean_shutdown flag.
func ReadOpenState(dev Device) (*OpenState, error) {
	hb := make([]byte, PrefixSize)
	if _, err := dev.ReadAt(hb, 0); err != nil {
		return nil, err
	}
	prefix, err := ParsePrefix(hb)
	if err != nil {
		return nil, err
	}

	a := make([]byte, MetaSlotSize)
	if _, err := dev.ReadAt(a, int64(prefix.MetaSlotAOff)); err != nil {
		return nil, err
	}
	b := make([]byte, MetaSlotSize)
	if _, err := dev.ReadAt(b, int64(prefix.MetaSlotBOff)); err != nil {
		return nil, err
	}

	live, which, err := MetaLive(a, b, prefix.ChecksumKind)
	if err != nil {
		// Both slots torn: no root to trust, fall back to the full scan. This is a
		// recovery path, not a failure, so it returns a state rather than the error.
		return &OpenState{Prefix: prefix, Which: -1, Outcome: OpenScanFallback}, nil
	}

	// A live root whose SRT shard count disagrees with the prefix is a torn SRT
	// swap or a file opened under the wrong shard geometry; a zero count is an SRT
	// never written (a fresh file), which agrees by construction.
	if live.SRTShardCount != 0 && live.SRTShardCount != prefix.ShardCount {
		return nil, ErrShardCount
	}

	outcome := OpenCrashed
	if live.CleanShutdown == 1 {
		outcome = OpenClean
	}
	return &OpenState{Prefix: prefix, Meta: live, Which: which, Outcome: outcome}, nil
}

// ReplayTail walks the append space forward from a 4KiB-aligned start, validating
// each segment in full (header magic and CRC, then the payload length and CRC) and
// handing every intact one to visit in file order. It stops at the first segment
// that fails to parse or verify: the durable tail, past which lies a torn or
// never-synced write. It returns the offset just past the last intact segment, the
// cursor the writer resumes at.
//
// This is the primitive both recovery paths share (spec 2064/f3/07 section 6). A
// crashed open replays from the live root's checkpoint log position to catch the
// segments appended since the last checkpoint; the scan fallback replays from the
// header page to rebuild the whole index from the segments themselves. A visit
// that returns an error stops the walk at that segment and propagates the error,
// so a consumer that cannot apply a durable segment fails recovery rather than
// dropping committed data.
func ReplayTail(dev Device, prefix *Prefix, from, size uint64, visit func(off uint64, h *SegHeader, payload []byte) error) (uint64, error) {
	cursor := from
	for cursor+SegHeaderLen <= size {
		hb := make([]byte, SegHeaderLen)
		if _, err := dev.ReadAt(hb, int64(cursor)); err != nil {
			break
		}
		h, err := ParseSegHeader(hb)
		if err != nil {
			break
		}
		if cursor+SegHeaderLen+h.PayloadLen > size {
			break
		}
		payload := make([]byte, h.PayloadLen)
		if _, err := dev.ReadAt(payload, int64(cursor)+SegHeaderLen); err != nil {
			break
		}
		if h.VerifyPayload(payload, prefix.ChecksumKind) != nil {
			break
		}
		if visit != nil {
			if err := visit(cursor, h, payload); err != nil {
				return cursor, err
			}
		}
		cursor += SegmentSpan(h.PayloadLen)
	}
	return cursor, nil
}

// IndexRebuilder folds a shard's index back from a full checkpoint and the delta
// chain layered over it (spec 2064/f3/07 section 5). Recovery loads the base full
// dump, applies each delta forward, and is left with the live index as of the
// last checkpoint's log position, from which ReplayTail catches the tail. The
// rebuilder keys entries by key_hash, the identity the store also keys on; a
// verify-on-read resolves any hash collision the checkpoint could not.
type IndexRebuilder struct {
	entries map[uint64]CkptEntry
	// LogPos is the global_seq the last applied checkpoint is consistent up to, the
	// offset tail replay resumes from. SeqHigh is the highest shard record sequence
	// it reflects, a cross-check against the replayed tail.
	LogPos  uint64
	SeqHigh uint64
}

// NewIndexRebuilder starts an empty rebuild. The first Apply is expected to be a
// full checkpoint, but a delta over the empty index is equally valid.
func NewIndexRebuilder() *IndexRebuilder {
	return &IndexRebuilder{entries: make(map[uint64]CkptEntry)}
}

// Apply layers one checkpoint payload over the accumulated index and returns its
// parsed header. A full dump replaces the accumulator (every live entry, no
// tombstones); a delta applies over it, removing tombstoned keys and inserting or
// overwriting the rest. LogPos and SeqHigh advance to the applied checkpoint, so
// the caller resolves the chain oldest-first and replays the tail from LogPos
// once the newest delta is in.
func (r *IndexRebuilder) Apply(payload []byte) (CkptHeader, error) {
	h, err := ParseCkptHeader(payload)
	if err != nil {
		return CkptHeader{}, err
	}
	entries, err := CkptEntries(payload, h)
	if err != nil {
		return CkptHeader{}, err
	}
	if h.FullOrDelta == CkptFull {
		r.entries = make(map[uint64]CkptEntry, len(entries))
	}
	for _, e := range entries {
		if h.FullOrDelta == CkptDelta && e.Flags&CkptTombstone != 0 {
			delete(r.entries, e.KeyHash)
			continue
		}
		r.entries[e.KeyHash] = e
	}
	r.LogPos = h.CkptLogPos
	r.SeqHigh = h.SeqHigh
	return h, nil
}

// Entries is the live index as of the last applied checkpoint, keyed by key_hash.
// The map is the rebuilder's own; a caller that mutates it is on its own.
func (r *IndexRebuilder) Entries() map[uint64]CkptEntry { return r.entries }

// Len is the number of live index entries accumulated so far.
func (r *IndexRebuilder) Len() int { return len(r.entries) }

// ReadSRT reads the shard root table the live meta root points at, the per-shard
// map from a checkpoint to its replay entry point that a 128-byte meta slot has no
// room for (spec 2064/f3/07 sections 3 and 6). It completes the three-way
// shard-count agreement recovery requires: the prefix, the meta slot's
// SRTShardCount (checked in ReadOpenState), and the SRT's own row count must all
// match, or the table is a torn swap or a wrong-geometry open and recovery refuses
// it with ErrShardCount.
//
// A file with no checkpoints yet (a fresh clean file, or a scan fallback with no
// trusted root) has no table: the meta is nil or its SRTLen is zero, and ReadSRT
// returns a nil table and no error, which the driver reads as "no roots, replay
// from the header page".
func ReadSRT(dev Device, prefix *Prefix, meta *MetaSlot) (*SRT, error) {
	if meta == nil || meta.SRTLen == 0 {
		return nil, nil
	}
	buf := make([]byte, meta.SRTLen)
	if _, err := dev.ReadAt(buf, int64(meta.SRTOff)); err != nil {
		return nil, err
	}
	srt, err := ParseSRT(buf, prefix.ChecksumKind)
	if err != nil {
		return nil, err
	}
	if uint64(len(srt.Rows)) != uint64(prefix.ShardCount) {
		return nil, ErrShardCount
	}
	return srt, nil
}

// RebuildShardIndex reconstructs one shard's index from its checkpoint chain. The
// SRT names the shard's newest checkpoint segment (index_ckpt_off); that segment
// is either a full dump or a delta whose header names the base it extends
// (base_ckpt_off), and so on back to a full or to a delta over the empty index.
// RebuildShardIndex walks that chain back to its base, then applies the
// checkpoints oldest-first into an IndexRebuilder, leaving the live index as of
// the newest checkpoint's log position. A caller then replays the tail past
// IndexRebuilder.LogPos to catch the records written since.
//
// A zero index_ckpt_off means the shard has never been checkpointed: it returns
// an empty rebuilder and the whole index comes from the tail replay. A segment
// that is not an index_ckpt, or a back-pointer that revisits a segment already in
// the chain, is a corrupt root and returns ErrCheckpoint.
func RebuildShardIndex(dev Device, prefix *Prefix, indexCkptOff uint64) (*IndexRebuilder, error) {
	r := NewIndexRebuilder()
	if indexCkptOff == 0 {
		return r, nil
	}
	// Walk the delta chain back to its base, newest first, caching each payload so
	// the apply pass does not re-read. The seen set catches a back-pointer cycle.
	type link struct {
		off     uint64
		payload []byte
	}
	var chain []link
	seen := make(map[uint64]bool)
	for off := indexCkptOff; ; {
		if seen[off] {
			return nil, ErrCheckpoint
		}
		seen[off] = true
		h, payload, err := readSegmentAt(dev, prefix.ChecksumKind, off)
		if err != nil {
			return nil, err
		}
		if h.Kind != KindIndexCkpt {
			return nil, ErrCheckpoint
		}
		ch, err := ParseCkptHeader(payload)
		if err != nil {
			return nil, err
		}
		chain = append(chain, link{off, payload})
		// A full dump or a delta over the empty index (base zero) is the base of the
		// chain: stop walking and apply forward from here.
		if ch.FullOrDelta == CkptFull || ch.BaseCkptOff == 0 {
			break
		}
		off = ch.BaseCkptOff
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if _, err := r.Apply(chain[i].payload); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// SegStatsRebuilder folds a shard's dead-byte accounting table back from a full
// seg-stats checkpoint and the delta chain layered over it (spec 2064/f3/07 section
// 6, "Dead-byte accounting that survives restart"). It is the durable half of the O10
// fix: without it a restart zeroes the per-segment (live, dead) counters and the
// store under-triggers compaction until organic churn rediscovers the garbage.
// Recovery loads the base full table, applies each delta forward, and is left with
// the accounting as of the last checkpoint's log position; the tail replay past
// LogPos re-derives the deltas since. The rebuilder keys entries by seg_off, the
// segment's identity.
type SegStatsRebuilder struct {
	entries map[uint64]SegStatsEntry
	// LogPos is the global_seq the last applied seg-stats checkpoint is consistent up
	// to, the offset the tail replay resumes the dead-byte re-derivation from.
	LogPos uint64
}

// NewSegStatsRebuilder starts an empty rebuild. The first Apply is expected to be a
// full table, but a delta over the empty table is equally valid.
func NewSegStatsRebuilder() *SegStatsRebuilder {
	return &SegStatsRebuilder{entries: make(map[uint64]SegStatsEntry)}
}

// Apply layers one seg-stats payload over the accumulated table and returns its
// parsed header. A full table replaces the accumulator; a delta applies over it,
// dropping segments flagged SegStatsFreed (compacted away) and inserting or
// overwriting the rest. LogPos advances to the applied checkpoint, so the caller
// resolves the chain oldest-first and replays the tail from LogPos once the newest
// delta is in.
func (r *SegStatsRebuilder) Apply(payload []byte) (SegStatsHeader, error) {
	h, err := ParseSegStatsHeader(payload)
	if err != nil {
		return SegStatsHeader{}, err
	}
	entries, err := SegStatsEntries(payload, h)
	if err != nil {
		return SegStatsHeader{}, err
	}
	if h.FullOrDelta == SegStatsFull {
		r.entries = make(map[uint64]SegStatsEntry, len(entries))
	}
	for _, e := range entries {
		if h.FullOrDelta == SegStatsDelta && e.Flags&SegStatsFreed != 0 {
			delete(r.entries, e.SegOff)
			continue
		}
		r.entries[e.SegOff] = e
	}
	r.LogPos = h.CkptLogPos
	return h, nil
}

// Entries is the live accounting table as of the last applied checkpoint, keyed by
// seg_off. The map is the rebuilder's own.
func (r *SegStatsRebuilder) Entries() map[uint64]SegStatsEntry { return r.entries }

// Len is the number of tracked segments accumulated so far.
func (r *SegStatsRebuilder) Len() int { return len(r.entries) }

// TotalDeadBytes sums the dead-byte counts across the table, the compaction trigger's
// fuel a reopened store reads to decide which segments to reclaim without a scan.
func (r *SegStatsRebuilder) TotalDeadBytes() uint64 {
	var n uint64
	for _, e := range r.entries {
		n += e.DeadBytes
	}
	return n
}

// RebuildShardSegStats reconstructs one shard's dead-byte accounting from its
// seg-stats checkpoint chain, the same walk RebuildShardIndex runs for the index. The
// SRT names the shard's newest seg-stats segment (segstats_off); that segment is a
// full table or a delta whose header names the base it extends, back to a full or a
// delta over the empty table. It walks the chain to its base, then applies the
// checkpoints oldest-first into a SegStatsRebuilder.
//
// A zero segstats_off means the shard has never checkpointed its accounting: it
// returns an empty rebuilder and the whole table comes from the tail replay. A
// segment that is not a seg_stats, or a back-pointer that revisits a segment already
// in the chain, is a corrupt root and returns ErrSegStats.
func RebuildShardSegStats(dev Device, prefix *Prefix, segStatsOff uint64) (*SegStatsRebuilder, error) {
	r := NewSegStatsRebuilder()
	if segStatsOff == 0 {
		return r, nil
	}
	type link struct {
		off     uint64
		payload []byte
	}
	var chain []link
	seen := make(map[uint64]bool)
	for off := segStatsOff; ; {
		if seen[off] {
			return nil, ErrSegStats
		}
		seen[off] = true
		h, payload, err := readSegmentAt(dev, prefix.ChecksumKind, off)
		if err != nil {
			return nil, err
		}
		if h.Kind != KindSegStats {
			return nil, ErrSegStats
		}
		sh, err := ParseSegStatsHeader(payload)
		if err != nil {
			return nil, err
		}
		chain = append(chain, link{off, payload})
		if sh.FullOrDelta == SegStatsFull || sh.BaseCkptOff == 0 {
			break
		}
		off = sh.BaseCkptOff
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if _, err := r.Apply(chain[i].payload); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// ChunkDirRebuilder folds a shard's cold-chunk directory back from a full chunk_dir
// checkpoint and the delta chain layered over it (spec 2064/f3/07 section 4, "Cold
// chunks and the value log"). Without it a restart would have to scan every cold chunk
// to answer a membership or rank query on a cold collection; with it a reopened store
// rebuilds the resident directory from the checkpoints plus tail replay and reads no
// cold bytes until a query needs the real element. Recovery loads the base full
// directory, applies each delta forward, and is left with the directory as of the last
// checkpoint's log position; the tail replay past LogPos re-derives the deltas since.
// The rebuilder keys collections by key_hash, the directory's own lookup key.
type ChunkDirRebuilder struct {
	collections map[uint64]ChunkDirCollection
	// LogPos is the global_seq the last applied chunk_dir checkpoint is consistent up
	// to, the offset the tail replay resumes the directory re-derivation from.
	LogPos uint64
}

// NewChunkDirRebuilder starts an empty rebuild. The first Apply is expected to be a
// full directory, but a delta over the empty directory is equally valid.
func NewChunkDirRebuilder() *ChunkDirRebuilder {
	return &ChunkDirRebuilder{collections: make(map[uint64]ChunkDirCollection)}
}

// Apply layers one chunk_dir payload over the accumulated directory and returns its
// parsed header. A full directory replaces the accumulator; a delta applies over it,
// dropping collections flagged ChunkDirTombstone (promoted back to the hot tier or
// deleted) and inserting or overwriting the rest. LogPos advances to the applied
// checkpoint, so the caller resolves the chain oldest-first and replays the tail from
// LogPos once the newest delta is in.
func (r *ChunkDirRebuilder) Apply(payload []byte) (ChunkDirHeader, error) {
	h, err := ParseChunkDirHeader(payload)
	if err != nil {
		return ChunkDirHeader{}, err
	}
	cols, err := ChunkDirCollections(payload, h)
	if err != nil {
		return ChunkDirHeader{}, err
	}
	if h.FullOrDelta == ChunkDirFull {
		r.collections = make(map[uint64]ChunkDirCollection, len(cols))
	}
	for _, c := range cols {
		if h.FullOrDelta == ChunkDirDelta && c.Flags&ChunkDirTombstone != 0 {
			delete(r.collections, c.KeyHash)
			continue
		}
		r.collections[c.KeyHash] = c
	}
	r.LogPos = h.CkptLogPos
	return h, nil
}

// Collections is the live cold-chunk directory as of the last applied checkpoint, keyed
// by key_hash. The map is the rebuilder's own.
func (r *ChunkDirRebuilder) Collections() map[uint64]ChunkDirCollection { return r.collections }

// Len is the number of cold collections accumulated so far.
func (r *ChunkDirRebuilder) Len() int { return len(r.collections) }

// TotalLiveBytes sums the live-byte counts across every chunk of every collection, the
// cold-tier residency a reopened store reads to size its cold footprint without a scan.
func (r *ChunkDirRebuilder) TotalLiveBytes() uint64 {
	var n uint64
	for _, c := range r.collections {
		for i := range c.Chunks {
			n += c.Chunks[i].ChunkLiveBytes
		}
	}
	return n
}

// RebuildShardChunkDir reconstructs one shard's cold-chunk directory from its chunk_dir
// checkpoint chain, the same walk RebuildShardIndex runs for the index. The SRT names
// the shard's newest chunk_dir segment (chunkdir_off); that segment is a full directory
// or a delta whose header names the base it extends, back to a full or a delta over the
// empty directory. It walks the chain to its base, then applies the checkpoints
// oldest-first into a ChunkDirRebuilder.
//
// A zero chunkdir_off means the shard has no cold chunks checkpointed: it returns an
// empty rebuilder and any cold directory comes from the tail replay. A segment that is
// not a chunk_dir, or a back-pointer that revisits a segment already in the chain, is a
// corrupt root and returns ErrChunkDir.
func RebuildShardChunkDir(dev Device, prefix *Prefix, chunkdirOff uint64) (*ChunkDirRebuilder, error) {
	r := NewChunkDirRebuilder()
	if chunkdirOff == 0 {
		return r, nil
	}
	type link struct {
		off     uint64
		payload []byte
	}
	var chain []link
	seen := make(map[uint64]bool)
	for off := chunkdirOff; ; {
		if seen[off] {
			return nil, ErrChunkDir
		}
		seen[off] = true
		h, payload, err := readSegmentAt(dev, prefix.ChecksumKind, off)
		if err != nil {
			return nil, err
		}
		if h.Kind != KindChunkDir {
			return nil, ErrChunkDir
		}
		ch, err := ParseChunkDirHeader(payload)
		if err != nil {
			return nil, err
		}
		chain = append(chain, link{off, payload})
		if ch.FullOrDelta == ChunkDirFull || ch.BaseCkptOff == 0 {
			break
		}
		off = ch.BaseCkptOff
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if _, err := r.Apply(chain[i].payload); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Recovery is the structural result of opening a file: the picked open state, the
// shard root table (nil when no checkpoint has been taken), and each shard's auxiliary
// tables rebuilt from their checkpoint chains (all nil on a scan fallback, where there
// is no trusted root to name the checkpoints). TailFrom is the offset the caller starts
// the tail replay at; the caller applies the tail's log segments, which carry records
// the akifile layer cannot interpret, over the rebuilt tables to reach the live state.
//
// The three per-shard slices are index-aligned with the SRT rows: Indexes[k] is shard
// k's rebuilt index, SegStats[k] its dead-byte accounting, ChunkDirs[k] its cold-chunk
// directory. A shard that never checkpointed a given table carries an empty rebuilder
// there, not a nil, so a consumer folds the tail over a table it can always index.
type Recovery struct {
	State     *OpenState
	SRT       *SRT
	Indexes   []*IndexRebuilder
	SegStats  []*SegStatsRebuilder
	ChunkDirs []*ChunkDirRebuilder
	TailFrom  uint64
}

// Recover runs the whole open sequence and assembles the structural recovery
// (spec 2064/f3/07 section 6): pick the live root, read the shard root table, and
// rebuild each shard's index, dead-byte accounting, and cold-chunk directory from
// their checkpoint chains. It stops at the boundary the akifile format owns: the tail
// replay past TailFrom is the caller's, because turning a log segment back into index
// entries is store knowledge this layer does not hold.
//
// The tail-replay start depends on the outcome. A scan fallback has no root, so it
// replays the whole append space from the header page. A file with no checkpoints
// yet likewise starts from the header page. A crashed open replays from the
// earliest shard's first un-checkpointed segment, the point before which every
// segment is already in a checkpoint. A clean open checkpointed everything on the
// way down, so there is nothing past the roots and TailFrom is the file size.
func Recover(dev Device) (*Recovery, error) {
	st, err := ReadOpenState(dev)
	if err != nil {
		return nil, err
	}
	rec := &Recovery{State: st}
	if st.Outcome == OpenScanFallback {
		rec.TailFrom = PageSize
		return rec, nil
	}

	srt, err := ReadSRT(dev, st.Prefix, st.Meta)
	if err != nil {
		return nil, err
	}
	rec.SRT = srt
	if srt == nil {
		// No checkpoint has been taken: the whole index comes from the tail replay.
		rec.TailFrom = PageSize
		return rec, nil
	}

	rec.Indexes = make([]*IndexRebuilder, len(srt.Rows))
	rec.SegStats = make([]*SegStatsRebuilder, len(srt.Rows))
	rec.ChunkDirs = make([]*ChunkDirRebuilder, len(srt.Rows))
	tailFrom := st.Meta.FileSize
	for i := range srt.Rows {
		idx, err := RebuildShardIndex(dev, st.Prefix, srt.Rows[i].IndexCkptOff)
		if err != nil {
			return nil, err
		}
		rec.Indexes[i] = idx
		ss, err := RebuildShardSegStats(dev, st.Prefix, srt.Rows[i].SegstatsOff)
		if err != nil {
			return nil, err
		}
		rec.SegStats[i] = ss
		cd, err := RebuildShardChunkDir(dev, st.Prefix, srt.Rows[i].ChunkdirOff)
		if err != nil {
			return nil, err
		}
		rec.ChunkDirs[i] = cd
		if ft := srt.Rows[i].FirstTailSeg; ft != 0 && ft < tailFrom {
			tailFrom = ft
		}
	}
	if st.Outcome == OpenClean {
		rec.TailFrom = st.Meta.FileSize
	} else {
		rec.TailFrom = tailFrom
	}
	return rec, nil
}

// ReadExtentTable reads the coarse extent map the live meta root points at (spec
// 2064/f3/07 section 3). The map records the file's regions (the header page, the
// append space, free runs) so a tool or a fresh open sees the file's shape without
// a scan; it is a hint, not a source of truth. Recovery reaches segments through
// SRT roots and per-shard chains, never through this map, so a torn extent table
// only costs the shape hint, not the data.
//
// A file with no extent table (a fresh file, or a scan fallback with no trusted
// root) returns a nil slice and no error: the meta is nil or its ExtentTableLen is
// zero. A length that is not a whole number of extents is a torn table, returned
// as ErrLength.
func ReadExtentTable(dev Device, meta *MetaSlot) ([]Extent, error) {
	if meta == nil || meta.ExtentTableLen == 0 {
		return nil, nil
	}
	buf := make([]byte, meta.ExtentTableLen)
	if _, err := dev.ReadAt(buf, int64(meta.ExtentTableOff)); err != nil {
		return nil, err
	}
	return ParseExtents(buf)
}

// ReadTTLIndex reads the TTL reclaim index the live meta root points at (spec
// 2064/f3/07 section 3), the per-class expiry-bound map recovery step 11 and active
// expiry consult to reclaim wholly-expired segments without scanning them. Like the
// extent map it is a bare marshaled root the meta slot points straight at
// (TTLIndexOff/TTLIndexLen), read without walking a segment header.
//
// It is an accelerator, not a source of truth: the authoritative expiry is each
// record's own TTL in the tail, so a torn index only costs the fast path and recovery
// falls back to the per-segment scan. The payload carries the TTL3 magic and every
// length is bounds-checked, so a torn index surfaces as ErrMagic or ErrLength rather
// than a bad reclaim; the caller decides whether to degrade or refuse.
//
// A file with no TTL index (a fresh file, or a scan fallback with no trusted root)
// returns a nil slice and no error: the meta is nil or its TTLIndexLen is zero.
func ReadTTLIndex(dev Device, meta *MetaSlot) ([]TTLClass, error) {
	if meta == nil || meta.TTLIndexLen == 0 {
		return nil, nil
	}
	buf := make([]byte, meta.TTLIndexLen)
	if _, err := dev.ReadAt(buf, int64(meta.TTLIndexOff)); err != nil {
		return nil, err
	}
	h, err := ParseTTLIndexHeader(buf)
	if err != nil {
		return nil, err
	}
	return TTLClasses(buf, h)
}

// ReadFreeMap reads the free map the live meta root points at (spec 2064/f3/07
// sections 2-3), the writer's record of the reclaimed runs it allocates from before
// growing the file. Unlike the SRT and extent map, the free map has no length in the
// meta slot: FreeMapOff names a whole free_map segment, so the reader walks its 64-byte
// header for the payload length and reads the segment self-describingly. That also
// buys it the segment's payload CRC, which the bare roots forgo, so a torn free map is
// caught as ErrChecksum rather than read as a bad allocation set.
//
// FreeMapTotals then splits the runs into allocatable and pending-free bytes; the free
// total is the forward-progress signal a writer blocked on a full disk waits on. A
// segment at the pointer that is not a free_map is a misdirected or corrupt root,
// returned as ErrMagic. A file with no free map (a fresh file, or a scan fallback with
// no trusted root) returns a nil slice and no error.
func ReadFreeMap(dev Device, prefix *Prefix, meta *MetaSlot) ([]FreeExtent, error) {
	if meta == nil || meta.FreeMapOff == 0 {
		return nil, nil
	}
	h, payload, err := readSegmentAt(dev, prefix.ChecksumKind, meta.FreeMapOff)
	if err != nil {
		return nil, err
	}
	if h.Kind != KindFreeMap {
		return nil, ErrMagic
	}
	fh, err := ParseFreeMapHeader(payload)
	if err != nil {
		return nil, err
	}
	return FreeExtents(payload, fh)
}
