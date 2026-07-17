package akifile

// CommitRoots atomically installs a new root by flipping the stale meta slot
// (spec 2064/f3/07 sections 3 and 6). The caller supplies the roots the new slot
// names (the SRT and extent table it has already written to free space, plus the
// live accounting); CommitRoots stamps the next commit_seq, the current global_seq
// and durable file size, writes the slot into whichever of the two sits stale, and
// fsyncs. A commit is a durability barrier, so it always flushes regardless of the
// append sync policy.
//
// The flip is crash-atomic by construction: the two slots sit in separate sectors
// and the live slot is never touched, so a crash mid-write damages at most the
// stale slot and the previous root stays live. On success the freshly written slot
// becomes live and the next commit flips back to the other.
func (f *File) CommitRoots(m MetaSlot) error {
	stale := 1 - f.liveSlot
	m.CommitSeq = f.commitSeq + 1
	m.GlobalSeq = f.globalSeq
	m.FileSize = f.cursor

	b, err := m.Marshal(f.prefix.ChecksumKind)
	if err != nil {
		return err
	}
	off := f.prefix.MetaSlotAOff
	if stale == 1 {
		off = f.prefix.MetaSlotBOff
	}
	if err := f.writeAt(b, off); err != nil {
		return err
	}
	if err := f.dev.Sync(); err != nil {
		return err
	}

	f.commitSeq = m.CommitSeq
	f.liveSlot = stale
	f.lastSync = f.now()
	return nil
}

// CommitSeq is the commit sequence of the live root, the counter a commit advances.
func (f *File) CommitSeq() uint64 { return f.commitSeq }

// CheckpointStats is the live accounting a checkpoint records in the new root: the
// bytes and records the file holds, when it was taken, and whether it is a clean
// shutdown (the last checkpoint of a graceful close, which lets a reopen skip the
// tail replay).
type CheckpointStats struct {
	LiveBytes    uint64
	DeadBytes    uint64
	RecordCount  uint64
	LastCkptUnix uint64
	Clean        bool
}

// CheckpointGlobals names the two file-global roots a checkpoint stamps into the new
// meta slot alongside the SRT and extent map: the TTL reclaim index (a bare marshaled
// root, so it carries a length) and the free map (a self-describing segment the meta
// slot points at by offset alone). Both are produced by their own subsystems, active
// expiry and the allocator, and handed to the checkpoint already written to free space
// the way the per-shard roots are assembled into the SRT rows before the commit.
//
// The offsets are absolute file offsets a reader reads straight from: TTLIndexOff is
// the bare marshaled root, FreeMapOff is the free_map segment header. A zero offset
// names no root, so a checkpoint stamps exactly the globals it is given, the way a
// checkpoint with no extents leaves the extent map unnamed.
type CheckpointGlobals struct {
	TTLIndexOff uint64
	TTLIndexLen uint32
	FreeMapOff  uint64
}

// Checkpoint writes the current roots to free space and commits them (spec
// 2064/f3/07 section 5). It appends the shard root table and, if any, the extent
// map as owner-less segments past the append tail, then stamps a new meta slot
// pointing at those bytes together with the caller's accounting and flips it live.
//
// It only appends, never rewriting live data, so it does not block the writer: a
// checkpoint is forkless. The roots ride the segment grid like any other segment
// (their payload CRC guards them and a scan skips them), while the meta slot points
// straight at each payload so recovery reads a root without walking a header.
func (f *File) Checkpoint(srt *SRT, extents []Extent, stats CheckpointStats) error {
	return f.CheckpointWithGlobals(srt, extents, stats, CheckpointGlobals{})
}

// CheckpointWithGlobals is Checkpoint with the file-global roots stamped too: the same
// SRT and extent map append, plus the TTL index and free map offsets the caller has
// already written to free space (spec 2064/f3/07 section 5). The globals ride the same
// meta slot flip, so recovery reads them back atomically with the SRT and extents from
// the one live root. A zero-valued CheckpointGlobals is exactly Checkpoint.
func (f *File) CheckpointWithGlobals(srt *SRT, extents []Extent, stats CheckpointStats, globals CheckpointGlobals) error {
	srtBytes, err := srt.Marshal(f.prefix.ChecksumKind)
	if err != nil {
		return err
	}
	group := []Pending{{Shard: ShardOwnerless, Kind: KindSRT, Payload: srtBytes}}
	var extBytes []byte
	if len(extents) > 0 {
		extBytes = MarshalExtents(extents)
		group = append(group, Pending{Shard: ShardOwnerless, Kind: KindExtentTable, Payload: extBytes})
	}
	offs, err := f.AppendGroup(group)
	if err != nil {
		return err
	}

	m := MetaSlot{
		SRTOff:        offs[0] + SegHeaderLen,
		SRTLen:        uint32(len(srtBytes)),
		SRTShardCount: uint32(len(srt.Rows)),
		TTLIndexOff:   globals.TTLIndexOff,
		TTLIndexLen:   globals.TTLIndexLen,
		FreeMapOff:    globals.FreeMapOff,
		LiveBytes:     stats.LiveBytes,
		DeadBytes:     stats.DeadBytes,
		RecordCount:   stats.RecordCount,
		LastCkptUnix:  stats.LastCkptUnix,
	}
	if extBytes != nil {
		m.ExtentTableOff = offs[1] + SegHeaderLen
		m.ExtentTableLen = uint32(len(extBytes))
	}
	if stats.Clean {
		m.CleanShutdown = 1
	}
	return f.CommitRoots(m)
}
