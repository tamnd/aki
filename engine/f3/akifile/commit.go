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
