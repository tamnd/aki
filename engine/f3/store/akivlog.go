package store

import "github.com/tamnd/aki/engine/f3/akifile"

// akiVlog re-homes the per-shard scratch value log (vlog.go) onto the shared
// .aki value region. The scratch log owns its own file, truncates on open, and
// hands out plain uint64 offsets into that file; this adapter instead stages
// values through an akifile ValueLogWriter and cuts one value_log segment per
// flush into the single .aki, so the store keeps one durable file instead of a
// scratch file per shard. It mirrors the scratch log's owner-thread contract:
// one adapter per shard, plain fields, no atomics, because a second toucher
// does not exist.
//
// The scratch log's append returned an offset readable the instant it returned.
// The .aki segment offset is not known until the group-commit writer assigns it
// at flush, so the contract splits: stage returns a batch index readable through
// readStaged before the cut, and flush resolves the batch to absolute pointers
// the record publishes at the group boundary. That boundary is where the
// command's ack already waits on the group fsync, so the value log's cut lands
// on the same seam the record's durability does.
//
// This adapter is store-side but not yet wired into the reactor: it proves the
// re-home's accounting and read paths in isolation before any flip of the hot
// path onto it.
type akiVlog struct {
	w     *akifile.ValueLogWriter
	f     *akifile.File
	shard uint16

	// seq stamps each cut value_log segment, advanced once per non-empty flush
	// the way the scratch log advanced its flushed tail.
	seq uint64

	// total is every flushed value byte; dead is the subset an overwrite, a
	// delete, or an expiry unlinked. live = total - dead is what a compaction of
	// the value region keeps, dead is what it reclaims, the same accounting the
	// scratch log's LogBytes exposed.
	total uint64
	dead  uint64
}

// newAkiVlog builds a value log for shard backed by f's group-commit writer.
func newAkiVlog(f *akifile.File, shard uint16) *akiVlog {
	return &akiVlog{w: akifile.NewValueLogWriter(f, shard), f: f, shard: shard}
}

// stage frames val into the pending batch and returns its index in stage order,
// readable through readStaged until the next flush. The absolute pointer for the
// index comes back from flush.
func (l *akiVlog) stage(val []byte) int { return l.w.Stage(val) }

// staged reports how many values await the next flush.
func (l *akiVlog) staged() int { return l.w.Staged() }

// readStaged serves a staged value from the pending buffer before its segment is
// cut, the read-before-flush the scratch log gave from its own pending buffer.
func (l *akiVlog) readStaged(idx int) ([]byte, error) { return l.w.ReadStaged(idx) }

// flush cuts one value_log segment for the staged batch and returns a pointer per
// staged value in stage order. An empty batch is a no-op that leaves the sequence
// untouched, so shard_seq advances only on a real cut. The flushed value bytes
// join the total the moment they land.
func (l *akiVlog) flush() ([]akifile.ValuePointer, error) {
	if l.w.Staged() == 0 {
		return nil, nil
	}
	l.seq++
	ptrs, err := l.w.Flush(l.seq)
	if err != nil {
		return nil, err
	}
	for _, p := range ptrs {
		l.total += uint64(p.ValueLen)
	}
	return ptrs, nil
}

// readAt resolves a published value from its offset and length, verifying the
// frame's own trailing CRC. It is the read a record's in-place pointer takes: a
// 48-bit offset with the length beside it and no room for a stored CRC, so the
// frame's sum is the torn-blob guard.
func (l *akiVlog) readAt(off uint64, n int, dst []byte) ([]byte, error) {
	return l.f.ReadValueFrameAt(off, uint32(n), dst)
}

// unlink records n bytes an overwrite, a delete, or an expiry reap no longer
// references, so a later compaction of the value region knows what it can
// reclaim. It must fire at every site that supersedes a logged value, the way
// the scratch log's dead counter did.
func (l *akiVlog) unlink(n uint64) { l.dead += n }

// logBytes reports the total flushed value bytes and the dead subset, the pair a
// compaction of the value region weighs to decide when a rewrite is worth it.
func (l *akiVlog) logBytes() (total, dead uint64) { return l.total, l.dead }
