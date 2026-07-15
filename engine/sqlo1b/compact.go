package sqlo1b

// Compaction, doc 04 section 10: lookup-based relocation. The
// compactor walks one sealed vlog extent group by group and asks the
// index about every record it finds: an entry pointing at this exact
// position means live, anything else means the record was superseded
// or deleted and its bytes are garbage. Live segment records get one
// more probe, root liveness, so segments stranded by a GENBUMP die
// here instead of costing a per-segment delete. Survivors re-append
// to the open vlog stream and their index entries re-point; the
// emptied extent quarantines and the next checkpoint releases it.
//
// Memory stays bounded by construction: one input group image is read
// at a time and survivors alias those images in the pending map until
// the finishing write-through, so the worst case holds one extent of
// input buffers plus the open output group. That is the doc 04
// section 10 bound (F2's lookup-based property), independent of file
// size.
//
// Crash safety needs no WAL frames. Relocation only rewrites RAM
// index entries and appends past the last checkpoint's vlog cursor,
// and the freed extent quarantines tagged with the next superblock
// seq, so its space is not reusable before the checkpoint that makes
// the re-pointed index durable. A crash before that checkpoint
// recovers the pre-compaction state: the old extent is intact, the
// replayed index points into it, and the relocated bytes are
// unreferenced garbage the next compaction sweeps.

import (
	"context"
	"encoding/binary"
	"fmt"
)

// CompactStats reports what one CompactExtent call did.
type CompactStats struct {
	// Relocated records were live: appended to the open vlog stream
	// and re-pointed in the index.
	Relocated      int
	RelocatedBytes int
	// Superseded records were dead by index probe: overwritten,
	// deleted, or re-drained after a crash.
	Superseded int
	// DeadSegments failed the root liveness probe: their rootgen is
	// behind a durable GENBUMP, and their index entries are removed.
	DeadSegments int
	// Expired records were past due at compaction time; their index
	// entries are removed, the doc 04 lazy-expiry backstop.
	Expired int
}

// CompactExtent compacts one sealed vlog extent and quarantines it.
// Extent selection and pacing are the debt controller's job; this is
// the mechanism only.
func (s *Store) CompactExtent(ctx context.Context, ext uint64) (CompactStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cs CompactStats
	if s.broken != nil {
		return cs, s.broken
	}
	if err := ctx.Err(); err != nil {
		return cs, err
	}
	if ext >= s.grid.ExtentCount() {
		return cs, fmt.Errorf("sqlo1b: compact extent %d past the %d-extent grid", ext, s.grid.ExtentCount())
	}
	if st := s.grid.State(ext); st != StateSealed || ext == 0 {
		return cs, fmt.Errorf("sqlo1b: compact extent %d in state %s, want sealed", ext, st)
	}
	hb := make([]byte, ExtentHeaderSize)
	if _, err := s.f.ReadAt(hb, int64(ext)*int64(s.sb.ExtentSize)); err != nil {
		return cs, fmt.Errorf("sqlo1b: compact extent %d header: %w", ext, err)
	}
	hdr, err := DecodeExtentHeader(hb)
	if err != nil {
		return cs, err
	}
	if hdr.Kind != KindVlog {
		return cs, fmt.Errorf("sqlo1b: compact extent %d holds kind %d, only vlog extents compact in v0", ext, hdr.Kind)
	}
	if hdr.EFlags&EFlagBlob != 0 {
		return cs, fmt.Errorf("sqlo1b: compact extent %d is a blob extent; blob relocation lands with the debt controller", ext)
	}

	// Mutations begin below. From here an error poisons the store,
	// the ApplyBatch discipline: the index may hold a mix of old and
	// new positions that only stays coherent while this owner is the
	// sole writer, and a reopen recovers the checkpointed state.
	var deadBytes uint64
	fg := fileGroups{s.f, s.sb.ExtentSize}
	fail := func(err error) (CompactStats, error) {
		s.broken = err
		return cs, err
	}
	for grp := uint16(0); grp < hdr.GroupCount; grp++ {
		img, err := fg.ReadGroup(ext, grp)
		if err != nil {
			return fail(err)
		}
		view, err := ParseGroup(img)
		if err != nil {
			return fail(err)
		}
		for slot := range uint16(view.Records()) {
			raw, err := view.Record(slot)
			if err != nil {
				return fail(err)
			}
			rec, err := DecodeRecord(raw)
			if err != nil {
				return fail(err)
			}
			pos, err := NewPos(ext, grp, slot)
			if err != nil {
				return fail(err)
			}
			_, at, err := s.lookupPos(rec.Key)
			if err != nil {
				return fail(err)
			}
			if at != pos {
				// Superseded, deleted, or never the index's copy; a
				// miss reads as position zero, which is never a vlog
				// slot in an allocatable extent.
				cs.Superseded++
				deadBytes += uint64(len(raw))
				continue
			}
			if s.expiredRec(rec) {
				if err := s.dropEntry(rec.Key); err != nil {
					return fail(err)
				}
				cs.Expired++
				continue
			}
			if rec.RType == RecSeg || rec.RType == RecFence {
				live, err := s.rootLive(binary.LittleEndian.Uint64(rec.Key), rec.Rootgen)
				if err != nil {
					return fail(err)
				}
				if !live {
					if err := s.dropEntry(rec.Key); err != nil {
						return fail(err)
					}
					cs.DeadSegments++
					continue
				}
			}
			if err := s.relocate(rec, raw); err != nil {
				return fail(err)
			}
			cs.Relocated++
			cs.RelocatedBytes += len(raw)
		}
	}
	if err := s.finishApply(); err != nil {
		return fail(err)
	}
	if err := s.grid.Free(ext, s.sb.Seq+1); err != nil {
		return fail(err)
	}
	delete(s.garbageExt, ext)
	s.garbage -= min(s.garbage, deadBytes)
	return cs, nil
}

// dropEntry removes a key's index entry during compaction. Unlike
// applyDel it books no garbage: the bytes it strands are in the
// extent being freed.
func (s *Store) dropEntry(key []byte) error {
	h := KeyHash(key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	ci, ei, _, _, found, err := s.findInChain(chain, Fingerprint(h), key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("sqlo1b: compaction entry for %x vanished mid-scan", key)
	}
	if err := chain[ci].RemoveEntry(ei); err != nil {
		return err
	}
	s.entries--
	return nil
}

// relocate appends a live record's bytes to the open vlog stream and
// re-points its index entry. The bytes move verbatim, no re-encode.
func (s *Store) relocate(rec *Record, raw []byte) error {
	pos, err := s.appendVlog(raw)
	if err != nil {
		return err
	}
	h := KeyHash(rec.Key)
	bucket := BucketOf(PlacementBits(h), s.level, s.split)
	chain, err := s.mutableChain(bucket)
	if err != nil {
		return err
	}
	ci, ei, _, _, found, err := s.findInChain(chain, Fingerprint(h), rec.Key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("sqlo1b: compaction entry for %x vanished mid-relocation", rec.Key)
	}
	meta, err := entryMetaFor(rec, h, chain[ci].WindowBase())
	if err != nil {
		return err
	}
	return chain[ci].SetEntry(ei, meta, uint64(pos))
}
