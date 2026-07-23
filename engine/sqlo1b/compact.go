package sqlo1b

// Compaction, doc 04 section 10: lookup-based relocation. The
// compactor walks one sealed vlog extent group by group and asks the
// index about every record it finds: an entry pointing at this exact
// position means live, anything else means the record was superseded
// or deleted and its bytes are garbage. Live segment records get one
// more probe, root liveness, so segments stranded by a GENBUMP die
// here instead of costing a per-segment delete. Survivors re-append
// to the compaction output stream (gen-C extents, compressed frame
// groups) and their index entries re-point; the emptied extent
// quarantines and the next checkpoint releases it.
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
// Extent selection and pacing are the debt controller's job (see
// CompactStep); this is the mechanism only.
func (s *Store) CompactExtent(ctx context.Context, ext uint64) (CompactStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compactExtent(ctx, ext)
}

func (s *Store) compactExtent(ctx context.Context, ext uint64) (CompactStats, error) {
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

	// Mutations begin below. From here an error poisons the store,
	// the ApplyBatch discipline: the index may hold a mix of old and
	// new positions that only stays coherent while this owner is the
	// sole writer, and a reopen recovers the checkpointed state.
	var deadBytes uint64
	fail := func(err error) (CompactStats, error) {
		s.broken = err
		return cs, err
	}
	if hdr.EFlags&EFlagBlob != 0 {
		// Blob runs pack contiguously from group 0, so every occupied
		// group belongs to some run and the walk steps run by run.
		for grp := uint16(0); grp < hdr.GroupCount; {
			rec, raw, err := s.readBlobRun(ext, grp)
			if err != nil {
				return fail(err)
			}
			pos, err := NewBlobPos(ext, grp)
			if err != nil {
				return fail(err)
			}
			if err := s.compactRecord(rec, raw, pos, &cs, &deadBytes); err != nil {
				return fail(err)
			}
			grp += uint16(BlobRunGroups(grp, len(raw)))
		}
	} else if hdr.EFlags&EFlagCompressed != 0 {
		// A gen-C extent from an earlier compaction: frame groups.
		// Frame slot offsets bound every record exactly (the last one
		// ends at ulen), so no trim is needed before relocation.
		fg := fileGroups{s.f, s.sb.ExtentSize}
		for grp := uint16(0); grp < hdr.GroupCount; grp++ {
			img, err := fg.ReadGroup(ext, grp)
			if err != nil {
				return fail(err)
			}
			view, err := ParseCGroup(img)
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
				if err := s.compactRecord(rec, raw, pos, &cs, &deadBytes); err != nil {
					return fail(err)
				}
			}
		}
	} else {
		fg := fileGroups{s.f, s.sb.ExtentSize}
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
				// The slot slice runs to the next record or the slot
				// table, pad tail included on the last one, and a lone
				// big record's slice is nearly the whole group. Trim to
				// the encoded record before relocation or the padded
				// slice crosses BlobThreshold and misroutes to the blob
				// path. DecodeRecord already validated rlen.
				raw = raw[:binary.LittleEndian.Uint32(raw)]
				pos, err := NewPos(ext, grp, slot)
				if err != nil {
					return fail(err)
				}
				if err := s.compactRecord(rec, raw, pos, &cs, &deadBytes); err != nil {
					return fail(err)
				}
			}
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
	s.relocatedBytes += uint64(cs.RelocatedBytes)
	s.compactions++
	return cs, nil
}

// compactRecord runs one record through the liveness probes and
// either skips, drops, or relocates it.
func (s *Store) compactRecord(rec *Record, raw []byte, pos Pos, cs *CompactStats, deadBytes *uint64) error {
	_, at, err := s.lookupPos(rec.Key)
	if err != nil {
		return err
	}
	if at != pos {
		// Superseded, deleted, or never the index's copy; a miss
		// reads as position zero, which is never a vlog slot in an
		// allocatable extent.
		cs.Superseded++
		*deadBytes += uint64(len(raw))
		return nil
	}
	if s.expiredRec(rec) {
		if err := s.dropEntry(rec.Key); err != nil {
			return err
		}
		cs.Expired++
		return nil
	}
	if rec.RType == RecSeg || rec.RType == RecFence {
		live, err := s.rootLive(binary.LittleEndian.Uint64(rec.Key), rec.Rootgen)
		if err != nil {
			return err
		}
		if !live {
			if err := s.dropEntry(rec.Key); err != nil {
				return err
			}
			cs.DeadSegments++
			return nil
		}
	}
	if err := s.relocate(rec, raw); err != nil {
		return err
	}
	cs.Relocated++
	cs.RelocatedBytes += len(raw)
	return nil
}

// readBlobRun reads one whole blob run off the file: the rlen word
// sizes the run, then the record decodes from the full read. The raw
// bytes come back too because relocation moves them verbatim.
func (s *Store) readBlobRun(ext uint64, grp uint16) (*Record, []byte, error) {
	off := blobOffset(grp)
	base := int64(ext)*int64(s.sb.ExtentSize) + int64(off)
	var head [4]byte
	if _, err := s.f.ReadAt(head[:], base); err != nil {
		return nil, nil, fmt.Errorf("sqlo1b: blob run head at %d/%d: %w", ext, grp, err)
	}
	rlen := binary.LittleEndian.Uint32(head[:])
	if rlen <= BlobThreshold || uint64(off)+uint64(rlen) > uint64(s.sb.ExtentSize) {
		return nil, nil, fmt.Errorf("sqlo1b: blob run at %d/%d has rlen %d", ext, grp, rlen)
	}
	raw := make([]byte, rlen)
	if _, err := s.f.ReadAt(raw, base); err != nil {
		return nil, nil, fmt.Errorf("sqlo1b: blob run at %d/%d: %w", ext, grp, err)
	}
	rec, err := DecodeRecord(raw)
	if err != nil {
		return nil, nil, err
	}
	return rec, raw, nil
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

// relocate appends a live record's bytes to the compaction output
// stream and re-points its index entry. The bytes move verbatim, no
// re-encode; the output extents carry EFlagCompressed and hold frame
// groups (raw scheme in slice 1, the sampled selector later).
func (s *Store) relocate(rec *Record, raw []byte) error {
	pos, err := s.appendCompact(raw)
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
