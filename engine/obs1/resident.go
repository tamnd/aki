// Takeover rebuild of the resident structures (doc 02 section 4.3, doc
// 05 sections 2.1 and 2.3): a node that takes a group over rebuilds the
// group's directory and keymap from its winning manifest before serving.
//
// One spec deviation, recorded: doc 06 section 3 says regime A rebuilds
// the keymap "from chunk indexes", but the index carries only each run's
// first fingerprint, never the per-record fingerprints the keymap
// stores, so an index-only rebuild cannot exist. The rebuild reads
// segment data instead: one whole-object GET per segment covers the
// directory (which alone would need only the footer) and the keymap
// (which needs the record keys), the same request count as the footer
// plan at higher bytes, and the boot lab budgets it.
package obs1

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/tamnd/aki/engine/obs1/store"
)

// ResidentStats counts one group's rebuild.
type ResidentStats struct {
	Segments   int // segments read and booked
	Records    int // keymap claims fed, tombstones included
	Tombstones int // the tombstone claims among them
	Swept      int // shadow claims FinishRebuild removed
}

// RebuildResident reads every live segment the manifest names and feeds
// dir and km, either of which may be nil to skip it. Claims go through
// Shadow, so feed order does not matter and a higher SegSeq always wins;
// FinishRebuild runs at the end when a keymap is given. The caller runs
// this before the group serves and before its folder starts, so neither
// structure sees concurrent writers.
func RebuildResident(ctx context.Context, s Store, prefix string, m Manifest, dir *Directory, km *Keymap) (ResidentStats, error) {
	var st ResidentStats
	for _, ms := range m.Segs {
		key := segKey(prefix, m.Group, ms.SegSeq)
		obj, _, err := s.Get(ctx, key)
		if err != nil {
			return st, fmt.Errorf("obs1: rebuild group %d segment %d: %w", m.Group, ms.SegSeq, err)
		}
		seg, _, err := ParseSegment(obj)
		if err != nil {
			return st, fmt.Errorf("obs1: rebuild group %d segment %d: %w", m.Group, ms.SegSeq, err)
		}
		f := &seg.Footer
		if f.Group != m.Group || f.SegSeq != ms.SegSeq {
			return st, fmt.Errorf("obs1: segment %s says group %d seq %d, manifest says %d and %d",
				key, f.Group, f.SegSeq, m.Group, ms.SegSeq)
		}
		if dir != nil {
			if err := dir.Add(key, f); err != nil {
				return st, err
			}
		}
		if km != nil {
			if ms.SegSeq > (1<<32)-1 {
				return st, fmt.Errorf("obs1: SegSeq %d does not fit the locator's 32 bits", ms.SegSeq)
			}
			if err := feedKeymap(km, seg, uint32(ms.SegSeq), &st); err != nil {
				return st, fmt.Errorf("obs1: rebuild group %d segment %d: %w", m.Group, ms.SegSeq, err)
			}
		}
		st.Segments++
	}
	if km != nil {
		st.Swept = km.FinishRebuild()
	}
	return st, nil
}

// feedKeymap shadows every record in the segment's run chunks into km.
// Chunks without ChunkFlagRun are a demoter's collection chunks, which
// the keymap does not index; the placement feed at publish skips them
// the same way.
func feedKeymap(km *Keymap, seg *Segment, segSeq uint32, st *ResidentStats) error {
	for ci, e := range seg.Footer.Chunks {
		data := seg.BlockData[e.Block][e.OffInBlock:]
		total := binary.LittleEndian.Uint32(data[0:4])
		var outer store.FoldFrame
		if err := store.WalkStagedFrames(data[:total], func(f store.FoldFrame) error {
			outer = f
			return nil
		}); err != nil {
			return fmt.Errorf("chunk %d: %w", ci, err)
		}
		if !outer.Chunk || outer.Flags&store.ChunkFlagRun == 0 {
			continue
		}
		loc := KeyLoc{Seg: segSeq, Chunk: uint32(ci)}
		n := 0
		err := store.WalkStagedFrames(outer.Payload, func(r store.FoldFrame) error {
			if r.Chunk {
				return fmt.Errorf("nested chunk frame inside a run")
			}
			if err := km.Shadow(Fingerprint(r.Key), loc, r.Tombstone); err != nil {
				return err
			}
			n++
			st.Records++
			if r.Tombstone {
				st.Tombstones++
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("chunk %d: %w", ci, err)
		}
		if n != int(e.Count) {
			return fmt.Errorf("chunk %d holds %d records, its index entry says %d", ci, n, e.Count)
		}
	}
	return nil
}
