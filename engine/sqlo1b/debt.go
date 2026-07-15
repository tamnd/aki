package sqlo1b

// The debt controller, doc 04 section 10's policy half. CompactExtent
// is the mechanism; this file decides when it runs and on what. Debt
// is the per-extent garbage map: a sealed vlog extent whose booked
// garbage crosses a quarter of its payload capacity has fallen under
// the 75 percent utilization target and is worth compacting. Holding
// that threshold is what bounds relocation cost: an extent compacted
// at 25 percent garbage moves at most 3 live bytes per garbage byte
// reclaimed, which is where the write amplification budget comes
// from.
//
// The map is advisory (doc 03 section 9): reopen starts it empty and
// supersessions rebuild it, so the controller can only under-select
// right after a reopen, never corrupt anything. Pacing is the
// caller's job; CompactStep does one extent per call so the owner
// loop can interleave it with foreground work.

import (
	"context"
	"fmt"
	"slices"
)

// DebtTarget is the steady live-data utilization the controller
// holds sealed vlog extents at.
const DebtTarget = 0.75

// DebtStats is the controller's telemetry snapshot. The byte
// counters are runtime-only, like the garbage map: reopen resets
// them, so they read as rates since open, which is what a current-WA
// gauge wants.
type DebtStats struct {
	// GarbageBytes is the superblock's advisory total.
	GarbageBytes uint64
	// Candidates counts sealed extents with any booked garbage;
	// OverThreshold counts those past the compaction threshold.
	Candidates    int
	OverThreshold int
	// LogicalBytes counts encoded record bytes the store accepted
	// (batch puts, replayed puts, gen records). DataBytes counts
	// physical vlog group and blob run writes, the open group's
	// tear-safe rewrites included. IndexBytes counts chunk,
	// directory, and allocmap group images.
	LogicalBytes uint64
	DataBytes    uint64
	IndexBytes   uint64
	// RelocatedBytes and Compactions are the compactor's share of
	// the traffic.
	RelocatedBytes uint64
	Compactions    uint64
}

// WA is the data-file write amplification the store measures on
// itself: physical bytes written per logical record byte accepted.
// The WAL is not in the numerator; it adds one sequential pass by
// design, and the end-to-end number the exit gate holds under 2 is
// the bench harness's to measure from outside.
func (d DebtStats) WA() float64 {
	if d.LogicalBytes == 0 {
		return 0
	}
	return float64(d.DataBytes+d.IndexBytes) / float64(d.LogicalBytes)
}

// DebtStats snapshots the controller telemetry.
func (s *Store) DebtStats() DebtStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := DebtStats{
		GarbageBytes:   s.garbage,
		LogicalBytes:   s.logicalBytes,
		DataBytes:      s.dataBytes,
		IndexBytes:     s.indexBytes,
		RelocatedBytes: s.relocatedBytes,
		Compactions:    s.compactions,
	}
	need := s.debtThreshold()
	for ext, g := range s.garbageExt {
		if s.grid.State(ext) != StateSealed {
			continue
		}
		d.Candidates++
		if g >= need {
			d.OverThreshold++
		}
	}
	return d
}

// debtThreshold is the booked garbage that makes a sealed extent a
// candidate: the payload capacity share past the utilization target.
func (s *Store) debtThreshold() uint64 {
	capacity := uint64(s.sb.ExtentSize) - ExtentHeaderSize
	return capacity - uint64(float64(capacity)*DebtTarget)
}

// CompactStep runs one controller decision: pick the sealed extent
// most worth compacting and compact it. It reports false with no
// error when no extent crosses the debt threshold, which is the
// steady state; callers loop on true when they want the debt paid
// down all the way.
func (s *Store) CompactStep(ctx context.Context) (CompactStats, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return CompactStats{}, false, s.broken
	}
	ext, ok, err := s.selectDebt()
	if err != nil || !ok {
		return CompactStats{}, false, err
	}
	cs, err := s.compactExtent(ctx, ext)
	if err != nil {
		return cs, false, err
	}
	return cs, true, nil
}

// selectDebt picks the compaction victim: highest booked garbage
// first, slotted extents before blob extents at equal garbage (small
// records reclaim cheaper), lowest extent number as the final tie.
func (s *Store) selectDebt() (uint64, bool, error) {
	need := s.debtThreshold()
	var best []uint64
	var bestG uint64
	for ext, g := range s.garbageExt {
		if g < need || s.grid.State(ext) != StateSealed {
			continue
		}
		switch {
		case g > bestG:
			bestG = g
			best = append(best[:0], ext)
		case g == bestG:
			best = append(best, ext)
		}
	}
	if len(best) == 0 {
		return 0, false, nil
	}
	slices.Sort(best)
	if len(best) == 1 {
		return best[0], true, nil
	}
	for _, ext := range best {
		blob, err := s.extentIsBlob(ext)
		if err != nil {
			return 0, false, err
		}
		if !blob {
			return ext, true, nil
		}
	}
	return best[0], true, nil
}

func (s *Store) extentIsBlob(ext uint64) (bool, error) {
	hb := make([]byte, ExtentHeaderSize)
	if _, err := s.f.ReadAt(hb, int64(ext)*int64(s.sb.ExtentSize)); err != nil {
		return false, fmt.Errorf("sqlo1b: debt candidate %d header: %w", ext, err)
	}
	hdr, err := DecodeExtentHeader(hb)
	if err != nil {
		return false, err
	}
	return hdr.EFlags&EFlagBlob != 0, nil
}
