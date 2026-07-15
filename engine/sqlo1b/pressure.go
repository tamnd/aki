package sqlo1b

// The store half of the doc 04 section 13 backpressure ladder: the
// Pressure gauges and the two maintenance verbs behind the
// sqlo1.Maintainer surface. The ladder owns policy; this file only
// measures and executes.
//
// The WAL gauge is checkpoint lag over the policy's byte cadence, fed
// by the WAL's own since-trim counter, so it survives a reopen and a
// crash-looping process that never checkpoints still feels it.
//
// The free-extent gauge needs a budget to be meaningful, because
// allocStream grows the file on demand: with no cap the store never
// runs out of extents until the disk itself does, so the gauge reads
// zero. SetMaxBytes turns it on. The cap is a pressure budget, not a
// wall: writes shed at the door once headroom falls to the hard
// minimum, and in-flight drains may still grow the file inside that
// allowance, which is exactly why the minimum is sized to one full
// drain plus a checkpoint. Nothing below allocStream enforces the
// cap, so a shedding store can always finish the work it already
// accepted; the failure mode is refusing new writes, never wedging.

import (
	"context"

	"github.com/tamnd/aki/engine/sqlo1"
)

const (
	// shedDrainBytes is the headroom a shedding store still owes the
	// runtime: one full drain at the doc 04 section 7 dirty threshold
	// must land even after writes start bouncing.
	shedDrainBytes = 8 << 20
	// shedCheckpointExtents is the slack the checkpoint's index,
	// directory, and allocmap streams need on top of that drain.
	shedCheckpointExtents = 4
)

var _ sqlo1.Maintainer = (*Store)(nil)

// SetCheckpointPolicy sets the cadence the WAL pressure gauge is
// measured against. Only the byte half feeds the gauge; the interval
// half belongs to the runtime's timer, which owns the clock.
func (s *Store) SetCheckpointPolicy(p CheckpointPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ckptPolicy = p
}

// SetMaxBytes caps the data file for the free-extent gauge; 0 means
// unbounded and reads the gauge as zero. See the package comment on
// what the cap does and does not enforce.
func (s *Store) SetMaxBytes(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxBytes = n
}

// hardMinExtents is the shed floor in extents: one full drain plus
// checkpoint slack must still fit when writes start bouncing.
func (s *Store) hardMinExtents() uint64 {
	es := uint64(s.sb.ExtentSize)
	return (shedDrainBytes+es-1)/es + shedCheckpointExtents
}

// Pressure snapshots the WAL and free-extent gauges for the ladder.
func (s *Store) Pressure() sqlo1.Pressure {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p sqlo1.Pressure
	if b := s.ckptPolicy.Bytes; b > 0 {
		p.Wal = float64(s.wal.SinceTrim()) / float64(b)
	}
	if s.maxBytes <= 0 {
		return p
	}
	// Headroom is free extents in the file plus whole extents the cap
	// still allows the file to grow by. The gauge slides from 0 at the
	// reserve (twice the hard minimum) to 1 at the hard minimum, where
	// writes shed.
	headroom := s.grid.FreeCount()
	if maxExt := uint64(s.maxBytes) / uint64(s.sb.ExtentSize); maxExt > s.sb.ExtentCount {
		headroom += maxExt - s.sb.ExtentCount
	}
	hard := s.hardMinExtents()
	reserve := 2 * hard
	if headroom < reserve {
		p.Extent = float64(reserve-headroom) / float64(reserve-hard)
	}
	p.Shed = headroom <= hard
	return p
}

// CompactOnce is the ladder's compaction verb: one debt-controller
// step, reporting whether it found an extent worth paying down.
func (s *Store) CompactOnce(ctx context.Context) (bool, error) {
	_, compacted, err := s.CompactStep(ctx)
	return compacted, err
}
