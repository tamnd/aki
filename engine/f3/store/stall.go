package store

import (
	"errors"
	"syscall"
)

// Block-not-drop stall taxonomy (spec 2064/f3/06 section 8.3). When a parked
// write crosses the coarse stall window (engine/f3/shard backpressure.go) the
// cold migrator has stopped freeing arena space, and StallReason names why so the
// out-of-memory reply and the operator carry the cause rather than a bare "arena
// full". It is read once at stall-out, never on the hot path.
//
// Doc 06 section 8.3 enumerates four cases in which the cold tail stops
// advancing: the cold device is full (ENOSPC), a cold write failed with another
// I/O error, no migratable residue is left (every full segment holds only pinned
// rows), and a leaked epoch (a cross-shard reader that never quiesces holds a
// bracket pinning the retired segments). The last two are not reachable under the
// current keyspace and execution model: every resident record is demotable
// (cold.go demotable classifies only kindString, so there is always residue until
// the live charge falls), and a single-shard operation holds no epoch past its
// own batch (engine/f3/shard epoch.go), so no reader can leak one. They fold into
// the idle catch-all below and split out with the per-type cold engagement
// (kindSetMeta and the L18 exhaustiveness test) and the cross-shard F17
// arena-read path that first introduce them. The cases reachable today are the
// two cold-device failures, an open stream pinning migration, the tier being
// absent, and the arena sized under the resident cap, and StallReason names each.
func (s *Store) StallReason() string {
	if s.cold == nil || !s.ltmOn {
		return "no larger-than-memory tier"
	}
	if werr := s.cold.werr; werr != nil {
		if errors.Is(werr, syscall.ENOSPC) {
			return "cold device full"
		}
		return "cold write failed: " + werr.Error()
	}
	if s.openStreams > 0 {
		return "migration pinned by an open stream"
	}
	return "arena exhausted, migrator idle"
}
