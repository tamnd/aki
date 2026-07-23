package sqlo1b

import (
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

// Scrub, doc 03 section 15: a background sweep over sealed extents
// verifying what can be verified without a live store. A finding is
// provable damage with the extent as its blast radius; the scrubber
// reports and never mutates, because quarantining a damaged extent
// is the owner's call and reads must keep failing loudly per key in
// the meantime. This is the skeleton: header_crc, the referencer
// checksum where one is registered, and slot-table structure on vlog
// groups. Record rcrc and garbage-accounting rebuild belong to the
// vlog record slices.

// ScrubFinding is one provably damaged extent.
type ScrubFinding struct {
	Extent uint64
	Err    error
}

// ScrubReport is one full-file sweep's outcome. Findings is the
// quarantine report: the extents the owner should fence off.
type ScrubReport struct {
	Scanned  uint64 // sealed extents verified
	Skipped  uint64 // free, active, or quarantined extents
	Findings []ScrubFinding
}

// Clean reports a sweep with no findings.
func (r *ScrubReport) Clean() bool { return len(r.Findings) == 0 }

// Scrubber sweeps one shard's file against its grid.
type Scrubber struct {
	File       io.ReaderAt
	ExtentSize uint32
	Grid       *Grid
	// Sums is the referencer checksum registry, extent to xxhash64
	// as handed over at seal (the recovery fold's seal registry has
	// this shape); a registered extent gets whole-extent
	// verification on top of its header_crc.
	Sums map[uint64]uint64
	// Throttle, when set, receives the bytes each extent cost before
	// the sweep moves on; the runtime wires its rate cap here. Nil
	// sweeps at full speed.
	Throttle func(bytes int)
}

// Sweep walks every extent once: sealed extents verify, everything
// else is skipped (free space is noise, active tails are mutating
// under the owner, quarantined contents are already dead).
func (s *Scrubber) Sweep() ScrubReport {
	var rep ScrubReport
	buf := make([]byte, s.ExtentSize)
	for ext := uint64(1); ext < s.Grid.ExtentCount(); ext++ {
		if s.Grid.State(ext) != StateSealed {
			rep.Skipped++
			continue
		}
		rep.Scanned++
		if err := s.scrubExtent(ext, buf); err != nil {
			rep.Findings = append(rep.Findings, ScrubFinding{Extent: ext, Err: err})
		}
		if s.Throttle != nil {
			s.Throttle(len(buf))
		}
	}
	return rep
}

// scrubExtent verifies one sealed extent into buf: header_crc and
// the seal flag, the registered referencer checksum, and every
// closed vlog group's slot table. Records stay opaque here. Blob
// extents carry no slot tables and stop at the checksum, and
// non-vlog kinds own their payload layouts in later slices.
func (s *Scrubber) scrubExtent(ext uint64, buf []byte) error {
	if _, err := s.File.ReadAt(buf, int64(ext)*int64(s.ExtentSize)); err != nil {
		return fmt.Errorf("sqlo1b: scrub read extent %d: %w", ext, err)
	}
	h, err := DecodeExtentHeader(buf)
	if err != nil {
		return err
	}
	if !h.Sealed() {
		return fmt.Errorf("sqlo1b: extent %d sealed in the grid but not on disk", ext)
	}
	if want, ok := s.Sums[ext]; ok {
		if got := xxhash.Sum64(buf); got != want {
			return fmt.Errorf("sqlo1b: extent %d checksum %#x, referencer holds %#x", ext, got, want)
		}
	}
	if h.Kind != KindVlog || h.EFlags&EFlagBlob != 0 {
		return nil
	}
	for g := range uint64(h.GroupCount) {
		lo := g * GroupSize
		if g == 0 {
			lo = ExtentHeaderSize
		}
		hi := (g + 1) * GroupSize
		if hi > uint64(len(buf)) {
			return fmt.Errorf("sqlo1b: extent %d group_count %d past the extent", ext, h.GroupCount)
		}
		if h.EFlags&EFlagCompressed != 0 {
			// Compressed extents carry frame groups; ParseCGroup
			// validates the header bounds, the scheme, and the slot
			// offsets, so a scheme this build cannot decode is a
			// finding, not a silent skip.
			if _, err := ParseCGroup(buf[lo:hi]); err != nil {
				return fmt.Errorf("sqlo1b: extent %d group %d: %w", ext, g, err)
			}
			continue
		}
		if _, err := ParseGroup(buf[lo:hi]); err != nil {
			return fmt.Errorf("sqlo1b: extent %d group %d: %w", ext, g, err)
		}
	}
	return nil
}
