package sqlo1b

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Groups divide an extent into 4 KiB read, compression, and
// addressing quanta (doc 03 section 4.3). Group 0 begins after the
// extent header, so it carries 4032 payload bytes.
const (
	GroupSize     = 4096
	Group0Payload = GroupSize - ExtentHeaderSize
)

// Position packing (doc 03 section 5.1): extent in bits 63..24,
// group in 23..12, slot in 11..0. Slot 4095 is the byte-addressed
// escape for blob records, whose group field holds the starting
// group of a contiguous run.
const (
	maxExtent = 1<<40 - 1
	maxGroup  = 1<<12 - 1
	BlobSlot  = 1<<12 - 1
)

// Pos is a packed record position.
type Pos uint64

// NewPos packs a record position. Slot 4095 is reserved for the blob
// escape; blob positions come from NewBlobPos.
func NewPos(extent uint64, group, slot uint16) (Pos, error) {
	if extent > maxExtent {
		return 0, fmt.Errorf("sqlo1b: extent %d exceeds 40 bits", extent)
	}
	if group > maxGroup {
		return 0, fmt.Errorf("sqlo1b: group %d exceeds 12 bits", group)
	}
	if slot >= BlobSlot {
		return 0, fmt.Errorf("sqlo1b: slot %d out of range, 4095 is the blob escape", slot)
	}
	return Pos(extent<<24 | uint64(group)<<12 | uint64(slot)), nil
}

// NewBlobPos packs a byte-addressed blob position: the record starts
// at startGroup and occupies a contiguous run of groups within the
// extent.
func NewBlobPos(extent uint64, startGroup uint16) (Pos, error) {
	if extent > maxExtent {
		return 0, fmt.Errorf("sqlo1b: extent %d exceeds 40 bits", extent)
	}
	if startGroup > maxGroup {
		return 0, fmt.Errorf("sqlo1b: group %d exceeds 12 bits", startGroup)
	}
	return Pos(extent<<24 | uint64(startGroup)<<12 | BlobSlot), nil
}

// Extent reports the position's extent number.
func (p Pos) Extent() uint64 { return uint64(p) >> 24 }

// Group reports the position's group within the extent; for blob
// positions this is the starting group of the run.
func (p Pos) Group() uint16 { return uint16(p>>12) & maxGroup }

// Slot reports the position's slot within the group's slot table.
func (p Pos) Slot() uint16 { return uint16(p) & maxGroup }

// IsBlob reports whether the position uses the byte-addressed blob
// escape.
func (p Pos) IsBlob() bool { return p.Slot() == BlobSlot }

func (p Pos) String() string {
	if p.IsBlob() {
		return fmt.Sprintf("ext %d blob at group %d", p.Extent(), p.Group())
	}
	return fmt.Sprintf("ext %d group %d slot %d", p.Extent(), p.Group(), p.Slot())
}

// MakeFullPtr binds a position to the xxhash64 of the bytes the
// pointer's referencer guards (a whole extent for superblock roots, a
// directory chunk for directory entries; doc 03 section 5.2).
func MakeFullPtr(pos Pos, data []byte) FullPtr {
	return FullPtr{Pos: uint64(pos), Sum: xxhash.Sum64(data)}
}

// Verify checks data against the pointer's checksum; every read
// through a full pointer verifies before use.
func (p FullPtr) Verify(data []byte) error {
	if got := xxhash.Sum64(data); got != p.Sum {
		return fmt.Errorf("sqlo1b: full pointer checksum %#x, data hashes to %#x", p.Sum, got)
	}
	return nil
}
