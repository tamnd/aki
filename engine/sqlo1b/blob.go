package sqlo1b

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Blob records (doc 03 section 6.5): a record whose encoding exceeds
// BlobThreshold leaves the slotted group path and occupies a
// contiguous run of groups inside one extent, addressed by the
// slot-4095 escape carrying the starting group. Anything larger than
// one extent's payload is the type layer's problem (big strings
// chunk, segments are sized never to get close); the format layer
// refuses it.

// BlobThreshold is the largest encoded record the slotted path
// carries: one group minus envelope headroom, threshold lab-swept
// (doc 6.5). PlaceBlob rejects records at or under it, the slotted
// writer never accepts records over it, so every record has exactly
// one legal home.
const BlobThreshold = 3968

// blobOffset is a blob run's byte offset within its extent: group 0
// starts after the extent header, later groups on their boundary.
func blobOffset(startGroup uint16) int {
	if startGroup == 0 {
		return ExtentHeaderSize
	}
	return int(startGroup) * GroupSize
}

// BlobRunGroups reports how many groups a blob of rlen encoded bytes
// occupies from startGroup, counting the header-shortened group 0.
func BlobRunGroups(startGroup uint16, rlen int) int {
	off := blobOffset(startGroup)
	end := off + rlen
	return (end+GroupSize-1)/GroupSize - int(startGroup)
}

// PlaceBlob copies an encoded record into an extent image as a blob
// run starting at startGroup and returns the next free group, which
// equals the extent's group count when the run ends flush with the
// extent. The image is the whole extent, header space included, as
// the drain builds it.
func PlaceBlob(ext []byte, startGroup uint16, rec []byte) (uint16, error) {
	if len(ext) == 0 || len(ext)%GroupSize != 0 {
		return 0, fmt.Errorf("sqlo1b: extent image of %d bytes is not whole groups", len(ext))
	}
	groups := len(ext) / GroupSize
	if int(startGroup) >= groups {
		return 0, fmt.Errorf("sqlo1b: blob start group %d in a %d-group extent", startGroup, groups)
	}
	if len(rec) <= BlobThreshold {
		return 0, fmt.Errorf("sqlo1b: record of %d bytes belongs in a slotted group, blob threshold is %d", len(rec), BlobThreshold)
	}
	if rlen := binary.LittleEndian.Uint32(rec); uint64(rlen) != uint64(len(rec)) {
		return 0, fmt.Errorf("sqlo1b: blob rlen %d, handed %d bytes", rlen, len(rec))
	}
	off := blobOffset(startGroup)
	end := off + len(rec)
	if end > len(ext) {
		return 0, fmt.Errorf("sqlo1b: blob of %d bytes from group %d ends at %d, extent payload ends at %d", len(rec), startGroup, end, len(ext))
	}
	copy(ext[off:], rec)
	next := startGroup + uint16(BlobRunGroups(startGroup, len(rec)))
	if tail := int(next)*GroupSize - end; next < uint16(groups) && tail >= 4 {
		binary.LittleEndian.PutUint32(ext[end:], PadMarker)
	}
	return next, nil
}

// ReadBlob resolves a blob position against the data file: one group
// read for the head, one contiguous read for the remainder when the
// record runs past the first group. The envelope check is the
// verification; a blob never has a slot table.
func ReadBlob(r io.ReaderAt, extentSize uint32, pos Pos) (*Record, error) {
	if !pos.IsBlob() {
		return nil, fmt.Errorf("sqlo1b: %v is not a blob position", pos)
	}
	off := blobOffset(pos.Group())
	if off >= int(extentSize) {
		return nil, fmt.Errorf("sqlo1b: blob group %d past a %d-byte extent", pos.Group(), extentSize)
	}
	base := int64(pos.Extent())*int64(extentSize) + int64(off)
	head := make([]byte, min(GroupSize-off%GroupSize, int(extentSize)-off))
	if _, err := r.ReadAt(head, base); err != nil {
		return nil, fmt.Errorf("sqlo1b: blob head at %v: %w", pos, err)
	}
	rlen := binary.LittleEndian.Uint32(head)
	if rlen <= BlobThreshold {
		return nil, fmt.Errorf("sqlo1b: blob rlen %d at %v, under the %d threshold", rlen, pos, BlobThreshold)
	}
	if uint64(off)+uint64(rlen) > uint64(extentSize) {
		return nil, fmt.Errorf("sqlo1b: blob rlen %d at %v runs past the extent", rlen, pos)
	}
	buf := head
	if int(rlen) > len(head) {
		buf = make([]byte, rlen)
		copy(buf, head)
		if _, err := r.ReadAt(buf[len(head):], base+int64(len(head))); err != nil {
			return nil, fmt.Errorf("sqlo1b: blob tail at %v: %w", pos, err)
		}
	}
	return DecodeRecord(buf)
}
