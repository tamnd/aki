package sqlo1b

import (
	"encoding/binary"
	"fmt"
)

// PadMarker is the skip marker that fills a closed group's tail: a
// u32 of all ones followed by zeros up to the slot table. Sequential
// readers treat it as end-of-group (doc 03 section 4.3).
const PadMarker uint32 = 0xFFFFFFFF

// GroupBuilder buffers one group in RAM until it closes, so the slot
// table costs no extra IO. Records are opaque bytes here; the record
// envelope (doc 03 section 6) is the vlog layer's concern.
type GroupBuilder struct {
	buf   []byte
	used  int
	slots []uint16
}

// NewGroupBuilder starts an empty group of the given payload
// capacity: Group0Payload for group 0, GroupSize otherwise.
func NewGroupBuilder(capacity int) *GroupBuilder {
	return &GroupBuilder{buf: make([]byte, capacity)}
}

// tableLen is the slot table size for n records.
func tableLen(n int) int { return 2*n + 2 }

// Fits reports whether a record of rlen bytes fits alongside the
// grown slot table.
func (g *GroupBuilder) Fits(rlen int) bool {
	return rlen > 0 && g.used+rlen+tableLen(len(g.slots)+1) <= len(g.buf)
}

// Append copies one record into the group and returns its slot. The
// caller checks Fits first and starts the next group on false; a
// record never spans a group boundary (blobs bypass the builder).
func (g *GroupBuilder) Append(rec []byte) (uint16, error) {
	if len(rec) == 0 {
		return 0, fmt.Errorf("sqlo1b: empty record")
	}
	if len(g.slots) >= BlobSlot {
		return 0, fmt.Errorf("sqlo1b: group is at %d records, slot 4095 is the blob escape", len(g.slots))
	}
	if !g.Fits(len(rec)) {
		return 0, fmt.Errorf("sqlo1b: record of %d bytes does not fit, group has %d of %d used", len(rec), g.used, len(g.buf))
	}
	slot := uint16(len(g.slots))
	g.slots = append(g.slots, uint16(g.used))
	copy(g.buf[g.used:], rec)
	g.used += len(rec)
	return slot, nil
}

// Records reports how many records the group holds.
func (g *GroupBuilder) Records() int { return len(g.slots) }

// Image pads the tail, writes the slot table, and returns the group
// image at exactly its capacity, without ending the group: the caller
// may keep appending and take a fuller image later. Rewriting a fuller
// image over an earlier one on disk is tear-safe, because a settled
// record's bytes rewrite identically at the same offsets; only records
// appended since the last image can be garbled by a torn write, and
// those are WAL-covered. A gap of one to three bytes is too small for
// the marker and stays zero; the slot table still bounds the records,
// so only sequential scans lean on the marker.
func (g *GroupBuilder) Image() []byte {
	clear(g.buf[g.used:])
	tstart := len(g.buf) - tableLen(len(g.slots))
	if tstart-g.used >= 4 {
		binary.LittleEndian.PutUint32(g.buf[g.used:], PadMarker)
	}
	for i, off := range g.slots {
		binary.LittleEndian.PutUint16(g.buf[tstart+2*i:], off)
	}
	binary.LittleEndian.PutUint16(g.buf[len(g.buf)-2:], uint16(len(g.slots)))
	return g.buf
}

// Close is Image at the moment the group ends; the vlog layer stops
// appending once it closes a group.
func (g *GroupBuilder) Close() []byte { return g.Image() }

// GroupView reads a closed group image through its slot table.
type GroupView struct {
	buf    []byte
	n      int
	tstart int
}

// ParseGroup validates a closed group image: the record count and
// every slot offset must stay inside the region below the table, and
// offsets must strictly increase because records append in order.
func ParseGroup(b []byte) (*GroupView, error) {
	if len(b) < tableLen(0) {
		return nil, fmt.Errorf("sqlo1b: group image of %d bytes has no room for a slot table", len(b))
	}
	n := int(binary.LittleEndian.Uint16(b[len(b)-2:]))
	tstart := len(b) - tableLen(n)
	if tstart < 0 {
		return nil, fmt.Errorf("sqlo1b: group claims %d records, table would overflow %d bytes", n, len(b))
	}
	v := &GroupView{buf: b, n: n, tstart: tstart}
	prev := -1
	for slot := range n {
		off := int(binary.LittleEndian.Uint16(b[tstart+2*slot:]))
		if off <= prev || off >= tstart {
			return nil, fmt.Errorf("sqlo1b: slot %d offset %d out of order or past the table at %d", slot, off, tstart)
		}
		prev = off
	}
	return v, nil
}

// Records reports how many records the group holds.
func (v *GroupView) Records() int { return v.n }

// Record returns the bytes from a slot's record start to the next
// record. The last record's slice runs to the slot table and may
// carry the pad marker tail; records are length-prefixed by the
// envelope, so the caller trims.
func (v *GroupView) Record(slot uint16) ([]byte, error) {
	if int(slot) >= v.n {
		return nil, fmt.Errorf("sqlo1b: slot %d of %d records", slot, v.n)
	}
	start := int(binary.LittleEndian.Uint16(v.buf[v.tstart+2*int(slot):]))
	end := v.tstart
	if int(slot)+1 < v.n {
		end = int(binary.LittleEndian.Uint16(v.buf[v.tstart+2*(int(slot)+1):]))
	}
	return v.buf[start:end], nil
}
