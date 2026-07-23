package sqlo1b

import (
	"encoding/binary"
	"fmt"
)

// Compressed-group frames, doc 03 section 7. A compressed extent sets
// EFlagCompressed in its header and every group in it carries a frame:
//
//	u8  scheme     encoding scheme id
//	u8  dict_id    dictionary slot, 0 for schemes that take none
//	u16 n          record count
//	u32 ulen       uncompressed payload bytes
//	u32 clen       compressed payload bytes
//	u8[clen]       payload
//	u16 uslot_off[n]  record offsets into the UNCOMPRESSED payload
//
// The slot table indexes the uncompressed bytes, so a point read
// decodes the payload once and slices records without touching the
// scheme again. Groups keep the 4 KiB stride; the disk win is packing
// more records per group, with ulen bounded by the u16 offsets.
//
// Unlike the raw slotted group, a frame is NOT tear-safe under
// in-place rewrite of a growing group: the slot table sits at
// 12+clen and moves as the payload grows, and the header mutates.
// Compaction output is the only writer and does not need tear
// safety, by the compact.go crash argument: relocations append past
// the last checkpoint's cursors and re-point only the RAM index, so
// a crash before the next checkpoint leaves the relocated bytes
// unreferenced garbage. The one hole that argument leaves, a group
// still open ACROSS a checkpoint whose positions the committed index
// now references, is closed operationally: Drain force-closes the
// open compact group before the data-file sync (see store.go).

// Encoding scheme ids (doc 03 section 7, doc 04 section 11).
// Registered: SchemeRaw and the scalar cascade (cascade.go). The
// zstd slice registers SchemeZstd; SchemeFSST plus SchemeZstdDict
// live in the boxed stretch per the cascade (#1295) and zdict
// (#1296) lab verdicts.
const (
	SchemeRaw      uint8 = 0 // identity, the tag-0 passthrough
	SchemeDict     uint8 = 1 // value dictionary
	SchemeDictRLE  uint8 = 2 // dictionary plus run-length
	SchemeFor      uint8 = 3 // frame-of-reference bit packing
	SchemeFSST     uint8 = 4 // string symbol table (boxed)
	SchemeZstd     uint8 = 5 // plain zstd
	SchemeZstdDict uint8 = 6 // zstd with trained dictionary (boxed)

	// NumSchemes sizes per-scheme telemetry arrays.
	NumSchemes = 7
)

// CFrameHeader is the fixed frame header size.
const CFrameHeader = 12

// cframeMaxUlen bounds the uncompressed payload: slot offsets are u16
// and must stay strictly below ulen.
const cframeMaxUlen = 1<<16 - 1

// cDecode is the scheme registry's decode side: payload bytes to
// uncompressed bytes of exactly ulen. Registered: identity and the
// scalar cascade schemes (cascade.go); an unknown scheme fails
// loudly so a newer file never half-reads on an older build.
func cDecode(scheme, dictID uint8, comp []byte, ulen int) ([]byte, error) {
	switch scheme {
	case SchemeRaw:
		if dictID != 0 {
			return nil, fmt.Errorf("sqlo1b: raw frame names dictionary %d", dictID)
		}
		if len(comp) != ulen {
			return nil, fmt.Errorf("sqlo1b: raw frame has clen %d, ulen %d", len(comp), ulen)
		}
		return comp, nil
	case SchemeDict, SchemeDictRLE, SchemeFor:
		if dictID != 0 {
			return nil, fmt.Errorf("sqlo1b: scheme %d frame names dictionary %d", scheme, dictID)
		}
		out, err := cascadeDecode(scheme, comp, ulen)
		if err != nil {
			return nil, err
		}
		if len(out) != ulen {
			return nil, fmt.Errorf("sqlo1b: scheme %d frame decoded to %d bytes, ulen %d", scheme, len(out), ulen)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("sqlo1b: group scheme %d not supported by this build", scheme)
	}
}

// CGroupBuilder buffers one compressed-frame group in RAM. Slice 1
// always emits SchemeRaw; the sampled-selection slice will re-encode
// the buffered payload at close time and fall back to raw below the
// win floor.
type CGroupBuilder struct {
	capacity int
	buf      []byte
	used     int
	slots    []uint16
	scheme   uint8 // stamped by Seal, raw until then
}

// NewCGroupBuilder starts an empty frame group of the given on-disk
// capacity: Group0Payload for group 0, GroupSize otherwise.
func NewCGroupBuilder(capacity int) *CGroupBuilder {
	return &CGroupBuilder{capacity: capacity, buf: make([]byte, capacity)}
}

// Fits reports whether a record of rlen bytes fits under the raw
// projection: header plus payload plus the grown slot table. Later
// schemes only shrink clen, so the raw projection is the conservative
// bound.
func (g *CGroupBuilder) Fits(rlen int) bool {
	return rlen > 0 &&
		CFrameHeader+g.used+rlen+2*(len(g.slots)+1) <= g.capacity &&
		g.used+rlen <= cframeMaxUlen
}

// Append copies one record into the frame payload and returns its
// slot. The caller checks Fits first; a record never spans groups.
func (g *CGroupBuilder) Append(rec []byte) (uint16, error) {
	if len(rec) == 0 {
		return 0, fmt.Errorf("sqlo1b: empty record")
	}
	if len(g.slots) >= BlobSlot {
		return 0, fmt.Errorf("sqlo1b: frame group is at %d records, slot 4095 is the blob escape", len(g.slots))
	}
	if !g.Fits(len(rec)) {
		return 0, fmt.Errorf("sqlo1b: record of %d bytes does not fit, frame has %d of %d used", len(rec), g.used, g.capacity)
	}
	slot := uint16(len(g.slots))
	g.slots = append(g.slots, uint16(g.used))
	copy(g.buf[CFrameHeader+g.used:], rec)
	g.used += len(rec)
	return slot, nil
}

// Records reports how many records the frame holds.
func (g *CGroupBuilder) Records() int { return len(g.slots) }

// Scheme reports the frame's scheme: raw while the group is open,
// the sampled cascade's pick once Seal runs.
func (g *CGroupBuilder) Scheme() uint8 { return g.scheme }

// Image assembles the raw frame at exactly the group capacity:
// header, payload, slot table, zero tail. Callers may keep appending
// and take a fuller image later; in-place rewrites of earlier images
// are NOT tear-safe (see the package comment above), which the
// compact stream tolerates and no other writer may. Open-group
// flush-through always writes this raw image, because only the final
// Seal may spend encode work and change the layout.
func (g *CGroupBuilder) Image() []byte {
	g.buf[0] = SchemeRaw
	g.buf[1] = 0
	binary.LittleEndian.PutUint16(g.buf[2:], uint16(len(g.slots)))
	binary.LittleEndian.PutUint32(g.buf[4:], uint32(g.used))
	binary.LittleEndian.PutUint32(g.buf[8:], uint32(g.used))
	tstart := CFrameHeader + g.used
	for i, off := range g.slots {
		binary.LittleEndian.PutUint16(g.buf[tstart+2*i:], off)
	}
	clear(g.buf[tstart+2*len(g.slots):])
	return g.buf
}

// Seal ends the group through the sampled cascade (cascade.go): the
// winning scheme's bytes replace the payload when they clear the 8
// percent floor, otherwise the image stays raw. The group must not
// grow afterwards; the compressed image fits by construction because
// the raw projection fit and clen only shrinks.
func (g *CGroupBuilder) Seal() []byte {
	scheme, comp := cSelect(g.buf[CFrameHeader : CFrameHeader+g.used])
	if scheme == SchemeRaw {
		return g.Image()
	}
	g.scheme = scheme
	g.buf[0] = scheme
	g.buf[1] = 0
	binary.LittleEndian.PutUint16(g.buf[2:], uint16(len(g.slots)))
	binary.LittleEndian.PutUint32(g.buf[4:], uint32(g.used))
	binary.LittleEndian.PutUint32(g.buf[8:], uint32(len(comp)))
	copy(g.buf[CFrameHeader:], comp)
	tstart := CFrameHeader + len(comp)
	for i, off := range g.slots {
		binary.LittleEndian.PutUint16(g.buf[tstart+2*i:], off)
	}
	clear(g.buf[tstart+2*len(g.slots):])
	return g.buf
}

// CGroupView reads one frame group image with its payload decoded.
type CGroupView struct {
	payload []byte // uncompressed
	table   []byte // raw uslot_off region
	n       int
	scheme  uint8
}

// ParseCGroup validates a frame image and decodes its payload: the
// header must bound the payload and table inside the image, the
// scheme must be registered, and slot offsets must strictly increase
// below ulen.
func ParseCGroup(b []byte) (*CGroupView, error) {
	if len(b) < CFrameHeader {
		return nil, fmt.Errorf("sqlo1b: frame image of %d bytes has no header", len(b))
	}
	scheme, dictID := b[0], b[1]
	n := int(binary.LittleEndian.Uint16(b[2:]))
	ulen := int(binary.LittleEndian.Uint32(b[4:]))
	clen := int(binary.LittleEndian.Uint32(b[8:]))
	if ulen > cframeMaxUlen {
		return nil, fmt.Errorf("sqlo1b: frame ulen %d past the u16 offset bound", ulen)
	}
	if CFrameHeader+clen+2*n > len(b) {
		return nil, fmt.Errorf("sqlo1b: frame claims clen %d and %d records, past %d bytes", clen, n, len(b))
	}
	if n > 0 && ulen == 0 {
		return nil, fmt.Errorf("sqlo1b: frame claims %d records in an empty payload", n)
	}
	payload, err := cDecode(scheme, dictID, b[CFrameHeader:CFrameHeader+clen], ulen)
	if err != nil {
		return nil, err
	}
	table := b[CFrameHeader+clen : CFrameHeader+clen+2*n]
	prev := -1
	for slot := range n {
		off := int(binary.LittleEndian.Uint16(table[2*slot:]))
		if off <= prev || off >= ulen {
			return nil, fmt.Errorf("sqlo1b: frame slot %d offset %d out of order or past ulen %d", slot, off, ulen)
		}
		prev = off
	}
	return &CGroupView{payload: payload, table: table, n: n, scheme: scheme}, nil
}

// Records reports how many records the frame holds.
func (v *CGroupView) Records() int { return v.n }

// Scheme reports the frame's encoding scheme.
func (v *CGroupView) Scheme() uint8 { return v.scheme }

// Record returns one record's exact bytes: offsets bound every record
// and the last one ends at ulen, so there is no pad tail to trim,
// unlike the raw GroupView.
func (v *CGroupView) Record(slot uint16) ([]byte, error) {
	if int(slot) >= v.n {
		return nil, fmt.Errorf("sqlo1b: frame slot %d of %d records", slot, v.n)
	}
	start := int(binary.LittleEndian.Uint16(v.table[2*int(slot):]))
	end := len(v.payload)
	if int(slot)+1 < v.n {
		end = int(binary.LittleEndian.Uint16(v.table[2*(int(slot)+1):]))
	}
	return v.payload[start:end], nil
}
