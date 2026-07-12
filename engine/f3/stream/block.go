package stream

import (
	"bytes"
	"encoding/binary"
)

// A block is the native band's packed entry chunk (spec 2064/f3/14 sections 3.2,
// 3.3): a contiguous run of consecutive entries in one byte blob, closed when it
// fills a byte budget or an entry cap, whichever binds first, and append-frozen
// thereafter. It is the unit the directory indexes (slice 3) and the cleanest
// chunked-cold unit in the engine, spilled whole to cold storage (M9).
//
// Representation follows the list band's struct-form choice (13-list-model.md
// section 2.2): the entry bytes live in a []byte blob and the 48-byte on-disk
// header (section 3.2) is mirrored into struct fields for hot access, serialized
// into its packed form only when a block spills to cold storage (M9). The blob
// holds only entry data; firstID, lastID, and the live/deleted counts are the
// header fields.
//
// Entry encoding is the master-delta form of section 3.3. The first entry is
// the master, stored whole with its field names. Every later entry with the same
// field-name set (the overwhelmingly common fixed-schema case) stores a flags
// byte, its ID delta against the block firstID, and its values only; the names
// are implied by the master in master order. An entry whose field set differs
// stores its names in full, exactly as Redis's listpack general form does. This
// is what collapses the ~100x field-name overhead (section 3.3) to one copy per
// block.
const (
	// blockBudget is the per-block blob byte budget, 4 KiB, matching Redis's
	// stream-node knob. blockCap is the entry cap, 128, whichever binds first.
	// Both frozen by labs/f3/m5/01_block_capacity: at 4096/128 the 64B
	// fixed-schema entry lands at 7.36 B/entry overhead inside the 6-8 memory
	// bar, the cap and the budget bind together at ~128 entries with almost no
	// slack, and a COUNT-100 cold window costs the minimum two preads.
	blockBudget = 4096
	blockCap    = 128
)

// Entry flags byte (section 3.3). Bit 0 marks a same-schema entry whose field
// names are implied by the master; bit 1 marks an XDEL tombstone (set in the
// XDEL slice, skipped by the walk here so the path is already correct).
const (
	entrySameSchema = 1 << 0
	entryDeleted    = 1 << 1
)

// field is one field-value pair of a stream entry. The bytes are caller-owned on
// append and are views into the block blob on a walk (valid until the next block
// mutation).
type field struct {
	name  []byte
	value []byte
}

// nameRef locates a master field name inside the block blob: names live once, in
// the master frame, and same-schema entries resolve to them by offset.
type nameRef struct {
	off int
	n   int
}

// block is one packed entry chunk. names is the master schema, set by the first
// entry and read by every same-schema decode.
type block struct {
	blob    []byte
	first   streamID
	last    streamID
	count   int // entries written, live or deleted
	deleted int // XDEL tombstones (set in a later slice)
	names   []nameRef
}

// newBlock returns an empty, open block. Its first appendEntry becomes the
// master.
func newBlock() *block { return &block{} }

func (b *block) firstID() streamID { return b.first }
func (b *block) lastID() streamID  { return b.last }

// entries is the total entry count, live plus tombstoned; live is the count the
// directory and XLEN report.
func (b *block) entries() int { return b.count }
func (b *block) live() int    { return b.count - b.deleted }

// size is the entry-data byte count, excluding the header the struct fields
// mirror.
func (b *block) size() int { return len(b.blob) }

// full reports whether the block has closed on either the entry cap or the byte
// budget, so a further appendEntry would be rejected.
func (b *block) full() bool { return b.count >= blockCap || len(b.blob) >= blockBudget }

// appendEntry packs one entry with ID id and the given fields. The first entry
// always lands (the master), setting the block firstID; a later entry lands only
// if it fits the entry cap and the byte budget, and appendEntry returns false
// when the block is full so the caller closes it and opens a fresh one. The
// caller guarantees ids arrive strictly increasing (the append invariant,
// enforced at the command layer in a later slice).
func (b *block) appendEntry(id streamID, fields []field) bool {
	if b.count == 0 {
		b.appendMaster(id, fields)
		return true
	}
	same := b.sameSchema(fields)
	if b.count >= blockCap || len(b.blob)+b.frameLen(id, fields, same) > blockBudget {
		return false
	}
	before := len(b.blob)
	b.blob = append(b.blob, 0) // flags
	if same {
		b.blob[before] = entrySameSchema
	}
	b.blob = putIDDelta(b.blob, b.first, id)
	if same {
		for i := range fields {
			b.blob = appendBytes(b.blob, fields[i].value)
		}
	} else {
		b.blob = binary.AppendUvarint(b.blob, uint64(len(fields)))
		for i := range fields {
			b.blob = appendBytes(b.blob, fields[i].name)
			b.blob = appendBytes(b.blob, fields[i].value)
		}
	}
	b.last = id
	b.count++
	return true
}

// appendMaster writes the first entry whole and records its field names as the
// block's master schema.
func (b *block) appendMaster(id streamID, fields []field) {
	b.first = id
	b.last = id
	b.blob = append(b.blob, 0) // flags: master is a general frame, same-schema bit clear
	b.blob = putIDDelta(b.blob, id, id)
	b.blob = binary.AppendUvarint(b.blob, uint64(len(fields)))
	b.names = b.names[:0]
	for i := range fields {
		b.blob = binary.AppendUvarint(b.blob, uint64(len(fields[i].name)))
		off := len(b.blob)
		b.blob = append(b.blob, fields[i].name...)
		b.names = append(b.names, nameRef{off: off, n: len(fields[i].name)})
		b.blob = appendBytes(b.blob, fields[i].value)
	}
	b.count = 1
}

// sameSchema reports whether fields carry exactly the block's master field
// names, in master order, so the entry can drop its names.
func (b *block) sameSchema(fields []field) bool {
	if len(fields) != len(b.names) {
		return false
	}
	for i := range fields {
		nr := b.names[i]
		if !bytes.Equal(fields[i].name, b.blob[nr.off:nr.off+nr.n]) {
			return false
		}
	}
	return true
}

// frameLen prices the frame appendEntry would write for a non-master entry,
// without encoding it, so the budget check runs before any bytes commit. It
// stays in lockstep with appendEntry; a test pins that the blob grows by exactly
// this many bytes.
func (b *block) frameLen(id streamID, fields []field, same bool) int {
	n := 1 + idDeltaLen(b.first, id) // flags + ID delta
	if same {
		for i := range fields {
			n += uvlen(uint64(len(fields[i].value))) + len(fields[i].value)
		}
		return n
	}
	n += uvlen(uint64(len(fields)))
	for i := range fields {
		n += uvlen(uint64(len(fields[i].name))) + len(fields[i].name)
		n += uvlen(uint64(len(fields[i].value))) + len(fields[i].value)
	}
	return n
}

// walk decodes the block's live entries in order and yields each as (id,
// fields), stopping early if fn returns false. scratch is a caller-owned []field
// reused across entries to keep the walk allocation-free; the field name and
// value slices it yields are views into the block blob, valid only until the
// next block mutation. Tombstoned entries are decoded (to advance past them) and
// skipped.
func (b *block) walk(scratch []field, fn func(id streamID, fields []field) bool) {
	pos := 0
	for i := 0; i < b.count; i++ {
		flags := b.blob[pos]
		pos++
		id, n := readIDDelta(b.blob[pos:], b.first)
		pos += n
		fields := scratch[:0]
		if flags&entrySameSchema != 0 {
			for _, nr := range b.names {
				vl, n := binary.Uvarint(b.blob[pos:])
				pos += n
				val := b.blob[pos : pos+int(vl)]
				pos += int(vl)
				fields = append(fields, field{name: b.blob[nr.off : nr.off+nr.n], value: val})
			}
		} else {
			nf, n := binary.Uvarint(b.blob[pos:])
			pos += n
			for j := 0; j < int(nf); j++ {
				nl, n := binary.Uvarint(b.blob[pos:])
				pos += n
				name := b.blob[pos : pos+int(nl)]
				pos += int(nl)
				vl, n := binary.Uvarint(b.blob[pos:])
				pos += n
				val := b.blob[pos : pos+int(vl)]
				pos += int(vl)
				fields = append(fields, field{name: name, value: val})
			}
		}
		if flags&entryDeleted != 0 {
			continue
		}
		if !fn(id, fields) {
			return
		}
	}
}

// covers reports whether id falls in the block's [firstID, lastID] span. The
// counted directory floor picks the one block that could hold id in O(log C);
// covers then confirms id is not past its last live entry before a tombstone
// scan.
func (b *block) covers(id streamID) bool {
	return b.count > 0 && id.cmp(b.first) >= 0 && id.cmp(b.last) <= 0
}

// tombstone flips the deleted flag on the entry with ID id, if it is present and
// still live, and reports whether it deleted one. The block stays append-frozen;
// only the one flags byte changes and the deleted counter bumps (section 6.5,
// the tombstone side of the tombstone-vs-rewrite choice). Entries are ordered,
// so the scan stops once it passes id.
func (b *block) tombstone(id streamID) bool {
	pos := 0
	for i := 0; i < b.count; i++ {
		flagsAt := pos
		flags := b.blob[pos]
		pos++
		eid, n := readIDDelta(b.blob[pos:], b.first)
		pos += n
		pos = b.skipBody(pos, flags)
		switch eid.cmp(id) {
		case 0:
			if flags&entryDeleted != 0 {
				return false
			}
			b.blob[flagsAt] = flags | entryDeleted
			b.deleted++
			return true
		case 1:
			return false // ordered past id
		}
	}
	return false
}

// tombstoneWhile flips the deleted flag on live entries from the front while pred
// holds, up to limit entries, and returns how many it tombstoned. It is the exact
// XTRIM boundary path (section 6.6): the block stays append-frozen, only flags
// bytes change and deleted bumps. pred defines a prefix of the ordered entries
// (oldest-k passes a constant true bounded by limit, MINID passes id < threshold),
// so the walk stops at the first live entry pred rejects.
func (b *block) tombstoneWhile(limit int, pred func(id streamID) bool) int {
	n := 0
	pos := 0
	for i := 0; i < b.count && n < limit; i++ {
		flagsAt := pos
		flags := b.blob[pos]
		pos++
		eid, m := readIDDelta(b.blob[pos:], b.first)
		pos += m
		pos = b.skipBody(pos, flags)
		if flags&entryDeleted != 0 {
			continue
		}
		if !pred(eid) {
			break
		}
		b.blob[flagsAt] = flags | entryDeleted
		b.deleted++
		n++
	}
	return n
}

// skipBody advances past an entry body (value frames, plus names on a general
// entry) starting just after the ID delta, and returns the offset of the next
// entry. It mirrors the body layout walk decodes, so the two must stay in step;
// a test tombstones then walks to pin that they agree.
func (b *block) skipBody(pos int, flags byte) int {
	if flags&entrySameSchema != 0 {
		for range b.names {
			vl, n := binary.Uvarint(b.blob[pos:])
			pos += n + int(vl)
		}
		return pos
	}
	nf, n := binary.Uvarint(b.blob[pos:])
	pos += n
	for j := 0; j < int(nf); j++ {
		nl, n := binary.Uvarint(b.blob[pos:])
		pos += n + int(nl)
		vl, n := binary.Uvarint(b.blob[pos:])
		pos += n + int(vl)
	}
	return pos
}

// projectedFrame prices the bytes appendEntry would add for (id, fields), whether
// this is the block's master or a later entry, so a band gate can decide before
// committing (the inline-to-native threshold check, section 4.3).
func (b *block) projectedFrame(id streamID, fields []field) int {
	if b.count == 0 {
		n := 1 + idDeltaLen(id, id) + uvlen(uint64(len(fields)))
		for i := range fields {
			n += uvlen(uint64(len(fields[i].name))) + len(fields[i].name)
			n += uvlen(uint64(len(fields[i].value))) + len(fields[i].value)
		}
		return n
	}
	return b.frameLen(id, fields, b.sameSchema(fields))
}

// appendBytes writes a uvarint length prefix and the bytes, the frame form both
// bands share (section 3.3).
func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}
