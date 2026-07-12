package hash

// The inline hash band (spec 2064/f3/10 section 4): a small hash is one packed
// blob in the key's record and carries no table, no vector, and no per-field
// allocation. A write whose result would breach either inline threshold converts
// the whole hash one way to the native field table (field.go), never backward
// (F4). This slice carries the inline and native bands and the one-way transition
// between them; the partitioned and cold bands are later work.
//
// The inline caps are Redis's own hash-max-listpack-entries (128) and
// hash-max-listpack-value (64). aki hardcodes the Redis defaults here the same
// way set.go hardcodes its listpack caps: there is no config plumb for them yet,
// and sharing the thresholds is what keeps OBJECT ENCODING honest by construction
// (spec 2064/f3/10 section 4.4). CONFIG SET on either knob would land with that
// plumb, not here.
const (
	// maxListpackEntries is hash-max-listpack-entries: a hash past 128 fields
	// leaves the inline band for the native table.
	maxListpackEntries = 128
	// maxListpackValue is hash-max-listpack-value: a field name or value past 64
	// bytes forces the native band on its own.
	maxListpackValue = 64
)

// encoding is the hash's storage shape, and what OBJECT ENCODING reports.
type encoding uint8

const (
	encListpack  encoding = iota // the inline blob band
	encHashtable                 // the native field table (field.go)
)

func (e encoding) String() string {
	if e == encListpack {
		return "listpack"
	}
	return "hashtable"
}

// hash is one key's hash. Exactly one of the two representations is live at a
// time, named by enc. It is owner-local: only the shard goroutine touches it, so
// nothing here locks.
type hash struct {
	enc encoding

	// inline band: one packed blob, laid out as [count:uint8][flags:uint8] then
	// count entries of [flen:uint8][field][vlen:uint8][value] (spec 2064/f3/10
	// section 4.1). Field names and values are both capped at 64 bytes inline, so
	// a single length byte holds either. The flags byte reserves bit 0 for the
	// ttl-present marker of the listpackex variant, which is always 0 this slice
	// (field TTL is a later slice); it is written so the blob layout is already
	// the one field TTL extends rather than a shape it has to migrate.
	blob []byte

	// native band: the field table, built by inlineToNative and never converted
	// back (F4).
	ft *ftable
}

// blob header offsets: count in byte 0, flags in byte 1, first entry at byte 2.
const (
	blobCountOff = 0
	blobFlagsOff = 1
	blobHeader   = 2
)

// newHash builds an empty inline hash. Every hash is born in the listpack band;
// the first write that breaches a threshold promotes it (spec 2064/f3/10 section
// 4.1, matching Redis's listpack-first hashes).
func newHash() *hash {
	return &hash{enc: encListpack, blob: []byte{0, 0}}
}

// card is the live field count, answering HLEN in O(1) on both bands: an inline
// hash reads its count byte, a native hash reads the table header.
func (h *hash) card() int {
	if h.enc == encListpack {
		return int(h.blob[blobCountOff])
	}
	return h.ft.card()
}

// get returns field's value bytes and whether it is present. The slice aliases
// internal storage and is valid only until the next mutation. Zero allocation on
// both bands: the inline scan compares in place and the table probe takes the
// argument bytes as the key without a copy.
func (h *hash) get(field []byte) ([]byte, bool) {
	if h.enc == encListpack {
		off := h.inlineIndex(field)
		if off < 0 {
			return nil, false
		}
		return h.inlineValueAt(off), true
	}
	return h.ft.get(field)
}

// has reports whether field is present.
func (h *hash) has(field []byte) bool {
	if h.enc == encListpack {
		return h.inlineIndex(field) >= 0
	}
	return h.ft.has(field)
}

// strlen returns the byte length of field's value, or 0 when the field is
// absent, answering HSTRLEN.
func (h *hash) strlen(field []byte) int {
	if h.enc == encListpack {
		off := h.inlineIndex(field)
		if off < 0 {
			return 0
		}
		return len(h.inlineValueAt(off))
	}
	return h.ft.strlen(field)
}

// set writes field to value and reports whether the field was newly created (the
// HSET return counts new fields). On the inline band it promotes to native first
// when the write would breach a threshold, then applies (spec 2064/f3/10 section
// 4.2), so the pair never lands in a blob it does not fit.
func (h *hash) set(field, value []byte) bool {
	if h.enc == encHashtable {
		return h.ft.set(field, value)
	}
	off := h.inlineIndex(field)
	exists := off >= 0
	if h.mustPromote(field, value, exists) {
		h.inlineToNative()
		return h.ft.set(field, value)
	}
	if exists {
		h.inlineOverwrite(off, value)
		return false
	}
	h.inlineAppend(field, value)
	return true
}

// setNX writes field to value only when it is absent, reporting whether it set
// (HSETNX). A new field that would breach a threshold promotes first; an existing
// field is left untouched and reports 0.
func (h *hash) setNX(field, value []byte) bool {
	if h.enc == encHashtable {
		return h.ft.setNX(field, value)
	}
	if h.inlineIndex(field) >= 0 {
		return false
	}
	if h.mustPromote(field, value, false) {
		h.inlineToNative()
		return h.ft.setNX(field, value)
	}
	h.inlineAppend(field, value)
	return true
}

// del removes field and reports whether it was present. Removal never changes the
// band: a hash only ever converts upward (F4), so a shrinking table stays a
// table, matching Redis.
func (h *hash) del(field []byte) bool {
	if h.enc == encHashtable {
		return h.ft.del(field)
	}
	off := h.inlineIndex(field)
	if off < 0 {
		return false
	}
	end := h.inlineEntryEnd(off)
	h.blob = append(h.blob[:off], h.blob[end:]...)
	h.blob[blobCountOff]--
	return true
}

// mustPromote reports whether applying (field, value) to the inline band would
// breach an inline threshold, which forces the one-way promotion to native: a
// field name or value over 64 bytes, or a genuinely new field taking the count
// past 128 (spec 2064/f3/10 sections 2.1 and 4.2).
func (h *hash) mustPromote(field, value []byte, exists bool) bool {
	if len(field) > maxListpackValue || len(value) > maxListpackValue {
		return true
	}
	return !exists && h.card()+1 > maxListpackEntries
}

// inlineIndex returns the byte offset of field's entry (pointing at its flen
// byte), or -1 when absent. The length is checked before the byte compare so most
// misses cost one byte load.
func (h *hash) inlineIndex(field []byte) int {
	b := h.blob
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		vlen := int(b[fstart+flen])
		if flen == len(field) && string(b[fstart:fstart+flen]) == string(field) {
			return i
		}
		i = fstart + flen + 1 + vlen
	}
	return -1
}

// inlineValueAt returns the value bytes of the entry at off, aliasing the blob.
func (h *hash) inlineValueAt(off int) []byte {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	return h.blob[voff+1 : voff+1+vlen]
}

// inlineEntryEnd returns the byte just past the entry at off, the splice point
// for a delete or an overwrite.
func (h *hash) inlineEntryEnd(off int) int {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	return voff + 1 + vlen
}

// inlineAppend writes one new entry at the blob tail and bumps the count. The
// append copies the field and value bytes, so the argument views are never
// retained.
func (h *hash) inlineAppend(field, value []byte) {
	h.blob = append(h.blob, byte(len(field)))
	h.blob = append(h.blob, field...)
	h.blob = append(h.blob, byte(len(value)))
	h.blob = append(h.blob, value...)
	h.blob[blobCountOff]++
}

// inlineOverwrite replaces the value of the entry at off, splicing the blob when
// the new value is a different length. The field bytes stay put; only the value
// run is rewritten. An equal-length overwrite, the common HSET-over-HSET case,
// copies over the value run in place and allocates nothing (F7).
func (h *hash) inlineOverwrite(off int, value []byte) {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	if vlen == len(value) {
		copy(h.blob[voff+1:voff+1+vlen], value)
		return
	}
	end := voff + 1 + vlen
	tail := append([]byte(nil), h.blob[end:]...)
	h.blob = h.blob[:voff]
	h.blob = append(h.blob, byte(len(value)))
	h.blob = append(h.blob, value...)
	h.blob = append(h.blob, tail...)
}

// inlineToNative promotes the inline hash to the native field table, the one-way
// transition of spec 2064/f3/10 section 4.2: allocate a table sized to the field
// count, replay every blob entry into it, then drop the blob. It runs inline in
// the write that breached the threshold.
func (h *hash) inlineToNative() {
	ft := newFtable(h.card() + 1)
	h.eachInline(func(field, value []byte) { ft.set(field, value) })
	h.ft = ft
	h.blob = nil
	h.enc = encHashtable
}

// eachInline visits every inline entry in blob order. The slices alias the blob
// and are valid only for the call. Used by the promotion replay and the tests;
// the native band's own walk is ftable.each.
func (h *hash) eachInline(fn func(field, value []byte)) {
	b := h.blob
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		field := b[fstart : fstart+flen]
		voff := fstart + flen
		vlen := int(b[voff])
		value := b[voff+1 : voff+1+vlen]
		fn(field, value)
		i = voff + 1 + vlen
	}
}

// each visits every field-value pair regardless of band, the shared walk the
// tests and future enumeration commands use. The slices alias internal storage
// and are valid only for the call.
func (h *hash) each(fn func(field, value []byte)) {
	if h.enc == encListpack {
		h.eachInline(fn)
		return
	}
	h.ft.each(fn)
}
