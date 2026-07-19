package hash

import "encoding/binary"

// The inline hash band (spec 2064/f3/10 section 4): a small hash is one packed
// blob in the key's record and carries no table, no vector, and no per-field
// allocation. A write whose result would breach either inline threshold converts
// the whole hash one way to the native field table (field.go), never backward
// (F4). This slice carries the inline and native bands and the one-way transition
// between them; the partitioned and cold bands are later work.
//
// The inline caps are hash-max-listpack-entries and hash-max-listpack-value. The
// binding rule of doc 4.4 is to share the rival's thresholds so OBJECT ENCODING
// is honest by construction, and the tested rival is the redis 8.8.0 build, whose
// defaults are 512 entries and 64 value bytes. The doc's parenthetical 128 is the
// classic default, but 512 is what the live differential measures, so that is what
// aki matches. aki hardcodes these the same way set.go hardcodes its listpack
// caps: there is no config plumb yet, and CONFIG SET will read the real configured
// value when the plumb lands, not here.
const (
	// maxListpackEntries is hash-max-listpack-entries: a hash past 512 fields
	// leaves the inline band for the native table (redis 8.8.0 build default).
	maxListpackEntries = 512
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

	// clock is the per-key access clock OBJECT IDLETIME reads back: the batch
	// second-resolution time (store.LRUClock) stamped on every read and write the
	// way Redis stamps robj.lru, folded to sixteen bits. It rides the alignment
	// padding after enc, so a hash carries a real idle clock at zero added bytes,
	// the same free-header trick the string cell uses (store record offKindBits).
	// It wraps every ~18.2h, the fidelity price of spending no bytes.
	clock uint16

	// inline band: one packed blob, laid out as [count:uint16-le][flags:uint8]
	// then count entries of [flen:uint8][field][vlen:uint8][value] (spec
	// 2064/f3/10 section 4.1). Field names and values are both capped at 64 bytes
	// inline, so a single length byte holds either. The count is two bytes because
	// the inline band now holds up to 512 fields, past what a single byte counts.
	// The flags byte reserves bit 0 for the ttl-present marker of the listpackex
	// variant, which is always 0 this slice (field TTL is a later slice); it is
	// written so the blob layout is already the one field TTL extends rather than
	// a shape it has to migrate.
	blob []byte

	// native band: the field table, built by inlineToNative and never converted
	// back (F4).
	ft *ftable

	// nextExp is the field-TTL next-expire hint (spec 2064/f3/10 section 6.1): the
	// smallest absolute unix-ms expiry among the live fields, or 0 when no field
	// carries a TTL. It is a lower bound, exact right after a reap and never larger
	// than the true minimum, so a command reaps (deletes fired fields on the spot)
	// only once now reaches it. A hash that never sets a field TTL keeps it 0 and
	// pays one comparison per command for the whole machinery. Clearing a TTL
	// (HPERSIST, an HSET overwrite) may leave it pointing at a field that no longer
	// expires, which only costs one early scan that recomputes it; it is never left
	// too high. The active sweep that reaps untouched keys on a timer is deferred to
	// M9, so a fired field survives until the next command lands on its key.
	nextExp uint64

	// acct is the hash's footprint as last posted to the registry running sum, so
	// note can post only the delta since the last mutation and the total stays the
	// exact walked sum without rewalking the registry (spec 2064/f3/06 section 6).
	// It is meaningful only when the store runs a cold tier; a store with no cold
	// region never accounts and leaves it zero.
	acct uint64

	// expireAt is the whole key's absolute deadline in unix ms, 0 for a hash with
	// no key-level TTL (spec 2064/f3/16 section 2). This is the key-level EXPIRE
	// deadline, distinct from and outer to the per-field TTL above (nextExp): when
	// it fires the whole hash is dropped, fields and all. It lives inline in the
	// header, not in a side "expires" dict, to hold the memory bar, and is not
	// counted in residentBytes (a fixed per-hash field, like acct). The live funnel
	// drops the hash once cx.NowMs reaches this deadline, before any field reap.
	expireAt int64
}

// residentBytes estimates the hash's resident-byte footprint, the figure the
// registry sums to weigh the shard's hash heap against the resident cap (spec
// 2064/f3/06 section 6). An inline listpack hash is its packed blob's capacity; a
// native hashtable is its field table's slab, records, vectors, and TTL column
// (ftable.residentBytes). Zero preads, O(1). Owner goroutine only.
func (h *hash) residentBytes() uint64 {
	if h.enc == encHashtable {
		return h.ft.residentBytes()
	}
	return uint64(cap(h.blob))
}

// blob flags byte, at blobFlagsOff. Bit 0 is the listpackex marker: once a field
// TTL is set on an inline hash it flips on, the blob grows an eight-byte expiry
// slot per entry, and OBJECT ENCODING reports listpackex. It is sticky, cleared
// only by promotion to the native band or the key's deletion, never by HPERSIST
// (spec 2064/f3/10 section 6.4), matching Redis's listpackex encoding.
const (
	blobFlagEx    = 1 << 0
	inlineExpSize = 8 // absolute unix-ms expiry, little-endian, trailing each entry
)

// blob header offsets: a uint16 little-endian count in bytes 0:2, the flags byte
// at 2, the first entry at 3.
const (
	blobCountOff = 0
	blobFlagsOff = 2
	blobHeader   = 3
)

// newHash builds an empty inline hash. Every hash is born in the listpack band;
// the first write that breaches a threshold promotes it (spec 2064/f3/10 section
// 4.1, matching Redis's listpack-first hashes).
func newHash() *hash {
	return &hash{enc: encListpack, blob: []byte{0, 0, 0}}
}

// inlineCount reads and writes the two-byte little-endian field count in the blob
// header. Kept in one place so the append and delete paths stay in sync.
func (h *hash) inlineCount() int { return int(binary.LittleEndian.Uint16(h.blob[blobCountOff:])) }

func (h *hash) setInlineCount(n int) {
	binary.LittleEndian.PutUint16(h.blob[blobCountOff:], uint16(n))
}

// card is the live field count, answering HLEN in O(1) on both bands: an inline
// hash reads its two-byte count header, a native hash reads the table header.
func (h *hash) card() int {
	if h.enc == encListpack {
		return h.inlineCount()
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
	h.setInlineCount(h.inlineCount() - 1)
	return true
}

// mustPromote reports whether applying (field, value) to the inline band would
// breach an inline threshold, which forces the one-way promotion to native: a
// field name or value over 64 bytes, or a genuinely new field taking the count
// past 512 (spec 2064/f3/10 sections 2.1 and 4.2).
func (h *hash) mustPromote(field, value []byte, exists bool) bool {
	if len(field) > maxListpackValue || len(value) > maxListpackValue {
		return true
	}
	return !exists && h.card()+1 > maxListpackEntries
}

// exStride is the trailing bytes each inline entry carries for its field TTL:
// eight once the listpackex sticky bit is set, zero before. Every blob walk adds
// it to step from one entry to the next; the field and value parsing is unchanged
// because the expiry slot sits after the value.
func (h *hash) exStride() int {
	if h.blob[blobFlagsOff]&blobFlagEx != 0 {
		return inlineExpSize
	}
	return 0
}

// inlineIndex returns the byte offset of field's entry (pointing at its flen
// byte), or -1 when absent. The length is checked before the byte compare so most
// misses cost one byte load.
func (h *hash) inlineIndex(field []byte) int {
	b := h.blob
	ex := h.exStride()
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		vlen := int(b[fstart+flen])
		if flen == len(field) && string(b[fstart:fstart+flen]) == string(field) {
			return i
		}
		i = fstart + flen + 1 + vlen + ex
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
// for a delete. It includes the trailing expiry slot when the sticky bit is set,
// so a delete removes the field's TTL along with it.
func (h *hash) inlineEntryEnd(off int) int {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	return voff + 1 + vlen + h.exStride()
}

// inlineAppend writes one new entry at the blob tail and bumps the count. The
// append copies the field and value bytes, so the argument views are never
// retained.
func (h *hash) inlineAppend(field, value []byte) {
	h.blob = append(h.blob, byte(len(field)))
	h.blob = append(h.blob, field...)
	h.blob = append(h.blob, byte(len(value)))
	h.blob = append(h.blob, value...)
	if h.blob[blobFlagsOff]&blobFlagEx != 0 {
		// The listpackex layout carries an eight-byte expiry slot per entry; a new
		// field is born without a TTL, so the slot starts zero.
		h.blob = append(h.blob, 0, 0, 0, 0, 0, 0, 0, 0)
	}
	h.setInlineCount(h.inlineCount() + 1)
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
	h.eachInlineEx(func(field, value []byte, at uint64) {
		ft.set(field, value)
		if at != 0 {
			ft.setFieldExp(field, at)
		}
	})
	h.ft = ft
	h.blob = nil
	h.enc = encHashtable
	// The sticky listpackex bit does not carry to the native band: OBJECT ENCODING
	// reports hashtable once promoted, with or without field TTLs, matching Redis.
	// nextExp carries over unchanged; the native reap recomputes it exactly.
}

// eachInlineEx visits every inline entry with its field TTL, the promotion replay
// and reap walk. at is 0 when the entry carries no expiry, always so before the
// sticky bit is set. The slices alias the blob and are valid only for the call.
func (h *hash) eachInlineEx(fn func(field, value []byte, at uint64)) {
	b := h.blob
	ex := h.exStride()
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		field := b[fstart : fstart+flen]
		voff := fstart + flen
		vlen := int(b[voff])
		value := b[voff+1 : voff+1+vlen]
		var at uint64
		if ex != 0 {
			at = binary.LittleEndian.Uint64(b[voff+1+vlen:])
		}
		fn(field, value, at)
		i = voff + 1 + vlen + ex
	}
}

// makeInlineEx flips the inline hash to its listpackex form: it rewrites the blob
// with an eight-byte zero expiry slot after every value and sets the sticky bit,
// so the field-TTL setters have a slot to write into. A no-op once the bit is
// already set. Runs on the first HEXPIRE-family setter that lands on an inline
// hash, the one-time cost Redis pays converting listpack to listpackex.
func (h *hash) makeInlineEx() {
	if h.blob[blobFlagsOff]&blobFlagEx != 0 {
		return
	}
	old := h.blob
	nb := make([]byte, blobHeader, len(old)+inlineExpSize*h.inlineCount())
	copy(nb, old[:blobHeader])
	for i := blobHeader; i < len(old); {
		flen := int(old[i])
		voff := i + 1 + flen
		vlen := int(old[voff])
		end := voff + 1 + vlen
		nb = append(nb, old[i:end]...)
		nb = append(nb, 0, 0, 0, 0, 0, 0, 0, 0)
		i = end
	}
	h.blob = nb
	h.blob[blobFlagsOff] |= blobFlagEx
}

// inlineExpAt reads the expiry slot of the entry at off; the caller has confirmed
// the sticky bit is set.
func (h *hash) inlineExpAt(off int) uint64 {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	return binary.LittleEndian.Uint64(h.blob[voff+1+vlen:])
}

// setInlineExpAt writes the expiry slot of the entry at off; the caller has
// confirmed the sticky bit is set.
func (h *hash) setInlineExpAt(off int, at uint64) {
	flen := int(h.blob[off])
	voff := off + 1 + flen
	vlen := int(h.blob[voff])
	binary.LittleEndian.PutUint64(h.blob[voff+1+vlen:], at)
}

// inlineFieldExp returns field's absolute-ms expiry, or 0 when it is absent or
// carries no TTL.
func (h *hash) inlineFieldExp(field []byte) uint64 {
	if h.blob[blobFlagsOff]&blobFlagEx == 0 {
		return 0
	}
	off := h.inlineIndex(field)
	if off < 0 {
		return 0
	}
	return h.inlineExpAt(off)
}

// inlineSetFieldExp writes field's expiry, flipping the sticky bit on first use,
// and reports whether the field was present.
func (h *hash) inlineSetFieldExp(field []byte, at uint64) bool {
	h.makeInlineEx()
	off := h.inlineIndex(field)
	if off < 0 {
		return false
	}
	h.setInlineExpAt(off, at)
	return true
}

// inlineReap deletes every inline field whose expiry has fired at or before now
// and returns the smallest surviving expiry (0 when none remain). A delete
// splices the entry, so the cursor holds after one and steps only past a survivor.
func (h *hash) inlineReap(now uint64) uint64 {
	if h.blob[blobFlagsOff]&blobFlagEx == 0 {
		return 0
	}
	var next uint64
	for i := blobHeader; i < len(h.blob); {
		b := h.blob
		flen := int(b[i])
		voff := i + 1 + flen
		vlen := int(b[voff])
		at := binary.LittleEndian.Uint64(b[voff+1+vlen:])
		end := voff + 1 + vlen + inlineExpSize
		if at != 0 && at <= now {
			h.blob = append(b[:i], b[end:]...)
			h.setInlineCount(h.inlineCount() - 1)
			continue
		}
		if at != 0 && (next == 0 || at < next) {
			next = at
		}
		i = end
	}
	return next
}

// eachInline visits every inline entry in blob order. The slices alias the blob
// and are valid only for the call. Used by the promotion replay and the tests;
// the native band's own walk is ftable.each.
func (h *hash) eachInline(fn func(field, value []byte)) {
	b := h.blob
	ex := h.exStride()
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		field := b[fstart : fstart+flen]
		voff := fstart + flen
		vlen := int(b[voff])
		value := b[voff+1 : voff+1+vlen]
		fn(field, value)
		i = voff + 1 + vlen + ex
	}
}

// at resolves the field-value pair at forward position idx to its bytes, the
// HRANDFIELD draw's index-to-pair step. The native band indexes the dense draw
// vector directly (ftable.at); the inline band walks its blob to the position.
// Both slices alias internal storage and are valid only until the next mutation,
// so the caller emits before drawing again. The caller guarantees idx is in
// [0, card). Position order is not member order and carries no HSCAN guarantee;
// it only has to be a stable bijection onto [0, card) for a uniform rank to land
// on a uniform pair, which both bands are.
func (h *hash) at(idx int) (field, value []byte) {
	if h.enc == encHashtable {
		return h.ft.at(idx)
	}
	b := h.blob
	ex := h.exStride()
	for i := blobHeader; i < len(b); {
		flen := int(b[i])
		fstart := i + 1
		voff := fstart + flen
		vlen := int(b[voff])
		if idx == 0 {
			return b[fstart : fstart+flen], b[voff+1 : voff+1+vlen]
		}
		idx--
		i = voff + 1 + vlen + ex
	}
	return nil, nil
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

// fieldExp returns field's absolute unix-ms expiry, or 0 when the field is absent
// or carries no TTL, the read HTTL and its siblings resolve through.
func (h *hash) fieldExp(field []byte) uint64 {
	if h.enc == encHashtable {
		return h.ft.fieldExp(field)
	}
	return h.inlineFieldExp(field)
}

// setFieldExp writes field's expiry (absolute unix ms; 0 clears it), flipping the
// inline band to listpackex on first use, and folds the value into the next-expire
// hint. Reports whether the field was present; the caller reaps first, so present
// means live.
func (h *hash) setFieldExp(field []byte, at uint64) bool {
	var ok bool
	if h.enc == encHashtable {
		ok = h.ft.setFieldExp(field, at)
	} else {
		ok = h.inlineSetFieldExp(field, at)
	}
	if ok && at != 0 && (h.nextExp == 0 || at < h.nextExp) {
		h.nextExp = at
	}
	return ok
}

// clearFieldExp drops field's TTL, the HPERSIST and HSET-overwrite path. It leaves
// the sticky listpackex bit and the next-expire hint alone: the bit is sticky by
// design, and the hint is a lower bound that a later reap recomputes.
func (h *hash) clearFieldExp(field []byte) {
	if h.enc == encHashtable {
		h.ft.clearFieldExp(field)
		return
	}
	if h.blob[blobFlagsOff]&blobFlagEx != 0 {
		if off := h.inlineIndex(field); off >= 0 {
			h.setInlineExpAt(off, 0)
		}
	}
}

// reap deletes every field whose TTL has fired at or before now and refreshes the
// next-expire hint. Gated by the hint, so a hash with nothing due (the common
// case, and every hash with no field TTL) returns after one comparison. Called at
// command entry so every read and write sees a hash free of fired fields, the
// lazy expiry that stands in for the active sweep deferred to M9.
func (h *hash) reap(now uint64) {
	if h.nextExp == 0 || now < h.nextExp {
		return
	}
	if h.enc == encHashtable {
		h.nextExp = h.ft.reap(now)
	} else {
		h.nextExp = h.inlineReap(now)
	}
}

// encName is what OBJECT ENCODING reports: listpackex for an inline hash that has
// taken a field TTL (the sticky bit), listpack before, and hashtable for the
// native band regardless of field TTLs, matching the Redis 8.8 differential.
func (h *hash) encName() string {
	if h.enc == encListpack && h.blob[blobFlagsOff]&blobFlagEx != 0 {
		return "listpackex"
	}
	return h.enc.String()
}
