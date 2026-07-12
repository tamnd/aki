package hash

import (
	"bytes"

	"github.com/tamnd/aki/engine/f3/store"
	structs "github.com/tamnd/aki/engine/f3/struct"
)

// The native hash band (spec 2064/f3/10 section 3): the hashtable encoding is a
// dense entry vector of embedded field-value records indexed by the Swiss-style
// field table from engine/f3/struct, the same kernel M1's set uses. It answers
// probe, insert, overwrite, and delete in about one probe, and it is the band
// OBJECT ENCODING reports as hashtable for Redis parity.
//
// The field name maps to a record ordinal through the table; the record carries
// the slab offsets and lengths of the field and value bytes. wyhash (store.Hash)
// supplies the hash, the same hasher set/member.go feeds the shared table, so the
// two types share both the kernel and the hash function. Delete is swap-remove on
// a dense draw vector with a free list over the record slots (spec 2064/f3/10
// section 3.6): the table's ordinals stay stable so a delete never has to repoint
// the table, and the freed slot returns for the next insert.

// fentry is one field's record. field and value both live in the shared slab;
// the record holds their offsets and lengths, so the value may relocate on an
// overwrite that grows it without moving the field (spec 2064/f3/10 section 3.3,
// minus the TTL slot and the value-log pointer, which are later slices).
type fentry struct {
	foff  uint32 // slab offset of the field bytes
	voff  uint32 // slab offset of the value bytes
	vslot uint32 // index in the draw vector, kept current by swap-remove
	flen  uint16 // field byte length
	vlen  uint32 // value byte length
}

// ftable is the native band for one hash. It is owner-local, so nothing locks.
type ftable struct {
	tbl  structs.Table // control bytes plus record ordinals, field name to ordinal
	ents []fentry      // indexed by record ordinal
	slab []byte        // field and value bytes, appended; holes left by delete/overwrite until compaction
	vec  []uint32      // draw vector: live record ordinals, the swap-remove target
	free []uint32      // ordinals of deleted records, reused before the slab grows
	dead int           // slab bytes behind deleted or overwritten records, drives compaction

	// streams counts the HGETALL/HKEYS/HVALS enumerations pumping off this table
	// right now. While it is nonzero, slab compaction and free-slot reuse both
	// stand down: an open stream is reading slab offsets snapshotted at command
	// time, so moving bytes or repurposing a record's ordinal would slide the read
	// (hgetall.go, the same pin set's htable takes for SMEMBERS).
	streams int
}

// newFtable builds an empty table sized for hint fields, so the inline-to-native
// replay fills it in one pass without a resize (spec 2064/f3/10 section 3.4).
func newFtable(hint int) *ftable {
	f := &ftable{tbl: structs.MakeTable(hint)}
	if hint > 0 {
		f.ents = make([]fentry, 0, hint)
		f.vec = make([]uint32, 0, hint)
	}
	return f
}

// Match confirms a tag hit: the field stored at ord must equal key. It is the
// structs.Set half the table probes through, and it allocates nothing.
func (f *ftable) Match(ord uint32, key []byte) bool {
	e := &f.ents[ord]
	return bytes.Equal(f.slab[e.foff:e.foff+uint32(e.flen)], key)
}

// Rehash recomputes a field's hash from its bytes for a table resize, since the
// record caches none (spec 2064/f3/10 section 3.4).
func (f *ftable) Rehash(ord uint32) uint64 {
	e := &f.ents[ord]
	return store.Hash(f.slab[e.foff : e.foff+uint32(e.flen)])
}

// card is the live field count, straight off the table header (O(1) HLEN).
func (f *ftable) card() int { return f.tbl.Len() }

// get returns the value bytes of field and whether it is present. The slice
// aliases the slab and is valid only until the next mutation.
func (f *ftable) get(field []byte) ([]byte, bool) {
	ord, ok := f.tbl.Find(store.Hash(field), field, f)
	if !ok {
		return nil, false
	}
	e := &f.ents[ord]
	return f.slab[e.voff : e.voff+e.vlen], true
}

// has reports whether field is present.
func (f *ftable) has(field []byte) bool {
	_, ok := f.tbl.Find(store.Hash(field), field, f)
	return ok
}

// strlen returns the value length of field, or 0 when absent.
func (f *ftable) strlen(field []byte) int {
	ord, ok := f.tbl.Find(store.Hash(field), field, f)
	if !ok {
		return 0
	}
	return int(f.ents[ord].vlen)
}

// set writes field to value and reports whether the field was newly created. An
// existing field is overwritten in place when the value fits, else the value is
// re-seated at the slab tail; a new field appends a record and inserts a slot.
func (f *ftable) set(field, value []byte) bool {
	hash := store.Hash(field)
	if ord, ok := f.tbl.Find(hash, field, f); ok {
		f.overwrite(ord, value)
		return false
	}
	ord := f.newRecord(field, value)
	f.tbl.Insert(hash, ord, f)
	return true
}

// setNX writes field to value only when it is absent, reporting whether it set.
// It never disturbs an existing field, matching HSETNX (spec 2064/f3/10 section
// 7.1).
func (f *ftable) setNX(field, value []byte) bool {
	hash := store.Hash(field)
	if _, ok := f.tbl.Find(hash, field, f); ok {
		return false
	}
	ord := f.newRecord(field, value)
	f.tbl.Insert(hash, ord, f)
	return true
}

// overwrite replaces the value at ord. When the new value is no longer than the
// old one it is written over the old bytes; when it grows it is appended fresh
// and the record repointed, the old bytes charged to the dead count (spec
// 2064/f3/10 section 3.6).
func (f *ftable) overwrite(ord uint32, value []byte) {
	e := &f.ents[ord]
	if uint32(len(value)) <= e.vlen {
		f.dead += int(e.vlen) - len(value)
		copy(f.slab[e.voff:], value)
		e.vlen = uint32(len(value))
		return
	}
	f.dead += int(e.vlen)
	e.voff = uint32(len(f.slab))
	f.slab = append(f.slab, value...)
	e.vlen = uint32(len(value))
	f.maybeCompact()
}

// newRecord seats field and value in the slab, takes a record ordinal (reusing a
// freed one first), and appends it to the draw vector.
func (f *ftable) newRecord(field, value []byte) uint32 {
	foff := uint32(len(f.slab))
	f.slab = append(f.slab, field...)
	voff := uint32(len(f.slab))
	f.slab = append(f.slab, value...)

	e := fentry{
		foff:  foff,
		voff:  voff,
		vslot: uint32(len(f.vec)),
		flen:  uint16(len(field)),
		vlen:  uint32(len(value)),
	}
	var ord uint32
	if n := len(f.free); n > 0 && f.streams == 0 {
		// A freed ordinal is reused only when no enumeration is mid-flight: a
		// streamed HGETALL may still be reading the field or value whose record this
		// slot once held, so reuse waits for it to drain (see the streams field).
		ord = f.free[n-1]
		f.free = f.free[:n-1]
		f.ents[ord] = e
	} else {
		ord = uint32(len(f.ents))
		f.ents = append(f.ents, e)
	}
	f.vec = append(f.vec, ord)
	return ord
}

// del removes field and reports whether it was present. It swap-removes from the
// draw vector so the vector stays dense (spec 2064/f3/10 section 3.6): read the
// victim's vslot, move the last ordinal into it, fix the moved record's vslot.
func (f *ftable) del(field []byte) bool {
	ord, ok := f.tbl.Delete(store.Hash(field), field, f)
	if !ok {
		return false
	}
	e := &f.ents[ord]
	v := e.vslot
	last := len(f.vec) - 1
	moved := f.vec[last]
	f.vec[v] = moved
	f.ents[moved].vslot = v
	f.vec = f.vec[:last]

	f.dead += int(e.flen) + int(e.vlen)
	f.free = append(f.free, ord)
	f.maybeCompact()
	return true
}

// at returns the field-value pair at draw-vector position idx, the native band's
// HRANDFIELD index step. Both slices alias the slab and are valid until the next
// mutation. The caller guarantees idx is in [0, card).
func (f *ftable) at(idx int) (field, value []byte) {
	e := &f.ents[f.vec[idx]]
	return f.slab[e.foff : e.foff+uint32(e.flen)], f.slab[e.voff : e.voff+e.vlen]
}

// scanPage is the hashtable band's downward HSCAN cursor, the field-table twin of
// set's htable.scanPage (doc 11 section 8.2, doc 20's swap-remove correctness
// proof carries because ftable.del swap-removes on the same dense vec). The cursor
// is the boundary: draw-vector positions [b, len) were returned by earlier pages
// and [0, b) remain. The page examines up to count positions downward from b and
// returns the new lower boundary, or 0 at the bottom. A fresh scan (cursor 0)
// opens with the whole vector unscanned; a resumed cursor is clamped to the
// current length, since a mid-scan shrink can only have carried the old boundary
// past the new end, and inserts land above the boundary where this walk never
// revisits them. MATCH filters on the field name.
func (f *ftable) scanPage(cursor uint64, count int, match []byte, emit func(field, value []byte)) uint64 {
	n := uint64(len(f.vec))
	if n == 0 {
		return 0
	}
	b := n
	if cursor != 0 && cursor < b {
		b = cursor
	}
	lo := uint64(0)
	if b > uint64(count) {
		lo = b - uint64(count)
	}
	for i := b; i > lo; i-- {
		e := &f.ents[f.vec[i-1]]
		field := f.slab[e.foff : e.foff+uint32(e.flen)]
		if match == nil || globMatch(match, field) {
			emit(field, f.slab[e.voff:e.voff+e.vlen])
		}
	}
	return lo
}

// each visits every field-value pair in draw-vector order. Both slices alias the
// slab and are valid only for the call.
func (f *ftable) each(fn func(field, value []byte)) {
	for _, ord := range f.vec {
		e := &f.ents[ord]
		field := f.slab[e.foff : e.foff+uint32(e.flen)]
		value := f.slab[e.voff : e.voff+e.vlen]
		fn(field, value)
	}
}

// drawLen is the draw-vector length, the high bound the HGETALL/HKEYS/HVALS
// snapshot starts from.
func (f *ftable) drawLen() int { return len(f.vec) }

// ordAt is the record ordinal at draw-vector index i, the stable handle the
// enumeration snapshot copies so a swap-remove reordering the live vector during
// the stream cannot disturb it.
func (f *ftable) ordAt(i int) uint32 { return f.vec[i] }

// fieldByOrd and valueByOrd return the bytes of the field or value at record
// ordinal ord, aliasing the slab. They are how a streamed enumeration reads a
// snapshotted ordinal; the streams pin keeps the slab and the record valid for
// the read.
func (f *ftable) fieldByOrd(ord uint32) []byte {
	e := &f.ents[ord]
	return f.slab[e.foff : e.foff+uint32(e.flen)]
}

func (f *ftable) valueByOrd(ord uint32) []byte {
	e := &f.ents[ord]
	return f.slab[e.voff : e.voff+e.vlen]
}

// flenByOrd and vlenByOrd are the field and value lengths alone, for presizing a
// reply frame without touching the bytes.
func (f *ftable) flenByOrd(ord uint32) int { return int(f.ents[ord].flen) }
func (f *ftable) vlenByOrd(ord uint32) int { return int(f.ents[ord].vlen) }

// pinStream and unpinStream bracket an open enumeration (HGETALL/HKEYS/HVALS
// stream): the pin freezes record reuse and slab compaction, the unpin releases
// them when the last stream drains. Both run on the owner goroutine, the pin at
// command time and the unpin from the stream's Release on the pump.
func (f *ftable) pinStream()   { f.streams++ }
func (f *ftable) unpinStream() { f.streams-- }

// maybeCompact rewrites the slab when the dead bytes behind deleted and
// overwritten records outgrow the live ones, so churn cannot grow the slab
// without bound. This is an amortized maintenance event, not a steady-path cost;
// the store's hole-punching arena is the real owner of dead-byte reclaim (spec
// 2064/f3/10 section 3.6), and this keeps the standalone band honest until it
// lands.
func (f *ftable) maybeCompact() {
	if f.streams > 0 {
		// An open enumeration is reading foff/voff offsets into the live slab;
		// moving bytes now would slide them under it. Compaction resumes once the
		// last stream drains, and the dead bytes it leaves are bounded by the window
		// the stream stays open for.
		return
	}
	if f.dead <= len(f.slab)/2 || f.dead < 4096 {
		return
	}
	packed := make([]byte, 0, len(f.slab)-f.dead)
	for _, ord := range f.vec {
		e := &f.ents[ord]
		foff := uint32(len(packed))
		packed = append(packed, f.slab[e.foff:e.foff+uint32(e.flen)]...)
		voff := uint32(len(packed))
		packed = append(packed, f.slab[e.voff:e.voff+e.vlen]...)
		e.foff = foff
		e.voff = voff
	}
	f.slab = packed
	f.dead = 0
}
