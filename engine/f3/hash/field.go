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
	if n := len(f.free); n > 0 {
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

// maybeCompact rewrites the slab when the dead bytes behind deleted and
// overwritten records outgrow the live ones, so churn cannot grow the slab
// without bound. This is an amortized maintenance event, not a steady-path cost;
// the store's hole-punching arena is the real owner of dead-byte reclaim (spec
// 2064/f3/10 section 3.6), and this keeps the standalone band honest until it
// lands.
func (f *ftable) maybeCompact() {
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
