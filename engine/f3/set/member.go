package set

import (
	"bytes"

	"github.com/tamnd/aki/engine/f3/store"
	structs "github.com/tamnd/aki/engine/f3/struct"
)

// The native member band (spec 2064/f3/11 section 2): the hashtable encoding is
// a Swiss-style member table over owner-local records, a byte slab of member
// bytes, and a dense draw vector of record ordinals. It answers membership,
// insert, and remove in about one probe with zero allocations on the steady
// path, and it carries the encoding OBJECT ENCODING still reports as hashtable
// for Redis parity.
//
// This slice builds the table, the records, the slab, and the draw vector's
// storage. The exactly-uniform weighted draw over the vector (SPOP and
// SRANDMEMBER, doc 11 sections 4.3 and 5) is the next slice; until it lands the
// draw path reaches the vector through at() in insertion-then-swap order, which
// is a legal draw order and needs no member sort.

// record is the fixed per-member cell, 12 bytes after alignment (doc 11 section
// 2.2, on the memory diet). The record caches no member hash (diet step one,
// recomputed on rehash) and no tag (diet step two, the tag lives in the table's
// control byte), which is what takes the native ledger from ~26-28 to ~21-23
// bytes per member (section 11.1).
type record struct {
	loc   uint32 // slab offset of this member's bytes
	vslot uint32 // this member's index in the draw vector, kept current by swap-remove
	mlen  uint16 // member byte length
	flags uint8  // band and tier bits, reserved for the algebra and LTM slices
}

// htable is the native band for one set. It is owner-local, so nothing locks.
type htable struct {
	tbl  structs.Table // control bytes plus record ordinals
	recs []record      // indexed by record ordinal
	slab []byte        // member bytes, appended; holes left by remove until compaction
	vec  []uint32      // draw vector: live record ordinals, the swap-remove target
	free []uint32      // ordinals of removed records, reused before the slab grows
	dead int           // slab bytes behind removed records, drives compaction
}

// newHashtable builds an empty table sized for hint members, so a band
// conversion fills it in one pass without a resize (doc 11 section 2.6).
func newHashtable(hint int) *htable {
	h := &htable{tbl: structs.MakeTable(hint)}
	if hint > 0 {
		h.recs = make([]record, 0, hint)
		h.vec = make([]uint32, 0, hint)
	}
	return h
}

// Match confirms a tag hit: the stored member at ord must equal key. It is the
// structs.Set half the table probes through, and it allocates nothing.
func (h *htable) Match(ord uint32, key []byte) bool {
	r := &h.recs[ord]
	return bytes.Equal(h.slab[r.loc:r.loc+uint32(r.mlen)], key)
}

// Rehash recomputes a member's hash from its bytes for a table resize, since the
// record caches none.
func (h *htable) Rehash(ord uint32) uint64 {
	r := &h.recs[ord]
	return store.Hash(h.slab[r.loc : r.loc+uint32(r.mlen)])
}

func (h *htable) card() int { return h.tbl.Len() }

func (h *htable) has(m []byte) bool {
	_, ok := h.tbl.Find(store.Hash(m), m, h)
	return ok
}

// add inserts m and reports whether the set gained it. A duplicate add costs one
// probe and no allocation; a genuine insert may grow the slab, the record slab,
// or the table, which are the excepted growth events.
func (h *htable) add(m []byte) bool {
	hash := store.Hash(m)
	if _, ok := h.tbl.Find(hash, m, h); ok {
		return false
	}
	ord := h.newRecord(m)
	h.tbl.Insert(hash, ord, h)
	return true
}

// newRecord seats m's bytes in the slab, takes a record ordinal (reusing a freed
// one first), and appends it to the draw vector.
func (h *htable) newRecord(m []byte) uint32 {
	loc := uint32(len(h.slab))
	h.slab = append(h.slab, m...)

	var ord uint32
	if n := len(h.free); n > 0 {
		ord = h.free[n-1]
		h.free = h.free[:n-1]
		h.recs[ord] = record{loc: loc, vslot: uint32(len(h.vec)), mlen: uint16(len(m))}
	} else {
		ord = uint32(len(h.recs))
		h.recs = append(h.recs, record{loc: loc, vslot: uint32(len(h.vec)), mlen: uint16(len(m))})
	}
	h.vec = append(h.vec, ord)
	return ord
}

// rem deletes m and reports whether it was present. It swap-removes from the
// draw vector so the vector stays dense, the doc 11 section 2.2 kernel: read the
// victim's vslot, move the last ordinal into it, fix the moved record's vslot.
func (h *htable) rem(m []byte) bool {
	ord, ok := h.tbl.Delete(store.Hash(m), m, h)
	if !ok {
		return false
	}
	r := &h.recs[ord]
	v := r.vslot
	last := len(h.vec) - 1
	moved := h.vec[last]
	h.vec[v] = moved
	h.recs[moved].vslot = v
	h.vec = h.vec[:last]

	h.dead += int(r.mlen)
	h.free = append(h.free, ord)
	h.maybeCompact()
	return true
}

// each visits every member in draw-vector order. The []byte aliases the slab and
// is valid only for the call.
func (h *htable) each(fn func(m []byte)) {
	for _, ord := range h.vec {
		r := &h.recs[ord]
		fn(h.slab[r.loc : r.loc+uint32(r.mlen)])
	}
}

// at returns the member at draw index i, aliasing the slab. The uniform draw
// picks i; this slice walks the vector directly, and the next slice layers the
// exactly-uniform weighted draw on the same vector.
func (h *htable) at(i int) []byte {
	r := &h.recs[h.vec[i]]
	return h.slab[r.loc : r.loc+uint32(r.mlen)]
}

// maybeCompact rewrites the slab when removed members leave more dead bytes than
// live ones, so churn cannot grow the slab without bound. The real hole-punching
// arena is the store's job (doc 11 section 2.4); this keeps the standalone band
// honest until that lands. Compaction is an amortized maintenance event, not a
// steady-path cost.
func (h *htable) maybeCompact() {
	if h.dead <= len(h.slab)/2 || h.dead < 4096 {
		return
	}
	packed := make([]byte, 0, len(h.slab)-h.dead)
	for _, ord := range h.vec {
		r := &h.recs[ord]
		loc := uint32(len(packed))
		packed = append(packed, h.slab[r.loc:r.loc+uint32(r.mlen)]...)
		r.loc = loc
	}
	h.slab = packed
	h.dead = 0
}
