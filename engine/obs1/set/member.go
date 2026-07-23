package set

import (
	"bytes"

	"github.com/tamnd/aki/engine/obs1/store"
	structs "github.com/tamnd/aki/engine/obs1/struct"
)

// The native member band (spec 2064/f3/11 section 2): the hashtable encoding is
// a Swiss-style member table over owner-local records, a byte slab of member
// bytes, and a dense draw vector of record ordinals. It answers membership,
// insert, and remove in about one probe with zero allocations on the steady
// path, and it carries the encoding OBJECT ENCODING still reports as hashtable
// for Redis parity.
//
// The table, records, slab, and draw vector are built here; the exactly-uniform
// draw over the vector (SPOP and SRANDMEMBER, doc 11 sections 4.3 and 5) lives in
// draw.go and reaches the vector through at(). The vector order is
// insertion-then-swap, which is a legal draw population and needs no member sort.

// record is the fixed per-member cell, 12 bytes after alignment (doc 11 section
// 2.2, on the memory diet). The record caches no member hash (diet step one,
// recomputed on rehash) and no tag (diet step two, the tag lives in the table's
// control byte), which is what takes the native ledger from ~26-28 to ~21-23
// bytes per member (section 11.1).
type record struct {
	loc   uint32 // resident: slab offset of the member's bytes; cold: chunk locator
	vslot uint32 // this member's index in the draw vector, kept current by swap-remove
	mlen  uint16 // member byte length
	band  uint8  // band and tier bits: tierCold marks a demoted member (cold.go)
}

// tierCold is the record band bit the LTM demotion pass sets when a member's bytes
// leave the native slab for a cold chunk (cold.go, spec 2064/f3/06 section 6). A
// cold record's loc no longer points into the slab; it is a chunk locator the cold
// read path resolves. The bit is never set unless the store runs the cold tier, so
// every M0-M6 read stays the resident branch below (the L9 zero-delta contract).
const tierCold uint8 = 0x01

// htable is the native band for one set. It is owner-local, so nothing locks.
type htable struct {
	tbl  structs.Table // control bytes plus record ordinals
	recs []record      // indexed by record ordinal
	slab []byte        // member bytes, appended; holes left by remove until compaction
	vec  []uint32      // draw vector: live record ordinals, the swap-remove target
	free []uint32      // ordinals of removed records, reused before the slab grows
	dead int           // slab bytes behind removed records, drives compaction

	// streams counts the SMEMBERS enumerations pumping off this table right
	// now. A streamed reply reads member bytes straight from the live slab
	// through a snapshot of the ordinals it took at command time (smembers.go),
	// so while any stream is open the table must not move those bytes or reuse a
	// freed record slot out from under it: record reuse and slab compaction both
	// stand down until the last stream drains. This is the set's echo of the
	// store's openStreams arena pin (doc 11 section 8.1), the only price the
	// downward-vector enumeration pays for not copying the members.
	streams int

	// alg is the algebra-indexed sorted-array maintenance (algebra.go), nil until
	// the set engages it. Engagement only happens under the algebraMaintain flag,
	// so while the flag is off this pointer stays nil and add/rem carry one
	// never-taken branch (doc 11 section 6.3).
	alg *sortedIndex

	// cold is the set's shared cold-tier state (cold.go), nil until the demotion
	// pass packs one of this set's members into a chunk. Once set it resolves a
	// cold record's locator to the packed member bytes. A partitioned set's
	// sub-tables share one *coldChunks, so the directory and offset table span the
	// whole set. It stays nil unless the store runs the cold tier, so the resident
	// read paths never dereference it (the tierCold branches gate on the record
	// bit, which is never set with the cold tier off).
	cold *coldChunks
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
// structs.Set half the table probes through, and it allocates nothing on the
// resident branch. A cold record's bytes are pread from its chunk (cold.go); the
// table probe is unchanged by demotion, so a cold member is found and confirmed on
// the same path a resident one is.
func (h *htable) Match(ord uint32, key []byte) bool {
	r := &h.recs[ord]
	if r.band&tierCold != 0 {
		m, ok := h.cold.member(r.loc)
		return ok && bytes.Equal(m, key)
	}
	return bytes.Equal(h.slab[r.loc:r.loc+uint32(r.mlen)], key)
}

// Rehash recomputes a member's hash from its bytes for a table resize, since the
// record caches none. A cold record's bytes are pread; a resize that rehashes a
// cold member is rare (it needs a resident insert into a partly cold table), so
// the pread cost sits off the steady path.
func (h *htable) Rehash(ord uint32) uint64 {
	r := &h.recs[ord]
	if r.band&tierCold != 0 {
		if m, ok := h.cold.member(r.loc); ok {
			return store.Hash(m)
		}
		return 0
	}
	return store.Hash(h.slab[r.loc : r.loc+uint32(r.mlen)])
}

// bytesOf returns a member's bytes, resident from the slab or cold from its chunk.
// A cold read aliases the shared cold scratch and is valid only until the next
// cold read, the same single-call lifetime the slab alias already carries, so
// every enumeration consumes each member before it asks for the next. With the
// cold tier off no record is ever cold, so this is the plain slab slice after one
// predicted-away branch.
func (h *htable) bytesOf(r *record) []byte {
	if r.band&tierCold != 0 {
		m, _ := h.cold.member(r.loc)
		return m
	}
	return h.slab[r.loc : r.loc+uint32(r.mlen)]
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
	if ord, ok := h.tbl.Find(hash, m, h); ok {
		if h.recs[ord].band&tierCold != 0 {
			// The add is a no-op (m is already present), but confirming it read a
			// cold chunk, which signals the chunk's neighbors are hot: bring the whole
			// chunk resident (cold.go, spec 2064/f3/06 sections 6.5 and 7.3). The
			// branch is never taken with the cold tier off, so the hot add is
			// unchanged (the L9 zero-delta contract).
			h.promote(ord)
		}
		return false
	}
	ord := h.newRecord(m)
	h.tbl.Insert(hash, ord, h)
	if h.alg != nil {
		h.alg.onAdd(hash, ord, h.tbl.Len())
	} else if algebraMaintain && h.tbl.Len() >= algebraFloor {
		// The set has just crossed the maintenance floor with the flag on: build
		// the sorted arrays once, then keep them current inline from here (doc 11
		// sections 6.3 and 6.7).
		h.engageAlgebra()
	}
	return true
}

// addRaw inserts m without ever engaging or maintaining the algebra arrays. It
// is the bulk-dedup insert the algebra driver uses to build a transient union
// table (driver.go) and the STORE forms will reuse to build a destination: the
// table is thrown away or rebuilt after, so paying the sorted-array tax on it
// would be pure waste. It is add() minus the algebra branch, byte for byte.
func (h *htable) addRaw(m []byte) bool {
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
	if n := len(h.free); n > 0 && h.streams == 0 {
		// A freed slot is reused only when no enumeration is mid-flight: a
		// streamed SMEMBERS may still be reading the member whose record this
		// slot once was, so reuse waits for it to drain (see the streams field).
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
	hash := store.Hash(m)
	ord, ok := h.tbl.Delete(hash, m, h)
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

	if h.alg != nil {
		// Drop the member from the sorted arrays before its ordinal returns to the
		// free list, so a later reuse of the ordinal never aliases a stale entry
		// (doc 11 section 6.3). SPOP reaches this same path through popOne.
		h.alg.onRemove(hash, ord)
	}
	if r.band&tierCold != 0 {
		// A cold member's bytes live in a chunk, not the slab, so its removal owes
		// nothing to slab dead accounting; it dirties the owning chunk instead, which
		// the promotion-and-repack pass reconciles (spec 2064/f3/06 section 6.5). The
		// directory keys on the fold coordinate, not the table hash.
		h.cold.markDirty(memberDisc(m))
	} else {
		h.dead += int(r.mlen)
	}
	h.free = append(h.free, ord)
	h.maybeCompact()
	return true
}

// each visits every member in draw-vector order. The []byte aliases the slab (or
// the cold scratch for a demoted member) and is valid only for the call.
func (h *htable) each(fn func(m []byte)) {
	for _, ord := range h.vec {
		fn(h.bytesOf(&h.recs[ord]))
	}
}

// eachUntil visits members in draw-vector order until fn returns false, the
// early-stop enumeration SINTERCARD's LIMIT walk rides. The []byte aliases the
// slab (or the cold scratch) and is valid only for the call.
func (h *htable) eachUntil(fn func(m []byte) bool) {
	for _, ord := range h.vec {
		if !fn(h.bytesOf(&h.recs[ord])) {
			return
		}
	}
}

// at returns the member at draw index i, aliasing the slab (or the cold scratch).
// The uniform draw picks i; this slice walks the vector directly, and the next
// slice layers the exactly-uniform weighted draw on the same vector.
func (h *htable) at(i int) []byte {
	return h.bytesOf(&h.recs[h.vec[i]])
}

// recordBytes is the fixed per-member record cell width, 12 bytes after
// alignment (doc 11 section 2.2): loc and vslot four each, mlen two, the
// band-and-tier byte one, padded to a four-byte boundary.
const recordBytes = 12

// residentBytes is the native band's live heap footprint: the member slab, the
// record cells, the draw vector, the free list, and the table's control-and-
// ordinal slots (member.go and structs.Table). It is the set-side term of the
// collection resident-byte estimate (spec 2064/f3/06 section 6.3), measured
// against the real capacities so it tracks a slab grow and the amortized
// compaction that follows a churn of removes.
func (h *htable) residentBytes() uint64 {
	n := uint64(cap(h.slab))
	n += uint64(cap(h.recs)) * recordBytes
	n += uint64(cap(h.vec)) * 4
	n += uint64(cap(h.free)) * 4
	n += uint64(h.tbl.Bytes())
	return n
}

// vlen is the draw vector length, the high bound the SSCAN downward cursor and
// the SMEMBERS snapshot both start from.
func (h *htable) vlen() int { return len(h.vec) }

// ordAt is the record ordinal at draw-vector index i, the stable handle the
// SMEMBERS snapshot copies so a later swap-remove reordering the live vector
// cannot disturb the enumeration.
func (h *htable) ordAt(i int) uint32 { return h.vec[i] }

// memberByOrd returns the bytes of the member at record ordinal ord, aliasing
// the slab (or the cold scratch for a demoted member). It is how a streamed
// enumeration reads a snapshotted ordinal; the streams pin keeps the slab and the
// record valid for the read, and a cold read is copied to the wire before the next
// ordinal reuses the scratch.
func (h *htable) memberByOrd(ord uint32) []byte {
	return h.bytesOf(&h.recs[ord])
}

// mlenByOrd is memberByOrd's length alone, for presizing a reply without
// touching the member bytes.
func (h *htable) mlenByOrd(ord uint32) int { return int(h.recs[ord].mlen) }

// pinStream and unpinStream bracket an open enumeration (SMEMBERS stream): the
// pin freezes record reuse and slab compaction, the unpin releases them when
// the last stream drains. Both run on the owner goroutine, the pin at command
// time and the unpin from the stream's Release on the pump.
func (h *htable) pinStream()   { h.streams++ }
func (h *htable) unpinStream() { h.streams-- }

// maybeCompact rewrites the slab when removed members leave more dead bytes than
// live ones, so churn cannot grow the slab without bound. The real hole-punching
// arena is the store's job (doc 11 section 2.4); this keeps the standalone band
// honest until that lands. Compaction is an amortized maintenance event, not a
// steady-path cost.
func (h *htable) maybeCompact() {
	if h.streams > 0 {
		// An open enumeration is reading loc offsets into the live slab; moving
		// bytes now would slide them under it. Compaction resumes once the last
		// stream drains, and the dead bytes it leaves are bounded by the window
		// it stays open for.
		return
	}
	if h.dead <= len(h.slab)/2 || h.dead < 4096 {
		return
	}
	packed := make([]byte, 0, len(h.slab)-h.dead)
	for _, ord := range h.vec {
		r := &h.recs[ord]
		if r.band&tierCold != 0 {
			continue // a cold member is not in the slab; its loc is a chunk locator
		}
		loc := uint32(len(packed))
		packed = append(packed, h.slab[r.loc:r.loc+uint32(r.mlen)]...)
		r.loc = loc
	}
	h.slab = packed
	h.dead = 0
}
