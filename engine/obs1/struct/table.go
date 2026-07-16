package structs

import (
	"encoding/binary"
	"math/bits"
)

// Table is the native member table's kernel: a Swiss-style open-addressed table
// of one-byte control slots and four-byte record ordinals (spec 2064/f3/11
// sections 2.1 and 2.5). It stores no member bytes and no pointers, so a full
// scan is invisible to the garbage collector, and it is owner-local: one shard
// goroutine is the only reader and writer, so nothing here is atomic and nothing
// locks.
//
// The layout and the probe are the frozen verdicts of lab 01
// (labs/f3/m1/01_member_table): 7/8 maximum load, doubling growth, eight-wide
// SWAR group probe with triangular group stepping, and a 7-bit H2 tag that
// rejects a slot before the record is read. The lab settled these against the
// looser and per-slot alternatives; the group scan is what keeps a lookup near
// one probe at 7/8 load, and 7/8 is the knee of the bytes-versus-probes curve.
//
// The table caches no per-member hash (the memory diet of doc 11 section 11.1
// drops the record's hash32 word), so growth recomputes each live member's hash
// through the Set callback. The table also carries no member key: the tag filters
// candidates and the Set confirms the survivor against the caller's own storage.
// This is the same kernel doc 11 line 144 shares with M4's field table, which is
// why the member layout lives in the caller, not here.
type Table struct {
	ctrl  []byte   // one control byte per slot: empty, deleted, or a 7-bit H2 tag
	ords  []uint32 // record ordinal per slot, meaningful only when the slot is full
	cap   uint32   // slot count, a power of two, always a whole number of groups
	count uint32   // live entries
	dead  uint32   // tombstoned slots awaiting reclaim on the next resize
}

// Set is the member store the table probes into. The table holds ordinals; the
// Set turns an ordinal back into member bytes to confirm a tag hit and to
// recompute a hash on growth. Passing a pointer that implements Set as this
// interface allocates nothing (the pointer already exists), which is what keeps
// the point ops off the heap.
type Set interface {
	// Match reports whether the member stored at ord equals key. It runs only on
	// a tag hit, so it is the confirm-before-compare of doc 11 line 189.
	Match(ord uint32, key []byte) bool
	// Rehash returns the full hash of the member stored at ord, recomputed from
	// its bytes. The table calls it only during a resize, once per live member,
	// because the record caches no hash (doc 11 section 11.1 diet step one).
	Rehash(ord uint32) uint64
}

const (
	ctrlEmpty   = 0x80 // 0b1000_0000: never held a member
	ctrlDeleted = 0xFE // 0b1111_1110: held a member, now a tombstone the probe walks past
	groupWidth  = 8    // control bytes scanned per SWAR word
	minCap      = 8    // one group; the smallest table that can be probed group-aligned

	loadNum = 7 // maximum load is loadNum/loadDen = 7/8
	loadDen = 8

	lowBits  = 0x0101010101010101
	highBits = 0x8080808080808080
)

// MakeTable returns a table sized to hold hint members at or under 7/8 load
// without a resize, so a band conversion (doc 11 section 2.6) fills in one pass.
func MakeTable(hint int) Table {
	var t Table
	t.init(capFor(hint))
	return t
}

// capFor is the smallest power-of-two slot count that seats n members under the
// 7/8 load ceiling, floored at one group.
func capFor(n int) uint32 {
	c := uint32(minCap)
	for uint64(c)*loadNum/loadDen < uint64(n) {
		c <<= 1
	}
	return c
}

func (t *Table) init(cap uint32) {
	t.ctrl = make([]byte, cap)
	for i := range t.ctrl {
		t.ctrl[i] = ctrlEmpty
	}
	t.ords = make([]uint32, cap)
	t.cap = cap
	t.count = 0
	t.dead = 0
}

// Len is the live member count.
func (t *Table) Len() int { return int(t.count) }

// maxLoad is the occupancy (live plus tombstoned) at which the next insert must
// resize, holding the table at or under 7/8.
func (t *Table) maxLoad() uint32 { return t.cap * loadNum / loadDen }

// tagOf takes the low 7 bits of the hash for the H2 tag; the high bit stays
// clear so a full slot's control byte can never read as empty (0x80) or deleted
// (0xFE).
func tagOf(h uint64) byte { return byte(h) & 0x7f }

// groupOf selects the home group from the hash bits above the tag, so the group
// choice and the tag are drawn from disjoint parts of the hash.
func groupOf(h uint64, numGroups uint32) uint32 {
	return uint32(h>>7) & (numGroups - 1)
}

// matchByte returns a mask with 0x80 set in each byte position of group equal to
// b, the portable SWAR byte match (abseil's, via the classic zero-byte trick,
// exact for any target byte).
func matchByte(group uint64, b byte) uint64 {
	x := group ^ (lowBits * uint64(b))
	return (x - lowBits) &^ x & highBits
}

// Find returns the ordinal of key's member and true, or false when key is
// absent. It probes group by group: the tag match yields candidate slots, the
// Set confirms each, and a group holding any empty slot ends the probe because
// the member would have been placed there.
func (t *Table) Find(h uint64, key []byte, s Set) (uint32, bool) {
	if _, ord, ok := t.findSlot(h, key, s); ok {
		return ord, true
	}
	return 0, false
}

// findSlot is the shared probe: it returns the full slot index, its ordinal, and
// whether key was found. Delete uses the slot index to tombstone in place.
func (t *Table) findSlot(h uint64, key []byte, s Set) (slot, ord uint32, ok bool) {
	tag := tagOf(h)
	numGroups := t.cap >> 3
	g := groupOf(h, numGroups)
	stride := uint32(0)
	for {
		base := g << 3
		word := binary.LittleEndian.Uint64(t.ctrl[base : base+8])
		m := matchByte(word, tag)
		for m != 0 {
			j := uint32(bits.TrailingZeros64(m)) >> 3
			o := t.ords[base+j]
			if s.Match(o, key) {
				return base + j, o, true
			}
			m &= m - 1
		}
		if matchByte(word, ctrlEmpty) != 0 {
			return 0, 0, false
		}
		stride++
		g = (g + stride) & (numGroups - 1)
	}
}

// Insert places key's ordinal. The caller has already confirmed key is absent
// (SADD probes before it inserts), so this only resizes if needed and drops the
// ordinal into the first empty-or-deleted slot on the probe path.
func (t *Table) Insert(h uint64, ord uint32, s Set) {
	if t.count+t.dead+1 > t.maxLoad() {
		t.resize(s)
		// resize may have changed cap; the hash is unchanged so re-place below.
	}
	t.place(h, ord)
	t.count++
}

// place drops ord into the first empty-or-deleted slot of the probe sequence.
// It never has to grow: Insert and resize both guarantee room first.
func (t *Table) place(h uint64, ord uint32) {
	tag := tagOf(h)
	numGroups := t.cap >> 3
	g := groupOf(h, numGroups)
	stride := uint32(0)
	for {
		base := g << 3
		word := binary.LittleEndian.Uint64(t.ctrl[base : base+8])
		if free := word & highBits; free != 0 {
			j := uint32(bits.TrailingZeros64(free)) >> 3
			slot := base + j
			if t.ctrl[slot] == ctrlDeleted {
				t.dead--
			}
			t.ctrl[slot] = tag
			t.ords[slot] = ord
			return
		}
		stride++
		g = (g + stride) & (numGroups - 1)
	}
}

// resize rebuilds the table. It doubles only when the live members alone need
// more room; when the pressure is tombstones it rebuilds at the same capacity,
// which reclaims them (doc 11 section 2.5's in-place rebuild at 1/4 dead falls
// out of this same path). Every live member is re-placed from a freshly
// recomputed hash, so no per-member hash is cached anywhere.
func (t *Table) resize(s Set) {
	newCap := t.cap
	if newCap < minCap {
		newCap = minCap
	}
	for t.count+1 > newCap*loadNum/loadDen {
		newCap <<= 1
	}
	oldCtrl, oldOrds := t.ctrl, t.ords
	t.init(newCap)
	for i, c := range oldCtrl {
		if c&0x80 == 0 { // full slot: high bit clear
			ord := oldOrds[i]
			t.place(s.Rehash(ord), ord)
			t.count++
		}
	}
}

// Delete tombstones key's slot and returns the freed ordinal. The tombstone is a
// hole the probe walks past; it is reclaimed on the next resize. Removal never
// shrinks the table (F4 sets convert one way; a shrinking table stays a table).
func (t *Table) Delete(h uint64, key []byte, s Set) (uint32, bool) {
	slot, ord, ok := t.findSlot(h, key, s)
	if !ok {
		return 0, false
	}
	t.ctrl[slot] = ctrlDeleted
	t.dead++
	t.count--
	return ord, true
}

// CapSlots is the allocated slot count, exported for the memory-accounting test
// so the bucket term is measured against the real table, not a model.
func (t *Table) CapSlots() int { return int(t.cap) }

// Bytes is the table's resident heap footprint: one control byte per slot plus a
// four-byte record ordinal per slot. It backs the collection resident-byte
// estimate the cold-tier accounting keeps (spec 2064/f3/06 section 6.3), so a
// native set's table term is measured against the real allocation, not modeled.
func (t *Table) Bytes() int { return len(t.ctrl) + len(t.ords)*4 }
