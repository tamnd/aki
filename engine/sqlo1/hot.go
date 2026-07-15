package sqlo1

import (
	"bytes"
	"hash/maphash"
)

// HotTable is the hot-tier structure set from doc 04 section 3: packed
// headers in a dense slice, key and value bytes in chunked arenas, and a
// hash-to-slot index (the Go 1.24+ builtin map is a Swiss table, which is
// the doc's stated reason the layout is arrays behind a map instead of a
// map of structs). The doc 04 section 4 state machine lives here too:
// writes dirty, drain cools dirty into resident, eviction drops resident
// to cold or ghost, and deletes leave dirty tombstones until drain
// carries them to disk. The eviction and drain policies (who calls
// drained and evict, and when) belong to later slices; the transitions
// themselves are pinned by an executable property test.
//
// Point ops on existing keys, delete-reinsert cycles, and full
// drain-evict-reinsert cycles run without allocating; the alloczero lab
// gates that.
type HotTable struct {
	seed  maphash.Seed
	index map[uint64]uint32
	// dups carries the extra slots when two live keys share a 64-bit
	// hash: the doc expects a handful at most at any scale that fits
	// RAM, so the side table stays tiny and the fast path is one map
	// hit plus one key compare.
	dups      map[uint64][]uint32
	hdrs      []hdr
	freeSlots []uint32
	keys      arena
	vals      arena
	ghosts    ghostRing
	// tick is the coarse stamp clock, advanced by the server once a
	// second; live counts keys visible to reads, so tombstones are out.
	tick uint32
	live int
}

// maxKlen is the klen field's reach. Longer keys are legal in Redis but
// pathological; they are rejected here and the compat milestone decides
// whether they route around the hot tier or hard-error.
const maxKlen = 1<<16 - 1

// NewHotTable preallocates for capacity resident keys; Put fails once
// the header slice is full until eviction (a later slice) makes room.
func NewHotTable(capacity int) *HotTable {
	return &HotTable{
		seed:   maphash.MakeSeed(),
		index:  make(map[uint64]uint32, capacity),
		dups:   make(map[uint64][]uint32),
		hdrs:   make([]hdr, 0, capacity),
		ghosts: newGhostRing(capacity / 16),
	}
}

// SetTick moves the coarse clock the stamps record. Stamps shift last
// into prev only when the tick has moved, so repeated touches within one
// tick cost one compare (the WATT-lite two-stamp rule, hotclock lab).
func (t *HotTable) SetTick(tick uint32) { t.tick = tick }

func (t *HotTable) touchRead(hd *hdr) {
	if hd.lastRead != t.tick {
		hd.prevRead = hd.lastRead
		hd.lastRead = t.tick
	}
}

func (t *HotTable) touchWrite(hd *hdr) {
	if hd.lastWrite != t.tick {
		hd.prevWrite = hd.lastWrite
		hd.lastWrite = t.tick
	}
}

// Len returns the live key count; tombstones do not count.
func (t *HotTable) Len() int {
	return t.live
}

// Get returns the value for key, aliasing the arena until the next write.
// A dirty tombstone is a miss: the key is gone, its header just has not
// drained yet.
func (t *HotTable) Get(key []byte) ([]byte, bool) {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return nil, false
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 {
		return nil, false
	}
	t.touchRead(hd)
	return t.vals.data(hd.valRef), true
}

// Put inserts or overwrites key and marks it dirty. It reports false
// when the table is full or the key is longer than klen reaches.
func (t *HotTable) Put(key, val []byte, tag uint8) bool {
	if len(key) > maxKlen {
		return false
	}
	h := maphash.Bytes(t.seed, key)
	if s, ok := t.lookup(h, key); ok {
		hd := &t.hdrs[s]
		switch {
		case hd.valRef == 0:
			// Reviving a tombstone: the header never left, so this is
			// an overwrite that happens to bring the key back to life.
			hd.valRef = t.vals.alloc(val)
			t.live++
		case !t.vals.update(hd.valRef, val):
			t.vals.release(hd.valRef)
			hd.valRef = t.vals.alloc(val)
		}
		hd.state = stateDirty
		hd.typeTag = tag
		t.touchWrite(hd)
		return true
	}

	var s uint32
	switch {
	case len(t.freeSlots) > 0:
		s = t.freeSlots[len(t.freeSlots)-1]
		t.freeSlots = t.freeSlots[:len(t.freeSlots)-1]
	case len(t.hdrs) < cap(t.hdrs):
		t.hdrs = t.hdrs[:len(t.hdrs)+1]
		s = uint32(len(t.hdrs) - 1)
	default:
		return false
	}
	t.hdrs[s] = hdr{
		keyRef:  t.keys.alloc(key),
		valRef:  t.vals.alloc(val),
		klen:    uint16(len(key)),
		state:   stateDirty,
		typeTag: tag,
	}
	hd := &t.hdrs[s]
	if g, ok := t.ghosts.take(h); ok {
		// The key was evicted recently enough for its ghost to survive:
		// restore its stamps so the evictor sees its real history, not a
		// newborn (hotclock lab: ghost restore is what makes the stingy
		// promotion coin safe).
		hd.lastRead = g.lastRead
		hd.lastWrite = g.lastWrite
	}
	t.touchWrite(hd)
	t.live++
	if _, occupied := t.index[h]; occupied {
		t.dups[h] = append(t.dups[h], s)
	} else {
		t.index[h] = s
	}
	return true
}

// Del writes a dirty tombstone: the key vanishes from reads immediately,
// but the header stays until drain carries the deletion to disk and
// drained retires the slot. It reports whether the key was live.
func (t *HotTable) Del(key []byte) bool {
	s, ok := t.lookup(maphash.Bytes(t.seed, key), key)
	if !ok {
		return false
	}
	hd := &t.hdrs[s]
	if hd.valRef == 0 {
		return false
	}
	t.vals.release(hd.valRef)
	hd.valRef = 0
	hd.state = stateDirty
	t.touchWrite(hd)
	t.live--
	return true
}

// drained is the drain scheduler's word that slot s is durable in a
// record group: dirty cools to resident with its new disk position, and
// the RAM copy stays so a later eviction is free. A drained tombstone
// has nothing left to say in RAM, so its header and index entry retire.
// It reports false when the slot is not dirty (the drain ran twice or
// raced a state change), and the caller must treat that as a no-op.
func (t *HotTable) drained(s uint32, vptr uint64) bool {
	hd := &t.hdrs[s]
	if hd.state != stateDirty {
		return false
	}
	if hd.valRef == 0 {
		t.removeSlot(maphash.Bytes(t.seed, t.keys.data(hd.keyRef)), s)
		return true
	}
	hd.state = stateResident
	hd.vptr = vptr
	return true
}

// evict drops resident slot s per doc 04 section 4: to cold (keep
// nothing) or to ghost (the stamps move to the ghost ring so a returning
// key gets its history back). Dirty is never evictable, it must drain
// first; that invariant is structural here, not a caller courtesy.
func (t *HotTable) evict(s uint32, toGhost bool) bool {
	hd := &t.hdrs[s]
	if hd.state != stateResident {
		return false
	}
	h := maphash.Bytes(t.seed, t.keys.data(hd.keyRef))
	if toGhost {
		t.ghosts.put(h, hd.lastRead, hd.lastWrite)
	}
	t.removeSlot(h, s)
	return true
}

// removeSlot frees a header slot and its arena bytes and repairs the
// index, promoting a dup when the primary occupant goes.
func (t *HotTable) removeSlot(h uint64, s uint32) {
	hd := &t.hdrs[s]
	t.keys.release(hd.keyRef)
	if hd.valRef != 0 {
		t.vals.release(hd.valRef)
		t.live--
	}
	*hd = hdr{}
	t.freeSlots = append(t.freeSlots, s)

	if t.index[h] == s {
		if ds := t.dups[h]; len(ds) > 0 {
			t.index[h] = ds[len(ds)-1]
			t.shrinkDups(h, len(ds)-1)
		} else {
			delete(t.index, h)
		}
		return
	}
	ds := t.dups[h]
	for i, d := range ds {
		if d == s {
			ds[i] = ds[len(ds)-1]
			t.shrinkDups(h, len(ds)-1)
			break
		}
	}
}

func (t *HotTable) shrinkDups(h uint64, n int) {
	if n == 0 {
		delete(t.dups, h)
		return
	}
	t.dups[h] = t.dups[h][:n]
}

// lookup resolves h to key's header slot, walking the collision side
// table only when the primary slot holds a different key.
func (t *HotTable) lookup(h uint64, key []byte) (uint32, bool) {
	s, ok := t.index[h]
	if !ok {
		return 0, false
	}
	if t.keyIs(s, key) {
		return s, true
	}
	for _, d := range t.dups[h] {
		if t.keyIs(d, key) {
			return d, true
		}
	}
	return 0, false
}

func (t *HotTable) keyIs(s uint32, key []byte) bool {
	hd := &t.hdrs[s]
	return int(hd.klen) == len(key) && bytes.Equal(t.keys.data(hd.keyRef), key)
}
