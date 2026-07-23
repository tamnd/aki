// The regime A keymap (spec 2064/obs1 doc 05 section 2.1): a per-group
// open-addressed table from key fingerprint to a packed cold locator.
// It answers the read path's first question, does this key have a cold
// copy and where, entirely in RAM, so a definitive miss costs zero
// bucket requests (OB10, R-I3).
//
// The fingerprint is the bloom's first hash, the same u64 the fold
// orders run chunks by and stamps into their FirstDisc, so the keymap,
// the segment layout, and the footer chunk index all speak one order.
// Fingerprint collisions, two live keys sharing all 64 bits, are
// disambiguated at the chunk read, which carries full keys; the doc's
// collision arithmetic needs the full width, which is why each entry
// keeps its whole fingerprint next to the locator: 16 bytes per slot
// rather than the doc's 8-byte headline. Quotienting the home-slot bits
// back out of the stored fingerprint is a recorded follow-up for the
// doc 10 memory gate; it changes nothing above this file.
//
// The table is robin hood with backward-shift deletion: no tombstone
// slots, load capped at 0.85, growth by doubling with an exact rehash
// off the stored fingerprints. Deletions are index-authoritative
// (doc 05 section 8): keydel removes the entry at apply time, so a
// deleted-then-cold key is a definitive miss even while its dead record
// still sits in a segment as compaction fuel.
//
// Rebuild at takeover (doc 06 section 2) replays segments newest-first
// or oldest-first in any order through Shadow, where a higher SegSeq
// claim, value or tombstone, shadows every lower one; FinishRebuild
// then sweeps the tombstone claims out, leaving only live keys.
package obs1

import (
	"fmt"
	"sync"
)

// KeyLoc is one cold key's unpacked locator: which segment holds its
// newest folded record, which chunk inside that segment, and which tier
// the bytes were last known to live on (0 is the bucket; the NVMe and
// separated-band tags arrive with their slices).
type KeyLoc struct {
	Seg   uint32
	Chunk uint32
	Tier  uint8
}

const (
	keymapMinSlots  = 1024
	keymapLoadNum   = 85
	keymapLoadDen   = 100
	keymapChunkBits = 24
	keymapTierBits  = 2

	locSegShift   = 0
	locChunkShift = 32
	locTierShift  = 56
	locShadowBit  = 58
)

// kmEntry is one slot: the full fingerprint and the packed locator.
// A zero loc marks an empty slot, which Put and Shadow keep unreachable
// by refusing SegSeq zero (the folder's counter starts at one).
type kmEntry struct {
	fp  uint64
	loc uint64
}

// Keymap is one group's resident cold-key index. All methods are safe
// for concurrent use; the lock is per group and the critical sections
// are a handful of probes, so contention is a non-event next to the
// bucket round trips the map exists to avoid.
type Keymap struct {
	mu    sync.Mutex
	slots []kmEntry
	n     int
}

// Fingerprint is the keymap's key hash, the bloom's first hash, shared
// with the fold's run order so every structure agrees on where a key
// sorts.
func Fingerprint(key []byte) uint64 {
	h1, _ := bloomHash(key)
	return h1
}

// NewKeymap builds an empty keymap at the minimum capacity.
func NewKeymap() *Keymap {
	return &Keymap{slots: make([]kmEntry, keymapMinSlots)}
}

func packLoc(l KeyLoc, shadow bool) (uint64, error) {
	if l.Seg == 0 {
		return 0, fmt.Errorf("obs1: keymap locator needs a nonzero SegSeq")
	}
	if l.Chunk >= 1<<keymapChunkBits {
		return 0, fmt.Errorf("obs1: keymap chunk %d exceeds %d bits", l.Chunk, keymapChunkBits)
	}
	if l.Tier >= 1<<keymapTierBits {
		return 0, fmt.Errorf("obs1: keymap tier %d exceeds %d bits", l.Tier, keymapTierBits)
	}
	loc := uint64(l.Seg)<<locSegShift |
		uint64(l.Chunk)<<locChunkShift |
		uint64(l.Tier)<<locTierShift
	if shadow {
		loc |= 1 << locShadowBit
	}
	return loc, nil
}

func unpackLoc(loc uint64) KeyLoc {
	return KeyLoc{
		Seg:   uint32(loc >> locSegShift),
		Chunk: uint32(loc>>locChunkShift) & (1<<keymapChunkBits - 1),
		Tier:  uint8(loc>>locTierShift) & (1<<keymapTierBits - 1),
	}
}

func locShadow(loc uint64) bool { return loc>>locShadowBit&1 == 1 }

// dist is the robin hood probe distance of a fingerprint found at slot i.
func kmDist(fp uint64, i, mask int) int {
	return (i - int(fp)&mask) & mask
}

// Put records fp's newest cold locator, inserting or overwriting.
func (m *Keymap) Put(fp uint64, l KeyLoc) error {
	loc, err := packLoc(l, false)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.growLocked()
	m.putLocked(kmEntry{fp: fp, loc: loc})
	return nil
}

// Lookup answers the read path: the locator of fp's newest folded
// record, or false for a definitive miss. Shadow claims left by an
// unfinished rebuild read as absent.
func (m *Keymap) Lookup(fp uint64) (KeyLoc, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mask := len(m.slots) - 1
	for i, d := int(fp)&mask, 0; ; i, d = (i+1)&mask, d+1 {
		e := m.slots[i]
		if e.loc == 0 || d > kmDist(e.fp, i, mask) {
			return KeyLoc{}, false
		}
		if e.fp == fp {
			if locShadow(e.loc) {
				return KeyLoc{}, false
			}
			return unpackLoc(e.loc), true
		}
	}
}

// Delete removes fp's entry, the apply-time half of index-authoritative
// deletion. It reports whether an entry existed, which the folder's
// tombstone filter reads as whether the key ever folded.
func (m *Keymap) Delete(fp uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteLocked(fp)
}

// Shadow is the rebuild insert: a claim from segment l.Seg, a value or
// a tombstone, lands only when it outranks the claim already held, so
// feeding segments in any order converges on the highest SegSeq per key
// (doc 06's shadowing rule).
func (m *Keymap) Shadow(fp uint64, l KeyLoc, tombstone bool) error {
	loc, err := packLoc(l, tombstone)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mask := len(m.slots) - 1
	for i, d := int(fp)&mask, 0; ; i, d = (i+1)&mask, d+1 {
		e := m.slots[i]
		if e.loc == 0 || d > kmDist(e.fp, i, mask) {
			break
		}
		if e.fp == fp {
			if unpackLoc(e.loc).Seg >= l.Seg {
				return nil
			}
			m.slots[i].loc = loc
			return nil
		}
	}
	m.growLocked()
	m.putLocked(kmEntry{fp: fp, loc: loc})
	return nil
}

// FinishRebuild sweeps the tombstone claims a rebuild left behind and
// returns how many it removed; what stays is exactly the live cold
// keyspace.
func (m *Keymap) FinishRebuild() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := 0
	// A backward shift in a cluster that wraps the array end can move an
	// entry into a slot the pass already scanned, so sweep until a whole
	// pass removes nothing; the second pass is the rare wrap case only.
	for {
		pass := 0
		for i := range m.slots {
			for m.slots[i].loc != 0 && locShadow(m.slots[i].loc) {
				m.deleteLocked(m.slots[i].fp)
				pass++
			}
		}
		removed += pass
		if pass == 0 {
			return removed
		}
	}
}

// Len is the number of resident entries, shadow claims included while a
// rebuild is in flight.
func (m *Keymap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

// Bytes is the table's resident cost, the doc 10 memory gate's line.
func (m *Keymap) Bytes() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.slots) * 16
}

// putLocked inserts or overwrites with robin hood displacement. The
// caller has grown the table, so a free slot always exists.
func (m *Keymap) putLocked(e kmEntry) {
	mask := len(m.slots) - 1
	for i, d := int(e.fp)&mask, 0; ; i, d = (i+1)&mask, d+1 {
		cur := m.slots[i]
		if cur.loc == 0 {
			m.slots[i] = e
			m.n++
			return
		}
		if cur.fp == e.fp && kmDist(cur.fp, i, mask) == d {
			m.slots[i] = e
			return
		}
		if cd := kmDist(cur.fp, i, mask); cd < d {
			m.slots[i] = e
			e, d = cur, cd
		}
	}
}

func (m *Keymap) deleteLocked(fp uint64) bool {
	mask := len(m.slots) - 1
	i, d := int(fp)&mask, 0
	for ; ; i, d = (i+1)&mask, d+1 {
		e := m.slots[i]
		if e.loc == 0 || d > kmDist(e.fp, i, mask) {
			return false
		}
		if e.fp == fp {
			break
		}
	}
	// Backward shift: pull successors home until a slot is empty or
	// already sitting at its home.
	for {
		next := (i + 1) & mask
		e := m.slots[next]
		if e.loc == 0 || kmDist(e.fp, next, mask) == 0 {
			m.slots[i] = kmEntry{}
			break
		}
		m.slots[i] = e
		i = next
	}
	m.n--
	return true
}

// growLocked doubles the table when one more insert would pass the load
// cap. The rehash is exact because every entry keeps its fingerprint.
func (m *Keymap) growLocked() {
	if (m.n+1)*keymapLoadDen <= len(m.slots)*keymapLoadNum {
		return
	}
	old := m.slots
	m.slots = make([]kmEntry, len(old)*2)
	m.n = 0
	for _, e := range old {
		if e.loc != 0 {
			m.putLocked(e)
		}
	}
}
