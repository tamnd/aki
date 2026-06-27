// Package findex is the S0 spike for the aki storage rewrite: a FASTER-style
// open-addressed hash index whose entries are a single 8-byte word holding a
// 15-bit tag and a 48-bit log address, with the full keys living in the log,
// not in the index.
//
// It exists to settle one design fork with a real benchmark before S1 builds
// on it (see ~/notes/Spec/2064/rewrite/01-index-structure.md): the v2 spike's
// map[string]valLoc keeps keys resident and is pointer-heavy, while this index
// holds a small fixed number of bytes per key independent of key size and is
// pointer-free, which is what a larger-than-memory store needs. The benchmark
// in findex_test.go measures resident-bytes-per-key and lookup ns for both so
// the choice is made on numbers, not assertion.
//
// Collision handling here is linear probing across the flat entry array. Doc
// 01's faithful design resolves same-(bucket,tag) collisions by chaining
// through the log's previous-address link; the spike uses linear probing
// because it is simpler, more cache-friendly at load <= 0.75, and identical on
// the two axes this spike measures (an entry is still 8 bytes and keys still
// live only in the log). In-log chaining and the 256-shard, latch-free CAS
// insert are later slices layered on a structure proven here.
package findex

import "encoding/binary"

// Entry layout in a uint64 word:
//
//	bits  0..47  log address (48 bits): byte offset of the record in the log.
//	bits 48..62  tag (15 bits): a slice of the key hash, forced nonzero.
//	bit     63   tentative (reserved for the latch-free insert; unused here).
//
// A word of 0 means an empty slot. Address 0 is never a real record because
// the log reserves its first 8 bytes, so 0 is unambiguously "empty".
const (
	addrBits = 48
	addrMask = (uint64(1) << addrBits) - 1
	tagShift = addrBits
	tagBits  = 15
	tagMask  = (uint64(1) << tagBits) - 1
)

func makeEntry(tag uint16, addr uint64) uint64 {
	return (uint64(tag&uint16(tagMask)) << tagShift) | (addr & addrMask)
}

func entryTag(e uint64) uint16  { return uint16((e >> tagShift) & tagMask) }
func entryAddr(e uint64) uint64 { return e & addrMask }

// Index is the open-addressed table plus its log. The table is a flat
// []uint64: pointer-free, so the Go GC marks it in O(1) and never scans
// entries. The log holds the key bytes and the value; keys are never resident
// in the table.
type Index struct {
	slots []uint64 // power-of-two count of entry words, pointer-free
	mask  uint64   // len(slots) - 1
	count int      // live keys

	log     []byte // append-only record arena; keys live here, not in slots
	maxLoad float64
}

// New returns an index sized for hint keys at a 0.75 target load factor.
func New(hint int) *Index {
	if hint < 64 {
		hint = 64
	}
	want := uint64(float64(hint) / 0.75)
	n := uint64(64)
	for n < want {
		n <<= 1
	}
	return &Index{
		slots:   make([]uint64, n),
		mask:    n - 1,
		log:     make([]byte, 8, 64*1024), // reserve offset 0 so addr 0 == empty
		maxLoad: 0.75,
	}
}

// Record layout in the log:
//
//	[keyLen:2][valLen:4][key][val]
//
// The spike's linear probing needs no per-record chain link, so the record is
// just framed key+value. The real engine adds the previous-address link and
// the full ValueHeader (doc 02).
const recHdr = 2 + 4

func (ix *Index) appendRecord(key, val []byte) uint64 {
	addr := uint64(len(ix.log))
	var hdr [recHdr]byte
	binary.LittleEndian.PutUint16(hdr[0:], uint16(len(key)))
	binary.LittleEndian.PutUint32(hdr[2:], uint32(len(val)))
	ix.log = append(ix.log, hdr[:]...)
	ix.log = append(ix.log, key...)
	ix.log = append(ix.log, val...)
	return addr
}

func (ix *Index) recKey(addr uint64) []byte {
	b := ix.log[addr:]
	kl := binary.LittleEndian.Uint16(b[0:])
	return b[recHdr : recHdr+int(kl)]
}

func (ix *Index) recVal(addr uint64) []byte {
	b := ix.log[addr:]
	kl := int(binary.LittleEndian.Uint16(b[0:]))
	vl := int(binary.LittleEndian.Uint32(b[2:]))
	off := recHdr + kl
	return b[off : off+vl]
}

// hash is FNV-1a, the same family the v2 store uses to pick shards, so the
// spike's distribution matches the engine it feeds.
func hash64(key []byte) uint64 {
	const off = 1469598103934665603
	const prime = 1099511628211
	h := uint64(off)
	for _, c := range key {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

func tagOf(h uint64) uint16 {
	// take a high slice of the hash, independent of the low probe bits, and
	// force nonzero so a set entry is never confused with empty.
	t := uint16((h >> 24) & tagMask)
	if t == 0 {
		t = 1
	}
	return t
}

// Get returns the value for key and whether it is present. The steady-state
// path is: hash, index to the home slot, linear-probe comparing the 15-bit
// tag, and on a tag match deref the log and compare the full key. Probing
// stops at the first empty slot.
func (ix *Index) Get(key []byte) ([]byte, bool) {
	h := hash64(key)
	tag := tagOf(h)
	i := h & ix.mask
	for {
		e := ix.slots[i]
		if e == 0 {
			return nil, false
		}
		if entryTag(e) == tag {
			addr := entryAddr(e)
			if bytesEqual(ix.recKey(addr), key) {
				return ix.recVal(addr), true
			}
		}
		i = (i + 1) & ix.mask
	}
}

// Put inserts or updates key. On update it appends a new record and repoints
// the key's slot at it in place (no second slot, so count is exact and the
// table never accumulates shadow entries; the prior log record is garbage for
// compaction). Grows by doubling when load exceeds the target.
func (ix *Index) Put(key, val []byte) {
	if float64(ix.count+1) > ix.maxLoad*float64(len(ix.slots)) {
		ix.grow()
	}
	h := hash64(key)
	tag := tagOf(h)
	i := h & ix.mask
	for {
		e := ix.slots[i]
		if e == 0 {
			ix.slots[i] = makeEntry(tag, ix.appendRecord(key, val))
			ix.count++
			return
		}
		if entryTag(e) == tag && bytesEqual(ix.recKey(entryAddr(e)), key) {
			ix.slots[i] = makeEntry(tag, ix.appendRecord(key, val)) // update in place
			return
		}
		i = (i + 1) & ix.mask
	}
}

// Delete removes key, returning whether it was present. Backward-shift
// deletion keeps the probe invariant (no tombstones) so Get's stop-at-empty
// stays correct.
func (ix *Index) Delete(key []byte) bool {
	h := hash64(key)
	tag := tagOf(h)
	i := h & ix.mask
	for {
		e := ix.slots[i]
		if e == 0 {
			return false
		}
		if entryTag(e) == tag && bytesEqual(ix.recKey(entryAddr(e)), key) {
			ix.backshift(i)
			ix.count--
			return true
		}
		i = (i + 1) & ix.mask
	}
}

// backshift closes the hole at i by pulling back any following entry that can
// legally move into it (standard linear-probe backward-shift deletion, no
// tombstones, so Get's stop-at-empty stays correct).
func (ix *Index) backshift(hole uint64) {
	ix.slots[hole] = 0
	j := (hole + 1) & ix.mask
	for {
		e := ix.slots[j]
		if e == 0 {
			return
		}
		home := hash64(ix.recKey(entryAddr(e))) & ix.mask
		// Forward ring distances from the hole. An entry at j may move back to
		// the hole only if its home is not cyclically in (hole, j]; that is
		// when home == hole, or home lies strictly past j.
		dj := (j - hole) & ix.mask
		dHome := (home - hole) & ix.mask
		if dHome == 0 || dHome > dj {
			ix.slots[hole] = e
			ix.slots[j] = 0
			hole = j
		}
		j = (j + 1) & ix.mask
	}
}

func (ix *Index) grow() {
	old := ix.slots
	n := uint64(len(old)) << 1
	ix.slots = make([]uint64, n)
	ix.mask = n - 1
	for _, e := range old {
		if e == 0 {
			continue
		}
		// re-probe each live entry into the larger table. Keys are in the log,
		// so rehash needs no side table and never double-counts: count is
		// unchanged because this moves entries, it does not add keys.
		tag := entryTag(e)
		i := hash64(ix.recKey(entryAddr(e))) & ix.mask
		for ix.slots[i] != 0 {
			i = (i + 1) & ix.mask
		}
		ix.slots[i] = makeEntry(tag, entryAddr(e))
	}
}

// Len returns the number of live keys.
func (ix *Index) Len() int { return ix.count }

// IndexBytes returns the resident size of the index table itself (the
// pointer-free []uint64), independent of the log. This is the number that
// matters for larger-than-memory: when values and keys have spilled to disk,
// this is what stays in RAM.
func (ix *Index) IndexBytes() int { return len(ix.slots) * 8 }

// LogBytes returns the size of the log arena (keys + values + headers).
func (ix *Index) LogBytes() int { return len(ix.log) }

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	return string(a) == string(b)
}

// The tag compare in Get is a scalar probe. On amd64 the home bucket can be
// scanned with a single SIMD compare over a cache-line of tags (the
// Swiss-table control-word technique from doc 01); that is a later slice, not
// this spike.
