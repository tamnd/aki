// Package f2raw is a minimal, from-scratch point store: one lock-free hash index
// over one in-memory hybrid log, and nothing else. It exists to measure the raw
// GET/SET/INCR ceiling on this hardware before any collection, tiering, or
// keyspace machinery layers above it can drag the number down.
//
// It is a deliberate return to first principles. The f1raw engine that preceded
// it accreted intra-key set partitioning, an ordered index, dense member vectors,
// per-key descriptor caches, a segmented arena, epoch reclamation, and a
// background cold-tier migrator. Each was defensible on its own, but together they
// dragged base GET/SET off the pace. f2raw keeps only the two pieces the FASTER
// paper actually needs and measures those honestly.
//
// The design is exactly two pieces:
//
//  1. A lock-free hash index. One flat array of cache-line buckets. A bucket is
//     seven 64-bit index entries plus a one-word link to an overflow bucket, so it
//     fills a 64-byte line exactly. An entry packs a 16-bit tag (high hash bits, a
//     fast reject that avoids touching the arena) and a 48-bit log address. The
//     bucket plus its overflow chain is open-addressed: a probe scans every entry
//     and verifies the key in the arena on a tag hit. An insert publishes with one
//     compare-and-swap on an empty entry; a replace CAS-swaps the entry to a new
//     record; a delete CAS-zeroes the entry, and because a lookup scans the whole
//     bucket it tolerates the hole with no tombstone. No per-shard mutex, no global
//     mutex, on any path.
//
//  2. An in-memory hybrid log. One preallocated byte arena with an atomic
//     bump-allocated tail; overflow buckets are bump-allocated from the same arena,
//     so there is one allocator and nothing to size twice. A record is immutable
//     once published except its value cell, which a same-key writer rewrites in
//     place under the record's own seqlock (the low bit of a per-record version
//     word, held for a single memcpy, contended only by another writer to the very
//     same key). A reader copies the value between two reads of an even version and
//     retries only if that exact record was mid-update, so a reader never blocks
//     another reader or a reader of a different key. A value too large for the
//     record's reserved capacity appends a fresh record and CAS-swaps the index
//     entry to it, read-copy-update style.
//
// Memory is grow-only: the arena is sized up front and never freed, so a published
// log address is valid forever and reads need no epoch framework to be safe. A new
// key that finds the arena full is reported as ErrFull, not spilled.
//
// One honest caveat: the value memcpy is a benign data race by construction (two
// readers and a writer touch the same bytes; the seqlock version check makes it
// correct, but Go's race detector cannot model that). Every other access is to
// immutable data or through an atomic.
package f2raw

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"runtime"
	"strconv"
	"sync/atomic"
	"unsafe"
)

// ErrFull is returned by Set when the record arena has no room for a new record.
// Raising the arena bytes is the response.
var ErrFull = errors.New("f2raw: arena full")

// ErrIndexFull is returned when the resident index runs out of overflow buckets: a
// bucket chain filled and the never-reclaimed overflow region had no room for
// another bucket. Raising IndexBuckets is the response.
var ErrIndexFull = errors.New("f2raw: index full (raise IndexBuckets)")

// ErrTooBig is returned when a key or value exceeds the 64 KiB field width.
var ErrTooBig = errors.New("f2raw: key or value over 64 KiB")

// ErrNotInt is returned by Incr when the existing value is not a base-10 integer,
// matching Redis's "value is not an integer or out of range".
var ErrNotInt = errors.New("f2raw: value is not an integer or out of range")

const (
	slotsPerBucket = 7  // index entries per cache-line bucket (7*8 + 8-byte link = 64)
	addrBits       = 48 // log address width in an index entry
	addrMask       = (uint64(1) << addrBits) - 1
	tagShift       = addrBits

	maxKey = 0xffff
	maxVal = 0xffff

	// Record header, all offsets from the record's 8-byte-aligned start:
	//   [0:4)   ver   uint32  seqlock; low bit set means a writer holds the latch
	//   [4:8)   vlen  uint32  current value length (<=64 KiB), read under the seqlock
	//   [8:10)  klen  uint16  key length, immutable
	//   [10:12) vcap  uint16  reserved value capacity in 8-byte words, immutable
	//   [12:16) pad
	//   [16:16+klen)              key bytes, immutable
	//   [16+align8(klen): +vcap*8) value bytes, rewritten in place under the seqlock
	hdrSize = 16
	offVer  = 0
	offVlen = 4
	offKlen = 8
	offVcap = 10

	verLockBit = 1 // low bit of ver: a writer is mid-update

	bucketSize = 64 // bytes; must equal unsafe.Sizeof(bucket{})
)

// bucket is one cache line: seven index entries and a link to an arena-allocated
// overflow bucket. An entry word is tag<<48 | addr; a zero word is an empty slot.
// addr is a byte offset into the arena, never zero for a live record because the
// allocator reserves offset 0. link is the arena offset of the next bucket in this
// bucket's overflow chain, or zero for none.
type bucket struct {
	slots [slotsPerBucket]atomic.Uint64
	link  atomic.Uint64
}

// Store is a lock-free hash-over-log point store for byte-string keys and values.
// The zero value is not usable; build one with New. It is safe for any number of
// concurrent readers and writers.
type Store struct {
	buckets []bucket
	mask    uint64 // len(buckets)-1, len is a power of two
	arena   []byte
	base    unsafe.Pointer // &arena[0], 8-aligned
	cap     uint64         // len(arena)
	tail    atomic.Uint64  // bump allocator; starts at 8 so offset 0 is reserved
	count   atomic.Int64   // live key count
}

// New builds a store with indexBuckets index buckets (rounded up to a power of
// two) and an arena of arenaBytes. Both grow-only: the index never rehashes and
// the arena never frees.
func New(indexBuckets, arenaBytes int) *Store {
	n := 1
	for n < indexBuckets {
		n <<= 1
	}
	if arenaBytes < 64 {
		arenaBytes = 64
	}
	s := &Store{
		buckets: make([]bucket, n),
		mask:    uint64(n - 1),
		arena:   make([]byte, arenaBytes),
		cap:     uint64(arenaBytes),
	}
	s.base = unsafe.Pointer(&s.arena[0])
	if uintptr(s.base)%8 != 0 {
		panic("f2raw: arena base not 8-aligned")
	}
	s.tail.Store(8) // reserve offset 0 so an empty index entry (addr 0) is unambiguous
	return s
}

// Len reports the live key count, an O(1) read of the counter. It is what DBSIZE
// returns.
func (s *Store) Len() int { return int(s.count.Load()) }

// BucketCount reports the number of primary index buckets, fixed at construction.
func (s *Store) BucketCount() int { return len(s.buckets) }

// ArenaBytes reports the arena's used/total split. used is the bytes the bump
// allocator has handed out (record headers, keys, inline values, overflow
// buckets); total is the fixed arena size.
func (s *Store) ArenaBytes() (used, total uint64) {
	t := s.tail.Load()
	if t < 8 {
		t = 8
	}
	if t > s.cap {
		t = s.cap
	}
	return t - 8, s.cap
}

// Close releases external resources. A pure in-memory store has none.
func (s *Store) Close() error { return nil }

// hash is the wyhash-style word-at-a-time mix: it reads eight key bytes per step
// and finishes a short key in one or two multiplies, so the table probe is not
// gated on a long scalar hash.
func hash(b []byte) uint64 {
	const (
		s0 = 0xa0761d6478bd642f
		s1 = 0xe7037ed1a0b428db
		s2 = 0x8ebc6af09c88c6e3
	)
	h := s0 ^ uint64(len(b))
	for len(b) >= 8 {
		h = mulFold(h^binary.LittleEndian.Uint64(b), s1)
		b = b[8:]
	}
	if len(b) > 0 {
		var t uint64
		for i := 0; i < len(b); i++ {
			t |= uint64(b[i]) << (8 * uint(i))
		}
		h = mulFold(h^t, s1)
	}
	return mulFold(h, s2)
}

func mulFold(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

// tagOf takes the high bits of the hash for the 16-bit entry tag. The |1 keeps it
// non-zero so it never collides with the empty-slot sentinel.
func tagOf(h uint64) uint64 { return (h >> 48) | 1 }

func align8(n uint64) uint64 { return (n + 7) &^ 7 }

func recSize(klen, vlen int) uint64 {
	return hdrSize + align8(uint64(klen)) + align8(uint64(vlen))
}

// alloc bump-allocates an 8-byte-aligned block of nbytes and returns its offset.
// The tail moves with one atomic add; ok is false when the arena is full.
func (s *Store) alloc(nbytes uint64) (uint64, bool) {
	n := align8(nbytes)
	end := s.tail.Add(n)
	if end > s.cap {
		return 0, false
	}
	return end - n, true
}

// verAt returns the atomic seqlock word for the record at off.
func (s *Store) verAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(s.base, off+offVer))
}

// vlenAt returns the atomic value-length word for the record at off. It is written
// under the latch and read under the seqlock, so a reader never pairs a length
// from one update with bytes from another.
func (s *Store) vlenAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(s.base, off+offVlen))
}

// klen and vcapBytes read immutable header fields with plain loads. They are
// written once before the record is published and never touched again; the
// publishing CAS establishes the happens-before a reader needs.
func (s *Store) klen(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena[off+offKlen:]))
}
func (s *Store) vcapBytes(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena[off+offVcap:])) * 8
}

// recordMatches reports whether the record at off carries this key. It reads only
// immutable fields (key length and key bytes), so it is safe to call on any live
// record concurrently with writers to that record's value.
func (s *Store) recordMatches(off uint64, key []byte) bool {
	if s.klen(off) != uint64(len(key)) {
		return false
	}
	start := off + hdrSize
	return string(s.arena[start:start+uint64(len(key))]) == string(key)
}

// bucketAt reinterprets an arena offset as an overflow bucket. Overflow buckets
// live in the arena, 8-byte aligned by the allocator, so their atomic words are
// valid.
func (s *Store) bucketAt(off uint64) *bucket {
	return (*bucket)(unsafe.Add(s.base, off))
}

// nextBucket returns the next bucket in b's overflow chain. With create set it
// allocates and links one when the chain ends; a lost CAS race wastes the freshly
// allocated bucket (harmless in a grow-only arena) and follows the winner.
func (s *Store) nextBucket(b *bucket, create bool) *bucket {
	if o := b.link.Load(); o != 0 {
		return s.bucketAt(o)
	}
	if !create {
		return nil
	}
	off, ok := s.alloc(bucketSize)
	if !ok {
		return nil
	}
	// Freshly allocated arena bytes are zero (make + a tail that never rewinds), so
	// the new bucket reads as empty slots and a nil link with no explicit clear.
	if b.link.CompareAndSwap(0, off) {
		return s.bucketAt(off)
	}
	return s.bucketAt(b.link.Load())
}

// spinsBeforeYield is how many times a contended seqlock spin busy-loops before it
// yields the P. The common case, an uncontended latch, wins on the first attempt.
const spinsBeforeYield = 24

// spinWait backs off one iteration of a seqlock spin loop: it busy-spins for the
// first spinsBeforeYield turns, then yields so a spinner never starves the latch
// holder on an oversubscribed machine. The uncontended path never reaches here.
func spinWait(spins int) int {
	if spins >= spinsBeforeYield {
		runtime.Gosched()
		return 0
	}
	return spins + 1
}

// find walks the bucket and its overflow chain for key, returning the matching
// record offset and the entry that points at it (bucket plus slot plus the
// observed word) so Delete and the replace path can CAS that exact entry.
func (s *Store) find(key []byte, h uint64) (off uint64, b *bucket, slot int, word uint64, found bool) {
	tag := tagOf(h)
	b = &s.buckets[h&s.mask]
	for b != nil {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i].Load()
			if w == 0 || w>>tagShift != tag {
				continue
			}
			a := w & addrMask
			if s.recordMatches(a, key) {
				return a, b, i, w, true
			}
		}
		b = s.nextBucket(b, false)
	}
	return 0, nil, 0, 0, false
}

// Get copies the value for key into dst (reusing its capacity) and reports whether
// the key is present. The read is lock-free: it loads the index entry, follows it
// into the arena, and copies the value out under the record's seqlock, retrying
// only if a writer is updating that exact record.
func (s *Store) Get(key, dst []byte) ([]byte, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h)
	if !found {
		return dst[:0], false
	}
	return s.readValue(off, dst), true
}

// readValue is the seqlock read. It spins while a writer holds the latch (odd
// version), copies the value under an even version, and retries if the version
// moved during the copy.
func (s *Store) readValue(off uint64, dst []byte) []byte {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	spins := 0
	for {
		v1 := verp.Load()
		if v1&verLockBit != 0 {
			spins = spinWait(spins)
			continue
		}
		n := uint64(s.vlenAt(off).Load())
		dst = append(dst[:0], s.arena[vbase:vbase+n]...)
		if verp.Load() == v1 {
			return dst
		}
		spins = spinWait(spins)
	}
}

// Set stores val under key, blind-upsert semantics. An existing key whose record
// has reserved room for val is updated in place under the per-record latch; a new
// key, or a value larger than the record reserved, appends a fresh record and
// publishes it. Both paths are lock-free across distinct keys.
func (s *Store) Set(key, val []byte) error {
	if len(key) == 0 {
		return errors.New("f2raw: empty key")
	}
	if len(key) > maxKey || len(val) > maxVal {
		return ErrTooBig
	}
	h := hash(key)
	if off, _, _, _, found := s.find(key, h); found && uint64(len(val)) <= s.vcapBytes(off) {
		s.inPlace(off, val)
		return nil
	}
	return s.publish(key, val, h)
}

// inPlace rewrites a record's value under its seqlock. The latch is the low bit of
// the version word, taken with a CAS so two writers to the same key serialize; it
// is held only for the value copy and the length store. Readers see the version go
// odd then even and retry across the window, so they never observe a torn value.
func (s *Store) inPlace(off uint64, val []byte) {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	spins := 0
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			spins = spinWait(spins)
			continue
		}
		if verp.CompareAndSwap(v, v+1) { // acquire: make it odd
			copy(s.arena[vbase:vbase+uint64(len(val))], val)
			s.vlenAt(off).Store(uint32(len(val)))
			verp.Store(v + 2) // release: back to even, one tick newer
			return
		}
		spins = spinWait(spins)
	}
}

// initRecord lays down a fresh record. The writes are plain because the record is
// private until publish CAS-installs its address into an index entry; that release
// pairs with the acquire a reader does when it loads the entry.
func (s *Store) initRecord(off uint64, key, val []byte) {
	binary.LittleEndian.PutUint32(s.arena[off+offVer:], 0)
	binary.LittleEndian.PutUint32(s.arena[off+offVlen:], uint32(len(val)))
	binary.LittleEndian.PutUint16(s.arena[off+offKlen:], uint16(len(key)))
	binary.LittleEndian.PutUint16(s.arena[off+offVcap:], uint16(align8(uint64(len(val)))/8))
	copy(s.arena[off+hdrSize:], key)
	copy(s.arena[off+hdrSize+align8(uint64(len(key))):], val)
}

// publish appends a new record and links it into the index. It allocates once,
// then scans the bucket chain in a loop: an existing live record of this key means
// a concurrent writer beat us, so update in place if it fits or CAS-swap the entry
// to our record; otherwise fill the first empty slot (a new key, count up);
// otherwise grow the chain with an overflow bucket and rescan.
func (s *Store) publish(key, val []byte, h uint64) error {
	off, ok := s.alloc(recSize(len(key), len(val)))
	if !ok {
		return ErrFull
	}
	s.initRecord(off, key, val)
	tag := tagOf(h)
	newWord := tag<<tagShift | off

outer:
	for {
		b := &s.buckets[h&s.mask]
		var emptyB *bucket
		emptySlot := -1
		var last *bucket
		for b != nil {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					if emptySlot < 0 {
						emptyB, emptySlot = b, i
					}
					continue
				}
				if w>>tagShift != tag {
					continue
				}
				a := w & addrMask
				if s.recordMatches(a, key) {
					// Another writer published this key first. Last writer wins:
					// update in place if our value fits, else swap the entry to ours.
					if uint64(len(val)) <= s.vcapBytes(a) {
						s.inPlace(a, val)
						return nil
					}
					if b.slots[i].CompareAndSwap(w, newWord) {
						return nil // replace: count unchanged
					}
					continue outer // entry changed under us; rescan
				}
			}
			last = b
			b = s.nextBucket(b, false)
		}
		if emptySlot >= 0 {
			if emptyB.slots[emptySlot].CompareAndSwap(0, newWord) {
				s.count.Add(1) // a genuinely new key
				return nil
			}
			continue // slot filled under us; rescan
		}
		if s.nextBucket(last, true) == nil {
			return ErrIndexFull
		}
	}
}

// Delete removes key and reports whether it was present. It CAS-zeroes the index
// entry that points at the key's record; because a lookup scans the whole bucket
// chain, the emptied slot is just a hole and needs no tombstone.
func (s *Store) Delete(key []byte) bool {
	h := hash(key)
	for {
		_, b, slot, word, found := s.find(key, h)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			s.count.Add(-1)
			return true
		}
	}
}

// Incr adds delta to the integer value at key and returns the new value. A missing
// key is created at delta, matching Redis INCR/DECR/INCRBY semantics. The whole
// read-modify-write is atomic against other Incr calls on the same key.
func (s *Store) Incr(key []byte, delta int64) (int64, error) {
	if len(key) == 0 {
		return 0, errors.New("f2raw: empty key")
	}
	h := hash(key)
	var buf [20]byte
	for {
		off, _, _, _, found := s.find(key, h)
		if found {
			n, ok := s.incrInPlace(off, delta)
			if !ok {
				return 0, ErrNotInt
			}
			if n != incrNeedsGrow {
				return n, nil
			}
			// The formatted result outgrew the record: read, compute, republish a
			// wider record under last-writer-wins. A lost race restarts the loop.
			cur, perr := s.readInt(off)
			if perr != nil {
				return 0, ErrNotInt
			}
			n2, oerr := addChecked(cur, delta)
			if oerr != nil {
				return 0, oerr
			}
			b := strconv.AppendInt(buf[:0], n2, 10)
			if err := s.publish(key, b, h); err != nil {
				return 0, err
			}
			return n2, nil
		}
		// Absent: install delta as a new record, losing gracefully to a concurrent
		// creator so the next loop iteration finds it and adds onto it.
		b := strconv.AppendInt(buf[:0], delta, 10)
		installed, existed, ierr := s.insertAbsent(key, b, h)
		if installed {
			return delta, nil
		}
		if existed {
			continue
		}
		return 0, ierr
	}
}

// incrNeedsGrow signals the formatted result no longer fits the record's reserved
// capacity, so the caller must republish a wider record. It is chosen far from any
// real counter and never escapes.
const incrNeedsGrow = int64(-1) << 62

// incrInPlace latches the record, parses its value as an integer, adds delta, and
// writes the result back in place when it fits. It returns ok=false on a
// non-integer value, and incrNeedsGrow when the result is valid but too wide.
func (s *Store) incrInPlace(off uint64, delta int64) (int64, bool) {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	spins := 0
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			spins = spinWait(spins)
			continue
		}
		if !verp.CompareAndSwap(v, v+1) {
			spins = spinWait(spins)
			continue
		}
		n := uint64(s.vlenAt(off).Load())
		cur, perr := parseInt(s.arena[vbase : vbase+n])
		if perr != nil {
			verp.Store(v + 2)
			return 0, false
		}
		res, oerr := addChecked(cur, delta)
		if oerr != nil {
			verp.Store(v + 2)
			return 0, false
		}
		var buf [20]byte
		b := strconv.AppendInt(buf[:0], res, 10)
		if uint64(len(b)) > s.vcapBytes(off) {
			verp.Store(v + 2)
			return incrNeedsGrow, true
		}
		copy(s.arena[vbase:vbase+uint64(len(b))], b)
		s.vlenAt(off).Store(uint32(len(b)))
		verp.Store(v + 2)
		return res, true
	}
}

// readInt reads a record's value as an integer outside the latch, used only on the
// rare grow path where the value is about to be republished anyway.
func (s *Store) readInt(off uint64) (int64, error) {
	vbase := off + hdrSize + align8(s.klen(off))
	n := uint64(s.vlenAt(off).Load())
	return parseInt(s.arena[vbase : vbase+n])
}

// insertAbsent publishes val under key only if the key is absent. It returns
// installed=true when this call created the record, existed=true when a concurrent
// writer already holds the key, and both false with a non-nil err otherwise.
func (s *Store) insertAbsent(key, val []byte, h uint64) (installed, existed bool, err error) {
	off, ok := s.alloc(recSize(len(key), len(val)))
	if !ok {
		return false, false, ErrFull
	}
	s.initRecord(off, key, val)
	tag := tagOf(h)
	newWord := tag<<tagShift | off
	for {
		b := &s.buckets[h&s.mask]
		var emptyB *bucket
		emptySlot := -1
		var last *bucket
		for b != nil {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					if emptySlot < 0 {
						emptyB, emptySlot = b, i
					}
					continue
				}
				if w>>tagShift != tag {
					continue
				}
				if s.recordMatches(w&addrMask, key) {
					return false, true, nil // key already present
				}
			}
			last = b
			b = s.nextBucket(b, false)
		}
		if emptySlot >= 0 {
			if emptyB.slots[emptySlot].CompareAndSwap(0, newWord) {
				s.count.Add(1)
				return true, false, nil
			}
			continue
		}
		if s.nextBucket(last, true) == nil {
			return false, false, ErrIndexFull
		}
	}
}

// Each calls fn for every live key with its value. It is a single-threaded
// convenience for tests and is not safe against concurrent writers.
func (s *Store) Each(fn func(key, val []byte) bool) {
	dst := make([]byte, 0, 64)
	for bi := range s.buckets {
		b := &s.buckets[bi]
		for b != nil {
			for i := 0; i < slotsPerBucket; i++ {
				w := b.slots[i].Load()
				if w == 0 {
					continue
				}
				off := w & addrMask
				klen := s.klen(off)
				key := s.arena[off+hdrSize : off+hdrSize+klen]
				dst = s.readValue(off, dst)
				if !fn(key, dst) {
					return
				}
			}
			b = s.nextBucket(b, false)
		}
	}
}

// parseInt parses a base-10 signed integer with no leading or trailing slack, the
// same strictness Redis applies before it will INCR a value.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, ErrNotInt
	}
	return strconv.ParseInt(string(b), 10, 64)
}

// addChecked adds delta to n and reports overflow, so Incr can return Redis's
// out-of-range error instead of wrapping.
func addChecked(n, delta int64) (int64, error) {
	r := n + delta
	if (delta > 0 && r < n) || (delta < 0 && r > n) {
		return 0, ErrNotInt
	}
	return r, nil
}

// assert the bucket is exactly one cache line at build time.
var _ = [1]struct{}{}[bucketSize-unsafe.Sizeof(bucket{})]
