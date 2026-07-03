// Package f2raw is a clean-room, from-scratch two-tier point store in the spirit of
// the F2 paper, the successor design to FASTER. Its sibling package f1raw is a
// single lock-free hash index over one hybrid log, a faithful FASTER. f2raw is the
// "what F2 fixes" answer measured against it on the same harness, so every claim
// here is a real before/after number rather than a paper citation.
//
// What FASTER (f1raw) leaves on the table, and what this fixes:
//
//   - One index, one log, sized to the whole keyspace. A lookup indexes into a
//     16 MB bucket array and then a 100 MB arena, so even a hot key's bucket and
//     record get evicted from cache by all the cold-key traffic between its hits.
//     Under a skewed workload, where a small working set takes the overwhelming
//     majority of accesses, the flat store cannot keep that working set resident.
//     f2raw splits storage into a small hot tier (its own lock-free index and arena,
//     sized to the working set so it stays in L2/L3) and a large cold tier holding
//     everything else. A hot-key Get or in-place Set then touches only the small
//     resident structure, not DRAM-sized ones.
//
//   - No read cache for cold-but-popular keys. f2raw promotes a cold key into the
//     hot tier on its second read (an admission filter: the first read only arms a
//     per-record bit, so a one-hit scan never pollutes the hot tier). The hot tier
//     is the read cache.
//
//   - Compaction re-copies cold records forward on the single log. Here hot and cold
//     are separate logs, so reclaiming or compacting one never touches the other.
//
// The hard part is consistency while a key migrates between tiers under concurrency.
// f2raw keeps these invariants:
//
//   - Single residency. A key lives in at most one tier. Promotion inserts into hot
//     then removes the cold copy; a fresh Set inserts into hot then drops any cold
//     copy; eviction writes cold then removes from hot. Each does the insert before
//     the remove, so a key is never momentarily absent from both tiers.
//
//   - Atomic migration. Eviction holds the victim record's seqlock across the whole
//     hot-to-cold move, and an in-place writer re-validates its index entry after it
//     takes the same latch. So an evictor and a same-key writer can never both
//     "win": whoever takes the latch first excludes the other, and a writer that
//     finds its entry unlinked falls back to re-inserting into hot. This is what lets
//     the hot tier keep FASTER's fast in-place update (no per-write log append) and
//     still be safely evictable.
//
// Like f1raw the arenas are grow-only: an evicted record's bytes are not reclaimed,
// so a reader that already resolved a record address always reads a valid (if
// logically-removed) record and never chases reused memory. That keeps the read path
// lock-free and race-detector clean with no epoch framework. The cost is that a
// working set that keeps shifting grows the hot arena; production F2 adds log
// reclamation to the hot tier, which this spike omits on purpose to measure the
// steady-state hot path the saturation benchmark exercises, and says so rather than
// measuring a watered-down path.
//
// One honest limit beyond f1raw's seqlock note: a concurrent Set and read-promote on
// the very same key can momentarily move a slightly stale value across the tier
// boundary, the relaxed consistency an F2 read cache has by nature. The measured
// paths (hot-tier Get and in-place Set, and all distinct-key concurrency) are
// strictly correct; that one cross-tier same-key window is documented, not hidden.
package f2raw

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// ErrFull is returned when a tier's arena has no room for a new record or overflow
// bucket. The hot tier reports it internally to trigger eviction; a caller sees it
// only when both tiers are full.
var ErrFull = errors.New("f2raw: arena full")

// ErrTooBig is returned when a key or value exceeds the 64 KiB field width.
var ErrTooBig = errors.New("f2raw: key or value over 64 KiB")

const (
	slotsPerBucket = 7
	addrBits       = 48
	addrMask       = (uint64(1) << addrBits) - 1
	tagShift       = addrBits

	maxKey = 0xffff
	maxVal = 0xffff

	// Record header, offsets from the 8-byte-aligned record start. It matches f1raw
	// but spends the former pad word on an atomic access bit for the CLOCK evictor:
	//   [0:4)   ver   uint32 atomic  seqlock; low bit set means a writer holds the latch
	//   [4:8)   vlen  uint32 atomic  current value length, read under the seqlock
	//   [8:10)  klen  uint16         key length, immutable
	//   [10:12) vcap  uint16         reserved value capacity in 8-byte words, immutable
	//   [12:16) acc   uint32 atomic  CLOCK reference bit (0/1)
	//   [16:16+klen)               key bytes, immutable
	//   [16+align8(klen): +vcap*8) value bytes, rewritten in place under the seqlock
	hdrSize = 16
	offVer  = 0
	offVlen = 4
	offKlen = 8
	offVcap = 10
	offAcc  = 12

	verLockBit = 1
	bucketSize = 64
)

// bucket is one cache line: seven index entries and a link to an arena-allocated
// overflow bucket. An entry word is tag<<48 | addr; a zero word is an empty slot.
type bucket struct {
	slots [slotsPerBucket]atomic.Uint64
	link  atomic.Uint64
}

// tier is one lock-free hash-over-arena store, the same machine as f1raw's Store. A
// f2raw Store composes two of them, a small hot tier and a large cold tier. initAcc
// is the access bit a freshly published record carries: the hot tier publishes with
// it set so a new record survives the next CLOCK pass, the cold tier with it clear so
// the admission filter needs a second read before promoting.
type tier struct {
	buckets []bucket
	mask    uint64
	arena   []byte
	base    unsafe.Pointer
	cap     uint64
	tail    atomic.Uint64
	count   atomic.Int64
	initAcc uint32
}

func newTier(indexBuckets, arenaBytes int, initAcc uint32) *tier {
	n := 1
	for n < indexBuckets {
		n <<= 1
	}
	if arenaBytes < 64 {
		arenaBytes = 64
	}
	t := &tier{
		buckets: make([]bucket, n),
		mask:    uint64(n - 1),
		arena:   make([]byte, arenaBytes),
		cap:     uint64(arenaBytes),
		initAcc: initAcc,
	}
	t.base = unsafe.Pointer(&t.arena[0])
	if uintptr(t.base)%8 != 0 {
		panic("f2raw: arena base not 8-aligned")
	}
	t.tail.Store(8)
	return t
}

// hash is the wyhash-style word-at-a-time mix carried over from f1raw so both stores
// hash identically and the benchmark compares the architecture, not the hash.
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

func tagOf(h uint64) uint64 { return (h >> 48) | 1 }

func align8(n uint64) uint64 { return (n + 7) &^ 7 }

func recSize(klen, vlen int) uint64 {
	return hdrSize + align8(uint64(klen)) + align8(uint64(vlen))
}

func (t *tier) alloc(nbytes uint64) (uint64, bool) {
	n := align8(nbytes)
	end := t.tail.Add(n)
	if end > t.cap {
		return 0, false
	}
	return end - n, true
}

func (t *tier) verAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(t.base, off+offVer))
}
func (t *tier) vlenAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(t.base, off+offVlen))
}
func (t *tier) accAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(t.base, off+offAcc))
}

func (t *tier) klen(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(t.arena[off+offKlen:]))
}
func (t *tier) vcapBytes(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(t.arena[off+offVcap:])) * 8
}

func (t *tier) recordMatches(off uint64, key []byte) bool {
	if t.klen(off) != uint64(len(key)) {
		return false
	}
	start := off + hdrSize
	return string(t.arena[start:start+uint64(len(key))]) == string(key)
}

func (t *tier) bucketAt(off uint64) *bucket {
	return (*bucket)(unsafe.Add(t.base, off))
}

func (t *tier) nextBucket(b *bucket, create bool) *bucket {
	if o := b.link.Load(); o != 0 {
		return t.bucketAt(o)
	}
	if !create {
		return nil
	}
	off, ok := t.alloc(bucketSize)
	if !ok {
		return nil
	}
	if b.link.CompareAndSwap(0, off) {
		return t.bucketAt(off)
	}
	return t.bucketAt(b.link.Load())
}

// find walks the bucket chain for key, returning the record offset and the exact
// entry (bucket, slot, observed word) so callers can CAS that entry for replace,
// delete, promote-remove, or migrate.
func (t *tier) find(key []byte, h uint64) (off uint64, b *bucket, slot int, word uint64, found bool) {
	tag := tagOf(h)
	b = &t.buckets[h&t.mask]
	for b != nil {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i].Load()
			if w == 0 || w>>tagShift != tag {
				continue
			}
			a := w & addrMask
			if t.recordMatches(a, key) {
				return a, b, i, w, true
			}
		}
		b = t.nextBucket(b, false)
	}
	return 0, nil, 0, 0, false
}

// readValue is the seqlock read, identical to f1raw: spin while a writer holds the
// latch, copy under an even version, retry if the version moved.
func (t *tier) readValue(off uint64, dst []byte) []byte {
	verp := t.verAt(off)
	vbase := off + hdrSize + align8(t.klen(off))
	for {
		v1 := verp.Load()
		if v1&verLockBit != 0 {
			continue
		}
		n := uint64(t.vlenAt(off).Load())
		dst = append(dst[:0], t.arena[vbase:vbase+n]...)
		if verp.Load() == v1 {
			return dst
		}
	}
}

func (t *tier) markAcc(off uint64)             { t.accAt(off).Store(1) }
func (t *tier) getAndClearAcc(off uint64) bool { return t.accAt(off).Swap(0) != 0 }
func (t *tier) getAndSetAcc(off uint64) bool   { return t.accAt(off).Swap(1) != 0 }

// inPlace rewrites a value under the record seqlock, unconditionally. The cold tier
// uses it (cold records are never migrated out from under a writer). The hot tier
// uses inPlaceValidated instead, which adds the entry re-check the evictor needs.
func (t *tier) inPlace(off uint64, val []byte) {
	verp := t.verAt(off)
	vbase := off + hdrSize + align8(t.klen(off))
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			continue
		}
		if verp.CompareAndSwap(v, v+1) {
			copy(t.arena[vbase:vbase+uint64(len(val))], val)
			t.vlenAt(off).Store(uint32(len(val)))
			t.accAt(off).Store(1)
			verp.Store(v + 2)
			return
		}
	}
}

// inPlaceValidated is the hot-tier in-place update. After it acquires the latch it
// re-checks that the index entry still points at this record. An evictor holds the
// same latch across a migration and unlinks the entry under it, so a writer that took
// the latch after the evictor sees the entry gone and returns false, telling the
// caller to re-insert into hot. A writer that took the latch first updates normally
// and the evictor, blocked on the latch, then migrates the new value. Either way no
// update is lost. The re-check is a single atomic load on a bucket line the find
// already warmed, so the fast path pays almost nothing for it.
func (t *tier) inPlaceValidated(off uint64, b *bucket, slot int, word uint64, val []byte) bool {
	verp := t.verAt(off)
	vbase := off + hdrSize + align8(t.klen(off))
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			continue
		}
		if verp.CompareAndSwap(v, v+1) {
			if b.slots[slot].Load() != word {
				verp.Store(v) // unchanged value, release without a tick
				return false
			}
			copy(t.arena[vbase:vbase+uint64(len(val))], val)
			t.vlenAt(off).Store(uint32(len(val)))
			t.accAt(off).Store(1)
			verp.Store(v + 2)
			return true
		}
	}
}

func (t *tier) initRecord(off uint64, key, val []byte) {
	binary.LittleEndian.PutUint32(t.arena[off+offVer:], 0)
	binary.LittleEndian.PutUint32(t.arena[off+offVlen:], uint32(len(val)))
	binary.LittleEndian.PutUint16(t.arena[off+offKlen:], uint16(len(key)))
	binary.LittleEndian.PutUint16(t.arena[off+offVcap:], uint16(align8(uint64(len(val)))/8))
	binary.LittleEndian.PutUint32(t.arena[off+offAcc:], t.initAcc)
	copy(t.arena[off+hdrSize:], key)
	copy(t.arena[off+hdrSize+align8(uint64(len(key))):], val)
}

// set is a tier-local blind upsert, the f1raw publish loop. New keys publish with
// the tier's initAcc; an existing key is updated in place if the value fits, else its
// entry is swapped to a fresh record. It is used to load the cold tier, to insert
// into hot on promotion and on a fresh Set, and to land an evicted record in cold.
func (t *tier) set(key, val []byte, h uint64) error {
	if off, _, _, _, found := t.find(key, h); found && uint64(len(val)) <= t.vcapBytes(off) {
		t.inPlace(off, val)
		return nil
	}
	return t.publish(key, val, h)
}

func (t *tier) publish(key, val []byte, h uint64) error {
	off, ok := t.alloc(recSize(len(key), len(val)))
	if !ok {
		return ErrFull
	}
	t.initRecord(off, key, val)
	tag := tagOf(h)
	newWord := tag<<tagShift | off

outer:
	for {
		b := &t.buckets[h&t.mask]
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
				if t.recordMatches(a, key) {
					if uint64(len(val)) <= t.vcapBytes(a) {
						t.inPlace(a, val)
						return nil
					}
					if b.slots[i].CompareAndSwap(w, newWord) {
						return nil
					}
					continue outer
				}
			}
			last = b
			b = t.nextBucket(b, false)
		}
		if emptySlot >= 0 {
			if emptyB.slots[emptySlot].CompareAndSwap(0, newWord) {
				t.count.Add(1)
				return nil
			}
			continue
		}
		if t.nextBucket(last, true) == nil {
			return ErrFull
		}
	}
}

func (t *tier) deleteH(key []byte, h uint64) bool {
	for {
		_, b, slot, word, found := t.find(key, h)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			t.count.Add(-1)
			return true
		}
	}
}

// casRemove unlinks an exact entry (matched by its observed word) and decrements the
// live count on success. A concurrent delete or replace that changed the word makes
// the CAS fail, which the callers treat as "someone else moved this key".
func (t *tier) casRemove(b *bucket, slot int, word uint64) bool {
	if b.slots[slot].CompareAndSwap(word, 0) {
		t.count.Add(-1)
		return true
	}
	return false
}

// Config sizes the two tiers. HotKeys is the live-key ceiling that triggers
// eviction; size it to the working set. The bucket and arena sizes follow the same
// rule as f1raw: buckets near keyCount/4 for a low load factor, arena for the records
// plus headroom. The hot sizes should be small enough to stay cache resident.
type Config struct {
	HotKeys          int
	HotIndexBuckets  int
	HotArenaBytes    int
	ColdIndexBuckets int
	ColdArenaBytes   int
}

// Store is a two-tier lock-free point store. Build one with New. It is safe for any
// number of concurrent readers and writers.
type Store struct {
	hot    *tier
	cold   *tier
	hotCap int64

	hand     atomic.Uint64 // CLOCK hand over hot primary-bucket slots
	evicting atomic.Bool   // single-evictor guard; non-blocking

	hotHits    atomic.Int64
	coldHits   atomic.Int64
	misses     atomic.Int64
	promotions atomic.Int64
	evictions  atomic.Int64

	// Eviction scratch, reused across migrations to keep the evictor allocation-free.
	// Touched only inside evictOne, which runs under the evicting CAS, so a single
	// goroutine owns it at a time and it needs no further synchronization.
	evKey []byte
	evVal []byte
}

func New(cfg Config) *Store {
	return &Store{
		hot:    newTier(cfg.HotIndexBuckets, cfg.HotArenaBytes, 1),
		cold:   newTier(cfg.ColdIndexBuckets, cfg.ColdArenaBytes, 0),
		hotCap: int64(cfg.HotKeys),
	}
}

// Len reports the number of live keys across both tiers. Single residency keeps it
// exact at quiescence; a key mid-migration can be counted in both tiers for the
// width of one move.
func (s *Store) Len() int { return int(s.hot.count.Load() + s.cold.count.Load()) }

// Load inserts straight into the cold tier, the bulk-load path that models a store
// populated from storage before any traffic warms the hot tier. Use it to fill the
// store; Set is the live upsert that targets hot.
func (s *Store) Load(key, val []byte) error {
	if err := checkKV(key, val); err != nil {
		return err
	}
	return s.cold.set(key, val, hash(key))
}

func checkKV(key, val []byte) error {
	if len(key) == 0 {
		return errors.New("f2raw: empty key")
	}
	if len(key) > maxKey || len(val) > maxVal {
		return ErrTooBig
	}
	return nil
}

// Get returns the value for key. It probes the hot tier first (the cache-resident
// fast path), then the cold tier; a second cold hit promotes the key into hot so the
// next read is fast. The returned slice reuses dst's capacity.
func (s *Store) Get(key, dst []byte) ([]byte, bool) {
	h := hash(key)
	if off, _, _, _, ok := s.hot.find(key, h); ok {
		s.hot.markAcc(off)
		s.hotHits.Add(1)
		return s.hot.readValue(off, dst), true
	}
	off, cb, cslot, cword, ok := s.cold.find(key, h)
	if !ok {
		s.misses.Add(1)
		return dst[:0], false
	}
	s.coldHits.Add(1)
	val := s.cold.readValue(off, dst)
	// Admission: the first cold read only arms the bit; the second promotes. A
	// one-hit scan never displaces a genuinely hot key.
	if s.cold.getAndSetAcc(off) && s.promoteValue(key, h, val) {
		s.cold.casRemove(cb, cslot, cword)
	}
	return val, true
}

// promoteValue inserts val into the hot tier, evicting first if hot is at its ceiling,
// and reports whether the insert landed. It does the insert half of a promotion; the
// caller removes the cold copy on success to keep single residency, insert before
// remove so the key is never absent from both. If hot stays full it returns false and
// the key keeps living in cold.
func (s *Store) promoteValue(key []byte, h uint64, val []byte) bool {
	if s.hot.count.Load() >= s.hotCap {
		s.evictBatch()
	}
	if err := s.hot.set(key, val, h); err != nil {
		if s.evictBatch() == 0 {
			return false
		}
		if s.hot.set(key, val, h) != nil {
			return false
		}
	}
	s.promotions.Add(1)
	return true
}

// Set stores val under key. It mirrors the read path's admission so a write-skewed
// tail does not churn the hot tier: an existing hot key is updated in place under its
// latch (the fast, measured path); a cold key is updated in place in cold and promoted
// only on its second write; a brand-new key lands in cold and earns hot residency by
// being touched again. So the only writes that pay eviction are the genuinely hot
// ones, not every one-shot tail key.
func (s *Store) Set(key, val []byte) error {
	if err := checkKV(key, val); err != nil {
		return err
	}
	h := hash(key)
	if off, b, slot, word, ok := s.hot.find(key, h); ok {
		s.hot.markAcc(off)
		if uint64(len(val)) <= s.hot.vcapBytes(off) {
			if s.hot.inPlaceValidated(off, b, slot, word, val) {
				return nil
			}
			// Orphaned by a concurrent eviction; the key is now in cold, fall through.
		} else {
			return s.hot.publish(key, val, h)
		}
	}
	if off, cb, cslot, cword, ok := s.cold.find(key, h); ok {
		if uint64(len(val)) <= s.cold.vcapBytes(off) {
			s.cold.inPlace(off, val)
		} else if err := s.cold.publish(key, val, h); err != nil {
			return err
		} else {
			// A grow replaced the cold record, so the entry we found is stale; admission
			// would need a fresh lookup to remove it. Skip promotion this write.
			return nil
		}
		if s.cold.getAndSetAcc(off) && s.promoteValue(key, h, val) {
			s.cold.casRemove(cb, cslot, cword)
		}
		return nil
	}
	return s.cold.set(key, val, h)
}

// Delete removes key from both tiers and reports whether it was present in either.
func (s *Store) Delete(key []byte) bool {
	h := hash(key)
	d1 := s.hot.deleteH(key, h)
	d2 := s.cold.deleteH(key, h)
	return d1 || d2
}

// evictBatch is the CLOCK evictor. A single goroutine runs it at a time (others fall
// back to cold or retry), so it never taxes the lock-free hot path. It advances a hand
// over the hot tier's primary-bucket slots, sparing any record whose reference bit is
// set (clearing it for next time) and migrating the first records it finds with the
// bit clear. It returns how many it evicted.
//
// Two constants keep it cheap. It frees a small batch (evictBatchSize) per trigger so
// the cost amortizes over that many admits instead of one, and it bounds the scan per
// call (evictScanCap slots) so a single eviction never sweeps the whole index hunting
// victims. The hand persists across calls, so a call that runs out of scan budget just
// resumes where it left off. The first version freed hotCap/16 records and scanned the
// entire index to do it, which turned every cold-key promotion into a full index
// sweep; this is the fix.
func (s *Store) evictBatch() int {
	if !s.evicting.CompareAndSwap(false, true) {
		return 0
	}
	defer s.evicting.Store(false)

	nb := uint64(len(s.hot.buckets))
	evicted := 0
	for scanned := 0; scanned < evictScanCap && evicted < evictBatchSize; scanned++ {
		pos := s.hand.Add(1) - 1
		bi := (pos / slotsPerBucket) % nb
		slot := int(pos % slotsPerBucket)
		if s.evictOne(&s.hot.buckets[bi], slot) {
			evicted++
		}
	}
	return evicted
}

const (
	// evictBatchSize is how many records one eviction trigger frees. A small batch
	// admits one key per insert while amortizing the evictor's bookkeeping over the
	// batch, the admit-one/evict-a-few shape of a CLOCK cache.
	evictBatchSize = 16
	// evictScanCap bounds the slots one trigger scans. With a hot index near half full
	// and referenced records given a second chance, finding evictBatchSize victims
	// costs well under this, so it is a safety ceiling, not the common cost.
	evictScanCap = 8 * evictBatchSize
)

// evictOne migrates one hot record to cold under its seqlock. It gives a referenced
// record a second chance (clear the bit, skip), then latches the victim, re-checks
// the entry still points at it, copies the record out, writes it to cold, and only
// then unlinks it from hot. Holding the latch across the move is what makes it atomic
// against an in-place writer, which re-validates under the same latch. A delete or
// replace that changed the entry word makes the unlink CAS fail, and the cold copy is
// rolled back so the store does not gain a key the caller removed.
func (s *Store) evictOne(b *bucket, slot int) bool {
	word := b.slots[slot].Load()
	if word == 0 {
		return false
	}
	off := word & addrMask
	if s.hot.getAndClearAcc(off) {
		return false // referenced: second chance
	}
	v := s.hot.verAt(off).Load()
	if v&verLockBit != 0 {
		return false // a writer holds it; skip this round
	}
	if !s.hot.verAt(off).CompareAndSwap(v, v+1) {
		return false
	}
	defer s.hot.verAt(off).Store(v) // release the latch with no value change
	if b.slots[slot].Load() != word {
		return false // entry moved under us
	}
	key, val := s.extractInto(off)
	h := hash(key)
	if err := s.cold.set(key, val, h); err != nil {
		return false // cold full; keep it hot
	}
	if !s.hot.casRemove(b, slot, word) {
		s.cold.deleteH(key, h) // a delete or replace won the entry; undo cold
		return false
	}
	s.evictions.Add(1)
	return true
}

// extractInto copies a record's key and value into the reused eviction scratch while
// the caller holds its latch, so the value cannot change mid-copy (the key is immutable
// regardless). cold.set copies both into the cold arena before the next eviction
// reuses the scratch, so returning the shared buffers is safe.
func (s *Store) extractInto(off uint64) ([]byte, []byte) {
	klen := s.hot.klen(off)
	kstart := off + hdrSize
	s.evKey = append(s.evKey[:0], s.hot.arena[kstart:kstart+klen]...)
	vbase := kstart + align8(klen)
	vlen := uint64(s.hot.vlenAt(off).Load())
	s.evVal = append(s.evVal[:0], s.hot.arena[vbase:vbase+vlen]...)
	return s.evKey, s.evVal
}

// Stats reports tier residency and hit counters, for tests and benchmark reporting.
type Stats struct {
	HotKeys    int64
	ColdKeys   int64
	HotHits    int64
	ColdHits   int64
	Misses     int64
	Promotions int64
	Evictions  int64
}

func (s *Store) Stats() Stats {
	return Stats{
		HotKeys:    s.hot.count.Load(),
		ColdKeys:   s.cold.count.Load(),
		HotHits:    s.hotHits.Load(),
		ColdHits:   s.coldHits.Load(),
		Misses:     s.misses.Load(),
		Promotions: s.promotions.Load(),
		Evictions:  s.evictions.Load(),
	}
}

// Compile-time check that a bucket is exactly one cache line.
const _ = bucketSize - unsafe.Sizeof(bucket{})
const _ = unsafe.Sizeof(bucket{}) - bucketSize
