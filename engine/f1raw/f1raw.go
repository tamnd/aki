// Package f1raw is a clean-room, from-scratch implementation of the FASTER point
// store: a single lock-free hash index over one in-memory hybrid log. It is the
// "F1" baseline in the engine family, written to measure the raw lock-free ceiling
// with none of the keyspace, header, write-behind, or value-cache machinery the
// integrated engine carries. It answers one question from first principles: how
// fast is a truly lock-free hash-over-log point store on this hardware, before any
// architecture decision layered above it can drag the number down.
//
// It is single-tier on purpose. FASTER mixes hot and cold records on one log and
// reclaims the cold region with log compaction that re-copies live records forward,
// which couples a small working set to the whole keyspace and pays write
// amplification under skew. f1raw keeps the resident hash index over one hybrid log
// and leans on the cold value log plus compaction to bound that cost.
//
// The design is the FASTER paper's two pieces and nothing else:
//
//  1. A lock-free hash index. One flat array of cache-line buckets. A bucket is
//     seven 64-bit index entries plus a one-word link to an overflow bucket, so it
//     fills a 64-byte line exactly. An entry packs a 16-bit tag (high hash bits, a
//     fast reject that avoids touching the arena) and a 48-bit log address. The
//     bucket plus its overflow chain is open-addressed: a probe scans every entry
//     and verifies the key in the arena on a tag hit. Inserts publish with a single
//     compare-and-swap on an empty entry word; a replace CAS-swaps the entry to a
//     new record; a delete CAS-zeroes the entry, and because a lookup scans the
//     whole bucket it tolerates the hole with no tombstone. There is no per-shard
//     mutex and no global mutex on any path. That is the whole point of the spike:
//     the integrated hot/ engine still takes a per-shard write latch, and the
//     keyspace bolted a database-wide access mutex on top of it. Here no lock taxes
//     the cores at all.
//
//  2. An in-memory hybrid log. One preallocated byte arena with an atomic
//     bump-allocated tail; overflow buckets are bump-allocated from the same arena,
//     so there is one allocator and nothing to size twice. A record is immutable
//     once published except its value cell, which a same-key writer rewrites in
//     place under the record's own seqlock (the FASTER record latch: the low bit of
//     a per-record version word, held for a single memcpy, contended only by another
//     writer to the very same key). A reader copies the value between two reads of an
//     even version and retries only if that exact record was mid-update, so a reader
//     never blocks another reader or a reader of a different key. A value too large
//     for the record's reserved capacity appends a fresh record and CAS-swaps the
//     index entry to it, read-copy-update style. Different keys never coordinate.
//
// Memory is grow-only: the arena is sized up front and never freed, so a published
// log address is valid forever and reads need no epoch-protection framework to be
// safe. In-place update is what lets a sustained same-key write benchmark run on a
// bounded arena; production F2 reclaims the cold region with epochs, which this
// spike omits on purpose to measure the steady-state hot path the saturation
// benchmark exercises, and says so rather than measuring a watered-down path. A new
// key that finds the arena full is reported as ErrFull, not handled by spilling.
//
// One honest caveat about the seqlock: the value memcpy is, by construction, a
// benign data race (two readers and a writer touch the same bytes; the version
// check makes it correct, but Go's race detector cannot model that). Every other
// access is to immutable data or through an atomic, so all paths except concurrent
// writes to one hot key are race-detector clean. The single test that exercises a
// hot key under contention skips under -race and runs as a plain stress test
// otherwise.
package f1raw

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ErrFull is returned by Set when the arena has no room for a new record or
// overflow bucket. The spike sizes the arena for the working set up front; spilling
// cold records to disk is the production tier's job, not this measurement's.
var ErrFull = errors.New("f1raw: arena full")

// ErrTooBig is returned when a key or value exceeds the 64 KiB field width.
var ErrTooBig = errors.New("f1raw: key or value over 64 KiB")

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
	//   [12:13) kind  uint8   record kind (the spec's type_tag), immutable
	//   [13:14) flags uint8   record flags (flagSep marks a cold pointer), immutable
	//   [14:16) pad
	//   [16:16+klen)              key bytes, immutable
	//   [16+align8(klen): +vcap*8) value bytes, rewritten in place under the seqlock
	hdrSize  = 16
	offVer   = 0
	offVlen  = 4
	offKlen  = 8
	offVcap  = 10
	offKind  = 12
	offFlags = 13

	verLockBit = 1 // low bit of ver: a writer is mid-update

	// stringKind is the record kind for the plain string keyspace. It is zero so a
	// zero-initialized arena and the existing string hot path need no change: Get,
	// Set, Delete, and Incr all operate on this kind. Collection element rows use
	// distinct non-zero kinds (GetKind/PutKind/DeleteKind in coll.go), which keeps a
	// composite element key from ever matching a string key of the same bytes.
	stringKind = 0

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
	base    unsafe.Pointer // &arena[0], 8-aligned, stable for the store's life
	cap     uint64
	tail    atomic.Uint64 // next free arena offset; starts at 8 so a real addr is never 0
	count   atomic.Int64

	// topCount tracks the live top-level keys, the subset of records whose kind topKind
	// admits: a plain string record and a collection header row, not element rows, expire
	// sidecars, or stream PEL rows. It is maintained at exactly the points count is, so it
	// stays consistent with the index, and it is what DBSIZE reads without a scan. topKind
	// is the server-supplied policy classifier, set once before serving through
	// SetTopKindFunc; a nil topKind leaves topCount at zero and DBSIZE would report zero,
	// so the server must set it at startup.
	topKind  func(kind byte) bool
	topCount atomic.Int64

	// oidx is the ordered element index (oindex.go): the in-memory per-collection
	// sorted run that lets a bounded cursor enumerate one collection's elements in key
	// order. It is maintained explicitly by the collection element path (CollInsert /
	// CollRemove) and never touched by the string hot path, so Get/Set/Incr pay
	// nothing for it.
	oidx *oindex

	// rvec is the set type's dense member vector (randvec.go): the resident array of member
	// offsets that answers a uniform random draw (SPOP, SRANDMEMBER) in O(1) instead of an
	// order-statistic descent over oidx. It is a striped, prefix-keyed map built lazily on the
	// first draw against a set, so a keyspace that never draws at random allocates nothing here
	// and the string and point-collection paths never touch it.
	rvec *randVec

	// pdescs is the partition descriptor table (partdesc.go): per partitioned set, the cached P
	// partition-vector pointers that turn the weighted draw's count read from O(P) shard-map
	// lookups into O(P) atomic loads (spec 2064/f1_rewrite_ltm/19 section 5). It is keyed by the
	// whole-set prefix, built lazily on the first draw against a partitioned set, and torn down with
	// the set's vectors by CollRandDrop, so an unpartitioned keyspace allocates nothing here.
	pdescs *partDescs

	// cold is the on-disk value log for the larger-than-memory string regime, or nil
	// for a pure in-memory store. When set, a string value longer than sepThreshold is
	// written to the log and the in-memory record holds a cold pointer (cold.go). The
	// in-memory path is byte-identical whether cold is nil or not, because a value at
	// or under the threshold never touches the log.
	cold         *coldLog
	sepThreshold int

	// The deferred ordered-index removal state (tombstone.go). When deferred removal is
	// enabled, an element delete hands the composite key to tombHead (a lock-free Treiber
	// stack of removal batches) instead of splicing the skip list inline, and a background
	// folder goroutine drains the stack under oi.mu off the delete's critical path. tombPend
	// is the number of queued-but-not-yet-spliced element keys; it gates the enumeration
	// liveness filter so a read pays the per-node liveness probe only while a splice is
	// actually outstanding. All three are zero-valued and unused until EnableDeferredRemoval
	// starts the folder, so a store that never enables it behaves exactly as before.
	tombHead   atomic.Pointer[tombNode]
	tombPend   atomic.Int64
	folderOn   atomic.Bool
	folderStop chan struct{}
	folderDone chan struct{}
	folderWake chan struct{}
	// folderMu serializes a drain (whether run by the folder or by a foreground
	// SyncPendingRemovals) against another drain, so a foreground caller that needs the ordered
	// index reconciled right now waits for any in-flight folder splice to finish rather than
	// racing it. It is taken only on the drain path, never on the delete hot path, so an
	// element delete never blocks on it.
	folderMu sync.Mutex
	// tombPool recycles drained tombNodes (and the byte and offset arenas each carries) back to
	// the producers. A delete-heavy pipeline pushes one node per drain and the folder discards it
	// right after the splice, so without recycling every drain allocates a node plus two slices
	// that live only until the folder runs, feeding a steady stream of short-lived garbage that
	// shows up as GC-driven p99 spikes on the single-hot-key delete gate. The folder returns each
	// node here after splicing and enqueueTomb takes one back, reusing the arenas, so the steady
	// state allocates nothing. sync.Pool may drop entries under GC pressure, in which case a push
	// falls back to a fresh node, so correctness never depends on the pool being warm.
	tombPool sync.Pool
}

// New builds a store whose primary hash index has indexBuckets buckets (rounded up
// to a power of two) and whose arena holds arenaBytes of records and overflow
// buckets. Size indexBuckets near the expected key count over a small load factor
// (each bucket holds seven keys before it spills to an overflow bucket), and
// arenaBytes for the working set: keyCount * (hdrSize + align8(keyLen) +
// align8(valLen)) plus headroom for overflow buckets. Both are fixed for the
// store's life, matching the spike's memory-only, grow-only contract.
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
		panic("f1raw: arena base not 8-aligned")
	}
	s.tail.Store(8) // reserve offset 0 so an empty index entry (addr 0) is unambiguous
	s.oidx = newOIndex(s)
	s.rvec = newRandVec()
	s.pdescs = newPartDescs()
	return s
}

// NewWithCold builds a store like New but with a cold value log at coldPath for the
// larger-than-memory string regime. A string value longer than sepThreshold bytes is
// written to the log and kept out of the in-memory arena, so the resident footprint
// stays near the index-plus-keys size on a dataset whose values exceed RAM. A
// non-positive sepThreshold uses defaultSepThreshold. The store owns the log and
// closes it in Close.
func NewWithCold(indexBuckets, arenaBytes int, coldPath string, sepThreshold int) (*Store, error) {
	s := New(indexBuckets, arenaBytes)
	cl, err := openColdLog(coldPath)
	if err != nil {
		return nil, err
	}
	if sepThreshold <= 0 {
		sepThreshold = defaultSepThreshold
	}
	s.cold = cl
	s.sepThreshold = sepThreshold
	return s, nil
}

// defaultSepThreshold is the inline-versus-separated cutoff: a value at or under it
// stays inline in the in-memory arena, a larger value goes to the cold log. The spec
// biases this to the 256-to-1024-byte range; 512 is the midpoint default and it is a
// lab knob the benchmark sweeps (checklist section 4, the value-pointer rule in 03).
const defaultSepThreshold = 512

// Len reports the number of live records, which for a keyspace with collections is more than
// the number of logical keys, since every element and sidecar row is a record. Use TopLen for
// the logical key count.
func (s *Store) Len() int { return int(s.count.Load()) }

// SetTopKindFunc installs the classifier that decides which record kinds are top-level keys, the
// same policy the server hands ScanKeys. Call it once at startup before serving traffic: the
// classifier is read on every publish and delete without synchronization, so it must be set
// before the first write. It also seeds nothing retroactively, so set it on an empty store.
func (s *Store) SetTopKindFunc(f func(kind byte) bool) { s.topKind = f }

// TopLen reports the number of live top-level keys, an O(1) read of the counter maintained
// alongside the record count. It is what DBSIZE returns. A key whose TTL has passed but which
// has not been reaped yet is still counted, matching Redis DBSIZE, which counts the dict entry
// until lazy or active expiry removes it.
func (s *Store) TopLen() int { return int(s.topCount.Load()) }

// addTop adjusts the top-level key counter by d when kind names a top-level key. It is called at
// every point count changes, with the same delta, so topCount tracks the top-level subset of the
// record count exactly.
func (s *Store) addTop(kind byte, d int64) {
	if s.topKind != nil && s.topKind(kind) {
		s.topCount.Add(d)
	}
}

// BucketCount reports the number of primary index buckets, fixed at construction. RANDOMKEY
// uses it to pick a random primary bucket to start a scan from, and it bounds the cursor space
// ScanKeys walks.
func (s *Store) BucketCount() int { return len(s.buckets) }

// ArenaBytes reports the resident arena's split: used is the bytes the bump allocator has
// handed out (record headers, keys, inline values, and overflow buckets), cap is the arena's
// fixed size. used never rewinds except on a flush, so used/cap is the resident fill the
// introspection path (INFO used_memory) surfaces. A separated value's bytes live in the cold
// log, not the arena, so this counts only the resident footprint, which is the figure the
// larger-than-memory regime keeps near index-plus-keys. tail starts at 8 (offset 0 is
// reserved so an empty index slot is unambiguous), so used excludes that reserved byte.
func (s *Store) ArenaBytes() (used, total uint64) {
	t := s.tail.Load()
	if t < 8 {
		t = 8
	}
	if t > s.cap {
		t = s.cap // a failed alloc advances tail past cap; report the arena as full, not over
	}
	return t - 8, s.cap
}

// Close releases the store's external resources. For a pure in-memory store it is a
// no-op; for a store with a cold value log it closes the log file. The in-memory
// arena and index are garbage-collected with the store.
func (s *Store) Close() error {
	if s.folderOn.Load() {
		// Stop the folder and let it splice out every tombstone it still holds, so a close
		// leaves the ordered index reconciled with the hash index rather than carrying dead
		// nodes a later reopen would have to reconcile.
		close(s.folderStop)
		<-s.folderDone
	}
	if s.cold != nil {
		return s.cold.close()
	}
	return nil
}

// hash is the wyhash-style word-at-a-time mix: it reads eight key bytes per step and
// finishes a short key in one or two multiplies, so the table probe is not gated on a
// long scalar hash. The output is internal to this store and matched against nothing
// outside it, so the only requirement is that one run is self-consistent.
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

// tagOf takes the high bits of the hash for the 16-bit entry tag. A non-zero tag
// lets a probe reject a slot without touching the arena; the |1 keeps it non-zero so
// it never collides with the empty-slot sentinel.
func tagOf(h uint64) uint64 { return (h >> 48) | 1 }

func align8(n uint64) uint64 { return (n + 7) &^ 7 }

func recSize(klen, vlen int) uint64 {
	return hdrSize + align8(uint64(klen)) + align8(uint64(vlen))
}

// alloc bump-allocates an 8-byte-aligned block of nbytes and returns its offset. The
// tail moves with one atomic add; ok is false when the arena is full. A failed
// allocation has already advanced the tail past cap, which only wastes the tail of a
// full arena and never corrupts a live record.
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
// under the latch and read under the seqlock, so a reader never sees a length from
// one update paired with bytes from another.
func (s *Store) vlenAt(off uint64) *atomic.Uint32 {
	return (*atomic.Uint32)(unsafe.Add(s.base, off+offVlen))
}

// klen, vcapBytes, and keyAt read immutable header fields with plain loads. They are
// written once before the record is published and never touched again, and the
// publishing CAS on the index entry establishes the happens-before a reader needs,
// so the plain reads are race-detector clean.
func (s *Store) klen(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena[off+offKlen:]))
}
func (s *Store) vcapBytes(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena[off+offVcap:])) * 8
}

// recordMatches reports whether the record at off carries this key in the given kind
// namespace. It reads only immutable fields (the kind byte, key length, and the key
// bytes), so it is safe to call on any live record concurrently with writers to that
// record's value. The kind byte is a cheap one-byte reject checked before the key
// bytes, so a probe never confuses a string record with a same-key collection row.
func (s *Store) recordMatches(off uint64, key []byte, kind byte) bool {
	if s.arena[off+offKind] != kind {
		return false
	}
	if s.klen(off) != uint64(len(key)) {
		return false
	}
	start := off + hdrSize
	return string(s.arena[start:start+uint64(len(key))]) == string(key)
}

// bucketAt reinterprets an arena offset as an overflow bucket. Overflow buckets live
// in the arena, 8-byte aligned by the allocator, so their atomic words are valid.
func (s *Store) bucketAt(off uint64) *bucket {
	return (*bucket)(unsafe.Add(s.base, off))
}

// nextBucket returns the next bucket in b's overflow chain. With create set it
// allocates and links one when the chain ends; a lost CAS race wastes the freshly
// allocated bucket (harmless in a grow-only arena) and follows the winner. It
// returns nil when the chain ends and create is false, or when the arena is full.
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

// find walks the bucket and its overflow chain for key, returning the matching
// record offset and the entry that points at it (bucket plus slot plus the observed
// word) so Delete and the replace path can CAS that exact entry. The probe rejects a
// slot on tag mismatch before touching the arena and verifies the key on a tag hit.
func (s *Store) find(key []byte, h uint64, kind byte) (off uint64, b *bucket, slot int, word uint64, found bool) {
	tag := tagOf(h)
	b = &s.buckets[h&s.mask]
	for b != nil {
		for i := 0; i < slotsPerBucket; i++ {
			w := b.slots[i].Load()
			if w == 0 || w>>tagShift != tag {
				continue
			}
			a := w & addrMask
			if s.recordMatches(a, key, kind) {
				return a, b, i, w, true
			}
		}
		b = s.nextBucket(b, false)
	}
	return 0, nil, 0, 0, false
}

// Get copies the value for key into dst (reusing its capacity) and reports whether
// the key is present. The read is lock-free: it loads the index entry, follows it
// into the arena, and copies the value out under the record's seqlock, retrying only
// if a writer is updating that exact record. No reader ever blocks another reader or
// a reader of a different key.
func (s *Store) Get(key, dst []byte) ([]byte, bool) {
	h := hash(key)
	off, _, _, _, found := s.find(key, h, stringKind)
	if !found {
		return dst[:0], false
	}
	if s.cold != nil && s.isSep(off) {
		return s.readSeparated(off, dst)
	}
	return s.readValue(off, dst), true
}

// readValue is the seqlock read. It spins while a writer holds the latch (odd
// version), copies the value under an even version, and retries if the version moved
// during the copy. The value memcpy is the one access this package makes to mutable
// bytes without an atomic; it is correct by the seqlock and benign, and is the
// reason the hot-key contention test skips under -race.
func (s *Store) readValue(off uint64, dst []byte) []byte {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	for {
		v1 := verp.Load()
		if v1&verLockBit != 0 {
			continue
		}
		n := uint64(s.vlenAt(off).Load())
		dst = append(dst[:0], s.arena[vbase:vbase+n]...)
		if verp.Load() == v1 {
			return dst
		}
	}
}

// Set stores val under key, blind-upsert semantics. An existing key whose record has
// reserved room for val is updated in place under the per-record latch; a new key,
// or a value larger than the existing record reserved, appends a fresh record and
// publishes it. Both paths are lock-free across distinct keys.
func (s *Store) Set(key, val []byte) error {
	if len(key) == 0 || len(key) > maxKey || len(val) > maxVal {
		if len(key) == 0 {
			return errors.New("f1raw: empty key")
		}
		return ErrTooBig
	}
	h := hash(key)
	if s.cold != nil && len(val) > s.sepThreshold {
		// A large value goes to the cold log as a fresh separated record; it is never
		// updated in place, so any prior record for the key is replaced by the index
		// swap inside publish.
		return s.setSeparated(key, val, h)
	}
	// Inline path. The in-place fast update requires the existing record to be inline
	// too: a separated record's value cell is a 12-byte pointer, so a small inline
	// value must not be memcpy'd over it. When the target is separated, fall through
	// to publish, which swaps the entry to a fresh inline record.
	if off, _, _, _, found := s.find(key, h, stringKind); found && !s.isSep(off) && uint64(len(val)) <= s.vcapBytes(off) {
		s.inPlace(off, val)
		return nil
	}
	return s.publish(key, val, h, stringKind, 0)
}

// inPlace rewrites a record's value under its seqlock. The latch is the low bit of
// the version word, taken with a CAS so two writers to the same key serialize; it is
// held only for the value copy and the length store. Readers see the version go odd
// then even and retry across the window, so they never observe a torn value. vlen is
// stored atomically so a reader pairs the new length with the new bytes.
func (s *Store) inPlace(off uint64, val []byte) {
	verp := s.verAt(off)
	vbase := off + hdrSize + align8(s.klen(off))
	for {
		v := verp.Load()
		if v&verLockBit != 0 {
			continue
		}
		if verp.CompareAndSwap(v, v+1) { // acquire: make it odd
			copy(s.arena[vbase:vbase+uint64(len(val))], val)
			s.vlenAt(off).Store(uint32(len(val)))
			verp.Store(v + 2) // release: back to even, one tick newer
			return
		}
	}
}

// initRecord lays down a fresh record. The writes are plain because the record is
// private until publish CAS-installs its address into an index entry; that release
// pairs with the acquire a reader does when it loads the entry, so the bytes are
// visible and the plain writes are race-detector clean.
func (s *Store) initRecord(off uint64, key, val []byte, kind, flags byte) {
	binary.LittleEndian.PutUint32(s.arena[off+offVer:], 0)
	binary.LittleEndian.PutUint32(s.arena[off+offVlen:], uint32(len(val)))
	binary.LittleEndian.PutUint16(s.arena[off+offKlen:], uint16(len(key)))
	binary.LittleEndian.PutUint16(s.arena[off+offVcap:], uint16(align8(uint64(len(val)))/8))
	// The kind and flags bytes are written explicitly rather than left to the
	// zero-init arena, because Reset rewinds the tail without scrubbing bytes, so a
	// reused offset may still hold a prior record's kind or flags.
	s.arena[off+offKind] = kind
	s.arena[off+offFlags] = flags
	copy(s.arena[off+hdrSize:], key)
	copy(s.arena[off+hdrSize+align8(uint64(len(key))):], val)
}

// publish appends a new record and links it into the index. It allocates once, then
// scans the bucket chain in a loop: an existing live record of this key means a
// concurrent writer beat us, so update in place if it fits or CAS-swap the entry to
// our record (a replace, count unchanged); otherwise fill the first empty slot (a
// new key, count up); otherwise grow the chain with an overflow bucket and rescan. A
// lost CAS just restarts the scan. The one allocated record is reused across
// restarts, and is harmlessly abandoned only when another writer published the key.
func (s *Store) publish(key, val []byte, h uint64, kind, flags byte) error {
	off, ok := s.alloc(recSize(len(key), len(val)))
	if !ok {
		return ErrFull
	}
	s.initRecord(off, key, val, kind, flags)
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
				if s.recordMatches(a, key, kind) {
					// Another writer published this key first. Last writer wins: update
					// their record in place if our value fits, else swap the entry to ours.
					// A separated write (flags != 0) or a target that is itself separated
					// must never update in place: a cold pointer is immutable, so both take
					// the entry-swap path to a fresh record instead of a memcpy.
					if flags == 0 && !s.isSep(a) && uint64(len(val)) <= s.vcapBytes(a) {
						s.inPlace(a, val)
						return nil
					}
					if b.slots[i].CompareAndSwap(w, newWord) {
						// The old record a is now unlinked. If it was separated, its cold
						// value is dead space no live record points at anymore; account it
						// so a later compaction pass sees the real waste.
						s.markSepDead(a)
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
				s.addTop(kind, 1)
				return nil
			}
			continue // slot filled under us; rescan
		}
		// Every entry in the chain is taken: extend it and rescan into the new bucket.
		if s.nextBucket(last, true) == nil {
			return ErrFull
		}
	}
}

// Delete removes key and reports whether it was present. It CAS-zeroes the index
// entry that points at the key's record; because a lookup scans the whole bucket
// chain, the emptied slot is just a hole and needs no tombstone. The record bytes
// stay valid in the grow-only arena, so a reader that already resolved the address
// reads a consistent (if now-removed) value rather than chasing freed memory. A lost
// CAS (a concurrent replace or delete) retries from a fresh lookup.
func (s *Store) Delete(key []byte) bool {
	h := hash(key)
	for {
		off, b, slot, word, found := s.find(key, h, stringKind)
		if !found {
			return false
		}
		if b.slots[slot].CompareAndSwap(word, 0) {
			// A separated string's cold value is now unreferenced; account it as dead.
			s.markSepDead(off)
			s.count.Add(-1)
			s.addTop(stringKind, -1)
			return true
		}
	}
}

// Each calls fn for every live key with its value. It is a single-threaded
// convenience for tests and is not safe to run against concurrent writers.
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

// Compile-time check that a bucket is exactly one cache line. A drift here would
// split an atomic word across two lines and wreck both the correctness reasoning and
// the performance the spike measures.
const _ = bucketSize - unsafe.Sizeof(bucket{})
const _ = unsafe.Sizeof(bucket{}) - bucketSize
