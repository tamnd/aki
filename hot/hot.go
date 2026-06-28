// Package hot is the aki F2 hot tier: an in-memory, sharded hash store with
// lock-free reads and in-place replacement. It is the clean re-architecture of
// the v2 store/ engine, built to avoid the two bugs the PR #255 review found.
//
// What was wrong with store/ and what this fixes:
//
//   - store/ guards every read with a per-shard sync.RWMutex. RLock does an
//     atomic add to the lock's shared reader-counter, so the lock's cache line
//     bounces between cores on every read even when no two cores touch the same
//     shard. The locktax spike measured a lock-free atomic-load read beating the
//     RWMutex read 2.3x to 2.6x on every clean box. Here a read takes no lock at
//     all: it loads the shard's table pointer and each slot with an atomic load,
//     exactly the rewrite/01 + rewrite/05 read path. Writers take a per-shard
//     plain sync.Mutex (not an RWMutex) and publish with an atomic store.
//   - store/ appends a new log record on every SET and INCR and never reclaims
//     the dead ones, so an overwrite-heavy or increment-heavy workload grows
//     memory without bound at a constant live keyset. That is the one feature the
//     FASTER paper is named for, left out: "FASTER: A Concurrent Key-Value Store
//     with In-Place Updates" updates a record in place when it sits in the
//     mutable tail. Here every write replaces the key's entry pointer and drops
//     the old entry, which the GC then reclaims. Memory tracks the live set, so
//     overwriting one key a million times holds one entry, not a million.
//
// The model, per shard:
//
//   - An open-addressed table of slots, each slot an atomic pointer to an entry.
//     A slot is nil (empty), the shared tombstone sentinel (a deleted slot that
//     readers probe past), or a live entry.
//   - An entry is immutable once published: it carries the key hash and a single
//     backing slice holding the key bytes followed by the value bytes. Because an
//     entry is never mutated after it is stored, a reader returns the value slice
//     directly with no copy, and an in-flight reader holding an old entry stays
//     valid while a writer publishes a new one.
//   - A read loads the table pointer, then probes slots with atomic loads,
//     comparing the full 64-bit hash before the key bytes. No lock.
//   - A write takes the shard's plain mutex, finds the slot, and atomically
//     stores a fresh entry (insert or overwrite) or the tombstone (delete). Grow
//     and tombstone-clearing rehash build a brand new table and publish it with
//     one atomic store; readers on the old table keep a consistent snapshot.
//
// This is the F2 hot tier. The cold tier (a log-structured on-disk extension for
// the larger-than-memory path) and the off-heap pointer-free index (the GC win,
// fuzz-gated) layer behind this later; they are deliberately not in this first
// cut so its correctness is plain and checkable under the race detector.
package hot

import (
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
)

// Tunables shape a Store. The zero value is not valid; use DefaultTunables.
type Tunables struct {
	// Shards is the number of independent table+lock shards. Must be a power of
	// two so the shard for a key is a mask of its hash. 256 matched the shardmap
	// that beat Valkey in the memengine spike and is the rewrite/01 default.
	Shards int

	// IndexHintPerShard presizes each shard's table for this many keys, avoiding
	// grow churn during a known-size load. Zero uses a small default and grows.
	IndexHintPerShard int
}

// DefaultTunables returns the standard shape: 256 shards, tables grow on demand.
func DefaultTunables() Tunables { return Tunables{Shards: 256} }

const (
	// shardShift picks the shard from the high bits of the hash, independent of
	// the low bits a shard's table uses for its home slot.
	shardShift = 48

	// minTableCap is the smallest table a shard starts or shrinks to.
	minTableCap = 8

	// entryOverhead approximates the per-entry resident cost beyond the key and
	// value bytes (the entry struct plus the slice header and GC bookkeeping). It
	// keeps used-memory accounting close to real RSS without measuring it.
	entryOverhead = 56
)

// inlineCap is the largest key+value byte count an entry holds in its inline
// array, so a write of that size or smaller is a single allocation (the entry
// struct) with no separate buffer. Sized to cover the common small-value case
// (a redis value plus a short key); larger records fall back to a heap slice.
const inlineCap = 80

// entry is one key/value record, immutable after it is published into a slot. The
// key bytes followed by the value bytes live either in the inline array (the
// common small case, one allocation total) or, when they exceed inlineCap, in the
// kv slice (two allocations). Either way the bytes are immutable after publish, so
// a reader returns the value with no copy and an in-flight reader stays valid
// while a writer installs a new entry.
type entry struct {
	hash   uint64
	klen   uint32
	ilen   uint32 // bytes used in inline when kv is nil
	inline [inlineCap]byte
	kv     []byte // non-nil only when key+value exceeds inlineCap
}

// data returns the full key+value backing, from the inline array or the heap
// slice. The returned slice aliases immutable storage and must not be mutated.
func (e *entry) data() []byte {
	if e.kv != nil {
		return e.kv
	}
	return e.inline[:e.ilen]
}

func (e *entry) key() []byte { return e.data()[:e.klen] }
func (e *entry) val() []byte { return e.data()[e.klen:] }

// tombstone marks a slot whose key was deleted. Readers probe past it; an insert
// reuses the first one it passes. It is cleared in bulk by a rehash.
var tombstone = &entry{}

// slot is a single table position. The atomic pointer is the publish point: a
// writer stores under the shard lock, every reader loads without a lock.
type slot struct{ e atomic.Pointer[entry] }

// htable is one open-addressed table. It is replaced wholesale on grow or rehash,
// never resized in place, so a reader that loaded an old table keeps a consistent
// snapshot while a new one is published.
type htable struct {
	slots []slot
	mask  uint64
}

func newHTable(capPow2 int) *htable {
	return &htable{slots: make([]slot, capPow2), mask: uint64(capPow2 - 1)}
}

// shard owns one table. Reads are lock-free; mu serialises writers only and is a
// plain Mutex, not an RWMutex, because the read path never touches it.
type shard struct {
	mu    sync.Mutex
	table atomic.Pointer[htable]
	count int // live keys, guarded by mu
	tomb  int // tombstone slots, guarded by mu
	bytes int // approximate resident bytes of live entries, guarded by mu
}

// Store is the hot-tier engine. It is safe for concurrent use: the shard for a
// key is fixed by the key's hash, reads take no lock, and writers take only the
// owning shard's mutex.
type Store struct {
	shards []*shard
	mask   uint64
	t      Tunables
}

// New builds a Store. It returns an error only for invalid tunables.
func New(t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errors.New("hot: Shards must be a power of two")
	}
	initCap := minTableCap
	if t.IndexHintPerShard > 0 {
		initCap = pow2AtLeast(t.IndexHintPerShard * 2)
	}
	s := &Store{
		shards: make([]*shard, t.Shards),
		mask:   uint64(t.Shards - 1),
		t:      t,
	}
	for i := range s.shards {
		sh := &shard{}
		sh.table.Store(newHTable(initCap))
		s.shards[i] = sh
	}
	return s, nil
}

// Close releases the Store. The hot tier holds no files, so this is a no-op kept
// for parity with the durable engine's interface.
func (s *Store) Close() error { return nil }

func (s *Store) shardFor(h uint64) *shard {
	return s.shards[(h>>shardShift)&s.mask]
}

// Set stores value under key, replacing any previous value. The bytes are copied,
// so the caller may reuse both slices after Set returns.
func (s *Store) Set(key, value []byte) error {
	h := hash64(key)
	s.shardFor(h).set(key, value, h)
	return nil
}

// SetWithPrev is Set that also reports the displaced value's length, or -1 when
// the key is new, so the keyspace layer keeps used_memory exact across an
// overwrite without a second probe.
func (s *Store) SetWithPrev(key, value []byte) (prevValLen int, err error) {
	h := hash64(key)
	return s.shardFor(h).set(key, value, h), nil
}

// SetWithPrev2 is SetWithPrev for a value supplied as two segments stored back to
// back (value = v0 followed by v1). The store engine writes the two segments
// straight into its log page without joining them, which is where the seam earns
// its keep; the hot store holds each value in one contiguous entry, so it joins the
// segments first. The method exists on both engines so either satisfies hlEngine.
func (s *Store) SetWithPrev2(key, v0, v1 []byte) (prevValLen int, err error) {
	buf := make([]byte, len(v0)+len(v1))
	copy(buf, v0)
	copy(buf[len(v0):], v1)
	h := hash64(key)
	return s.shardFor(h).set(key, buf, h), nil
}

// Get returns the value stored under key. found is false if the key is absent.
// The returned slice aliases the immutable entry and must not be mutated by the
// caller; it stays valid even if the key is later overwritten or deleted, because
// a write publishes a new entry and never edits this one.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	h := hash64(key)
	v, ok := s.shardFor(h).get(key, h)
	return v, ok, nil
}

// Delete removes key, returning whether it was present.
func (s *Store) Delete(key []byte) (bool, error) {
	h := hash64(key)
	_, ok := s.shardFor(h).del(key, h)
	return ok, nil
}

// DeleteWithPrev is Delete that also reports the removed value's length (-1 when
// the key was absent), so the keyspace layer subtracts the freed bytes from
// used_memory without re-reading the record it just dropped.
func (s *Store) DeleteWithPrev(key []byte) (prevValLen int, ok bool, err error) {
	h := hash64(key)
	n, found := s.shardFor(h).del(key, h)
	return n, found, nil
}

// Len returns the number of live keys across all shards.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += sh.count
		sh.mu.Unlock()
	}
	return n
}

// MemBytes returns the approximate resident bytes of live entries across all
// shards. Because every write drops the old entry, this tracks the live set and
// does not grow under an overwrite-heavy workload, which is the bug it replaces.
func (s *Store) MemBytes() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += sh.bytes
		sh.mu.Unlock()
	}
	return n
}

// IndexBytes returns the total resident bytes held by the slot tables (8 bytes
// per slot on a 64-bit build). This is the per-key index cost.
func (s *Store) IndexBytes() int {
	n := 0
	for _, sh := range s.shards {
		t := sh.table.Load()
		n += len(t.slots) * 8
	}
	return n
}

// set inserts or overwrites key. It returns the previous value length, or -1 for
// a new key. Held under the shard mutex; readers run lock-free throughout.
func (sh *shard) set(key, value []byte, h uint64) int {
	sh.mu.Lock()
	t := sh.table.Load()
	// Grow before the table fills so a probe always meets an empty slot, which is
	// what lets a reader terminate on nil. Threshold is load factor 0.75 counting
	// tombstones, so a tombstone-heavy table rehashes and clears them.
	if (sh.count+sh.tomb+1)*4 >= len(t.slots)*3 {
		t = sh.grow()
	}
	i := h & t.mask
	firstTomb := -1
	for {
		e := t.slots[i].e.Load()
		if e == nil {
			ne := newEntry(h, key, value)
			if firstTomb >= 0 {
				t.slots[firstTomb].e.Store(ne)
				sh.tomb--
			} else {
				t.slots[i].e.Store(ne)
			}
			sh.count++
			sh.bytes += len(ne.data()) + entryOverhead
			sh.mu.Unlock()
			return -1
		}
		if e == tombstone {
			if firstTomb < 0 {
				firstTomb = int(i)
			}
			i = (i + 1) & t.mask
			continue
		}
		if e.hash == h && string(e.key()) == string(key) {
			ne := newEntry(h, key, value)
			prev := len(e.val())
			t.slots[i].e.Store(ne) // publish the new entry; the old one is now garbage
			sh.bytes += len(ne.data()) - len(e.data())
			sh.mu.Unlock()
			return prev
		}
		i = (i + 1) & t.mask
	}
}

// get reads key with no lock. It loads the table pointer once, then probes slots
// with atomic loads, comparing the full hash before the key bytes.
func (sh *shard) get(key []byte, h uint64) ([]byte, bool) {
	t := sh.table.Load()
	i := h & t.mask
	for {
		e := t.slots[i].e.Load()
		if e == nil {
			return nil, false
		}
		if e != tombstone && e.hash == h && string(e.key()) == string(key) {
			return e.val(), true
		}
		i = (i + 1) & t.mask
	}
}

// del tombstones key. It returns the removed value length (-1 when absent) and
// whether the key was present. A run of deletes triggers a rehash that clears the
// tombstones so probe lengths stay bounded.
func (sh *shard) del(key []byte, h uint64) (int, bool) {
	sh.mu.Lock()
	t := sh.table.Load()
	i := h & t.mask
	for {
		e := t.slots[i].e.Load()
		if e == nil {
			sh.mu.Unlock()
			return -1, false
		}
		if e != tombstone && e.hash == h && string(e.key()) == string(key) {
			prev := len(e.val())
			t.slots[i].e.Store(tombstone)
			sh.count--
			sh.tomb++
			sh.bytes -= len(e.data()) + entryOverhead
			if sh.tomb*2 >= len(t.slots) {
				sh.grow() // clear tombstones; keeps probe length bounded under churn
			}
			sh.mu.Unlock()
			return prev, true
		}
		i = (i + 1) & t.mask
	}
}

// grow builds a fresh table sized for the live count, copies live entries into
// it (dropping tombstones), and publishes it with one atomic store. Called under
// the shard mutex. A new table large enough that the live set sits at load factor
// 0.5; never smaller than the current table, so this also serves as a same-size
// rehash that only clears tombstones.
func (sh *shard) grow() *htable {
	old := sh.table.Load()
	need := (sh.count + 1) * 2
	newCap := max(len(old.slots), minTableCap)
	for newCap < need {
		newCap <<= 1
	}
	nt := newHTable(newCap)
	for i := range old.slots {
		e := old.slots[i].e.Load()
		if e == nil || e == tombstone {
			continue
		}
		j := e.hash & nt.mask
		for nt.slots[j].e.Load() != nil {
			j = (j + 1) & nt.mask
		}
		nt.slots[j].e.Store(e)
	}
	sh.tomb = 0
	sh.table.Store(nt)
	return nt
}

// newEntry builds an immutable record holding the key followed by the value. When
// they fit in the inline array the entry is a single allocation; otherwise the
// bytes go to a heap slice (two allocations).
func newEntry(h uint64, key, val []byte) *entry {
	e := &entry{hash: h, klen: uint32(len(key))}
	n := len(key) + len(val)
	if n <= inlineCap {
		copy(e.inline[:], key)
		copy(e.inline[len(key):], val)
		e.ilen = uint32(n)
	} else {
		kv := make([]byte, n)
		copy(kv, key)
		copy(kv[len(key):], val)
		e.kv = kv
	}
	return e
}

// hash64 is FNV-1a over the key bytes. The full 64-bit value selects the shard
// (high bits), the home slot (low bits), and gates the key compare on a read.
func hash64(b []byte) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

func pow2AtLeast(n int) int {
	if n < minTableCap {
		return minTableCap
	}
	if n&(n-1) == 0 {
		return n
	}
	return 1 << bits.Len(uint(n))
}
