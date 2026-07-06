package f1raw

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

// The dense member vector is the set type's second resident structure, the one that
// makes a uniform random draw an O(1) array index instead of an O(log n) skip-list
// descent (spec 2064/f1_rewrite_ltm/18). The ordered index (oindex.go) answers three
// questions cheaply: the k-th element in order, the rank of an element, and an in-order
// walk from a cursor, which is what SMEMBERS and SSCAN need. A uniform random member is
// none of those: it is "give me an arbitrary live member with equal probability," and
// the cheapest structure for that is an unordered dense array where the index space is
// the probability space. Redis reaches for exactly this (its set is a dict backed by a
// flat array, and SRANDMEMBER walks to a random occupied bucket); aki carries one
// representation and paid a descent for the random draw, so this restores the O(1) draw
// as a small side structure the two random commands consult and nothing else touches.
//
// "Dense" means the vector has no holes: a set's live members occupy slots 0 through
// card-1 in no particular order, so a draw is vector[rand(len)] with no arithmetic and an
// exactly-uniform distribution. It is append-on-add and swap-remove-on-delete, the
// standard technique for keeping an unordered set dense under removals: an add appends the
// new member's arena offset, a remove moves the last slot into the removed slot and
// shortens by one. The swap needs the removed member's slot, and finding it by scanning
// would be O(n), so the vector carries a back-index (offset -> slot) that gives the slot
// in O(1). This is the same dict-plus-array pairing Redis keeps for a hashtable-encoded
// set.
//
// The vector holds arena offsets, not key bytes, for two reasons. An offset is eight
// bytes and fixed width, so the vector packs densely and a draw touches one cache line of
// offsets rather than chasing variable-length keys. And the arena is grow-only, so an
// offset stays valid for the store's life and resolves to the member's immutable composite
// key on demand, the same contract the ordered index's nodes rely on. A set member row is
// always resident (a member has no value; the row is the membership), so an offset always
// resolves without a cold read, which is why the draw is O(1) even in the larger-than-
// memory regime.
//
// Consistency contract. The hash index is authoritative; the vector is a derived hint. A
// vector is built lazily on the first draw against a set (deriveOnFirstDraw), by scanning
// the set's live members through the ordered index, and thereafter every membership change
// maintains it: an add appends, a remove swap-removes. Until a vector exists, an add and a
// remove are no-ops on it, so a set is never left with a partial vector (the first draw
// builds the whole thing from the authoritative index). The vector is never persisted; on
// restart the map is empty and the first draw rebuilds it. This is the CPU-cache relation:
// the vector accelerates a query over state that is already durable, and duplicating it on
// disk would only add a consistency burden a crash could break.
//
// Concurrency. Vectors live in a striped map keyed by the collection's byte prefix, so two
// sets' vectors are independent and a hot set's draws contend only with that set's own
// writers, which the server's per-key stripe lock already serializes. The non-destructive draw
// is lock-free (spec 2064/19 slice 1): the shard publishes its prefix map through an atomic
// pointer and each memberVec publishes its slots slice through an atomic pointer, so a draw
// atomic-loads the map snapshot, indexes it to the vector, atomic-loads the vector's slots
// snapshot, and reads one slot, touching no lock and writing no shared cache line. That is what
// lets many concurrent draws against one hot set scale across cores: an RWMutex read lock would
// still bounce a single shared reader counter and cap the draw, which a measurement confirmed,
// so the read path takes no lock at all. The draw takes a caller-supplied random word (the
// server's per-connection splitmix64) so two draws never share a counter either.
//
// Every mutation still serializes on the shard's write mutex: add, remove, the lazy first-draw
// build, the destructive SPOP draw that swap-removes, and the rare structural change (a new set's
// first vector, a dropped set). A structural change copies the shard's prefix map and atomically
// swaps the pointer (copy-on-write), which is cheap because it is per-set-lifecycle, not per-op,
// and a shard holds few sets. An add or remove mutates its memberVec's working slots and back
// index under the mutex and republishes the slots snapshot. The lock order is stripe lock
// (server, per key) then vector shard mutex then oindex mutex; the ordered-index folder takes
// only oi.mu, so the three never form a cycle.
//
// Because reads are lock-free, a draw and a concurrent write on one key are not ordered against
// each other: a draw may return a member a concurrent SREM or SPOP is removing, or miss one a
// concurrent SADD is adding. This is the same arbitrary order two clients' commands have under
// Redis, which serializes on one thread but gives no cross-client happens-before, so the drawn
// offset always resolves to valid arena bytes and the relaxation changes nothing a client can
// rely on. This is the single-hot-key read fix's first slice; it scales the read draw but not the
// write path, which SPOP/SADD/SREM still serialize on one key, the target of the intra-key
// partitioning in spec 2064/19.

// randVecShards is the number of independent stripes the vector map is split into. It is a
// power of two so a prefix hash maps to a shard with a mask, and it is sized well above the
// core count so two hot sets almost never share a shard and so the shard mutex is
// effectively a per-set lock the server's stripe lock already implies.
const randVecShards = 256

// vecSlots is an immutable snapshot header a reader loads to draw. It carries the slots
// slice a memberVec published under the write lock; a reader atomic-loads the pointer, so it
// reads a consistent (backing, len) pair in one go rather than a torn three-word slice header,
// then indexes it with no lock. The backing array a snapshot points at is never freed while a
// reader holds the snapshot (the arena of offsets is grow-only and the slice keeps it alive),
// so an index into it always resolves.
type vecSlots struct {
	s []uint64
}

// memberVec is one set's dense vector plus its back-index. slots holds the arena offsets of
// the set's live member rows with no holes, so len(slots) is the cardinality and slots[i]
// is a uniform random member for i drawn in [0, len). back maps an offset to its slot so a
// remove finds the victim's slot in O(1) rather than scanning. The invariant after every
// operation is len(slots) == len(back) and back[slots[i]] == i for all i.
//
// view is the atomically-published read snapshot of slots. Every mutation republishes it
// (publish, below) under the shard write lock, and a draw loads it lock-free (spec 2064/19
// slice 1). slots and back are the writer's working copies, touched only under the write
// lock; readers never look at them, only at view.
type memberVec struct {
	view  atomic.Pointer[vecSlots]
	slots []uint64
	back  map[uint64]int
}

func newMemberVec(capHint int) *memberVec {
	v := &memberVec{
		slots: make([]uint64, 0, capHint),
		back:  make(map[uint64]int, capHint),
	}
	v.publish()
	return v
}

// publish stores the current slots slice as the read snapshot. It is called under the shard
// write lock after every mutation, so a lock-free draw always loads a slots view that is a
// valid dense vector (every slot a live member offset). The published slice shares the
// writer's backing array; a reader that loaded an older snapshot keeps its own backing alive
// through the slice, so a concurrent append that reallocates does not disturb it.
func (v *memberVec) publish() {
	v.view.Store(&vecSlots{s: v.slots})
}

// add appends off as a new member and republishes the read snapshot. It is idempotent against
// a duplicate offset: an offset is unique per live record and a re-added member is a fresh
// record at a new offset, so a duplicate should never occur, but skipping one keeps a stray
// double-call from corrupting the density invariant rather than silently biasing the draw
// toward the doubled member.
func (v *memberVec) add(off uint64) {
	if _, ok := v.back[off]; ok {
		return
	}
	v.back[off] = len(v.slots)
	v.slots = append(v.slots, off)
	v.publish()
}

// remove swap-drops the member at off, republishes the read snapshot, and reports whether it
// was present. It reads the victim's slot from back, moves the last slot's offset into the
// hole, fixes the moved offset's back-index entry, shortens by one, and deletes the victim's
// entry. When the victim is itself the last slot the move is a self-assignment and harmless.
// It is O(1) and touches only the one moved member's slot, which is what keeps a delete burst
// O(k) rather than O(k*n). A lock-free draw that loaded the pre-remove snapshot may still draw
// the just-removed member for one more read; that is the same benign race two clients running
// SREM and SRANDMEMBER against one key have under Redis's arbitrary cross-client order, and the
// returned offset still resolves to valid arena bytes.
func (v *memberVec) remove(off uint64) bool {
	i, ok := v.back[off]
	if !ok {
		return false
	}
	last := len(v.slots) - 1
	moved := v.slots[last]
	v.slots[i] = moved
	v.back[moved] = i
	v.slots = v.slots[:last]
	delete(v.back, off)
	v.publish()
	return true
}

// vecMap is one shard's immutable prefix-to-vector map snapshot. A structural change (a new
// set's first vector, a dropped set) copies it under the write mutex and swaps the pointer, so
// a lock-free draw loads a consistent map without a lock and a writer never mutates a map a
// reader is walking (copy-on-write).
type vecMap struct {
	m map[string]*memberVec
}

// randVecShard is one stripe of the vector map: a write mutex the mutations serialize on, an
// atomically-published prefix-to-vector map the lock-free draw loads, and a per-shard PRNG the
// destructive draw uses. The non-destructive draw (CollRandSelect) takes no lock: it loads the
// map snapshot and the target vector's slots snapshot through atomic pointers, so concurrent
// draws on one hot set scale across cores instead of serializing on a lock or a shared reader
// counter (spec 2064/19 slice 1). Every mutation (add, remove, the lazy build, a destructive
// draw, a put or drop) takes the write mutex. The per-shard PRNG is a splitmix64 counter mixed
// on each draw, seeded distinctly per shard; it is advanced only under the write mutex (by the
// destructive draw), so the read path never touches it and instead takes a caller-supplied
// random word, which is what keeps concurrent read draws off any shared counter.
type randVecShard struct {
	mu   sync.Mutex
	view atomic.Pointer[vecMap]
	rng  uint64
}

// randVec is the store's whole set-random-access structure: a fixed array of shards. It is
// created empty with the store and populated lazily, so a keyspace that never runs SPOP or
// SRANDMEMBER against any set allocates nothing here.
type randVec struct {
	shards [randVecShards]randVecShard
}

func newRandVec() *randVec {
	rv := &randVec{}
	for i := range rv.shards {
		// Seed each shard's PRNG with a distinct odd constant derived from its index so no
		// two shards draw the same sequence and no shard is seeded to zero (splitmix64 is
		// fine from zero, but a distinct seed keeps draws across shards independent).
		rv.shards[i].rng = 0x9e3779b97f4a7c15 * (uint64(i)*2 + 1)
	}
	return rv
}

// shardFor returns the shard a prefix maps to, hashing the prefix bytes with the store's
// own hash and masking to the shard count.
func (rv *randVec) shardFor(prefix []byte) *randVecShard {
	return &rv.shards[hash(prefix)&(randVecShards-1)]
}

// get returns the vector for prefix, or nil if the shard has none. It loads the atomically-
// published map snapshot, so it is safe with no lock (the draw's fast path) and equally safe
// under the write mutex (the mutation paths). A shard that has never held a vector reads as a
// nil snapshot, which returns the zero value without allocating.
func (sh *randVecShard) get(prefix []byte) *memberVec {
	vm := sh.view.Load()
	if vm == nil {
		return nil
	}
	return vm.m[string(prefix)]
}

// put installs v under prefix by copy-on-write: it copies the current map, adds the entry, and
// atomically swaps the pointer, so a concurrent lock-free draw walks either the old or the new
// map, never a half-updated one. The caller holds the shard write mutex, so two puts do not
// race to copy. The copy is O(sets in this shard), which is small because the map is striped
// 256 ways, and a put happens once per set's first draw, not per draw.
func (sh *randVecShard) put(prefix []byte, v *memberVec) {
	old := sh.view.Load()
	nm := make(map[string]*memberVec, mapLen(old)+1)
	if old != nil {
		for k, vv := range old.m {
			nm[k] = vv
		}
	}
	nm[string(prefix)] = v
	sh.view.Store(&vecMap{m: nm})
}

// drop removes prefix's vector by the same copy-on-write swap, a no-op when the shard has no
// such vector. The caller holds the shard write mutex.
func (sh *randVecShard) drop(prefix []byte) {
	old := sh.view.Load()
	if old == nil {
		return
	}
	if _, ok := old.m[string(prefix)]; !ok {
		return
	}
	nm := make(map[string]*memberVec, len(old.m))
	for k, vv := range old.m {
		if k == string(prefix) {
			continue
		}
		nm[k] = vv
	}
	sh.view.Store(&vecMap{m: nm})
}

// mapLen is the entry count of a possibly-nil map snapshot, used only to size a copy.
func mapLen(vm *vecMap) int {
	if vm == nil {
		return 0
	}
	return len(vm.m)
}

// next draws the next splitmix64 value from the shard's PRNG. The caller holds the shard
// mutex, so the counter advance is serialized without an extra lock.
func (sh *randVecShard) next() uint64 {
	sh.rng += 0x9e3779b97f4a7c15
	z := sh.rng
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// drawIndex returns a uniform index in [0, n) with no modulo bias, using Lemire's
// multiply-shift reduction: the high word of a 64x64 product of a random word and n is
// uniform over [0, n) up to a rejection that is negligible for the small n a set cardinality
// takes, and it is one multiply where a modulo would be a divide. n must be positive.
func (sh *randVecShard) drawIndex(n int) int {
	hi, _ := bits.Mul64(sh.next(), uint64(n))
	return int(hi)
}

// drawIndexWord is the same Lemire multiply-shift reduction as drawIndex but on a random word
// the caller supplies instead of the shard's PRNG, so a read draw holding only the shard read
// lock can pick a uniform index without advancing any shared counter. r is one random 64-bit
// word from the caller's own PRNG (the server's per-connection splitmix64), and n must be
// positive.
func drawIndexWord(r uint64, n int) int {
	hi, _ := bits.Mul64(r, uint64(n))
	return int(hi)
}

// deriveOnFirstDraw builds the vector for prefix by scanning the set's live members through
// the ordered index and appending each offset, installing it in the shard map. It is called
// under the shard mutex from the select paths when no vector exists yet, so two concurrent
// first-draws against the same set do not both build: the first takes the mutex and builds,
// the second finds the vector already present. The scan reads the same ordered run the
// enumeration commands walk, so it captures exactly the live members (the scan's liveness
// filter skips a tombstoned-but-not-yet-spliced node), and it runs while the shard mutex is
// held, so any add that lands after the scan goes through CollRandInsert against the now-
// present vector and is not lost. The build is O(card) once; every draw after is O(1).
func (s *Store) deriveOnFirstDraw(prefix []byte) *memberVec {
	v := newMemberVec(64)
	var after []byte
	buf := make([]uint64, 0, 512)
	for {
		buf = buf[:0]
		offs, last := s.oidx.scanBatch(prefix, after, 512, buf)
		for _, off := range offs {
			v.add(off)
		}
		if last == nil || len(offs) == 0 {
			break
		}
		after = append(after[:0], last...)
	}
	return v
}

// CollRandInsert records a newly-added member's offset in the set's dense vector for the
// whole-set (P=1) prefix, building the vector eagerly if it does not exist yet. Doc 20 makes
// the dense vector the authoritative membership structure, so it can no longer be lazy: a set
// that is only ever enumerated (SMEMBERS) and never drawn from still needs a vector to read.
// On the first write to a set with no vector this builds it whole through deriveOnFirstDraw,
// which scans the ordered index CollInsert has already updated with this member, so the built
// vector holds every live member; the resolve-and-add below is then idempotent for this
// member and only appends when the vector already existed. The whole-set vector needs no
// partition descriptor: CollRandDrop drops it directly under prefix (doc 20 section 6.1); the
// partition case goes through CollPartRandInsert instead so its vector is descriptor-named.
// Call it under the set's stripe lock, right after PutKind reports a newly-created member and
// CollInsert adds its ordered-index node, with the member's live composite key so the offset
// can be resolved through the authoritative hash index. It resolves the offset itself (the
// same find CollInsert does) rather than taking a caller-passed offset, so the two insert
// calls share the same failure mode: a member removed by a concurrent writer resolves to
// not-found and neither structure records it.
func (s *Store) CollRandInsert(prefix, key []byte, kind byte) {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		v = s.deriveOnFirstDraw(prefix)
		sh.put(prefix, v)
	}
	off, _, _, _, found := s.find(key, hash(key), kind)
	if found {
		v.add(off)
	}
	sh.mu.Unlock()
}

// CollRandRemove drops a member from the set's dense vector, if the vector exists, and
// reports whether it removed a slot. Call it under the set's stripe lock with the member's
// composite key BEFORE the hash record is deleted, so the offset is still resolvable through
// the authoritative index: the swap-remove is keyed by the offset the hash index reports for
// key, so once DeleteKind has cleared the record the offset can no longer be found. It is a
// no-op when no vector exists (the lazy contract) or when the member is not in the vector
// (already removed, or never recorded because the vector post-dates the member and a draw
// will rebuild the whole thing).
func (s *Store) CollRandRemove(prefix, key []byte, kind byte) bool {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		sh.mu.Unlock()
		return false
	}
	off, _, _, _, found := s.find(key, hash(key), kind)
	if !found {
		sh.mu.Unlock()
		return false
	}
	ok := v.remove(off)
	sh.mu.Unlock()
	return ok
}

// SetVecLen returns the number of live members in the set bounded by prefix, reading the dense
// member vector rather than counting the ordered index. It builds the vector on first use (the
// same lazy build the random draw runs) and reads the published snapshot length, so a set that
// has never been drawn from or enumerated pays one O(card) build and every read after is O(1).
// prefix is the whole-set prefix uvarint(len(skey))|skey for an unpartitioned set or one
// partition's scan prefix ...|byte(part) for a partitioned set.
func (s *Store) SetVecLen(prefix []byte) int {
	v := s.collPartVec(prefix)
	return len(v.view.Load().s)
}

// SetVecScanDown appends up to limit members of the set bounded by prefix to dst as arena-stable
// composite keys, walking the dense member vector's published snapshot downward from index hi
// (pass hi < 0 to start at the current top). It returns the grown slice and the next lower
// boundary, which is 0 once the vector is fully scanned. Walking high index to low is what keeps
// an SSCAN cursor stable under the vector's swap-remove: a remove moves the tail slot into the
// hole and shrinks, so a downward walk never skips a member that is present for the whole scan,
// and a shrink below the cursor clamps to the current top rather than reading past the end (spec
// 2064/f1_rewrite_ltm/20 section 5). SMEMBERS drives it under the set's stripe read lock, so the
// snapshot is frozen and one drained sequence of calls returns every member exactly once; SSCAN
// drives it lock-free across calls, so a concurrent add or remove may add or drop a member from
// the window, which the SCAN contract allows. It builds the vector on first use and is otherwise
// lock-free. The keys are arena subslices valid for the store's life, so the caller reads them
// without copying.
func (s *Store) SetVecScanDown(prefix []byte, hi, limit int, dst [][]byte) ([][]byte, int) {
	v := s.collPartVec(prefix)
	vs := v.view.Load()
	n := len(vs.s)
	upper := hi
	if hi < 0 || upper > n {
		upper = n
	}
	lo := upper - limit
	if lo < 0 {
		lo = 0
	}
	for i := upper - 1; i >= lo; i-- {
		dst = append(dst, s.keyAt(vs.s[i]))
	}
	return dst, lo
}

// SetVecAt returns the composite key of the member at dense index idx in the set bounded by prefix,
// or reports false when idx falls outside the current live count. It reads the same published vector
// snapshot the random draw and SetVecLen read, building the vector on first use, so a caller sampling
// several distinct indices under the set's stripe lock gets an O(1) array index per pick rather than
// the O(log n) order-statistic skip-list descent CollSelectAt walked. prefix is the whole-set prefix
// uvarint(len(skey))|skey; a partitioned set samples through the weighted partition draw, not this
// call. The out-of-range report lets a sampler that races a shrink retry its draw rather than read
// past the end. The key is an arena subslice valid for the store's life, read without copying.
func (s *Store) SetVecAt(prefix []byte, idx int) (key []byte, ok bool) {
	v := s.collPartVec(prefix)
	vs := v.view.Load()
	if idx < 0 || idx >= len(vs.s) {
		return nil, false
	}
	return s.keyAt(vs.s[idx]), true
}

// SetPartVecLen returns partition part's live member count for the P-partition set whose
// partition-scan base is base (uvarint(len(skey))|skey|<byte>, final byte rewritten to part
// internally). Unlike SetVecLen, which resolves a vector straight through the randVec shard, it
// resolves the partition vector through the set's partition descriptor, the same path the weighted
// draw uses (partDescFor then descPartVec). That is what keeps an enumerated-but-never-drawn
// partitioned set's vectors registered for descriptor-driven teardown: CollRandDrop drops only the
// partition vectors a descriptor names, so a partition vector built off a descriptor is torn down
// with the set (DEL, expiry, a grow's Phase 4 re-home) while one built straight through the shard
// would linger and go stale. p is the partition count; part is the partition to size.
func (s *Store) SetPartVecLen(base []byte, p, part int) int {
	d := s.partDescFor(base[:len(base)-1], p)
	v := s.descPartVec(d, base, part)
	return len(v.view.Load().s)
}

// SetPartVecScanDown is SetVecScanDown for one partition of a P-partition set, resolving the
// partition vector through the descriptor for the same teardown-registration reason SetPartVecLen
// documents. base is the partition-scan prefix whose final byte it rewrites to part; the downward
// walk from hi and the returned next-lower boundary are identical to SetVecScanDown.
func (s *Store) SetPartVecScanDown(base []byte, p, part, hi, limit int, dst [][]byte) ([][]byte, int) {
	d := s.partDescFor(base[:len(base)-1], p)
	v := s.descPartVec(d, base, part)
	vs := v.view.Load()
	n := len(vs.s)
	upper := hi
	if hi < 0 || upper > n {
		upper = n
	}
	lo := upper - limit
	if lo < 0 {
		lo = 0
	}
	for i := upper - 1; i >= lo; i-- {
		dst = append(dst, s.keyAt(vs.s[i]))
	}
	return dst, lo
}

// CollRandSelect returns the composite key of a uniform random live member of the set
// bounded by prefix, or reports empty. It builds the vector on the first draw against a set
// (deriveOnFirstDraw) and draws an O(1) array index thereafter. The returned key is a
// subslice of the immutable arena, valid for the store's life, so the caller reads it
// without copying and re-resolves the value (a set member has none) or returns it directly.
// It is the non-destructive draw behind SRANDMEMBER no-count.
//
// r is one random 64-bit word from the caller's own PRNG (the server's per-connection
// splitmix64), which is what lets the common case take no lock at all: many draws against one
// hot set run in parallel, each atomic-loading the vector's slots snapshot and picking its slot
// from its own random word, with no lock and no shared counter to serialize on (spec 2064/19
// slice 1). Only the first draw against a set, when no vector exists yet, takes the write mutex
// to build it, and that build happens once.
func (s *Store) CollRandSelect(prefix []byte, r uint64) (key []byte, ok bool) {
	sh := s.rvec.shardFor(prefix)
	// Fast path: the vector already exists, so the whole draw is lock-free. get loads the map
	// snapshot atomically, and view.Load reads a consistent slots snapshot, so a concurrent
	// mutation that republishes either pointer cannot tear this read.
	if v := sh.get(prefix); v != nil {
		vs := v.view.Load()
		n := len(vs.s)
		if n == 0 {
			return nil, false
		}
		return s.keyAt(vs.s[drawIndexWord(r, n)]), true
	}

	// No vector yet: build it under the write mutex. Re-check after taking the mutex so two
	// concurrent first-draws build only once (the loser finds the winner's vector present).
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		v = s.deriveOnFirstDraw(prefix)
		sh.put(prefix, v)
	}
	sh.mu.Unlock()
	vs := v.view.Load()
	n := len(vs.s)
	if n == 0 {
		return nil, false
	}
	return s.keyAt(vs.s[drawIndexWord(r, n)]), true
}

// CollRandSelectRemove draws a uniform random live member and swap-removes it from the
// vector in one critical section, returning its composite key. It is the fused select-then-
// remove the no-count SPOP hot path runs: the caller still deletes the member's hash record
// through DeleteKind (the returned key is exactly that row's key) and enqueues the ordered-
// index tombstone, but the vector's O(1) pick and swap-remove happen here under one shard
// mutex acquisition instead of a separate select and remove. Because the returned offset is
// resolved to its key before the shard mutex is released and the arena never moves a
// published record, the returned key stays valid after the caller drops the hash record.
func (s *Store) CollRandSelectRemove(prefix []byte) (key []byte, ok bool) {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		v = s.deriveOnFirstDraw(prefix)
		sh.put(prefix, v)
	}
	n := len(v.slots)
	if n == 0 {
		sh.mu.Unlock()
		return nil, false
	}
	i := sh.drawIndex(n)
	off := v.slots[i]
	v.remove(off)
	sh.mu.Unlock()
	return s.keyAt(off), true
}

// CollRandEnsure builds the vector for prefix if it does not exist yet, so a caller can warm
// a large set's random-access structure ahead of a draw storm rather than paying the O(card)
// build on the first draw. It is idempotent and safe to call on any set; the select paths
// call the same builder lazily, so this is an explicit-warmup convenience and a test hook,
// not a requirement.
func (s *Store) CollRandEnsure(prefix []byte) {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	if sh.get(prefix) == nil {
		sh.put(prefix, s.deriveOnFirstDraw(prefix))
	}
	sh.mu.Unlock()
}

// CollRandDrop discards the whole vector and back-index for a destroyed set in one step, for
// the paths that drop a set at once (DEL, expiry, FLUSHDB's per-key drop, an overwrite by a
// different-typed value). It is O(1) in the vector rather than the O(card) a member-by-member
// swap-remove would cost, and a no-op when the set had no vector.
func (s *Store) CollRandDrop(prefix []byte) {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	sh.drop(prefix)
	sh.mu.Unlock()
	// If this set engaged partitioning, its P partition vectors live under partition-prefixed keys
	// this unpartitioned prefix does not cover, and a partition descriptor caches their pointers.
	// dropPartVecs drops both so a recreated set under the same key rebuilds fresh rather than
	// reading a stale cached pointer as empty (partdesc.go). It is a cheap lock-free descriptor probe
	// that returns at once for the common unpartitioned set, which has no descriptor.
	s.dropPartVecs(prefix)
}

// RENAME has no vector re-key primitive on purpose. The member rows carry their set's key name
// inside their composite key, so a rename cannot rewrite a member's key in place: moveIndexedFamily
// publishes a fresh record under the new prefix and deletes the old one, which gives every moved
// member a new arena offset. An offset-preserving map move would therefore leave the vector
// pointing at the deleted source records, so RENAME instead drops the source vector (CollRandDrop)
// and lets the destination build its own on first draw from the freshly-published rows.
