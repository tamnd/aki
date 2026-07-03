package f1raw

import (
	"math/bits"
	"sync"
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
// writers, which the server's per-key stripe lock already serializes. A shard's mutex
// guards its map and every memberVec in it. The lock order is stripe lock (server, per key)
// then vector shard mutex then oindex mutex; a draw takes only the shard mutex, a mutating
// command holds its stripe lock and takes the shard mutex under it, and the ordered-index
// folder takes only oi.mu, so the three never form a cycle.

// randVecShards is the number of independent stripes the vector map is split into. It is a
// power of two so a prefix hash maps to a shard with a mask, and it is sized well above the
// core count so two hot sets almost never share a shard and so the shard mutex is
// effectively a per-set lock the server's stripe lock already implies.
const randVecShards = 256

// memberVec is one set's dense vector plus its back-index. slots holds the arena offsets of
// the set's live member rows with no holes, so len(slots) is the cardinality and slots[i]
// is a uniform random member for i drawn in [0, len). back maps an offset to its slot so a
// remove finds the victim's slot in O(1) rather than scanning. The invariant after every
// operation is len(slots) == len(back) and back[slots[i]] == i for all i.
type memberVec struct {
	slots []uint64
	back  map[uint64]int
}

func newMemberVec(capHint int) *memberVec {
	return &memberVec{
		slots: make([]uint64, 0, capHint),
		back:  make(map[uint64]int, capHint),
	}
}

// add appends off as a new member. It is idempotent against a duplicate offset: an offset
// is unique per live record and a re-added member is a fresh record at a new offset, so a
// duplicate should never occur, but skipping one keeps a stray double-call from corrupting
// the density invariant rather than silently biasing the draw toward the doubled member.
func (v *memberVec) add(off uint64) {
	if _, ok := v.back[off]; ok {
		return
	}
	v.back[off] = len(v.slots)
	v.slots = append(v.slots, off)
}

// remove swap-drops the member at off and reports whether it was present. It reads the
// victim's slot from back, moves the last slot's offset into the hole, fixes the moved
// offset's back-index entry, shortens by one, and deletes the victim's entry. When the
// victim is itself the last slot the move is a self-assignment and harmless. It is O(1)
// and touches only the one moved member's slot, which is what keeps a delete burst O(k)
// rather than O(k*n).
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
	return true
}

// randVecShard is one stripe of the vector map: a mutex, the prefix-keyed vectors it holds,
// and a per-shard PRNG for the uniform draw. The PRNG is a splitmix64 counter mixed on each
// draw, seeded distinctly per shard so two shards do not draw identical sequences; it needs
// no lock beyond the shard mutex already held for the draw and no global rand source, so a
// draw allocates nothing and touches no shared counter.
type randVecShard struct {
	mu  sync.Mutex
	m   map[string]*memberVec
	rng uint64
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

// get returns the vector for prefix, or nil if the shard has none. The shard map is created
// lazily on the first put, so a shard that has never held a vector reads as a nil map, which
// returns the zero value without allocating. The caller holds the shard mutex.
func (sh *randVecShard) get(prefix []byte) *memberVec {
	if sh.m == nil {
		return nil
	}
	return sh.m[string(prefix)]
}

// put installs v under prefix, creating the shard map on first use so an untouched shard
// costs nothing. The caller holds the shard mutex.
func (sh *randVecShard) put(prefix []byte, v *memberVec) {
	if sh.m == nil {
		sh.m = make(map[string]*memberVec)
	}
	sh.m[string(prefix)] = v
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

// CollRandInsert records a newly-added member's offset in the set's dense vector, if the
// vector already exists. It is a no-op when no vector exists for prefix, which is the lazy
// contract: a set that has never been drawn from has no vector, and building a partial one
// from a single insert would leave the earlier members unreachable by a draw, so the vector
// is instead built whole by the first draw's deriveOnFirstDraw. Call it under the set's
// stripe lock, right after PutKind reports a newly-created member and CollInsert adds its
// ordered-index node, with the member's live composite key so the offset can be resolved
// through the authoritative hash index. It resolves the offset itself (the same find
// CollInsert does) rather than taking a caller-passed offset, so the two insert calls share
// the same failure mode: a member removed by a concurrent writer resolves to not-found and
// neither structure records it.
func (s *Store) CollRandInsert(prefix, key []byte, kind byte) {
	sh := s.rvec.shardFor(prefix)
	sh.mu.Lock()
	v := sh.get(prefix)
	if v == nil {
		sh.mu.Unlock()
		return
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

// CollRandSelect returns the composite key of a uniform random live member of the set
// bounded by prefix, or reports empty. It builds the vector on the first draw against a set
// (deriveOnFirstDraw) and draws an O(1) array index thereafter. The returned key is a
// subslice of the immutable arena, valid for the store's life, so the caller reads it
// without copying and re-resolves the value (a set member has none) or returns it directly.
// It is the non-destructive draw behind SRANDMEMBER no-count.
func (s *Store) CollRandSelect(prefix []byte) (key []byte, ok bool) {
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
	off := v.slots[sh.drawIndex(n)]
	sh.mu.Unlock()
	return s.keyAt(off), true
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
	delete(sh.m, string(prefix))
	sh.mu.Unlock()
}

// CollRandRekey moves a set's vector from oldPrefix to newPrefix, for RENAME: the member
// rows do not move under a rename (only the key's prefix binding changes), so their arena
// offsets stay valid and the vector is re-keyed rather than rebuilt. When the source has no
// vector this is a no-op and the destination will build one lazily on its first draw. When
// the two prefixes hash to different shards the vector moves between shards under both
// mutexes, taken in a fixed order (lower shard index first) so two concurrent rekeys of the
// same pair cannot deadlock.
func (s *Store) CollRandRekey(oldPrefix, newPrefix []byte) {
	oi := hash(oldPrefix) & (randVecShards - 1)
	ni := hash(newPrefix) & (randVecShards - 1)
	if oi == ni {
		sh := &s.rvec.shards[oi]
		sh.mu.Lock()
		if v := sh.get(oldPrefix); v != nil {
			delete(sh.m, string(oldPrefix))
			sh.put(newPrefix, v)
		}
		sh.mu.Unlock()
		return
	}
	lo, hiIdx := oi, ni
	if lo > hiIdx {
		lo, hiIdx = hiIdx, lo
	}
	loSh, hiSh := &s.rvec.shards[lo], &s.rvec.shards[hiIdx]
	loSh.mu.Lock()
	hiSh.mu.Lock()
	src := &s.rvec.shards[oi]
	dst := &s.rvec.shards[ni]
	if v := src.get(oldPrefix); v != nil {
		delete(src.m, string(oldPrefix))
		dst.put(newPrefix, v)
	}
	hiSh.mu.Unlock()
	loSh.mu.Unlock()
}
