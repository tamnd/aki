package f1raw

import (
	"sync"
	"sync/atomic"
)

// The partition descriptor is the fix for slice 4's inverted contention scaling (spec
// 2064/f1_rewrite_ltm/19 sections 5 and 11.4). Slice 4 routed the weighted draw correctly, but its
// count read resolved each of the P partition vectors with a shard-map lookup (weightedCounts ->
// collPartVec -> sh.get), so a single draw cost O(P) map probes and the per-op cost rose with P
// instead of falling: the map-lookup tax grew faster than splitting the tiny pop critical section
// across P locks saved. Section 5's intended count source is per-set state that answers the P
// counts with no map lookup, so the weighted read becomes O(P) atomic loads and the lock split
// finally shows as throughput that rises with P.
//
// The descriptor caches the P partition-vector pointers of one set, keyed by the set's whole-set
// prefix uvarint(len(skey))|skey (no partition byte). A draw resolves the descriptor once (one
// lock-free map load), then reads each partition's count as len(vec.view.Load().s) off the cached
// pointer, an atomic load with no map probe. A partition's pointer is filled lazily the first time
// a draw touches that partition (the same lazy build the vectors already do), so a set never drawn
// from allocates no descriptor and a partition never drawn from caches no pointer.
//
// Invalidation is the whole reason this is its own slice. A cached pointer to a dropped-then-
// rebuilt vector would misread a recreated set as empty, so the descriptor must die exactly when
// the set's vectors do. CollRandDrop is the single set-teardown primitive (DEL, expiry, FLUSHDB's
// per-key drop, an overwrite by a different-typed value, and RENAME's source drop all route through
// it), so CollRandDrop is made descriptor-aware: it consults the descriptor for the dropped set,
// drops the P partition vectors it names, and drops the descriptor, all under the same call the
// unpartitioned drop already made. Because a partition vector is dropped only through that path and
// that path also drops the descriptor, a cached pointer is never stale for a live descriptor: the
// pointer and the vector it names are torn down together. This also closes a latent slice-4 gap,
// that CollRandDrop dropped only the unpartitioned prefix and left the P partition vectors resident
// for a set that had engaged partitioning.

// partDescShards is the stripe count of the descriptor map, a power of two so a prefix hashes to a
// shard with a mask, sized like the vector map so two hot sets almost never share a shard.
const partDescShards = 256

// partVecDesc caches one partitioned set's P partition-vector pointers. p is the partition count the
// set engaged; vecs holds one atomically-published pointer per partition, nil until the first draw
// against that partition resolves and installs it. The pointers alias the memberVecs the randVec
// shards hold, so the descriptor adds no second copy of a vector: it is a lookup shortcut, an O(P)
// atomic-load count read in place of O(P) shard-map probes.
type partVecDesc struct {
	p    int
	vecs []atomic.Pointer[memberVec]
}

func newPartVecDesc(p int) *partVecDesc {
	return &partVecDesc{p: p, vecs: make([]atomic.Pointer[memberVec], p)}
}

// partDescMap is one shard's immutable prefix-to-descriptor snapshot, swapped by copy-on-write so a
// lock-free reader walks a consistent map (the same discipline vecMap uses).
type partDescMap struct {
	m map[string]*partVecDesc
}

// partDescShard is one stripe of the descriptor map: a write mutex the structural changes serialize
// on and an atomically-published prefix-to-descriptor map a resolve loads without a lock.
type partDescShard struct {
	mu   sync.Mutex
	view atomic.Pointer[partDescMap]
}

// partDescs is the store's whole descriptor table, a fixed array of shards created empty and
// populated lazily, so a keyspace that never draws from a partitioned set allocates nothing here.
type partDescs struct {
	shards [partDescShards]partDescShard
}

func newPartDescs() *partDescs {
	return &partDescs{}
}

// shardFor returns the shard a set prefix maps to, hashing the prefix with the store's own hash and
// masking to the shard count.
func (pd *partDescs) shardFor(prefix []byte) *partDescShard {
	return &pd.shards[hash(prefix)&(partDescShards-1)]
}

// get returns the descriptor for prefix, or nil if the shard has none. It loads the published map
// snapshot, so it is safe with no lock (the resolve fast path) and under the write mutex alike.
func (sh *partDescShard) get(prefix []byte) *partVecDesc {
	pm := sh.view.Load()
	if pm == nil {
		return nil
	}
	return pm.m[string(prefix)]
}

// put installs d under prefix by copy-on-write, so a concurrent lock-free resolve walks either the
// old or the new map, never a half-updated one. The caller holds the shard write mutex.
func (sh *partDescShard) put(prefix []byte, d *partVecDesc) {
	old := sh.view.Load()
	n := 1
	if old != nil {
		n += len(old.m)
	}
	nm := make(map[string]*partVecDesc, n)
	if old != nil {
		for k, v := range old.m {
			nm[k] = v
		}
	}
	nm[string(prefix)] = d
	sh.view.Store(&partDescMap{m: nm})
}

// drop removes prefix's descriptor by the same copy-on-write swap, a no-op when the shard has no
// such descriptor. The caller holds the shard write mutex.
func (sh *partDescShard) drop(prefix []byte) {
	old := sh.view.Load()
	if old == nil {
		return
	}
	if _, ok := old.m[string(prefix)]; !ok {
		return
	}
	nm := make(map[string]*partVecDesc, len(old.m))
	for k, v := range old.m {
		if k == string(prefix) {
			continue
		}
		nm[k] = v
	}
	sh.view.Store(&partDescMap{m: nm})
}

// partDescFor resolves the descriptor for the set whose whole-set prefix is setPrefix, creating it
// with p partitions on the first draw against the set. The fast path is a lock-free map load; only
// the first draw takes the shard write mutex to install a fresh descriptor. If a descriptor exists
// but carries a different partition count than p (a set that re-engaged partitioning at a new P), it
// is replaced, so the descriptor always matches the P the caller is drawing under. setPrefix must be
// the prefix without a partition byte; the caller passes base[:len(base)-1].
func (s *Store) partDescFor(setPrefix []byte, p int) *partVecDesc {
	sh := s.pdescs.shardFor(setPrefix)
	if d := sh.get(setPrefix); d != nil && d.p == p {
		return d
	}
	sh.mu.Lock()
	d := sh.get(setPrefix)
	if d == nil || d.p != p {
		d = newPartVecDesc(p)
		sh.put(setPrefix, d)
	}
	sh.mu.Unlock()
	return d
}

// descPartVec returns partition i's vector through the descriptor, resolving and caching the pointer
// on the first touch. The fast path is one atomic load of the cached pointer. On a miss it rewrites
// base's final byte to i, resolves the vector through the randVec shard (a map load, or a lazy build
// under that shard's write mutex on the very first draw against the partition), and installs the
// pointer with a compare-and-swap. collPartVec is idempotent, so two racing misses resolve the same
// vector object and the loser's CAS is harmless; either way the installed pointer is returned. base
// must be the partition-scan prefix whose final byte this rewrites.
func (s *Store) descPartVec(d *partVecDesc, base []byte, i int) *memberVec {
	if v := d.vecs[i].Load(); v != nil {
		return v
	}
	base[len(base)-1] = byte(i)
	v := s.collPartVec(base)
	d.vecs[i].CompareAndSwap(nil, v)
	return d.vecs[i].Load()
}

// weightedCountsDesc reads the P partition counts into counts through the descriptor and returns
// their sum. Each count is len(vec.view.Load().s) off the cached partition pointer, so after the
// first draw warms every pointer the whole reduction is P atomic loads with no map probe, which is
// the O(P) atomic-load count read section 5 calls for and the fix for slice 4's O(P) map lookups.
// base's final byte is rewritten per partition to resolve any not-yet-cached pointer; counts must
// have room for at least d.p entries.
func (s *Store) weightedCountsDesc(d *partVecDesc, base []byte, counts []int) (total int) {
	for i := 0; i < d.p; i++ {
		v := s.descPartVec(d, base, i)
		n := len(v.view.Load().s)
		counts[i] = n
		total += n
	}
	return total
}

// dropPartVecs tears down a partitioned set's descriptor and the P partition vectors it names, the
// partition-aware half of CollRandDrop. It loads the descriptor for setPrefix lock-free and returns
// at once when there is none (the common case: an unpartitioned set, or a non-set key, has no
// descriptor). When a descriptor exists it drops each partition's vector from its randVec shard and
// then drops the descriptor, so a recreated set under the same key rebuilds fresh pointers rather
// than reading a stale cached one as empty. setPrefix must be the whole-set prefix without a
// partition byte, exactly what CollRandDrop already receives.
func (s *Store) dropPartVecs(setPrefix []byte) {
	dsh := s.pdescs.shardFor(setPrefix)
	d := dsh.get(setPrefix)
	if d == nil {
		return
	}
	base := make([]byte, 0, len(setPrefix)+1)
	base = append(base, setPrefix...)
	base = append(base, 0)
	last := len(base) - 1
	for i := 0; i < d.p; i++ {
		base[last] = byte(i)
		vsh := s.rvec.shardFor(base)
		vsh.mu.Lock()
		vsh.drop(base)
		vsh.mu.Unlock()
	}
	dsh.mu.Lock()
	dsh.drop(setPrefix)
	dsh.mu.Unlock()
}
