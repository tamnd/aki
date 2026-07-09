package setmergefloor

import (
	"encoding/binary"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// sink keeps the compiler from folding the benchmarked work away.
var sink int

// splitmix64 is a small deterministic PRNG so the fixtures are reproducible without touching wall
// clock or the global rand source (labs run under go test, but a fixed stream keeps a re-run's
// numbers comparable). Each call advances the state and returns a well-mixed 64-bit word.
type splitmix64 struct{ s uint64 }

func (r *splitmix64) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// hashKey is a word-at-a-time mixing hash over a byte key, standing in for engine/f1raw's hash(): the
// engine hashes the composite memberKey the same way, folding the key bytes 8 at a time. Its cost is
// what the point-probe pays per member and the merge does not, because the merge compares member
// hashes the async folder already computed off the read path.
func hashKey(b []byte) uint64 {
	var h uint64 = 0x9e3779b97f4a7c15
	i := 0
	for ; i+8 <= len(b); i += 8 {
		h ^= binary.LittleEndian.Uint64(b[i : i+8])
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33
	}
	var tail uint64
	for s := 0; i < len(b); i++ {
		tail |= uint64(b[i]) << (s * 8)
		s++
	}
	h ^= tail
	h *= 0xc4ceb9fe1a85ec53
	return h ^ (h >> 33)
}

// memberHash hashes a member's 8-byte payload alone, the set-independent hash the folder stores in the
// sorted array so the same member in two different sets produces the same uint64 the two-pointer walk
// matches on. The merge compares these, already computed; it does no hashing at SINTER time.
func memberHash(member uint64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], member)
	return hashKey(buf[:])
}

// buildMemberKey writes the composite key the engine probes membership with: uvarint(len(setKey)) |
// setKey | member. setMemberExists builds exactly this into a scratch buffer before hashing it, so the
// probe pays the length prefix, the setKey copy, and the member copy on every single member it checks.
// dst is a caller-owned scratch buffer reused across probes so the build itself does not allocate.
func buildMemberKey(dst []byte, setKey string, member uint64) []byte {
	dst = dst[:0]
	dst = binary.AppendUvarint(dst, uint64(len(setKey)))
	dst = append(dst, setKey...)
	var m [8]byte
	binary.LittleEndian.PutUint64(m[:], member)
	dst = append(dst, m[:]...)
	return dst
}

// compositeHash is the full per-probe cost: build the composite key, then hash it. This is what
// ExistsKind runs for every member the probe drives off, and the cost the merge sheds by pre-folding.
func compositeHash(scratch []byte, setKey string, member uint64) uint64 {
	return hashKey(buildMemberKey(scratch, setKey, member))
}

// globalIndex mirrors engine/f1raw's find: a single open-addressed table shared by every set in the
// keyspace, not a per-set dict. It is deliberately large and filled with many sets' composite keys so
// a lookup for one set's member walks a cache-cold table, the structural cost the point-probe pays and
// Redis/Valkey's per-set cache-hot dict does not. The table is a flat []uint64 of stored composite
// keys (0 = empty), power-of-two capacity, linear probe from the home slot.
type globalIndex struct {
	slots []uint64
	mask  uint64
}

func newGlobalIndex(capacityPow2 int) *globalIndex {
	n := 1 << capacityPow2
	return &globalIndex{slots: make([]uint64, n), mask: uint64(n - 1)}
}

func (g *globalIndex) insertHash(key uint64) {
	if key == 0 {
		key = 1 // reserve 0 as the empty sentinel; collapsing one key is harmless for the model
	}
	i := key & g.mask
	for g.slots[i] != 0 {
		if g.slots[i] == key {
			return
		}
		i = (i + 1) & g.mask
	}
	g.slots[i] = key
}

func (g *globalIndex) hasHash(key uint64) bool {
	if key == 0 {
		key = 1
	}
	i := key & g.mask
	for {
		s := g.slots[i]
		if s == 0 {
			return false
		}
		if s == key {
			return true
		}
		i = (i + 1) & g.mask
	}
}

// mergeSet mirrors a set's sorted member-hash array plus the per-call machinery the merge path runs
// before it walks the array: SyncSortedHashes (drain the fold journal so the array is current) and
// PinMerge (take the partition RLock, snapshot the array header so the two-pointer walk reads a stable
// slice). At steady state the journal is empty, so sync is a cheap atomic check, but the pin's
// lock/unlock and the snapshot are paid on every call regardless of cardinality. That fixed per-call
// cost is what the probe (which drives straight off the smaller source with no pin) avoids, and it is
// why the merge only wins once the array is long enough to amortize it.
type mergeSet struct {
	mu         sync.RWMutex
	sorted     []uint64
	journalLen atomic.Int64
}

func newMergeSet(members []uint64) *mergeSet {
	s := make([]uint64, len(members))
	for i, m := range members {
		s[i] = memberHash(m)
	}
	slices.Sort(s)
	return &mergeSet{sorted: s}
}

// sync models SyncSortedHashes: fold any pending journal entries into the sorted array. A steady-state
// read (the array is already current) loads the journal length and returns, the common case a repeated
// SINTER on unchanged sources hits.
func (s *mergeSet) sync() {
	if s.journalLen.Load() == 0 {
		return
	}
}

// pin models PinMerge: RLock the partition set, snapshot the sorted-array header, RUnlock, so the walk
// reads a slice that cannot be reallocated under it. For a set below the partition threshold this is a
// single header, but the lock round-trip is the fixed cost the floor weighs against the probe.
func (s *mergeSet) pin() []uint64 {
	s.mu.RLock()
	v := s.sorted
	s.mu.RUnlock()
	return v
}

// mergeSINTER is the merge read: sync and pin both sources, then two-pointer the pre-folded sorted
// member-hash arrays, collecting the intersection into a result buffer (the SINTER reply shape;
// SINTERCARD would skip the buffer and only count). It hashes nothing and probes no shared table: the
// arrays are compact and per-set, so the walk stays in each core's own cache. Cost = two fixed pins
// plus a linear pass, i.e. ns/op = fixed_pin_cost + c*(|a|+|b|).
func mergeSINTER(a, b *mergeSet) int {
	a.sync()
	b.sync()
	av := a.pin()
	bv := b.pin()
	out := make([]uint64, 0, min(len(av), len(bv)))
	i, j := 0, 0
	for i < len(av) && j < len(bv) {
		switch {
		case av[i] < bv[j]:
			i++
		case av[i] > bv[j]:
			j++
		default:
			out = append(out, av[i])
			i++
			j++
		}
	}
	return len(out)
}

// probeSINTER is the point-probe read the driver keeps below the floor: drive off the smaller source
// and, for each of its members, build the composite key, hash it, and ask the shared global index
// whether the other set contains it. Each member therefore pays a key build, a word-at-a-time hash,
// and a scattered cache-cold table lookup, none of which the merge pays. It has almost no fixed cost,
// which is why it wins for tiny sources, but that per-member cost is what the merge sheds at scale.
func probeSINTER(g *globalIndex, otherSetKey string, smaller []uint64) int {
	var scratch [64]byte
	n := 0
	for _, m := range smaller {
		if g.hasHash(compositeHash(scratch[:], otherSetKey, m)) {
			n++
		}
	}
	return n
}

// fixture holds one SINTER(A,B) instance at a given per-source cardinality: A and B each have n
// members overlapping in half, the sorted member-hash arrays for the merge, and the shared global
// index seeded with B's composites so the probe can ask "is A's member in B".
type fixture struct {
	setKeyB  string
	aMembers []uint64
	setA     *mergeSet
	setB     *mergeSet
	index    *globalIndex
}

// The cold global index is sized to defeat the cache, not just L2. A 2^25-slot table is 256 MB and a
// ~50% fill scatters ~16M live keys through it, so a probe misses even the large shared last-level
// cache of a modern desktop (the M4's SLC, the GamingPC's L3) and pays a real DRAM latency per member,
// which is the cost the live three-way SINTER pays and labs/seteager's compact per-set probe table hid.
const (
	coldCapPow2 = 25      // 2^25 slots * 8 B = 256 MB
	fillEntries = 1 << 24 // ~16M filler keys, ~50% occupancy
)

// coldIndex is the shared cache-cold filler, built once and reused by every fixture: rebuilding a
// 256 MB table per cardinality would dominate the run for no modelling gain, and each fixture keys its
// own set B under a distinct set key so the shared table never aliases another fixture's members.
var (
	coldOnce  sync.Once
	coldIndex *globalIndex
)

func sharedColdIndex() *globalIndex {
	coldOnce.Do(func() {
		coldIndex = newGlobalIndex(coldCapPow2)
		fr := splitmix64{s: 0xf111}
		for range fillEntries {
			coldIndex.insertHash(fr.next())
		}
	})
	return coldIndex
}

// buildInstance makes one SINTER(A,B) at cardinality n under a distinct set-B key, seeding B's
// composites into the shared cold index. A and B overlap in half: the first n/2 members are shared, so
// both the merge (member-hash equality) and the probe (composite hit) report exactly n/2.
func buildInstance(n int, seed uint64, setKeyB string) *fixture {
	r := splitmix64{s: seed}
	shared := make([]uint64, n/2)
	for i := range shared {
		shared[i] = r.next()
	}
	aMembers := make([]uint64, 0, n)
	bMembers := make([]uint64, 0, n)
	aMembers = append(aMembers, shared...)
	bMembers = append(bMembers, shared...)
	for len(aMembers) < n {
		aMembers = append(aMembers, r.next())
	}
	for len(bMembers) < n {
		bMembers = append(bMembers, r.next())
	}
	idx := sharedColdIndex()
	var scratch [64]byte
	for _, m := range bMembers {
		idx.insertHash(compositeHash(scratch[:], setKeyB, m))
	}
	return &fixture{
		setKeyB:  setKeyB,
		aMembers: aMembers,
		setA:     newMergeSet(aMembers),
		setB:     newMergeSet(bMembers),
		index:    idx,
	}
}

func newFixture(n int) *fixture {
	return buildInstance(n, 0x5eed+uint64(n), "set:b:"+strconv.Itoa(n))
}

// newFixtures builds w independent SINTER(A,B) instances at cardinality n, each keyed under its own
// set-B key, all seeded into the one shared cold index. The w instances give the concurrent benches w
// distinct scattered probe regions so the workers thrash the shared last-level cache the way w live
// connections each probing a different pair of sets do, rather than all re-reading one hot region.
func newFixtures(n, w int) []*fixture {
	fs := make([]*fixture, w)
	for k := range w {
		fs[k] = buildInstance(n, 0x51ce+uint64(n)*1000+uint64(k), "set:b:"+strconv.Itoa(n)+":"+strconv.Itoa(k))
	}
	return fs
}

// cardinalities brackets the small-N crossover the floor gates: from 8 (deep in probe territory)
// through 128 (the settled floor) to 1024 (the old seteager floor the live gate proved too high).
var cardinalities = []int{8, 16, 32, 64, 128, 256, 512, 1024}

// BenchmarkFloorMerge runs the sorted-array merge at each per-source cardinality. Its ns/op is
// fixed_pin_cost + linear_walk; compared against BenchmarkFloorProbe at the same n, the crossover is
// the n where the merge's fixed cost is finally amortized by the probe's per-member build+hash+miss.
func BenchmarkFloorMerge(b *testing.B) {
	for _, n := range cardinalities {
		f := newFixture(n)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			for b.Loop() {
				sink = mergeSINTER(f.setA, f.setB)
			}
		})
	}
}

// BenchmarkFloorProbe runs the point-probe off the smaller source into the cache-cold global index at
// each per-source cardinality (both sources are equal here, so it drives off A). The crossover with
// BenchmarkFloorMerge is where the merge overtakes it. Unlike labs/seteager, which probed a compact
// per-set table with no key build and so put the crossover near 1024, this pays the composite-key
// build, the hash, and the shared-index miss the live engine actually pays per member, which is why
// the crossover lands far lower and the floor drops to 128.
func BenchmarkFloorProbe(b *testing.B) {
	for _, n := range cardinalities {
		f := newFixture(n)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			for b.Loop() {
				sink = probeSINTER(f.index, f.setKeyB, f.aMembers)
			}
		})
	}
}

// BenchmarkFloorMergePar runs the merge under GOMAXPROCS concurrent workers, each on its own tiny
// sorted arrays. Because those arrays are per-worker and fit in each core's L1, concurrency barely
// moves the merge's per-op cost: the sequential walk of a hot array is bandwidth the cores each own.
func BenchmarkFloorMergePar(b *testing.B) {
	for _, n := range cardinalities {
		fs := newFixtures(n, 64)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				f := fs[int(next.Add(1)-1)%len(fs)]
				local := 0
				for pb.Next() {
					local = mergeSINTER(f.setA, f.setB)
				}
				sink = local
			})
		})
	}
}

// BenchmarkFloorProbePar runs the probe under GOMAXPROCS concurrent workers, each probing a distinct
// scattered region of the one shared cold index. This is the regime the GamingPC gate measures: many
// connections all point-probing the shared global index at once, evicting each other's lines from the
// last-level cache so each probe pays a real miss on top of the per-member build and hash. Under this
// contention the probe's per-member cost rises while the merge's does not, so the crossover with
// BenchmarkFloorMergePar collapses below the uncontended crossover, which is why the floor is set for
// this regime and lands at 128 rather than the seteager single-key 1024.
func BenchmarkFloorProbePar(b *testing.B) {
	for _, n := range cardinalities {
		fs := newFixtures(n, 64)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				f := fs[int(next.Add(1)-1)%len(fs)]
				local := 0
				for pb.Next() {
					local = probeSINTER(f.index, f.setKeyB, f.aMembers)
				}
				sink = local
			})
		})
	}
}

// TestModelsAgree pins that the probe and the merge report the same intersection size at every swept
// cardinality, so a benchmark that is fast because it is wrong gets caught. Both must equal n/2, the
// shared half the fixture builds.
func TestModelsAgree(t *testing.T) {
	for _, n := range cardinalities {
		f := newFixture(n)
		want := n / 2
		if got := mergeSINTER(f.setA, f.setB); got != want {
			t.Fatalf("n=%d: merge = %d, want %d", n, got, want)
		}
		if got := probeSINTER(f.index, f.setKeyB, f.aMembers); got != want {
			t.Fatalf("n=%d: probe = %d, want %d", n, got, want)
		}
	}
}
