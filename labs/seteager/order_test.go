package seteager

import (
	"encoding/binary"
	"slices"
	"sort"
	"strconv"
	"testing"
)

// This file models the write and read costs of keeping a SET's members in hash
// order three ways, so the choice of eager-order container for the SINTER merge is
// a measurement rather than a guess. Nothing here imports aki; the containers are
// plain-Go models of the three shapes doc.go describes. Fixtures are built before
// b.Loop so the timed region is only the operation under test.

const (
	// labN sizes the merge-read and steady-state-insert fixtures. At 1<<20 a
	// partition of a P=64 set holds ~16k members, a real large-set shape, and the
	// flat array is 8 MiB of uint64, well past L2 so the merge is memory-bound the
	// way the real one is.
	labN = 1 << 20
	// buildN sizes the from-empty build benchmarks. It is smaller than labN because
	// the flat sorted build is O(n^2): at 1<<16 that is ~4.3e9 shift words, about a
	// second, enough to show the quadratic without making the suite unbearable.
	buildN = 1 << 16
)

// hashBytes is f1raw's index hash (engine/f1raw/f1raw.go hash), copied so the lab
// distributes members across partitions and sort order exactly as the real engine.
func hashBytes(b []byte) uint64 {
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
		for i := range len(b) {
			t |= uint64(b[i]) << (8 * uint(i))
		}
		h = mulFold(h^t, s2)
	}
	return mulFold(h, s1)
}

func mulFold(a, b uint64) uint64 {
	hi, lo := mul64(a, b)
	return hi ^ lo
}

// mul64 is bits.Mul64 spelled out so the lab has no dependency beyond the stdlib
// primitives, matching the engine's 64x64 fold.
func mul64(a, b uint64) (hi, lo uint64) {
	const mask = 0xffffffff
	al, ah := a&mask, a>>32
	bl, bh := b&mask, b>>32
	t := al * bl
	w0 := t & mask
	k := t >> 32
	t = ah*bl + k
	w1 := t & mask
	w2 := t >> 32
	t = al*bh + w1
	k = t >> 32
	hi = ah*bh + w2 + k
	lo = (t << 32) + w0
	return hi, lo
}

// memberHashes returns count distinct member hashes, hashed from m0..m{count-1} so
// the distribution across partitions and sort order matches the real engine's.
func memberHashes(count int) []uint64 {
	hs := make([]uint64, count)
	for i := range hs {
		hs[i] = hashBytes([]byte("member:" + strconv.Itoa(i)))
	}
	return hs
}

// overlapHashes returns two hash sets of labN each, overlapping in their middle half,
// the SINTER(A,B) shape the read benchmarks intersect.
func overlapHashes() (a, b []uint64) {
	all := memberHashes(labN + labN/2)
	a = append([]uint64(nil), all[:labN]...)
	b = append([]uint64(nil), all[labN/2:labN/2+labN]...)
	return a, b
}

// --- flat sorted array ----------------------------------------------------------

// insertFlat inserts h into a hash-sorted slice, keeping it sorted: a binary search
// for the position, then a tail memmove to open the slot. O(n) per insert, the price
// of one flat array per set. Duplicate hashes are allowed in (a real set dedups on
// the member, not the hash); the lab never inserts a duplicate so it does not matter.
func insertFlat(s []uint64, h uint64) []uint64 {
	i := sort.Search(len(s), func(k int) bool { return s[k] >= h })
	s = append(s, 0)
	copy(s[i+1:], s[i:])
	s[i] = h
	return s
}

// buildFlat builds a sorted array of the first n member hashes by repeated sorted
// insert, the O(n^2) naive-eager build.
func buildFlat(n int) []uint64 {
	hs := memberHashes(n)
	s := make([]uint64, 0, n)
	for _, h := range hs {
		s = insertFlat(s, h)
	}
	return s
}

// --- partitioned sorted arrays --------------------------------------------------

// partedSet is P hash-sorted arrays, member h in partition h&(P-1). This is f1raw's
// doc 19 partitioning with each partition additionally kept in hash order.
type partedSet struct {
	p     int
	parts [][]uint64
}

func newPartedSet(p int) *partedSet {
	return &partedSet{p: p, parts: make([][]uint64, p)}
}

// insert routes h to its partition and sorted-inserts there, O(n/P) because a
// partition holds ~n/P members.
func (ps *partedSet) insert(h uint64) {
	i := int(h & uint64(ps.p-1))
	ps.parts[i] = insertFlat(ps.parts[i], h)
}

// buildParted builds a P-partition set of the first n member hashes.
func buildParted(n, p int) *partedSet {
	hs := memberHashes(n)
	ps := newPartedSet(p)
	for _, h := range hs {
		ps.insert(h)
	}
	return ps
}

// --- skiplist -------------------------------------------------------------------

const slMaxLevel = 24

type slNode struct {
	key  uint64
	next []*slNode
}

// skiplist is a minimal ordered set of uint64 keys: O(log n) insert, in-order walk
// by following next[0]. It stands in for the dropped oindex's shape. The level RNG is
// a splitmix64 counter (no math/rand so the lab is deterministic and needs no seed).
type skiplist struct {
	head  *slNode
	level int
	rng   uint64
}

func newSkiplist() *skiplist {
	return &skiplist{head: &slNode{next: make([]*slNode, slMaxLevel)}, level: 1, rng: 0x9e3779b97f4a7c15}
}

func (s *skiplist) randLevel() int {
	s.rng += 0x9e3779b97f4a7c15
	z := s.rng
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z ^= z >> 31
	lvl := 1
	// Quarter-probability per level (two low bits zero), the classic skiplist growth.
	for lvl < slMaxLevel && z&3 == 0 {
		lvl++
		z >>= 2
	}
	return lvl
}

func (s *skiplist) insert(key uint64) {
	var update [slMaxLevel]*slNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	lvl := s.randLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &slNode{key: key, next: make([]*slNode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
}

func buildSkiplist(n int) *skiplist {
	hs := memberHashes(n)
	s := newSkiplist()
	for _, h := range hs {
		s.insert(h)
	}
	return s
}

// --- probe (the thing to beat) --------------------------------------------------

// probeSet is an open-addressed hash set, the model of f1raw's random-probe membership
// test. It is the read baseline: the merge has to beat this by 2x to clear the gate.
type probeSet struct {
	slots []uint64
	mask  uint64
}

func newProbeSet(hs []uint64) *probeSet {
	n := 1
	for n < len(hs)*2 {
		n <<= 1
	}
	t := &probeSet{slots: make([]uint64, n), mask: uint64(n - 1)}
	for _, h := range hs {
		if h == 0 {
			h = 1
		}
		for i := h & t.mask; ; i = (i + 1) & t.mask {
			if t.slots[i] == 0 {
				t.slots[i] = h
				break
			}
			if t.slots[i] == h {
				break
			}
		}
	}
	return t
}

func (t *probeSet) has(h uint64) bool {
	if h == 0 {
		h = 1
	}
	for i := h & t.mask; ; i = (i + 1) & t.mask {
		s := t.slots[i]
		if s == 0 {
			return false
		}
		if s == h {
			return true
		}
	}
}

// mergeSorted counts the intersection of two hash-sorted slices with one forward pass,
// the sequential-streaming read the prefetcher serves. A real command byte-confirms a
// hash tie against the arena; the confirm cost is measured in the setintersect lab, so
// here the merge is hash-only to isolate the access pattern.
func mergeSorted(ah, bh []uint64) int {
	n, i, j := 0, 0, 0
	for i < len(ah) && j < len(bh) {
		switch {
		case ah[i] < bh[j]:
			i++
		case ah[i] > bh[j]:
			j++
		default:
			n++
			i++
			j++
		}
	}
	return n
}

var sink int

// --- build benchmarks (write cost from empty) -----------------------------------

func BenchmarkBuildFlatSorted(b *testing.B) {
	for b.Loop() {
		sink = len(buildFlat(buildN))
	}
}

func BenchmarkBuildPartitioned(b *testing.B) {
	for _, p := range []int{64, 256} {
		b.Run("P="+strconv.Itoa(p), func(b *testing.B) {
			for b.Loop() {
				sink = buildParted(buildN, p).p
			}
		})
	}
}

func BenchmarkBuildSkiplist(b *testing.B) {
	for b.Loop() {
		sink = buildSkiplist(buildN).level
	}
}

// --- steady-state insert benchmarks (write cost on a large set) -----------------

// BenchmarkInsertFlatSorted charges one sorted insert into a set that already holds
// labN members, then removes it so the size stays fixed across iterations. The insert
// is a binary search plus an O(n) tail shift; the paired remove is another O(n) shift,
// so the reported ns/op is ~2x one insert, which the /2 in the lesson accounts for.
func BenchmarkInsertFlatSorted(b *testing.B) {
	base := buildFlat(labN)
	s := make([]uint64, len(base))
	copy(s, base)
	newH := hashBytes([]byte("member:newcomer"))
	for b.Loop() {
		s = insertFlat(s, newH)
		// remove it again: find and shift down, restoring the fixed size.
		i := sort.Search(len(s), func(k int) bool { return s[k] >= newH })
		copy(s[i:], s[i+1:])
		s = s[:len(s)-1]
	}
	sink = len(s)
}

// BenchmarkInsertPartitioned charges the same insert-then-remove into one partition of
// a P-partition set of labN members, so the shift is over ~labN/P members, not labN.
func BenchmarkInsertPartitioned(b *testing.B) {
	newH := hashBytes([]byte("member:newcomer"))
	for _, p := range []int{64, 256} {
		b.Run("P="+strconv.Itoa(p), func(b *testing.B) {
			ps := buildParted(labN, p)
			idx := int(newH & uint64(p-1))
			for b.Loop() {
				ps.parts[idx] = insertFlat(ps.parts[idx], newH)
				part := ps.parts[idx]
				i := sort.Search(len(part), func(k int) bool { return part[k] >= newH })
				copy(part[i:], part[i+1:])
				ps.parts[idx] = part[:len(part)-1]
			}
			sink = len(ps.parts[idx])
		})
	}
}

// BenchmarkInsertSkiplist charges one skiplist insert into a set of labN members. It
// does not pair a remove (a skiplist delete is also O(log n) and would only add noise);
// the set grows by one per iteration, which does not change the O(log n) cost the
// benchmark isolates.
func BenchmarkInsertSkiplist(b *testing.B) {
	s := buildSkiplist(labN)
	newH := hashBytes([]byte("member:newcomer"))
	for b.Loop() {
		s.insert(newH)
	}
	sink = s.level
}

// --- merge/read benchmarks ------------------------------------------------------

// BenchmarkMergeFlat is the read ceiling: one straight two-pointer pass over two
// hash-sorted arrays, the layout the prefetcher likes best.
func BenchmarkMergeFlat(b *testing.B) {
	a, bset := overlapHashes()
	slices.Sort(a)
	slices.Sort(bset)
	for b.Loop() {
		sink = mergeSorted(a, bset)
	}
}

// BenchmarkMergePartitionedSameP intersects two P-partition sets partition-by-partition:
// a member in both sets hashes the same, so it lands in the same partition index in both,
// so partition k of A can only intersect partition k of B. That makes SINTER P independent
// sequential merges over sorted arrays. This is the read the partitioned container actually
// runs, and the question is whether splitting the one big merge into P smaller ones keeps
// the sequential-streaming win (it should: each partition merge is still forward-only, and
// the P merges are independent so they parallelize).
func BenchmarkMergePartitionedSameP(b *testing.B) {
	a, bset := overlapHashes()
	const p = 64
	pa, pb := newPartedSet(p), newPartedSet(p)
	for _, h := range a {
		i := int(h & uint64(p-1))
		pa.parts[i] = append(pa.parts[i], h)
	}
	for _, h := range bset {
		i := int(h & uint64(p-1))
		pb.parts[i] = append(pb.parts[i], h)
	}
	for i := range p {
		slices.Sort(pa.parts[i])
		slices.Sort(pb.parts[i])
	}
	for b.Loop() {
		n := 0
		for i := range p {
			n += mergeSorted(pa.parts[i], pb.parts[i])
		}
		sink = n
	}
}

// slKeys walks a skiplist in order into a slice, so the merge benchmark can two-pointer
// over the walked order. Walking into a slice first would hide the pointer-chase cost the
// benchmark wants to expose, so BenchmarkMergeSkiplist walks the two lists in lockstep
// instead, following next[0] pointers, which is the in-order read a skiplist actually
// offers. slKeys exists only for the correctness check below.
func slKeys(s *skiplist) []uint64 {
	var out []uint64
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		out = append(out, n.key)
	}
	return out
}

// BenchmarkMergeSkiplist intersects two skiplists by walking both in order through their
// next[0] pointers, the merge the dropped-oindex shape would run. Every step chases a
// pointer to a node the allocator scattered across the heap, so it pays a cache miss per
// element where the array merge streams: this is the cost of buying O(log n) writes.
func BenchmarkMergeSkiplist(b *testing.B) {
	a, bset := overlapHashes()
	sa, sb := newSkiplist(), newSkiplist()
	for _, h := range a {
		sa.insert(h)
	}
	for _, h := range bset {
		sb.insert(h)
	}
	for b.Loop() {
		n := 0
		x, y := sa.head.next[0], sb.head.next[0]
		for x != nil && y != nil {
			switch {
			case x.key < y.key:
				x = x.next[0]
			case x.key > y.key:
				y = y.next[0]
			default:
				n++
				x = x.next[0]
				y = y.next[0]
			}
		}
		sink = n
	}
}

// BenchmarkProbeBaseline is the random-probe read the merge has to beat by 2x: for each
// member of A, probe an open-addressed set of B, one cache miss per member.
func BenchmarkProbeBaseline(b *testing.B) {
	a, bset := overlapHashes()
	t := newProbeSet(bset)
	for b.Loop() {
		n := 0
		for _, h := range a {
			if t.has(h) {
				n++
			}
		}
		sink = n
	}
}

// TestContainersAgree checks the three containers and the probe all report the same
// intersection count on a small fixture, so a benchmark that is fast because it is wrong
// gets caught. It is not a benchmark; it guards the models.
func TestContainersAgree(t *testing.T) {
	const n = 4096
	all := memberHashes(n + n/2)
	a := append([]uint64(nil), all[:n]...)
	bset := append([]uint64(nil), all[n/2:n/2+n]...)

	fa := append([]uint64(nil), a...)
	fb := append([]uint64(nil), bset...)
	slices.Sort(fa)
	slices.Sort(fb)
	want := mergeSorted(fa, fb)

	// probe
	if got := func() int {
		tb := newProbeSet(bset)
		c := 0
		for _, h := range a {
			if tb.has(h) {
				c++
			}
		}
		return c
	}(); got != want {
		t.Fatalf("probe = %d, want %d", got, want)
	}

	// partitioned same-P
	const p = 64
	pa, pb := newPartedSet(p), newPartedSet(p)
	for _, h := range a {
		pa.insert(h)
	}
	for _, h := range bset {
		pb.insert(h)
	}
	pc := 0
	for i := range p {
		pc += mergeSorted(pa.parts[i], pb.parts[i])
	}
	if pc != want {
		t.Fatalf("partitioned = %d, want %d", pc, want)
	}

	// skiplist
	sa, sb := newSkiplist(), newSkiplist()
	for _, h := range a {
		sa.insert(h)
	}
	for _, h := range bset {
		sb.insert(h)
	}
	if got := mergeSorted(slKeys(sa), slKeys(sb)); got != want {
		t.Fatalf("skiplist = %d, want %d", got, want)
	}
}
