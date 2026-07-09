package mergeresolve

import (
	"encoding/binary"
	"math/bits"
	"sort"
	"strconv"
	"testing"
)

// The fixture is SINTER(A, B) with |A| = |B| = labN and a 50% overlap: A's upper half is B's lower
// half, so exactly labN/2 driver members match and each match runs the confirm-and-emit the two
// strategies differ on. labN is sized so the two arenas together dwarf L2, the condition under which a
// resolution is a real DRAM read rather than a cache hit, so the saved resolution shows.
const (
	labN    = 1 << 20
	overlap = labN / 2
)

// member builds the byte payload for element i, a realistic ~14-byte key so the arena row is not a
// degenerate few bytes and the resolve genuinely touches a fresh cache line.
func member(i int) []byte {
	return []byte("member:" + strconv.Itoa(i))
}

// hash64 is f1raw's word-at-a-time index hash, copied so the merge orders on the same key the engine
// does. Its exact value does not matter to this lab, only that it spreads the members.
func hash64(b []byte) uint64 {
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

// arena models f1raw's record store: every set's members are length-prefixed rows in one shared byte
// slab, and a sorted array holds arena offsets, not the bytes. resolve(off) decodes the row at off and
// returns its member subslice, the model of keyAtTiered on the resident arena: a random read into the
// slab plus a length decode. Because A and B interleave their rows in one slab, consecutive sorted
// entries point to scattered offsets, so a resolve is a cache miss exactly as in production.
type arena struct {
	buf []byte
}

// put appends member m as uint32-length-prefixed bytes and returns its offset.
func (a *arena) put(m []byte) uint64 {
	off := uint64(len(a.buf))
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(m)))
	a.buf = append(a.buf, lb[:]...)
	a.buf = append(a.buf, m...)
	return off
}

// resolve models keyAtTiered on the RESIDENT arena: a zero-copy subslice into the slab.
func (a *arena) resolve(off uint64) []byte {
	n := binary.LittleEndian.Uint32(a.buf[off:])
	start := off + 4
	return a.buf[start : start+uint64(n)]
}

// resolveCopy models keyAtTiered on the SEGMENTED (larger-than-memory) arena: it hands back a fresh
// caller-owned copy, an allocation, because the cold/segmented tiers cannot lend a stable subslice.
// This is the path where fusing the third resolve away actually saves work: one fewer alloc+copy per
// matched member.
func (a *arena) resolveCopy(off uint64) []byte {
	n := binary.LittleEndian.Uint32(a.buf[off:])
	start := off + 4
	out := make([]byte, n)
	copy(out, a.buf[start:start+uint64(n)])
	return out
}

// sortedSet is one operand: member hashes in ascending order and the parallel arena offsets, the shape
// the real sortedHashes snapshot publishes.
type sortedSet struct {
	h   []uint64
	off []uint64
}

// buildFixture writes both sets' members into one shared arena (interleaved, so offsets scatter) and
// returns each set as a hash-sorted (hash, offset) array. A is [0, labN); B is [overlap, overlap+labN),
// so their intersection is exactly [overlap, labN), labN/2 members.
func buildFixture() (ar *arena, a, b sortedSet) {
	ar = &arena{buf: make([]byte, 0, (labN+labN)*20)}
	type he struct {
		h   uint64
		off uint64
	}
	aes := make([]he, labN)
	bes := make([]he, labN)
	// Interleave the two sets' rows in the slab so a sorted walk hits scattered offsets.
	for i := 0; i < labN; i++ {
		am := member(i)
		aes[i] = he{hash64(am), ar.put(am)}
		bm := member(i + overlap)
		bes[i] = he{hash64(bm), ar.put(bm)}
	}
	sort.Slice(aes, func(i, j int) bool { return aes[i].h < aes[j].h })
	sort.Slice(bes, func(i, j int) bool { return bes[i].h < bes[j].h })
	a = sortedSet{h: make([]uint64, labN), off: make([]uint64, labN)}
	b = sortedSet{h: make([]uint64, labN), off: make([]uint64, labN)}
	for i := range aes {
		a.h[i], a.off[i] = aes[i].h, aes[i].off
		b.h[i], b.off[i] = bes[i].h, bes[i].off
	}
	return ar, a, b
}

// walkSplit is the original merge shape: on a hash match, confirm resolves BOTH offsets, then emit
// resolves offA a SECOND time to materialize it. Three resolutions per matched member.
func walkSplit(ar *arena, a, b sortedSet, emit func(m []byte)) {
	confirm := func(offA, offB uint64) bool {
		return string(ar.resolve(offA)) == string(ar.resolve(offB))
	}
	i, j := 0, 0
	for i < len(a.h) && j < len(b.h) {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			if confirm(a.off[i], b.off[j]) {
				emit(ar.resolve(a.off[i]))
			}
			i++
			j++
		}
	}
}

// walkFused is the optimized shape: on a hash match, resolve offA once, resolve offB once, compare, and
// reuse offA's bytes for the emit. Two resolutions per matched member.
func walkFused(ar *arena, a, b sortedSet, emit func(m []byte)) {
	i, j := 0, 0
	for i < len(a.h) && j < len(b.h) {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			ka := ar.resolve(a.off[i])
			kb := ar.resolve(b.off[j])
			if string(ka) == string(kb) {
				emit(ka)
			}
			i++
			j++
		}
	}
}

// walkSplitCopy and walkFusedCopy are the two shapes on the segmented arena, where each resolve is a
// fresh copy. Split copies offA twice (once to confirm, once to emit); fused copies it once and reuses
// it. Both copy offB once to confirm.
func walkSplitCopy(ar *arena, a, b sortedSet, emit func(m []byte)) {
	confirm := func(offA, offB uint64) bool {
		return string(ar.resolveCopy(offA)) == string(ar.resolveCopy(offB))
	}
	i, j := 0, 0
	for i < len(a.h) && j < len(b.h) {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			if confirm(a.off[i], b.off[j]) {
				emit(ar.resolveCopy(a.off[i]))
			}
			i++
			j++
		}
	}
}

func walkFusedCopy(ar *arena, a, b sortedSet, emit func(m []byte)) {
	i, j := 0, 0
	for i < len(a.h) && j < len(b.h) {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			ka := ar.resolveCopy(a.off[i])
			kb := ar.resolveCopy(b.off[j])
			if string(ka) == string(kb) {
				emit(ka)
			}
			i++
			j++
		}
	}
}

var sink int

// BenchmarkMergeSplit is the three-resolution merge (confirm resolves A and B, emit resolves A again).
func BenchmarkMergeSplit(b *testing.B) {
	ar, a, bset := buildFixture()
	for b.Loop() {
		n := 0
		walkSplit(ar, a, bset, func(m []byte) { n += len(m) })
		sink = n
	}
}

// BenchmarkMergeFused is the two-resolution merge (A resolved once, reused for confirm and emit). The
// gap against BenchmarkMergeSplit is the single saved DRAM read per matched member, the whole change.
func BenchmarkMergeFused(b *testing.B) {
	ar, a, bset := buildFixture()
	for b.Loop() {
		n := 0
		walkFused(ar, a, bset, func(m []byte) { n += len(m) })
		sink = n
	}
}

// BenchmarkMergeSplitCopy is the three-resolution merge on the segmented arena: offA is copied twice.
func BenchmarkMergeSplitCopy(b *testing.B) {
	ar, a, bset := buildFixture()
	for b.Loop() {
		n := 0
		walkSplitCopy(ar, a, bset, func(m []byte) { n += len(m) })
		sink = n
	}
}

// BenchmarkMergeFusedCopy is the two-resolution merge on the segmented arena: offA is copied once and
// reused. The gap against BenchmarkMergeSplitCopy is the saved alloc+copy per matched member, the LTM
// win the resident benchmarks (Split/Fused) correctly show is absent in memory-fit.
func BenchmarkMergeFusedCopy(b *testing.B) {
	ar, a, bset := buildFixture()
	for b.Loop() {
		n := 0
		walkFusedCopy(ar, a, bset, func(m []byte) { n += len(m) })
		sink = n
	}
}

// TestFusedMatchesSplit checks the fused walk emits exactly what the split walk does, so the
// optimization cannot change the SINTER result.
func TestFusedMatchesSplit(t *testing.T) {
	ar, a, bset := buildFixture()
	var split, fused []string
	walkSplit(ar, a, bset, func(m []byte) { split = append(split, string(m)) })
	walkFused(ar, a, bset, func(m []byte) { fused = append(fused, string(m)) })
	if len(split) != overlap {
		t.Fatalf("split emitted %d members, want %d", len(split), overlap)
	}
	if len(split) != len(fused) {
		t.Fatalf("fused emitted %d, split emitted %d", len(fused), len(split))
	}
	sort.Strings(split)
	sort.Strings(fused)
	for i := range split {
		if split[i] != fused[i] {
			t.Fatalf("member %d: fused %q != split %q", i, fused[i], split[i])
		}
	}
}
