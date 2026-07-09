package setmergecollect

import (
	"encoding/binary"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
)

// sink keeps the compiler from folding the benchmarked collect away.
var sink int

// splitmix64 is a small deterministic PRNG so the fixtures are reproducible without touching the wall
// clock or the global rand source. Each call advances the state and returns a well-mixed 64-bit word.
type splitmix64 struct{ s uint64 }

func (r *splitmix64) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// memberHash hashes a member's 8-byte payload into the set-independent uint64 the async folder stores
// in the sorted array, so the same member in two sets yields the same key the two-pointer walk matches.
func memberHash(member uint64) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], member)
	var h uint64 = 0x9e3779b97f4a7c15
	h ^= binary.LittleEndian.Uint64(b[:])
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	return h ^ (h >> 33)
}

// arena holds one SINTER(A,B) instance at a per-source cardinality: A and B each have n members
// overlapping in half, so the intersection is exactly n/2. The sorted arrays are the pre-folded merge
// input, and members holds each member's byte payload so the collect appends real []byte subslices the
// way the live path emits arena-stable members, not synthetic pointers.
type arena struct {
	sortedA []uint64
	sortedB []uint64
	members map[uint64][]byte
	lo      int // smaller cardinality, the presize hint plan.lo carries
}

func buildArena(n int, seed uint64) *arena {
	r := splitmix64{s: seed}
	shared := make([]uint64, n/2)
	for i := range shared {
		shared[i] = r.next()
	}
	aVals := append([]uint64(nil), shared...)
	bVals := append([]uint64(nil), shared...)
	for len(aVals) < n {
		aVals = append(aVals, r.next())
	}
	for len(bVals) < n {
		bVals = append(bVals, r.next())
	}
	members := make(map[uint64][]byte, 2*n)
	sortedA := make([]uint64, n)
	sortedB := make([]uint64, n)
	for i, v := range aVals {
		h := memberHash(v)
		sortedA[i] = h
		members[h] = payload(v)
	}
	for i, v := range bVals {
		h := memberHash(v)
		sortedB[i] = h
		members[h] = payload(v)
	}
	slices.Sort(sortedA)
	slices.Sort(sortedB)
	return &arena{sortedA: sortedA, sortedB: sortedB, members: members, lo: n}
}

// payload builds a 64-byte member body shaped like the set members the gate loads (m%063.0f), so the
// collect appends realistic subslices rather than tiny scalars.
func payload(v uint64) []byte {
	b := make([]byte, 64)
	b[0] = 'm'
	binary.LittleEndian.PutUint64(b[1:9], v)
	return b
}

// collectGrow is the pre-change collect: the two-pointer intersect emits each matched member into a
// result slice that starts at capacity zero, so the append chain reallocates and copies the backing
// array through log2(result) doublings. It returns the collected members so the benchmark measures the
// full append cost, not just the count.
func collectGrow(a *arena) [][]byte {
	out := make([][]byte, 0)
	i, j := 0, 0
	for i < len(a.sortedA) && j < len(a.sortedB) {
		switch {
		case a.sortedA[i] < a.sortedB[j]:
			i++
		case a.sortedA[i] > a.sortedB[j]:
			j++
		default:
			out = append(out, a.members[a.sortedA[i]])
			i++
			j++
		}
	}
	return out
}

// collectPresize is the post-change collect: the result slice is sized from the smaller cardinality
// (plan.lo), the exact upper bound for an intersect, so one allocation replaces the doubling chain and
// no matched member ever pays a copy.
func collectPresize(a *arena) [][]byte {
	out := make([][]byte, 0, a.lo)
	i, j := 0, 0
	for i < len(a.sortedA) && j < len(a.sortedB) {
		switch {
		case a.sortedA[i] < a.sortedB[j]:
			i++
		case a.sortedA[i] > a.sortedB[j]:
			j++
		default:
			out = append(out, a.members[a.sortedA[i]])
			i++
			j++
		}
	}
	return out
}

// collectReuse is the second change on this same path: the result slice is not allocated per command at
// all. The connection owns one scratch buffer that a command resets to length zero and grows to lo once,
// then every later SINTER/SDIFF/SUNION on that connection reuses it, so the steady state runs zero
// allocations. The caller frames the collected members into the reply (or inserts them into a STORE
// destination) and drops the slice before the next command, and a connection runs one command at a time,
// so the reuse is safe with the same arena-stable subslices. scratch models that per-connection buffer:
// the benchmark hands the same backing slice back each iteration, which is exactly the live reuse.
func collectReuse(a *arena, scratch [][]byte) [][]byte {
	out := scratch[:0]
	if cap(out) < a.lo {
		out = make([][]byte, 0, a.lo)
	}
	i, j := 0, 0
	for i < len(a.sortedA) && j < len(a.sortedB) {
		switch {
		case a.sortedA[i] < a.sortedB[j]:
			i++
		case a.sortedA[i] > a.sortedB[j]:
			j++
		default:
			out = append(out, a.members[a.sortedA[i]])
			i++
			j++
		}
	}
	return out
}

// cardinalities brackets the gate's set sizes: 128 (the merge floor), 256 (the profiled dip), and the
// larger sizes the fanned-partition path takes.
var cardinalities = []int{128, 256, 512, 1024, 4096}

// BenchmarkCollectGrow measures the pre-change collect at each cardinality: allocs/op counts the
// doubling reallocations the grow-from-zero slice pays, and ns/op includes the pointer copies each
// doubling makes.
func BenchmarkCollectGrow(b *testing.B) {
	for _, n := range cardinalities {
		a := buildArena(n, 0x5eed+uint64(n))
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink = len(collectGrow(a))
			}
		})
	}
}

// BenchmarkCollectPresize measures the post-change collect at each cardinality: it should report one
// alloc/op and a lower ns/op than BenchmarkCollectGrow, the saving being exactly the doubling copies the
// presize removes.
func BenchmarkCollectPresize(b *testing.B) {
	for _, n := range cardinalities {
		a := buildArena(n, 0x5eed+uint64(n))
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				sink = len(collectPresize(a))
			}
		})
	}
}

// BenchmarkCollectReuse measures the reuse collect at each cardinality against a persistent scratch
// buffer: after the first iteration grows it to lo it should report zero allocs/op, the whole point of
// the change, and a ns/op at or below BenchmarkCollectPresize since it also skips the single make.
func BenchmarkCollectReuse(b *testing.B) {
	for _, n := range cardinalities {
		a := buildArena(n, 0x5eed+uint64(n))
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			var scratch [][]byte
			b.ReportAllocs()
			for b.Loop() {
				out := collectReuse(a, scratch)
				scratch = out
				sink = len(out)
			}
		})
	}
}

// BenchmarkCollectGrowPar and BenchmarkCollectPresizePar run the collect under GOMAXPROCS workers, each
// on its own arena, to show the saving holds in the fanned-partition regime the larger sets take, where
// every worker collecting a partition otherwise pays its own doubling chain concurrently.
func BenchmarkCollectGrowPar(b *testing.B) {
	for _, n := range cardinalities {
		as := buildArenas(n, 64)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				a := as[int(next.Add(1)-1)%len(as)]
				local := 0
				for pb.Next() {
					local = len(collectGrow(a))
				}
				sink = local
			})
		})
	}
}

func BenchmarkCollectPresizePar(b *testing.B) {
	for _, n := range cardinalities {
		as := buildArenas(n, 64)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				a := as[int(next.Add(1)-1)%len(as)]
				local := 0
				for pb.Next() {
					local = len(collectPresize(a))
				}
				sink = local
			})
		})
	}
}

// buildArenas makes w independent instances at cardinality n so the concurrent benches give each worker
// its own arrays, matching the fan-out where each partition worker collects a distinct member set.
func buildArenas(n, w int) []*arena {
	as := make([]*arena, w)
	for k := range as {
		as[k] = buildArena(n, 0x51ce+uint64(n)*1000+uint64(k))
	}
	return as
}

// TestCollectAgree pins that both collects return the same n/2 intersection at every cardinality, so a
// benchmark that is fast because it dropped members gets caught.
func TestCollectAgree(t *testing.T) {
	for _, n := range cardinalities {
		a := buildArena(n, 0x5eed+uint64(n))
		want := n / 2
		if got := len(collectGrow(a)); got != want {
			t.Fatalf("n=%d: grow = %d, want %d", n, got, want)
		}
		if got := len(collectPresize(a)); got != want {
			t.Fatalf("n=%d: presize = %d, want %d", n, got, want)
		}
		// The reuse collect must agree both on a fresh scratch (first command on a connection) and on a
		// scratch already grown by a prior command (steady state), the two states the live buffer takes.
		var scratch [][]byte
		first := collectReuse(a, scratch)
		if got := len(first); got != want {
			t.Fatalf("n=%d: reuse (fresh) = %d, want %d", n, got, want)
		}
		if got := len(collectReuse(a, first)); got != want {
			t.Fatalf("n=%d: reuse (warm) = %d, want %d", n, got, want)
		}
	}
}
