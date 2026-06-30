package f1raw

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
)

// The benchmarks measure the raw lock-free ceiling: the cost of a Get or a Set on
// this store with no wire, no RESP, no keyspace, and no lock. Run the parallel ones
// across core counts to see the scaling the lock-free index buys:
//
//	go test -bench . -benchmem ./engine/f1raw/
//	go test -bench Parallel -cpu 1,2,4,8 ./engine/f1raw/
//
// GetParallel and SetParallel are the lock-tax probe: a lock-bearing store's
// throughput flattens or inverts as cores climb because every op bounces one
// contended cache line; a truly lock-free store should scale close to linearly on
// distinct keys.

const benchKeys = 1 << 20 // 1,048,576 keys, the string-bench working set

// makeKey writes a fixed-width 16-byte key for n into buf and returns it. Fixed width
// keeps the hash and compare cost constant across iterations so the benchmark times
// the store, not key formatting.
func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func filledStore(keys int, valLen int) *Store {
	// Size the index near keys/4 buckets (load factor ~ keys/(4*7) < 1 per slot... in
	// fact ~ keys/28, comfortably below the 7-per-bucket spill point) and the arena for
	// every record plus headroom.
	buckets := keys / 4
	if buckets < 16 {
		buckets = 16
	}
	rec := int(recSize(16, valLen))
	arena := keys*rec + keys*rec/4 + 1<<20
	s := New(buckets, arena)
	val := make([]byte, valLen)
	var kb [16]byte
	for i := 0; i < keys; i++ {
		k := makeKey(kb[:], uint64(i))
		if err := s.Set(k, val); err != nil {
			panic(err)
		}
	}
	return s
}

func BenchmarkGet(b *testing.B) {
	s := filledStore(benchKeys, 64)
	var kb [16]byte
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], uint64(i)&(benchKeys-1))
		var ok bool
		dst, ok = s.Get(k, dst)
		if !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkSet(b *testing.B) {
	// Pre-fill so every Set is an in-place update on the bounded arena (the sustained
	// write path); a fresh-key Set would just measure the bump allocator and exhaust the
	// arena.
	s := filledStore(benchKeys, 64)
	val := make([]byte, 64)
	var kb [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], uint64(i)&(benchKeys-1))
		if err := s.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetParallel scales reads across GOMAXPROCS. It mirrors hot/bench_test.go
// exactly (uniform xorshift key pick over the 1M-key working set) so the curve
// compares directly against the lock-bearing engine and the gap is the lock tax, not
// a difference in access pattern. Contention is only on the shared (read-only during
// the run) buckets and records, so the lock-free read path should scale near-linearly.
func BenchmarkGetParallel(b *testing.B) {
	s := filledStore(benchKeys, 64)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var kb [16]byte
		var dst []byte
		var x uint32 = 2463534242
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			dst, _ = s.Get(makeKey(kb[:], uint64(x%benchKeys)), dst)
		}
	})
}

// BenchmarkSetParallel scales in-place updates across GOMAXPROCS, same uniform pick as
// hot/. Two goroutines almost never latch the same record, so throughput that climbs
// with cores is the headline lock-free result: no shared lock to serialize on, and no
// per-write allocation.
func BenchmarkSetParallel(b *testing.B) {
	s := filledStore(benchKeys, 64)
	val := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var kb [16]byte
		var x uint32 = 88172645
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			if err := s.Set(makeKey(kb[:], uint64(x%benchKeys)), val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSetParallelHotKey is the worst case for a lock-free store: every goroutine
// hammers in-place updates on one key, so they all contend on that record's seqlock.
// It bounds how bad single-key contention gets (only same-key writers serialize, and
// only for one memcpy), and pairs with the lock-tax comparison: a global-lock store
// pays this contention on every op; here only a genuine hot key pays it.
func BenchmarkSetParallelHotKey(b *testing.B) {
	s := New(16, 1<<16)
	val := make([]byte, 64)
	if err := s.Set([]byte("hotkey"), val); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := []byte("hotkey")
		for pb.Next() {
			if err := s.Set(key, val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// reportCores prints GOMAXPROCS once so a -cpu sweep is labeled in the output.
func BenchmarkCoresInfo(b *testing.B) {
	b.Skip(fmt.Sprintf("GOMAXPROCS=%d NumCPU=%d", runtime.GOMAXPROCS(0), runtime.NumCPU()))
}

// zipfSequence draws n key indices from a Zipfian distribution over [0,keys) with
// skew s (s>1; larger is more skewed). It is precomputed once so the benchmark loop
// pays no per-op RNG cost and times only the store, and it is seeded with a fixed
// value so f1raw and f2raw see the exact same access trace and the comparison is
// apples to apples. A Zipfian trace is the workload the F2 paper targets: a small
// hot set takes the overwhelming majority of accesses, the realistic shape of a
// production cache that a flat single-tier index cannot keep cache-resident.
func zipfSequence(n, keys int, s float64) []uint64 {
	r := rand.New(rand.NewSource(0x5eed))
	z := rand.NewZipf(r, s, 1, uint64(keys-1))
	seq := make([]uint64, n)
	for i := range seq {
		seq[i] = z.Uint64()
	}
	return seq
}

const skewSamples = 1 << 20 // length of the precomputed Zipfian access trace

// BenchmarkGetSkewed reads under a Zipfian trace instead of a uniform one. On a flat
// single-tier store this is barely faster than the uniform read: the hot keys still
// live scattered across the full 16 MB index and 100 MB arena, so a hot key's bucket
// and record get evicted from cache by all the cold-key traffic between its hits.
// That cache-residency miss under skew is exactly the flaw the two-tier f2raw fixes,
// and this is its f1raw baseline.
func BenchmarkGetSkewed(b *testing.B) {
	s := filledStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	var kb [16]byte
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], seq[i&(skewSamples-1)])
		dst, _ = s.Get(k, dst)
	}
}

// BenchmarkSetSkewed updates in place under the same Zipfian trace. Same story as the
// skewed read: a hot key's record is somewhere in the 100 MB arena, so the in-place
// memcpy keeps missing cache even though the working set is tiny.
func BenchmarkSetSkewed(b *testing.B) {
	s := filledStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	val := make([]byte, 64)
	var kb [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], seq[i&(skewSamples-1)])
		if err := s.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetSkewedParallel and BenchmarkSetSkewedParallel run the Zipfian trace
// across GOMAXPROCS. Each goroutine walks the shared trace from its own offset so
// the cores collectively reproduce the skew. This is the headline comparison point
// against f2raw: a flat store cannot exploit the skew, a two-tier store can.
func BenchmarkGetSkewedParallel(b *testing.B) {
	s := filledStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	var off uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var kb [16]byte
		var dst []byte
		i := int(atomic.AddUint64(&off, 1) * 0x9e3779b1)
		for pb.Next() {
			dst, _ = s.Get(makeKey(kb[:], seq[i&(skewSamples-1)]), dst)
			i++
		}
	})
}

func BenchmarkSetSkewedParallel(b *testing.B) {
	s := filledStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	val := make([]byte, 64)
	var off uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var kb [16]byte
		i := int(atomic.AddUint64(&off, 1) * 0x9e3779b1)
		for pb.Next() {
			if err := s.Set(makeKey(kb[:], seq[i&(skewSamples-1)]), val); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
