package f2raw

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
)

// These benchmarks mirror engine/f1raw/bench_test.go field for field: same key shape,
// same 1M working set, same uniform xorshift and fixed-seed Zipfian traces. The point
// is a head-to-head with the single-tier f1raw baseline on the identical workload, so
// any difference is the two-tier architecture and nothing else. Run both back to back
// in one session so a loaded machine moves them together:
//
//	go test -bench . -benchmem ./engine/f1raw/ ./engine/f2raw/
//
// The headline pair is BenchmarkGetSkewed / BenchmarkSetSkewed: under a Zipfian trace a
// small cache-resident hot tier serves the working set from L2/L3 while f1raw keeps
// missing DRAM across its flat 16 MB index and 100 MB arena. Under the uniform
// benchmarks there is no locality to exploit, so f2raw is at best even and may pay a
// little for the second probe on a hot miss; that is the honest tradeoff, and "pick the
// fastest as default" means pick per the realistic (skewed) workload.

const benchKeys = 1 << 20 // 1,048,576 keys, matching f1raw

// hotBudget is the hot-tier live-key ceiling for the benchmarks. It is a few percent of
// the keyspace, enough to hold the Zipfian working set at s=1.1 and small enough that
// the hot index and arena stay cache-resident, which is the whole point of the tier.
const hotBudget = 1 << 16 // 65,536 keys

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

// loadedStore fills the cold tier with every key (the bulk-load path that models a
// store populated from storage) and sizes the hot tier to hotBudget. It does not warm
// the hot tier; a benchmark that wants a warm hot set runs its trace once untimed
// first. The cold tier is sized for the full keyspace; the hot tier's arena is sized
// for the working set plus churn headroom, since the arena is grow-only.
func loadedStore(keys int, valLen int) *Store {
	rec := int(recSize(16, valLen))
	s := New(Config{
		HotKeys:          hotBudget,
		HotIndexBuckets:  hotBudget / 4,
		HotArenaBytes:    hotBudget*rec*8 + 1<<20,
		ColdIndexBuckets: keys / 4,
		ColdArenaBytes:   keys*rec + keys*rec/4 + 1<<20,
	})
	val := make([]byte, valLen)
	var kb [16]byte
	for i := 0; i < keys; i++ {
		if err := s.Load(makeKey(kb[:], uint64(i)), val); err != nil {
			panic(err)
		}
	}
	return s
}

// warm runs a Zipfian read trace once, untimed, to populate the hot tier the way real
// traffic would before the timed loop measures the steady state.
func warm(s *Store, seq []uint64) {
	var kb [16]byte
	var dst []byte
	for _, k := range seq {
		dst, _ = s.Get(makeKey(kb[:], k), dst)
	}
}

func BenchmarkGet(b *testing.B) {
	s := loadedStore(benchKeys, 64)
	var kb [16]byte
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], uint64(i)&(benchKeys-1))
		dst, _ = s.Get(k, dst)
	}
}

func BenchmarkSet(b *testing.B) {
	s := loadedStore(benchKeys, 64)
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

func BenchmarkGetParallel(b *testing.B) {
	s := loadedStore(benchKeys, 64)
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

func BenchmarkSetParallel(b *testing.B) {
	s := loadedStore(benchKeys, 64)
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

func BenchmarkCoresInfo(b *testing.B) {
	b.Skip(fmt.Sprintf("GOMAXPROCS=%d NumCPU=%d", runtime.GOMAXPROCS(0), runtime.NumCPU()))
}

// zipfSequence is identical to f1raw's: a fixed-seed Zipfian trace over [0,keys) so the
// two stores see the exact same access pattern and the comparison is apples to apples.
func zipfSequence(n, keys int, s float64) []uint64 {
	r := rand.New(rand.NewSource(0x5eed))
	z := rand.NewZipf(r, s, 1, uint64(keys-1))
	seq := make([]uint64, n)
	for i := range seq {
		seq[i] = z.Uint64()
	}
	return seq
}

const skewSamples = 1 << 20

// BenchmarkGetSkewed is the headline read comparison. The store is warmed so the
// Zipfian hot set lives in the small hot tier, and the timed loop then serves almost
// every read from that cache-resident structure instead of the DRAM-sized cold index
// and arena f1raw must traverse on every hit.
func BenchmarkGetSkewed(b *testing.B) {
	s := loadedStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	warm(s, seq)
	var kb [16]byte
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], seq[i&(skewSamples-1)])
		dst, _ = s.Get(k, dst)
	}
}

// BenchmarkSetSkewed updates in place under the same warmed Zipfian trace, so the
// in-place memcpy lands in the cache-resident hot record instead of somewhere in the
// 100 MB cold arena.
func BenchmarkSetSkewed(b *testing.B) {
	s := loadedStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	warm(s, seq)
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

func BenchmarkGetSkewedParallel(b *testing.B) {
	s := loadedStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	warm(s, seq)
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
	s := loadedStore(benchKeys, 64)
	seq := zipfSequence(skewSamples, benchKeys, 1.1)
	warm(s, seq)
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
