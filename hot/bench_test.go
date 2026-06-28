package hot

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// These microbenchmarks mirror store/bench_test.go exactly (1,000,000 keys,
// 64-byte values, b.RunParallel with an inline-xorshift uniform key pick) so the
// hot tier and the v2 store/ engine compare directly under the same harness. The
// read path here takes no lock; store/ takes a per-shard RWMutex. Run both with
// the same pin (taskset -c 0-7 GOMAXPROCS=8 -test.benchtime=2s) to read the gap.

const (
	benchKeys   = 1_000_000
	benchValLen = 64
)

func preload(b *testing.B, s *Store) [][]byte {
	b.Helper()
	keys := make([][]byte, benchKeys)
	val := make([]byte, benchValLen)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
		if err := s.Set(keys[i], val); err != nil {
			b.Fatalf("preload Set: %v", err)
		}
	}
	return keys
}

func benchStore(b *testing.B) *Store {
	// Presize so the load does not pay table growth, matching store/'s full
	// preallocation behaviour for a fair read comparison.
	s, err := New(Tunables{Shards: 256, IndexHintPerShard: benchKeys / 256})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

func BenchmarkGet(b *testing.B) {
	s := benchStore(b)
	keys := preload(b, s)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 2463534242
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			if _, _, err := s.Get(keys[x%benchKeys]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSet(b *testing.B) {
	s := benchStore(b)
	keys := preload(b, s)
	val := make([]byte, benchValLen)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 88172645
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			if err := s.Set(keys[x%benchKeys], val); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMixed95Get5Set(b *testing.B) {
	s := benchStore(b)
	keys := preload(b, s)
	val := make([]byte, benchValLen)
	var setCount int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 747796405
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			k := keys[x%benchKeys]
			if x%20 == 0 {
				s.Set(k, val)
				atomic.AddInt64(&setCount, 1)
			} else {
				s.Get(k)
			}
		}
	})
	_ = setCount
}
