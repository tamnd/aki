package store

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// These microbenchmarks mirror the memengine spike so the v2 engine ceiling
// compares directly: 1,000,000 keys, 64-byte values, b.RunParallel with an
// inline-xorshift uniform-random key pick. Build with `go test -c` and run pinned
// (taskset -c 0-7 GOMAXPROCS=8 -test.cpu 8 -test.benchtime=2s) to get the same
// 8-core aggregate Mops/s the memengine numbers were taken at.

const (
	benchKeys   = 1_000_000
	benchValLen = 64
)

// preload fills a store with benchKeys keys and returns the key set so the
// benchmark loop indexes into precomputed keys rather than formatting per op.
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

func benchStore(b *testing.B, t Tunables) *Store {
	s, err := New(t)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

func BenchmarkGet(b *testing.B) {
	s := benchStore(b, DefaultTunables())
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
	s := benchStore(b, DefaultTunables())
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
	s := benchStore(b, DefaultTunables())
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

// BenchmarkGetSpilled measures the GET path when the resident budget is a
// fraction of the working set, so most reads hit disk. It is the larger-than-
// memory counterpart to BenchmarkGet and shows the cold-read floor.
func BenchmarkGetSpilled(b *testing.B) {
	dir := b.TempDir()
	// 1M keys * ~70B/record ~= 70MB of log; cap residency near 16MB so most reads
	// fall to disk.
	s := benchStore(b, Tunables{Shards: 256, PageSize: 1 << 16, ResidentPagesPerShard: 1, Dir: dir})
	keys := preload(b, s)
	b.Logf("spilled %d pages", s.Spilled())
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 19937
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
