package f2raw

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// BenchmarkSetHot rewrites one hot key in place: the pure seqlock update cost.
func BenchmarkSetHot(b *testing.B) {
	s := New(1<<10, 1<<20)
	k := []byte("hot")
	v := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(k, v); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetHot reads one hot key: the pure lock-free read cost.
func BenchmarkGetHot(b *testing.B) {
	s := New(1<<10, 1<<20)
	s.Set([]byte("hot"), []byte("value"))
	k := []byte("hot")
	dst := make([]byte, 0, 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, _ = s.Get(k, dst)
	}
}

// BenchmarkGetParallel reads a spread of keys from many goroutines: the read path
// under the concurrency the saturation bench exercises.
func BenchmarkGetParallel(b *testing.B) {
	s := New(1<<20, 1<<28)
	const n = 1 << 16
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
		s.Set(keys[i], keys[i])
	}
	var ctr atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		dst := make([]byte, 0, 32)
		i := ctr.Add(1)
		for pb.Next() {
			dst, _ = s.Get(keys[i&(n-1)], dst)
			i++
		}
	})
}

// BenchmarkSetParallel writes distinct keys from many goroutines: the lock-free
// upsert path with no key contention.
func BenchmarkSetParallel(b *testing.B) {
	s := New(1<<20, 1<<30)
	var ctr atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var buf [20]byte
		base := ctr.Add(1) << 32
		for pb.Next() {
			k := strconv.AppendUint(buf[:0], base, 10)
			s.Set(k, k)
			base++
		}
	})
}

// BenchmarkIncrHot bumps one hot counter: the read-modify-write under latch.
func BenchmarkIncrHot(b *testing.B) {
	s := New(1<<10, 1<<20)
	k := []byte("counter")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Incr(k, 1); err != nil {
			b.Fatal(err)
		}
	}
}
