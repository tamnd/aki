package f2raw

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
)

// The benchmarks measure the raw lock-free ceiling: the cost of a Get or a Set on
// this store with no wire, no RESP, no keyspace, and no lock. Run the parallel ones
// across core counts to see the scaling the lock-free index buys:
//
//	go test -bench . -benchmem ./f2raw/
//	go test -bench Parallel -cpu 1,2,4,8 ./f2raw/
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
