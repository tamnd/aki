package store

import (
	"encoding/binary"
	"testing"
)

// The benchmarks measure the single-owner ceiling: the cost of a probe, a Get,
// a Set, and a raw arena append with no wire, no RESP, no queue hop, and no
// atomic anywhere. They mirror the f1 harness shapes (16-byte fixed keys,
// 64-byte values, a 1M-key working set) so the numbers compare directly and
// the delta is the coordination tax removed, not a workload change.

const benchKeys = 1 << 20

// makeKey writes a fixed-width 16-byte key for n into buf. Fixed width keeps
// the hash and compare cost constant so the benchmark times the store, not key
// formatting.
func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func filledStore(keys, valLen int) *Store {
	rec := int(recSize(16, valLen))
	s := New(keys*rec+keys*rec/4+(16<<20), 0)
	val := make([]byte, valLen)
	var kb [16]byte
	for i := 0; i < keys; i++ {
		if err := s.Set(makeKey(kb[:], uint64(i)), val); err != nil {
			panic(err)
		}
	}
	return s
}

// BenchmarkProbe times the index walk alone: hash to entry word, tag reject,
// key verify, no value copy. This is the number the 12-bit tag and the
// one-line bucket are accountable to.
func BenchmarkProbe(b *testing.B) {
	s := filledStore(benchKeys, 64)
	var kb [16]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := makeKey(kb[:], uint64(i)&(benchKeys-1))
		_, addr, _ := s.findEntry(Hash(k), k)
		if addr == 0 {
			b.Fatal("miss")
		}
	}
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
	// Pre-fill so every Set is an in-place update on the bounded arena (the
	// sustained write path); a fresh-key Set would measure the bump allocator,
	// which BenchmarkArenaAppend does on its own.
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

// BenchmarkArenaAppend times the raw allocate-and-frame path: one plain-add
// bump plus the header and byte stores, no index. The arena rewinds when it
// fills, so the steady state includes the once-per-segment advance at its
// real frequency.
func BenchmarkArenaAppend(b *testing.B) {
	s := New(256<<20, 0)
	var kb [16]byte
	val := make([]byte, 64)
	n := recSize(16, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off, ok := s.arena.allocRecord(n)
		if !ok {
			s.arena.reset()
			off, ok = s.arena.allocRecord(n)
			if !ok {
				b.Fatal("alloc failed on a fresh arena")
			}
		}
		s.initRecord(off, makeKey(kb[:], uint64(i)), val, kindString, 0)
	}
}
