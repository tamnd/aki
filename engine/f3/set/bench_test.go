package set

import (
	"encoding/binary"
	"testing"
)

// The native-band microbenchmarks (doc 11 section 2.4). SISMEMBER is the probe
// floor, hit and miss, at the 10k native cell and the 1M partitioned-scale cell;
// SADD is the insert path amortized over a full build, which folds in growth and
// rehash. Members are 16 bytes (the gate's default member size, doc 11 section
// 12). These are Go microbenchmarks, not the GamingPC gate; they order the
// mechanism and quote a floor, and the gate rows key on the server numbers.

// members16 returns n distinct 16-byte members, so the benchmarks never allocate
// keys inside the timed loop.
func members16(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b, uint64(i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(b[8:], uint64(i))
		out[i] = b
	}
	return out
}

func buildHT(keys [][]byte) *set {
	s := newSet([]byte("seed-not-an-int-so-listpack-then-table"))
	for _, k := range keys {
		s.add(k)
	}
	return s
}

func benchIsMember(b *testing.B, n int, hit bool) {
	keys := members16(n)
	s := buildHT(keys)
	if s.enc != encHashtable {
		b.Fatalf("enc = %s, want hashtable", s.enc)
	}
	probes := members16(n)
	if !hit {
		for i := range probes { // flip a high bit so none are present
			probes[i][15] ^= 0x80
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = s.has(probes[i%n])
	}
}

func BenchmarkSIsMember10kHit(b *testing.B)  { benchIsMember(b, 10_000, true) }
func BenchmarkSIsMember10kMiss(b *testing.B) { benchIsMember(b, 10_000, false) }
func BenchmarkSIsMember1MHit(b *testing.B)   { benchIsMember(b, 1_000_000, true) }
func BenchmarkSIsMember1MMiss(b *testing.B)  { benchIsMember(b, 1_000_000, false) }

func benchSAdd(b *testing.B, n int) {
	keys := members16(n)
	seed := []byte("seed-not-an-int-so-listpack-then-table")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := newSet(seed)
		for _, k := range keys {
			s.add(k)
		}
	}
	// Per-member insert cost, growth and rehash amortized across the build.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

func BenchmarkSAdd10k(b *testing.B) { benchSAdd(b, 10_000) }
func BenchmarkSAdd1M(b *testing.B)  { benchSAdd(b, 1_000_000) }

// BenchmarkSAddDup is the pure steady-state SADD probe: every member is already
// present, so it is one probe plus confirm with no insert, the common dedup hot
// path and the zero-allocation case.
func benchSAddDup(b *testing.B, n int) {
	keys := members16(n)
	s := buildHT(keys)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = s.add(keys[i%n])
	}
}

func BenchmarkSAddDup10k(b *testing.B) { benchSAddDup(b, 10_000) }
func BenchmarkSAddDup1M(b *testing.B)  { benchSAddDup(b, 1_000_000) }
