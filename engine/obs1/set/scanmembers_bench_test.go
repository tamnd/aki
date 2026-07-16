package set

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The enumeration microbenchmarks (doc 11 section 8). BenchmarkSScan pages a
// full 1M set through the downward cursor and reports the per-page work; the
// walk reads records straight off the vector with no allocation, so the row is
// a zero-alloc floor for tail latency. BenchmarkSMembers streams the whole set
// through the same encoder the server uses, so its one allocation is the
// ordinal snapshot, not the members. Members are 16 bytes, the gate default.

func benchSScan(b *testing.B, n, count int) {
	s := buildHT(members16(n))
	if s.enc != encHashtable {
		b.Fatalf("enc = %s, want hashtable", s.enc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var cur uint64
		for {
			next := s.scanPage(cur, count, nil, func(m []byte) { sinkBytes = m })
			if next == 0 {
				break
			}
			cur = next
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

// BenchmarkSScan pages the full 1M set at the default COUNT, the whole-set walk
// the row keys on.
func BenchmarkSScan(b *testing.B) { benchSScan(b, 1_000_000, 10) }

func benchSMembers(b *testing.B, n int) {
	s := buildHT(members16(n))
	dst := make([]byte, store.ChunkSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := s.ht.pinMembersStream()
		for {
			k, err := src.Next(dst)
			if err != nil {
				b.Fatal(err)
			}
			if k == 0 {
				break
			}
		}
		src.Release()
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/member")
}

func BenchmarkSMembers10k(b *testing.B) { benchSMembers(b, 10_000) }
func BenchmarkSMembers1M(b *testing.B)  { benchSMembers(b, 1_000_000) }
