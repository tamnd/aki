package set

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
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
	if s.enc != encHashtable && s.enc != encPartitioned {
		b.Fatalf("enc = %s, want hashtable or partitioned", s.enc)
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

// The draw microbenchmarks (doc 11 section 5). BenchmarkSRandMember is the pure
// P10 vector draw, non-mutating, which is the K11 port-lab bar of 4.8ns at 100k
// members and 12.2ns at 1M. BenchmarkSPop adds the swap-remove: it drains the set
// and refills off the clock, so the timed region is draw plus swap-remove plus
// table tombstone only. Members are 16 bytes (the gate default).

var sinkBytes []byte

func benchReg() *reg {
	return &reg{m: map[string]*set{}, rng: *rand.NewPCG(0x9e3779b97f4a7c15, 0xbf58476d1ce4e5b9)}
}

// BenchmarkSetLive drives the live funnel every set read and write routes through,
// the one that now stamps the per-key access clock OBJECT IDLETIME reads back. It
// fills a registry with a million one-member sets and resolves a rotating key each
// iteration, so the timed cost is the map lookup, the deadline check, and the clock
// stamp: the number the collection-clock slice must not move on the collection hot
// path (labs/f3/m9/02_collection_idle_clock).
func BenchmarkSetLive(b *testing.B) {
	const keys = 1 << 20
	g := benchReg()
	ks := members16(keys)
	for _, k := range ks {
		g.m[string(k)] = newSet(k)
	}
	cx := &shard.Ctx{St: store.New(16<<20, 0), NowMs: 1_000_000_000}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if g.live(cx, ks[i&(keys-1)]) == nil {
			b.Fatal("miss")
		}
	}
}

func benchSRandMember(b *testing.B, n int) {
	s := buildHT(members16(n))
	if s.enc != encHashtable && s.enc != encPartitioned {
		b.Fatalf("enc = %s, want hashtable or partitioned", s.enc)
	}
	g := benchReg()
	var sc [64]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkBytes = s.drawOne(g, sc[:])
	}
}

// The 100k cell is the exact shape of the f1 K11 port-lab draw bar (doc 11
// sections 1.5 and 11.4: 4.8ns at 100k members); the 10k and 1M cells bracket
// it, with 1M carrying the 12.2ns end of the same bar.
func BenchmarkSRandMember10k(b *testing.B)  { benchSRandMember(b, 10_000) }
func BenchmarkSRandMember100k(b *testing.B) { benchSRandMember(b, 100_000) }
func BenchmarkSRandMember1M(b *testing.B)   { benchSRandMember(b, 1_000_000) }

func benchSPop(b *testing.B, n int) {
	keys := members16(n)
	s := buildHT(keys)
	g := benchReg()
	var sc [64]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s.card() == 0 {
			b.StopTimer()
			s = buildHT(keys)
			b.StartTimer()
		}
		sinkBytes = s.popOne(g, sc[:])
	}
}

func BenchmarkSPop10k(b *testing.B) { benchSPop(b, 10_000) }
func BenchmarkSPop1M(b *testing.B)  { benchSPop(b, 1_000_000) }

// benchSRandMemberCount times the count-form draws: a positive count is the
// distinct sample, a negative count is with replacement. Both reuse the
// registry's scratch, so a steady-state count draw should not allocate.
func benchSRandMemberCount(b *testing.B, n, count int) {
	s := buildHT(members16(n))
	g := benchReg()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.sampleDistinct(g, count, func(m []byte) { sinkBytes = m })
	}
}

func BenchmarkSRandMemberCount10k(b *testing.B) { benchSRandMemberCount(b, 10_000, 10) }
func BenchmarkSRandMemberCount1M(b *testing.B)  { benchSRandMemberCount(b, 1_000_000, 10) }
