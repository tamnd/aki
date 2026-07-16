package set

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The SMOVE hot-pair microbenchmark (doc 11 section 9.2). SMOVE is two point ops,
// a remove at the source and an add at the destination, so the hot-pair drives one
// shuttle member back and forth between two co-located sets of the gate default
// member size (16 bytes). Both sets stay resident, so the timed region is one
// remove plus one add on the two bands with no growth or rehash. These are Go
// microbenchmarks, not the GamingPC gate; the gate row keys on the server number.
func benchSmove(b *testing.B, n int) {
	keys := members16(n)
	src := buildHT(keys)
	// A disjoint destination of the same shape, so both sit in the same band.
	dstKeys := members16(2 * n)[n:]
	dst := buildHT(dstKeys)
	if src.enc != encHashtable && src.enc != encPartitioned {
		b.Fatalf("src enc = %s, want hashtable or partitioned", src.enc)
	}
	shuttle := []byte("shuttle-member-x") // 16 bytes, in neither base population
	src.add(shuttle)

	g := &reg{m: map[string]*set{"a": src, "b": dst}}
	cx := &shard.Ctx{St: store.New(1<<20, 1<<16), NowMs: 1}
	a, bkey := []byte("a"), []byte("b")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1 == 0 {
			sink, _ = smove(g, cx, a, bkey, shuttle)
		} else {
			sink, _ = smove(g, cx, bkey, a, shuttle)
		}
	}
}

func BenchmarkSmove10k(b *testing.B) { benchSmove(b, 10_000) }
func BenchmarkSmove1M(b *testing.B)  { benchSmove(b, 1_000_000) }
