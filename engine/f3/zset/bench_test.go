package zset

import (
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The inline-band microbenchmarks (spec 2064/f3/12 section 4). ZSCORE and ZCARD
// are the zero-allocation reads the small-cardinality gate rows lean on, and
// ZADD is the insert path amortized over a full inline build (the memmove plus
// the ordered splice). These are Go microbenchmarks, not the GamingPC gate; they
// order the mechanism and quote a floor, and the gate rows key on the server
// numbers.

func buildInline(n int) *zset {
	z := newZset()
	for i := 0; i < n; i++ {
		z.update([]byte("m"+itoa(i)), float64(i), flags{})
	}
	return z
}

func BenchmarkZScoreInlineHit(b *testing.B) {
	z := buildInline(64)
	m := []byte("m40")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, sinkBool = z.score(m)
	}
}

func BenchmarkZScoreInlineMiss(b *testing.B) {
	z := buildInline(64)
	m := []byte("absent")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, sinkBool = z.score(m)
	}
}

func BenchmarkZCardInline(b *testing.B) {
	z := buildInline(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkInt = z.card()
	}
}

// ZADD over a churning member: re-add an existing member at a new score, the
// rescore memmove path that dominates a live inline leaderboard.
func BenchmarkZAddInlineRescore(b *testing.B) {
	z := buildInline(64)
	m := []byte("m32")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.update(m, float64(i%128), flags{})
	}
}

// ZSCORE formatting: the reply path over an inline hit, the shape ZSCORE ships.
func BenchmarkZScoreFormat(b *testing.B) {
	z := buildInline(64)
	m := []byte("m40")
	var buf [40]byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := z.score(m)
		sinkBytes = resp.FormatScore(buf[:0], s)
	}
}

var sinkBytes []byte
