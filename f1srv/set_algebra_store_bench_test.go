package f1srv

import (
	"strconv"
	"testing"
)

// benchAlgebraStoreCompute isolates the server-side compute of the three STORE forms with no
// socket: it drives commands straight through a connState's parse-dispatch-reply buffer. Two
// half-overlapping sets of `members` members (set a over m0..m{members-1}, set b over the shifted
// band m{members/2}..) reproduce the aki-bench range.go overlap model, so SINTER and SDIFF each
// yield members/2 and SUNION yields 1.5x members. The single-connection socket bench that preceded
// this was 81% loopback syscall and measured the round-trip, not the algebra; at box saturation the
// network amortizes across connections and this compute is the real bottleneck.
func benchAlgebraStoreCompute(b *testing.B, members int, store func(c *connState)) {
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 14, ArenaBytes: 1 << 26, ReadBufSize: 16 << 10, IncrStripes: 64, SetAlgebraMerge: true}
	srv := New(cfg)
	c := &connState{srv: srv, blockable: true}

	shift := members / 2
	member := func(n int) []byte { return []byte("member:" + strconv.Itoa(n)) }
	seta := []byte("seta")
	setb := []byte("setb")
	for i := range members {
		c.out = c.out[:0]
		c.cmdSAdd([][]byte{[]byte("SADD"), seta, member(i)})
		c.out = c.out[:0]
		c.cmdSAdd([][]byte{[]byte("SADD"), setb, member(i + shift)})
	}

	b.ResetTimer()
	for range b.N {
		c.out = c.out[:0]
		store(c)
	}
}

var (
	benchDst  = []byte("dst")
	benchSeta = []byte("seta")
	benchSetb = []byte("setb")
)

func sinterStoreArgs() [][]byte {
	return [][]byte{[]byte("SINTERSTORE"), benchDst, benchSeta, benchSetb}
}
func sunionStoreArgs() [][]byte {
	return [][]byte{[]byte("SUNIONSTORE"), benchDst, benchSeta, benchSetb}
}
func sdiffStoreArgs() [][]byte {
	return [][]byte{[]byte("SDIFFSTORE"), benchDst, benchSeta, benchSetb}
}

func BenchmarkSInterStoreCompute1k(b *testing.B) {
	benchAlgebraStoreCompute(b, 1000, func(c *connState) { c.cmdSInterStore(sinterStoreArgs()) })
}
func BenchmarkSUnionStoreCompute1k(b *testing.B) {
	benchAlgebraStoreCompute(b, 1000, func(c *connState) { c.cmdSUnionStore(sunionStoreArgs()) })
}
func BenchmarkSDiffStoreCompute1k(b *testing.B) {
	benchAlgebraStoreCompute(b, 1000, func(c *connState) { c.cmdSDiffStore(sdiffStoreArgs()) })
}
func BenchmarkSInterStoreCompute10k(b *testing.B) {
	benchAlgebraStoreCompute(b, 10000, func(c *connState) { c.cmdSInterStore(sinterStoreArgs()) })
}
func BenchmarkSUnionStoreCompute10k(b *testing.B) {
	benchAlgebraStoreCompute(b, 10000, func(c *connState) { c.cmdSUnionStore(sunionStoreArgs()) })
}
func BenchmarkSDiffStoreCompute10k(b *testing.B) {
	benchAlgebraStoreCompute(b, 10000, func(c *connState) { c.cmdSDiffStore(sdiffStoreArgs()) })
}
