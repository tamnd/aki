package f1srv

import (
	"strconv"
	"testing"
)

// buildAlgebraSets loads two sets a and b each with n members, sharing the middle
// half so the intersection is ~n/2, matching the aki-bench algebraPreload layout.
// It drives real SADD through a connState so the storage (member rows, dense vector,
// header) is exactly what the server builds.
func buildAlgebraSets(tb testing.TB, n int) (*connState, [][]byte, [][]byte) {
	tb.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 21, ArenaBytes: 1 << 30, ReadBufSize: 4 << 10, IncrStripes: 64}
	srv := New(cfg)
	c := &connState{srv: srv, rngState: 1}
	load := func(key string, lo int) {
		batch := make([][]byte, 0, 1002)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			c.out = c.out[:0]
			c.cmdSAdd(batch)
			batch = batch[:0]
		}
		for i := 0; i < n; i++ {
			if len(batch) == 0 {
				batch = append(batch, []byte("SADD"), []byte(key))
			}
			batch = append(batch, []byte("m"+strconv.Itoa(lo+i)))
			if len(batch) >= 1002 {
				flush()
			}
		}
		flush()
	}
	load("set:x:a", 0)
	load("set:x:b", n/2)
	sinterArgv := [][]byte{[]byte("SINTER"), []byte("set:x:a"), []byte("set:x:b")}
	sunionArgv := [][]byte{[]byte("SUNION"), []byte("set:x:a"), []byte("set:x:b")}
	return c, sinterArgv, sunionArgv
}

func BenchmarkSInterBig(b *testing.B) {
	c, sinterArgv, _ := buildAlgebraSets(b, 500000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.out = c.out[:0]
		c.cmdSInter(sinterArgv)
	}
}

func BenchmarkSUnionBig(b *testing.B) {
	c, _, sunionArgv := buildAlgebraSets(b, 500000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.out = c.out[:0]
		c.cmdSUnion(sunionArgv)
	}
}
