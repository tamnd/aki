package command

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// dispatch_hybrid_bench_test.go isolates the per-command user-space cost of the
// integrated command path on the hybrid engine: RESP is already parsed, the
// socket is gone, so what is left is dispatch + handler + reply encode, the
// exact work a pipelined burst pays per command once the read and write
// syscalls are amortized across the batch (see networking/conn.go drain).
//
// Note 291 found that at pipeline depth 16 the read/write syscalls fall to one
// per ~16 commands, so the wire gap on GET and INCR is this user-space cost, not
// the netpoller. This benchmark measures that cost directly and is the place to
// profile it (-cpuprofile) without the loopback kqueue/epoll noise a real socket
// adds, which is also what makes the number portable across macOS and Linux.

// newHybridDispatcher builds a dispatcher whose string point path runs on the
// hybrid-log store, matching `aki server --aki-engine hybrid` (server.go builds
// the same Tunables). Background workers are started so writes take the live
// write-behind path the wire bench exercises.
func newHybridDispatcher(tb testing.TB) *Dispatcher {
	tb.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "hb.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	tun := store.Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: ""}
	ks, err := keyspace.Open(p, keyspace.WithHybridLog(tun))
	if err != nil {
		tb.Fatalf("open hybrid keyspace: %v", err)
	}
	d := New(Config{Engine: NewEngine(ks)})
	d.StartBackground()
	tb.Cleanup(d.StopBackground)
	return d
}

// benchDispatch runs argv through the dispatcher b.N times against a reused
// offline connection, resetting its output buffer each iteration so the reply
// encode is measured but the buffer does not grow without bound.
func benchDispatch(b *testing.B, d *Dispatcher, argv [][]byte) {
	b.Helper()
	c := networking.NewOfflineConn()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		d.Handle(c, argv)
		c.ResetOut()
	}
}

// BenchmarkDispatchHybridGet measures the GET per-command cost on a populated
// key. GET is the wire path note 291 measured at 0.78x Redis, so this is where
// the user-space gap should show.
func BenchmarkDispatchHybridGet(b *testing.B) {
	d := newHybridDispatcher(b)
	key := []byte("bench:get:key")
	d.Handle(networking.NewOfflineConn(), [][]byte{[]byte("SET"), key, []byte("value")})
	benchDispatch(b, d, [][]byte{[]byte("GET"), key})
}

// BenchmarkDispatchHybridSet measures the SET per-command cost, the path that
// reaches wire parity with Redis (1.05x) so its user-space cost is the floor the
// others should reach.
func BenchmarkDispatchHybridSet(b *testing.B) {
	d := newHybridDispatcher(b)
	benchDispatch(b, d, [][]byte{[]byte("SET"), []byte("bench:set:key"), []byte("value")})
}

// BenchmarkDispatchHybridIncr measures the INCR per-command cost, the worst wire
// gap (0.78x Redis, 0.52x Valkey), so its user-space breakdown is the most
// useful to profile.
func BenchmarkDispatchHybridIncr(b *testing.B) {
	d := newHybridDispatcher(b)
	benchDispatch(b, d, [][]byte{[]byte("INCR"), []byte("bench:incr:key")})
}

// BenchmarkDispatchHybridGetParallel drives GET from all cores at once so the
// shared-line costs the single-stream benchmark hides (the value cache, the
// write-behind probe) show up the way they do under a saturating client.
func BenchmarkDispatchHybridGetParallel(b *testing.B) {
	d := newHybridDispatcher(b)
	for i := range 256 {
		key := []byte(fmt.Sprintf("bench:getp:%03d", i))
		d.Handle(networking.NewOfflineConn(), [][]byte{[]byte("SET"), key, []byte("value")})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := networking.NewOfflineConn()
		argv := [][]byte{[]byte("GET"), []byte("bench:getp:000")}
		i := 0
		for pb.Next() {
			argv[1] = []byte(fmt.Sprintf("bench:getp:%03d", i&255))
			d.Handle(c, argv)
			c.ResetOut()
			i++
		}
	})
}
