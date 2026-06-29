package command

import (
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// newHybridEngine opens an in-memory keyspace routed through the hybrid-log store
// and wraps it in an Engine, the same configuration `aki server --aki-engine
// hybrid` runs. It is the harness for the hybrid point-path microbenchmarks so the
// allocation numbers those paths pay are always available without a live server.
func newHybridEngine(b *testing.B) *Engine {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p, keyspace.WithHybridLog(store.Tunables{
		Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: "",
	}))
	if err != nil {
		b.Fatalf("open keyspace: %v", err)
	}
	return NewEngine(ks)
}

// BenchmarkHybridIncr drives the integrated fast-path increment straight on the
// hybrid-log store, the work `aki server --aki-engine hybrid` does per INCR once
// the read and write syscalls are amortized across a pipelined burst. The headline
// number here is allocs/op: the body is formatted into a stack array, so the
// formatting itself must not allocate, and this benchmark is the guard that keeps
// it that way.
func BenchmarkHybridIncr(b *testing.B) {
	e := newHybridEngine(b)
	key := []byte("counter")
	if _, _, err := e.hybridIncr(0, key, 0); err != nil {
		b.Fatalf("seed incr: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := e.hybridIncr(0, key, 1); err != nil {
			b.Fatalf("incr: %v", err)
		}
	}
}
