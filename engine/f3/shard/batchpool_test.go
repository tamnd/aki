package shard

import (
	"runtime"
	"runtime/debug"
	"testing"
)

// pinPool makes a sync.Pool's Put-then-Get identity deterministic for a test
// that asserts it. Two things break that identity: a GC may drain the pool
// (SetGCPercent(-1)), and a goroutine migrating off its P between Put and Get
// misses the per-P private slot (GOMAXPROCS(1) keeps it on one P). Both are
// restored on cleanup. The race detector adds a third: sync.Pool.Put randomly
// drops the value under -race, so an identity assertion cannot hold there; those
// tests skip under -race (raceEnabled) and the restamp invariant, which does not
// depend on a node surviving the pool, carries the coverage instead.
func pinPool(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("sync.Pool.Put randomly drops under -race; identity is not assertable")
	}
	prevGC := debug.SetGCPercent(-1)
	prevProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(prevProcs)
		debug.SetGCPercent(prevGC)
	})
}

// The hop-transport node pool is two-tier: a per-connection free channel (L1)
// backed by a runtime-shared sync.Pool (L2). take drains L1 first and falls
// through to L2; recycle fills L1 first and overflows to L2. Under a pipelined
// burst the reader outruns the writer's recycle and L1 runs dry, so the pool,
// not a per-command allocation, must back the overflow (labs/f3/m0/31).

// A node recycled onto a connection's own free list comes straight back on the
// next take, with its conn intact, the contention-free steady path.
func TestTakeReusesL1Node(t *testing.T) {
	r := New(2, 64<<20, 0)
	c := r.NewConn()
	b := c.take()
	c.recycle(b)
	got := c.take()
	if got != b {
		t.Fatalf("take after recycle returned a different node; L1 not reused")
	}
	if got.conn != c {
		t.Fatalf("L1 node lost its conn stamp")
	}
}

// When L1 is full, recycle overflows to the shared pool instead of dropping the
// node to the collector, and a later take past a dry L1 pulls it back.
func TestRecycleOverflowsToPool(t *testing.T) {
	pinPool(t)
	r := New(2, 64<<20, 0)
	r.resolveConnCaps(Config{FreeListCap: 1})
	c := r.NewConn()

	// Two live nodes, then recycle both: the first fills L1 (cap 1), the second
	// overflows to the shared pool.
	b1 := c.take()
	b2 := c.take()
	c.recycle(b1)
	c.recycle(b2)

	// Drain L1 (one node), then the next take must come from the pool, not a
	// fresh allocation: it is one of the two we recycled.
	g1 := c.take()
	g2 := c.take()
	if g1 != b1 && g1 != b2 {
		t.Fatalf("first take did not reuse a recycled node")
	}
	if g2 != b1 && g2 != b2 {
		t.Fatalf("second take (past dry L1) did not reuse the overflow node from the pool")
	}
	if g1 == g2 {
		t.Fatalf("take returned the same node twice")
	}
}

// A node one connection overflows to the shared pool can be handed to another
// connection on the same runtime, and take restamps its conn so replies route
// to the taker, not the original owner.
func TestPoolNodeRestampsConn(t *testing.T) {
	pinPool(t)
	r := New(2, 64<<20, 0)
	r.resolveConnCaps(Config{FreeListCap: 1})
	r.freeListCap = 0 // no L1: every recycle overflows to the pool (Config guards >0)
	c1 := r.NewConn()
	c2 := r.NewConn()

	b := c1.take()
	if b.conn != c1 {
		t.Fatalf("take did not stamp the originating conn")
	}
	c1.recycle(b) // straight to the pool, L1 cap 0

	got := c2.take()
	if got != b {
		t.Fatalf("c2 did not draw the pooled node c1 freed")
	}
	if got.conn != c2 {
		t.Fatalf("pooled node kept conn %p, want restamp to c2 %p", got.conn, c2)
	}
}

// take stamps the taking connection onto every node it returns, whatever the
// source. This is the invariant that makes a runtime-shared pool safe to hand a
// node between connections: replies always route to the current owner, never the
// node's previous one. It holds without pool identity, so it runs under -race
// where the pool-reuse assertions cannot.
func TestTakeAlwaysStampsTaker(t *testing.T) {
	r := New(2, 64<<20, 0)
	r.resolveConnCaps(Config{FreeListCap: 1})
	r.freeListCap = 0 // force every take through the shared pool, never L1
	c1 := r.NewConn()
	c2 := r.NewConn()

	b := c1.take()
	c1.recycle(b)
	got := c2.take()
	if got.conn != c2 {
		t.Fatalf("take returned a node stamped %p, want the taking conn %p", got.conn, c2)
	}
}
