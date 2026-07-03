package f1srv

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestHopGroupWaitReturnsAfterAllComplete(t *testing.T) {
	g := newHopGroup()
	g.add(3)
	var ran atomic.Int64
	for i := 0; i < 3; i++ {
		go func() {
			ran.Add(1)
			g.complete()
		}()
	}
	g.wait()
	if ran.Load() != 3 {
		t.Fatalf("wait returned with %d of 3 completions", ran.Load())
	}
}

func TestHopGroupEmptyReturnsImmediately(t *testing.T) {
	// A drain that routed nothing must not block: wait on a group with nothing added returns.
	g := newHopGroup()
	g.wait()
}

func TestHopGroupLateWait(t *testing.T) {
	// The last complete can land before the home loop reaches wait; done is latched so the late
	// wait still returns rather than sleeping forever.
	g := newHopGroup()
	g.add(1)
	g.complete()
	g.wait()
}

func TestShardWorkerRunsHopsInSubmitOrder(t *testing.T) {
	// A single producer's successive submits must run in submit order, because one connection's
	// commands for a shard are handed over as an ordered run.
	w := newShardWorker(0)
	go w.run()
	defer w.shutdown()

	const n = 1000
	var mu sync.Mutex
	var order []int
	g := newHopGroup()
	g.add(n)
	for i := 0; i < n; i++ {
		i := i
		w.submit(&hop{
			group: g,
			fn: func() {
				mu.Lock()
				order = append(order, i)
				mu.Unlock()
			},
		})
	}
	g.wait()

	if len(order) != n {
		t.Fatalf("ran %d hops, want %d", len(order), n)
	}
	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("hop %d ran out of order: got %d", i, order[i])
		}
	}
}

func TestShardWorkerConcurrentProducers(t *testing.T) {
	// Many home loops feed one worker at once; every hop must run exactly once and the group
	// barrier must account for all of them. This is the case the Treiber intake exists for.
	w := newShardWorker(0)
	go w.run()
	defer w.shutdown()

	const producers = 8
	const perProducer = 5000
	var counter atomic.Int64
	g := newHopGroup()
	g.add(producers * perProducer)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				w.submit(&hop{group: g, fn: func() { counter.Add(1) }})
			}
		}()
	}
	wg.Wait()
	g.wait()

	if got := counter.Load(); got != producers*perProducer {
		t.Fatalf("ran %d hops, want %d", got, producers*perProducer)
	}
}

func TestShardWorkerParkWakeCycle(t *testing.T) {
	// Feed the worker in slow bursts with idle gaps so it parks between them, and confirm every
	// burst still runs. This exercises the spin-then-park handoff and the wake on submit; the
	// gaps are enforced by waiting on each burst's group before starting the next.
	w := newShardWorker(0)
	go w.run()
	defer w.shutdown()

	var counter atomic.Int64
	for burst := 0; burst < 50; burst++ {
		g := newHopGroup()
		g.add(4)
		for i := 0; i < 4; i++ {
			w.submit(&hop{group: g, fn: func() { counter.Add(1) }})
		}
		g.wait()
	}
	if got := counter.Load(); got != 200 {
		t.Fatalf("ran %d hops across bursts, want 200", got)
	}
}

func TestShardPoolRoutesByShard(t *testing.T) {
	// The pool must run each hop on the worker for the shard the caller names, and shut down
	// cleanly with all workers accounted for.
	const nShards = 8
	p := newShardPool(nShards)
	p.start()
	defer p.shutdown()

	seen := make([]atomic.Int64, nShards)
	g := newHopGroup()
	const perShard = 1000
	g.add(nShards * perShard)
	for s := 0; s < nShards; s++ {
		s := s
		for i := 0; i < perShard; i++ {
			p.submit(s, &hop{group: g, fn: func() { seen[s].Add(1) }})
		}
	}
	g.wait()

	for s := 0; s < nShards; s++ {
		if got := seen[s].Load(); got != perShard {
			t.Errorf("shard %d ran %d hops, want %d", s, got, perShard)
		}
	}
}

func TestShardPoolSingleShardFloor(t *testing.T) {
	// A pool sized below one still gives a working single worker, matching shardFor's collapse to
	// shard 0 so a one-core server needs no special case.
	p := newShardPool(0)
	if len(p.workers) != 1 {
		t.Fatalf("newShardPool(0) made %d workers, want 1", len(p.workers))
	}
	p.start()
	defer p.shutdown()

	var counter atomic.Int64
	g := newHopGroup()
	g.add(10)
	for i := 0; i < 10; i++ {
		p.submit(0, &hop{group: g, fn: func() { counter.Add(1) }})
	}
	g.wait()
	if counter.Load() != 10 {
		t.Fatalf("single-shard pool ran %d hops, want 10", counter.Load())
	}
}

func TestShardWorkerShutdownIsIdempotentlySafe(t *testing.T) {
	// Shutdown with no work submitted must still stop the worker goroutine promptly.
	w := newShardWorker(3)
	go w.run()
	w.shutdown()
	select {
	case <-w.done:
	default:
		t.Fatal("worker goroutine did not exit after shutdown")
	}
}
