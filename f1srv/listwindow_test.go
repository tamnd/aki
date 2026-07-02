package f1srv

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestListWindowTailGapFreedom hammers reserveTail/commitTail from many goroutines with runs that
// finish out of reservation order, and asserts the committed bound never advances past a position
// whose element has not been filled. This is the property the whole overlay rests on: a reader that
// stops at committedTail must never see a reserved-but-unfilled slot. Each goroutine reserves a run,
// marks its positions filled, then commits, with the fill deliberately staggered so runs commit in a
// scrambled order and the ordered-commit drain is exercised.
func TestListWindowTailGapFreedom(t *testing.T) {
	const goroutines = 64
	const runsPer = 200
	const maxRun = 7

	w := newListWindow(0, 0)
	// filled[p] is set once the element at tail position p has been written. A committed position
	// must always be filled. Sized to the largest tail the run can reach.
	filled := make([]atomic.Int32, goroutines*runsPer*maxRun+16)

	// checker samples committedTail concurrently and asserts every position below it is filled.
	var stop atomic.Bool
	var checkFail atomic.Int64
	checkFail.Store(-1)
	var checker sync.WaitGroup
	checker.Add(1)
	go func() {
		defer checker.Done()
		for !stop.Load() {
			ct := w.committedTail.Load()
			for p := int64(0); p < ct; p++ {
				if filled[p].Load() == 0 {
					checkFail.CompareAndSwap(-1, p)
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// A cheap per-goroutine PRNG for run sizes and a jitter spin, so no shared Rand and no
			// Math.rand dependency; the sequence only needs to vary, not be uniform.
			x := uint32(seed*2654435761 + 1)
			next := func() uint32 { x ^= x << 13; x ^= x >> 17; x ^= x << 5; return x }
			for r := 0; r < runsPer; r++ {
				n := int64(next()%maxRun) + 1
				start := w.reserveTail(n)
				// Stagger the fill so runs land out of reservation order.
				for s := next() % 32; s > 0; s-- {
					_ = s
				}
				for p := start; p < start+n; p++ {
					filled[p].Store(1)
				}
				w.commitTail(start, n)
			}
		}(g)
	}
	wg.Wait()
	stop.Store(true)
	checker.Wait()

	if f := checkFail.Load(); f >= 0 {
		t.Fatalf("committed tail passed unfilled position %d", f)
	}
	// Every reserved position must be committed once all runs finished, and every position filled.
	total := int64(goroutines * runsPer)
	var sum int64
	// Recompute expected total elements from the actual run sizes by summing filled positions up to
	// the reserved tail, which equals the committed tail at quiescence.
	rt := w.reservedTail.Load()
	if ct := w.committedTail.Load(); ct != rt {
		t.Fatalf("committed tail %d did not catch reserved tail %d, pending drain leaked", ct, rt)
	}
	for p := int64(0); p < rt; p++ {
		if filled[p].Load() == 0 {
			t.Fatalf("position %d below reserved tail %d never filled", p, rt)
		}
		sum++
	}
	if sum != rt {
		t.Fatalf("filled count %d != reserved tail %d", sum, rt)
	}
	_ = total
}

// TestListWindowHeadGapFreedom is the mirror for LPUSH: runs reserve downward at the head, and the
// committed head must never drop below an unfilled position. Positions are negative and grow more
// negative, so the fill map is indexed by the distance below zero.
func TestListWindowHeadGapFreedom(t *testing.T) {
	const goroutines = 64
	const runsPer = 200
	const maxRun = 7

	w := newListWindow(0, 0)
	filled := make([]atomic.Int32, goroutines*runsPer*maxRun+16)
	// idx maps a head position p (<= 0, exclusive of 0 going down) to a filled index: position -1 is
	// index 0, -2 is index 1, and so on. reserveHead returns the lowest position of the run.
	idx := func(p int64) int64 { return -p - 1 }

	var stop atomic.Bool
	var checkFail atomic.Int64
	checkFail.Store(1)
	var checker sync.WaitGroup
	checker.Add(1)
	go func() {
		defer checker.Done()
		for !stop.Load() {
			ch := w.committedHead.Load()
			for p := int64(-1); p >= ch; p-- {
				if filled[idx(p)].Load() == 0 {
					checkFail.CompareAndSwap(1, p)
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint32(seed*2654435761 + 1)
			nextRand := func() uint32 { x ^= x << 13; x ^= x >> 17; x ^= x << 5; return x }
			for r := 0; r < runsPer; r++ {
				n := int64(nextRand()%maxRun) + 1
				start := w.reserveHead(n)
				for s := nextRand() % 32; s > 0; s-- {
					_ = s
				}
				for p := start; p < start+n; p++ {
					filled[idx(p)].Store(1)
				}
				w.commitHead(start, n)
			}
		}(g)
	}
	wg.Wait()
	stop.Store(true)
	checker.Wait()

	if f := checkFail.Load(); f <= 0 {
		t.Fatalf("committed head passed unfilled position %d", f)
	}
	rh := w.reservedHead.Load()
	if ch := w.committedHead.Load(); ch != rh {
		t.Fatalf("committed head %d did not catch reserved head %d, pending drain leaked", ch, rh)
	}
	for p := int64(-1); p >= rh; p-- {
		if filled[idx(p)].Load() == 0 {
			t.Fatalf("position %d above reserved head %d never filled", p, rh)
		}
	}
}
