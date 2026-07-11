// Lab: timer-park pricing (spec 2064/f3 doc 03 section 9, M3 slice 8 PR 3).
//
// PR 3 bakes two new things into the shard owner loop: an idle park that, when a
// deadline is armed, selects over the waker channel and a reusable timer instead
// of a plain channel receive, and a fireTimers pass bounded by a new constant,
// timerFireCap. Both sit next to the P1 latency path, so this lab prices them
// before the constant lands in tuning.go.
//
// Two questions, two sweeps:
//
//  1. What does the timed park cost over the plain park, and is that cost paid
//     only when a timer is armed? The design constraint is that a worker with no
//     armed timer parks exactly as it does today, so the no-timer control here
//     must match the plain park within noise. The timed arm pays a timer Reset
//     plus a two-case select on every park; this sweep names that overhead in ns
//     so the P1 cost of the machinery is on the record.
//
//  2. Where should timerFireCap sit? When many deadlines come due at once the
//     owner fires them in capped batches, one batch per loop pass. A small cap
//     keeps each pass short (less time stolen from command processing) but
//     spreads the tail delivery across more passes; a large cap delivers the
//     whole burst in one pass but holds the loop longer. This sweep fires a fixed
//     burst at caps {16, 32, 64, 128} and reports both the whole-burst time and
//     the longest single pass, so the knee is visible.
//
// The park primitive and the deadline heap are unexported in the shard package,
// so this lab reprices them standalone, the way lab 03 reprices the waiter FIFO:
// the same three-state waker (running, spinning, parked) with the same
// claim-then-send wake, and the same min-heap shape timer.go ships. macOS has no
// raw futex, so the park is a Go channel receive; the absolute park floor on
// Linux differs (a futex wake is a ~1-2us syscall path), and the overhead this
// lab measures is the delta between the two park shapes, which is transport
// independent. See README.md for the numbers and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stRunning uint32 = iota
	stSpinning
	stParked
)

// pworker is the standalone park primitive: the waker state word, the park
// channel, and (for the timed variant) one reusable timer, exactly the shapes
// worker.go and waker.go carry.
type pworker struct {
	state atomic.Uint32
	ch    chan struct{}
	tmr   *time.Timer
}

func newPworker() *pworker {
	return &pworker{ch: make(chan struct{}, 1)}
}

// wake is the producer side: claim the park with one CAS and send the single
// token, the waker.go rule.
func (w *pworker) wake() bool {
	if w.state.Load() == stParked && w.state.CompareAndSwap(stParked, stRunning) {
		w.ch <- struct{}{}
		return true
	}
	return false
}

// park is the plain no-timer park: store parked and block on the channel. This
// is the control, byte-for-byte the path a worker with no armed timer takes.
func (w *pworker) park() {
	w.state.Store(stParked)
	<-w.ch
}

// timedPark is the armed-timer park: store parked, arm the reusable timer with
// the drain-before-reset idiom, and select over the channel and the timer. far
// is a deadline chosen so the timer never actually fires in this measurement, so
// the wake always comes through the channel and the two arms differ only by the
// timer machinery, which is the overhead we want.
func (w *pworker) timedPark(far time.Duration) {
	w.state.Store(stParked)
	if w.tmr == nil {
		w.tmr = time.NewTimer(far)
	} else {
		if !w.tmr.Stop() {
			select {
			case <-w.tmr.C:
			default:
			}
		}
		w.tmr.Reset(far)
	}
	select {
	case <-w.ch:
		if !w.tmr.Stop() {
			select {
			case <-w.tmr.C:
			default:
			}
		}
	case <-w.tmr.C:
		// Never taken at the far deadline; present so the shape matches worker.go.
		if !w.state.CompareAndSwap(stParked, stRunning) {
			<-w.ch
		}
	}
}

// parkRoundTrip runs reps park-then-wake round trips and returns ns per round
// trip and the wake-to-receive latency percentiles. A producer goroutine spins
// until the consumer is observed parked, stamps, then wakes; the consumer
// records the delivery latency. timed selects the park shape.
func parkRoundTrip(reps int, timed bool) (nsPer float64, p50, p99 int64) {
	w := newPworker()
	const far = time.Hour
	lat := make([]int64, 0, reps)
	var wg sync.WaitGroup
	wg.Add(1)
	stamp := new(atomic.Int64)
	go func() {
		defer wg.Done()
		for i := 0; i < reps; i++ {
			for w.state.Load() != stParked {
				// spin until the consumer has stored parked
			}
			stamp.Store(time.Now().UnixNano())
			w.wake()
		}
	}()
	start := time.Now()
	for i := 0; i < reps; i++ {
		if timed {
			w.timedPark(far)
		} else {
			w.park()
		}
		lat = append(lat, time.Now().UnixNano()-stamp.Load())
	}
	el := time.Since(start)
	wg.Wait()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	return float64(el.Nanoseconds()) / float64(reps), lat[len(lat)*50/100], lat[len(lat)*99/100]
}

// The min-heap of timer.go, repriced standalone for the fire-batch sweep.
type ptimer struct {
	deadlineMs int64
	fire       func()
	heapPos    int
}

type pheap struct{ a []*ptimer }

func (h *pheap) push(t *ptimer) {
	t.heapPos = len(h.a)
	h.a = append(h.a, t)
	i := t.heapPos
	for i > 0 {
		p := (i - 1) / 2
		if h.a[p].deadlineMs <= h.a[i].deadlineMs {
			break
		}
		h.a[p], h.a[i] = h.a[i], h.a[p]
		h.a[p].heapPos, h.a[i].heapPos = p, i
		i = p
	}
}

func (h *pheap) popMin() *ptimer {
	a := h.a
	n := len(a)
	t := a[0]
	a[0] = a[n-1]
	a[0].heapPos = 0
	a[n-1] = nil
	h.a = a[:n-1]
	i, m := 0, len(h.a)
	for {
		l := 2*i + 1
		if l >= m {
			break
		}
		c := l
		if r := l + 1; r < m && h.a[r].deadlineMs < h.a[l].deadlineMs {
			c = r
		}
		if h.a[i].deadlineMs <= h.a[c].deadlineMs {
			break
		}
		h.a[i], h.a[c] = h.a[c], h.a[i]
		h.a[i].heapPos, h.a[c].heapPos = i, c
		i = c
	}
	return t
}

func (h *pheap) popDue(nowMs int64, limit int, out []*ptimer) []*ptimer {
	for len(h.a) > 0 && h.a[0].deadlineMs <= nowMs && len(out) < limit {
		out = append(out, h.popMin())
	}
	return out
}

// fireBurst arms burst deadlines all due now, then drains them in capped passes
// exactly as the owner loop does, returning the whole-burst time and the longest
// single pass (the command-starvation term). fired counts the fire calls.
func fireBurst(burst, cap int) (totalNs, maxPassNs float64) {
	h := &pheap{}
	now := time.Now().UnixMilli()
	fired := 0
	fire := func() { fired++ }
	for i := 0; i < burst; i++ {
		h.push(&ptimer{deadlineMs: now, fire: fire})
	}
	scratch := make([]*ptimer, 0, cap)
	start := time.Now()
	var maxPass time.Duration
	for len(h.a) > 0 {
		ps := time.Now()
		scratch = h.popDue(now, cap, scratch[:0])
		for _, t := range scratch {
			t.fire()
		}
		if d := time.Since(ps); d > maxPass {
			maxPass = d
		}
	}
	total := time.Since(start)
	if fired != burst {
		panic("fire count mismatch")
	}
	return float64(total.Nanoseconds()), float64(maxPass.Nanoseconds())
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep for a fast check")
	flag.Parse()

	reps := 200000
	if *quick {
		reps = 20000
	}

	fmt.Println("timed-park overhead: park-then-wake round trip, no timer ever fires")
	fmt.Println("| park shape | ns/round trip | wake p50 ns | wake p99 ns |")
	fmt.Println("|---|---|---|---|")
	// Warm both shapes so the first-park timer allocation and channel warmup do
	// not skew the measured runs.
	parkRoundTrip(reps/10, false)
	parkRoundTrip(reps/10, true)
	plainNs, pp50, pp99 := parkRoundTrip(reps, false)
	fmt.Printf("| plain (no-timer control) | %.0f | %d | %d |\n", plainNs, pp50, pp99)
	timedNs, tp50, tp99 := parkRoundTrip(reps, true)
	fmt.Printf("| timed (armed) | %.0f | %d | %d |\n", timedNs, tp50, tp99)
	fmt.Printf("\ntimed-park overhead over the plain park: %.0f ns/park\n\n", timedNs-plainNs)

	burst := 4096
	if *quick {
		burst = 512
	}
	fmt.Printf("fire-batch cap sweep: %d simultaneously-due timers drained in capped passes\n", burst)
	fmt.Println("| cap | passes | whole-burst us | longest pass us |")
	fmt.Println("|---|---|---|---|")
	for _, cap := range []int{16, 32, 64, 128} {
		// Median of a few runs; the cells are small and noisy.
		var bestTotal, bestPass float64
		const runs = 5
		totals := make([]float64, runs)
		passes := make([]float64, runs)
		for r := 0; r < runs; r++ {
			totals[r], passes[r] = fireBurst(burst, cap)
		}
		sort.Float64s(totals)
		sort.Float64s(passes)
		bestTotal, bestPass = totals[runs/2], passes[runs/2]
		passesCount := (burst + cap - 1) / cap
		fmt.Printf("| %d | %d | %.2f | %.2f |\n", cap, passesCount, bestTotal/1e3, bestPass/1e3)
	}
}
