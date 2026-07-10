// Lab: spin-before-park window (spec 2064/f3/03 section 9, M0 lab 3).
//
// The question: how long should an idle owner worker spin on its inbound
// queue before parking? Doc 03 pre-registers a 4us window (PRED-X7). A
// parked worker costs nothing but adds a wake round trip (futex on Linux, a
// runtime park here) to the next command's latency; a spinning worker
// catches the next command in tens of nanoseconds but burns its core while
// idle. The window buys latency with idle CPU, and the right size depends on
// the gap between arrivals.
//
// Method: one pinned consumer polls a stamp ring (the queue-empty check is
// one plain load, like the Vyukov next-pointer probe) under the three-state
// protocol of doc 03 section 9.1: running, spinning with a deadline, parked
// on a wake channel after a final recheck that closes the lost-wake race.
// One pinned producer offers three loads: arrivals every 5us, arrivals every
// 50us, and back-to-back saturation. The spin window sweeps 0 (park
// immediately), 1us, 4us, 16us, 64us, and spin-forever. Reported per cell:
// wake-to-receive latency p50/p99 (producer stamps before publishing), parks
// per thousand messages, and spin burn, the share of wall time the consumer
// spent spinning empty, which is the idle CPU the window costs.
//
// macOS has no raw futex, so the park here is a Go channel receive; the
// parked-path latency floor on Linux will differ (futex wake is ~1-2us
// syscall path) and the gate box rerun settles the absolute numbers.
//
// See README.md for the numbers and the verdict.
package main

import (
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
)

const (
	ringBits = 12
	ringSize = 1 << ringBits
)

const (
	stRunning int32 = iota
	stSpinning
	stParked
)

type worker struct {
	ring  [ringSize]atomic.Int64 // message stamps, 0 = empty slot
	state atomic.Int32
	wake  chan struct{}

	// Consumer-side tallies for one run.
	lat      []int64
	parks    int64
	spinNS   int64
	recvNS   int64 // wall time of the consume loop
	received int
}

func newWorker() *worker {
	return &worker{wake: make(chan struct{}, 1)}
}

// consume receives total messages, spinning up to window before each park.
// window < 0 means spin forever.
func (w *worker) consume(total int, window time.Duration) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	t0 := time.Now()
	head := 0
	for w.received < total {
		slot := &w.ring[head&(ringSize-1)]
		if s := slot.Load(); s != 0 {
			w.lat = append(w.lat, time.Now().UnixNano()-s)
			slot.Store(0)
			head++
			w.received++
			continue
		}
		// Empty: spin for the window, then park.
		spinStart := time.Now()
		w.state.Store(stSpinning)
		deadline := spinStart.Add(window)
		for slot.Load() == 0 {
			if window >= 0 && time.Now().After(deadline) {
				// Park protocol: declare parked, then recheck once, so a
				// producer that published before loading the state either
				// sees parked (and wakes us) or we see its message here.
				w.state.Store(stParked)
				if slot.Load() != 0 {
					w.state.Store(stRunning)
					break
				}
				w.spinNS += time.Since(spinStart).Nanoseconds()
				w.parks++
				<-w.wake
				spinStart = time.Now() // park time is free, not spin burn
				w.state.Store(stRunning)
				break
			}
		}
		if st := w.state.Load(); st == stSpinning {
			w.state.Store(stRunning)
			w.spinNS += time.Since(spinStart).Nanoseconds()
		}
	}
	w.recvNS = time.Since(t0).Nanoseconds()
	w.state.Store(stRunning)
}

// produce publishes total messages with the given inter-arrival gap (0 means
// back to back), waking the consumer when it observes it parked.
func (w *worker) produce(total int, gap time.Duration) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	next := time.Now()
	tail := 0
	for sent := 0; sent < total; sent++ {
		if gap > 0 {
			next = next.Add(gap)
			for time.Now().Before(next) {
				// Busy pacing: sleep granularity is far coarser than the gap.
			}
		}
		slot := &w.ring[tail&(ringSize-1)]
		for slot.Load() != 0 {
			// Ring full under saturation; wait for the consumer.
		}
		slot.Store(time.Now().UnixNano())
		tail++
		if w.state.Load() == stParked && w.state.CompareAndSwap(stParked, stRunning) {
			w.wake <- struct{}{}
		}
	}
}

type cell struct {
	p50, p99  time.Duration
	parksPerK float64
	burn      float64 // spin CPU as a share of wall time
	mops      float64
}

func run(total int, gap, window time.Duration) cell {
	w := newWorker()
	done := make(chan struct{})
	go func() {
		w.consume(total, window)
		close(done)
	}()
	go w.produce(total, gap)
	<-done

	sort.Slice(w.lat, func(i, j int) bool { return w.lat[i] < w.lat[j] })
	return cell{
		p50:       time.Duration(w.lat[len(w.lat)/2]),
		p99:       time.Duration(w.lat[len(w.lat)*99/100]),
		parksPerK: float64(w.parks) * 1000 / float64(total),
		burn:      float64(w.spinNS) / float64(w.recvNS),
		mops:      float64(total) / (float64(w.recvNS) / 1e9) / 1e6,
	}
}

func windowName(d time.Duration) string {
	if d < 0 {
		return "forever"
	}
	return d.String()
}

func main() {
	windows := []time.Duration{0, 1 * time.Microsecond, 4 * time.Microsecond,
		16 * time.Microsecond, 64 * time.Microsecond, -1}

	fmt.Printf("cores=%d GOMAXPROCS=%d park=channel (no futex on darwin)\n",
		runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, load := range []struct {
		name  string
		gap   time.Duration
		total int
	}{
		{"low load, 5us between arrivals", 5 * time.Microsecond, 200000},
		{"idle-ish load, 50us between arrivals", 50 * time.Microsecond, 40000},
	} {
		fmt.Printf("\n%s (%d msgs)\n\n", load.name, load.total)
		fmt.Println("| window | p50 wake | p99 wake | parks/1k msgs | spin burn |")
		fmt.Println("|---|---|---|---|---|")
		for _, win := range windows {
			c := run(load.total, load.gap, win)
			fmt.Printf("| %s | %v | %v | %.0f | %.0f%% |\n",
				windowName(win), c.p50, c.p99, c.parksPerK, c.burn*100)
		}
	}

	// Saturation: the window should be irrelevant and parks near zero.
	fmt.Printf("\nsaturation, back-to-back (4M msgs)\n\n")
	fmt.Println("| window | Mmsgs/s | parks/1k msgs |")
	fmt.Println("|---|---|---|")
	for _, win := range []time.Duration{0, 4 * time.Microsecond} {
		c := run(4<<20, 0, win)
		fmt.Printf("| %s | %.1f | %.2f |\n", windowName(win), c.mops, c.parksPerK)
	}
}
