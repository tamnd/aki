// poolrate scores PRED-OBS1-O0A-POOL on the simulator (spec 2064/obs1
// milestone O0a): does a warm pool of 256 sustain 5,000 GET/s from one
// process without client-side queuing once every GET pays the doc 01
// section 2.2 latency model? One configuration per run, one CSV row to
// stdout.
//
// The pool is a slot semaphore in front of the sim, the same shape the
// wire client's MaxIdleConnsPerHost gives it: a request that finds all
// slots busy waits, and that wait is client-side queuing, the thing the
// prediction says stays at zero. The open arm fires arrivals on a fixed
// schedule at -rate and measures the slot wait; the closed arm runs one
// worker per slot flat out and measures the ceiling the pool supports.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

const key = "bench/4k"

func main() {
	arm := flag.String("arm", "open", "open | closed")
	rate := flag.Float64("rate", 5000, "arrivals per second for the open arm")
	pool := flag.Int("pool", 256, "slot count, the client core's MaxIdleConnsPerHost")
	dur := flag.Duration("dur", 10*time.Second, "run length")
	seed := flag.Uint64("seed", 1, "simulator seed")
	quick := flag.Bool("quick", false, "one short run per arm")
	header := flag.Bool("header", false, "print the CSV header and exit")
	flag.Parse()

	if *header {
		fmt.Println("arm,rate,pool,dur_s,ops,ops_per_s,inflight_peak,genlag_max_us,slotwait_p50_us,slotwait_p99_us,slotwait_max_us,get_p50_ms,get_p99_ms")
		return
	}
	if *quick {
		run("open", 500, 64, 2*time.Second, *seed)
		run("closed", 0, 64, 2*time.Second, *seed)
		return
	}
	run(*arm, *rate, *pool, *dur, *seed)
}

func run(arm string, rate float64, pool int, dur time.Duration, seed uint64) {
	s := sim.New(sim.Config{Seed: seed, Latency: sim.S3Standard})
	body := make([]byte, 4096)
	if _, err := s.Put(context.Background(), key, body); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var r row
	switch arm {
	case "open":
		r = openLoop(s, rate, pool, dur)
	case "closed":
		r = closedLoop(s, pool, dur)
	default:
		fmt.Fprintf(os.Stderr, "unknown arm %q\n", arm)
		os.Exit(2)
	}
	r.arm, r.rate, r.pool, r.dur = arm, rate, pool, dur
	fmt.Println(r)
}

type row struct {
	arm       string
	rate      float64
	pool      int
	dur       time.Duration
	ops       int
	opsPerS   float64
	peak      int64
	genLagMax time.Duration
	slotWaits []time.Duration
	getLats   []time.Duration
}

func (r row) String() string {
	return fmt.Sprintf("%s,%.0f,%d,%.0f,%d,%.1f,%d,%d,%d,%d,%d,%.2f,%.2f",
		r.arm, r.rate, r.pool, r.dur.Seconds(), r.ops, r.opsPerS, r.peak,
		r.genLagMax.Microseconds(),
		pct(r.slotWaits, 50).Microseconds(), pct(r.slotWaits, 99).Microseconds(),
		pct(r.slotWaits, 100).Microseconds(),
		float64(pct(r.getLats, 50))/1e6, float64(pct(r.getLats, 99))/1e6)
}

func pct(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	slices.Sort(d)
	i := len(d) * p / 100
	return d[min(i, len(d)-1)]
}

// openLoop fires n = rate*dur arrivals on the schedule start + i/rate.
// Each arrival takes a slot (waiting if all are busy: that wait is the
// client-side queuing being measured), runs one GET on the sim, and
// releases the slot. genLagMax proves the generator kept the schedule;
// a lagging generator would understate queuing.
func openLoop(s *sim.Sim, rate float64, pool int, dur time.Duration) row {
	ctx := context.Background()
	n := int(rate * dur.Seconds())
	slots := make(chan struct{}, pool)
	var (
		mu       sync.Mutex
		waits    = make([]time.Duration, 0, n)
		lats     = make([]time.Duration, 0, n)
		inflight atomic.Int64
		peak     atomic.Int64
		lagMax   atomic.Int64
		wg       sync.WaitGroup
	)
	start := time.Now()
	for i := range n {
		target := start.Add(time.Duration(float64(i) * float64(time.Second) / rate))
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		} else if lag := -d; lag > time.Duration(lagMax.Load()) {
			lagMax.Store(int64(lag))
		}
		wg.Go(func() {
			w0 := time.Now()
			slots <- struct{}{}
			wait := time.Since(w0)
			cur := inflight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			g0 := time.Now()
			_, _, err := s.Get(ctx, key)
			lat := time.Since(g0)
			inflight.Add(-1)
			<-slots
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			mu.Lock()
			waits = append(waits, wait)
			lats = append(lats, lat)
			mu.Unlock()
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	return row{
		ops: n, opsPerS: float64(n) / elapsed.Seconds(),
		peak: peak.Load(), genLagMax: time.Duration(lagMax.Load()),
		slotWaits: waits, getLats: lats,
	}
}

// closedLoop runs one worker per slot flat out until the deadline: the
// throughput ceiling the pool supports at the model, Little's law made
// empirical. Slot waits are zero by construction and stay blank.
func closedLoop(s *sim.Sim, pool int, dur time.Duration) row {
	ctx := context.Background()
	var (
		mu   sync.Mutex
		lats []time.Duration
		ops  atomic.Int64
		wg   sync.WaitGroup
	)
	start := time.Now()
	deadline := start.Add(dur)
	for range pool {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				g0 := time.Now()
				_, _, err := s.Get(ctx, key)
				lat := time.Since(g0)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				ops.Add(1)
				mu.Lock()
				lats = append(lats, lat)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	return row{
		ops: int(ops.Load()), opsPerS: float64(ops.Load()) / elapsed.Seconds(),
		peak: int64(pool), getLats: lats,
	}
}
