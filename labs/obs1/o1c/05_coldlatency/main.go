// Command coldlatency takes the first cut of the cold point-read
// distribution (spec 2064/obs1 doc 05 sections 5 and 9): one ranged GET of
// one block, decoded, with no cache, no NVMe, and no hedging in front,
// which is exactly the O1c serving shape. It draws end-to-end latency
// against block size and placement from the sim envelope, and it runs the
// I/O-pool queue (FCFS, capped in-flight GETs) against offered cold-read
// rate to find where the in-flight cap starts inflating the tail. The O4a
// re-verdict puts the cache and hedging in front and re-scores the doc 05
// section 9 table; this lab prices what those two have to buy back.
package main

import (
	"flag"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// prng is splitmix64, deterministic and import-free.
type prng struct{ s uint64 }

func (p *prng) next() uint64 {
	p.s += 0x9E3779B97F4A7C15
	z := p.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// normal draws a standard normal via Box-Muller.
func (p *prng) normal() float64 {
	u1 := (float64(p.next()>>11) + 1) / (1 << 53)
	u2 := float64(p.next()>>11) / (1 << 53)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

// expo draws an exponential with the given mean, for Poisson arrivals.
func (p *prng) expo(mean float64) float64 {
	u := (float64(p.next()>>11) + 1) / (1 << 53)
	return -mean * math.Log(u)
}

// drawLatency maps a standard normal onto the sim Dist's lognormal, the
// same two-line map sim uses, pinned to its constants so the O5 E-cloud
// refit moves this lab automatically.
func drawLatency(d sim.Dist, z float64) time.Duration {
	sigma := math.Log(float64(d.P99)/float64(d.P50)) / 2.3263
	return time.Duration(float64(d.P50) * math.Exp(sigma*z))
}

// expressGet is a LAB ASSUMPTION carried from the strict-latency lab
// (#963): doc 01's single-digit-millisecond first-byte envelope with no
// published tail. Replaced at the O5 E-cloud refit.
var expressGet = sim.Dist{P50: 6 * time.Millisecond, P99: 25 * time.Millisecond}

// linkMiBps is the assumed single-stream transfer bandwidth once the first
// byte arrives; doc 01 only states that aggregate bandwidth scales with
// parallel connections, so this is a disclosed lab assumption for the
// per-GET transfer term, replaced at the O5 refit.
const linkMiBps = 64.0

// decodeMiBps carries the zstd-1 cold-block decode rate measured by the
// zstd-worth lab (#1097): 44 to 109 us per 128 KiB block, about 1.2 to
// 3 GiB/s, taken at 2 GiB/s.
const decodeMiBps = 2048.0

// coldPointMs composes one cold point read in milliseconds: first byte
// from the placement's distribution, then transfer and decode of one
// block. The keymap and directory halves are RAM in regime A and add
// nothing a simulator would resolve.
func coldPointMs(d sim.Dist, blockBytes int, z float64) float64 {
	get := float64(drawLatency(d, z)) / float64(time.Millisecond)
	transfer := float64(blockBytes) / (linkMiBps * (1 << 20)) * 1000
	decode := float64(blockBytes) / (decodeMiBps * (1 << 20)) * 1000
	return get + transfer + decode
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(math.Ceil(q*float64(len(sorted)-1)))]
}

// runQueue pushes n Poisson arrivals at the offered rate (per second)
// through cap FCFS servers whose service times come from svc, and returns
// sorted end-to-end and wait times in milliseconds with the leading warm
// fraction discarded. Assigning each arrival to the earliest-free server
// is exact FCFS for a shared queue.
func runQueue(p *prng, n, cap int, ratePerSec float64, svc func(*prng) float64) (e2e, waits []float64) {
	servers := make([]float64, cap)
	interMs := 1000 / ratePerSec
	warm := n / 5
	t := 0.0
	for i := range n {
		t += p.expo(interMs)
		mi := 0
		for j := 1; j < cap; j++ {
			if servers[j] < servers[mi] {
				mi = j
			}
		}
		start := max(servers[mi], t)
		done := start + svc(p)
		servers[mi] = done
		if i >= warm {
			e2e = append(e2e, done-t)
			waits = append(waits, start-t)
		}
	}
	sort.Float64s(e2e)
	sort.Float64s(waits)
	return e2e, waits
}

func main() {
	quick := flag.Bool("quick", false, "small draws, smoke only")
	flag.Parse()
	nPoint, nQueue := 200000, 200000
	if *quick {
		nPoint, nQueue = 20000, 20000
	}

	placements := []struct {
		name string
		d    sim.Dist
	}{{"standard", sim.S3Standard.Get}, {"express", expressGet}}
	blocks := []int{32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10}

	fmt.Println("kind,placement,blockKiB,cap,util,ratePerSec,p50ms,p90ms,p99ms,p999ms,tailRatio,waitP99ms,meanSvcMs")
	for _, pl := range placements {
		for _, b := range blocks {
			p := &prng{s: uint64(b) + uint64(pl.d.P50)}
			samples := make([]float64, nPoint)
			for i := range samples {
				samples[i] = coldPointMs(pl.d, b, p.normal())
			}
			sort.Float64s(samples)
			p50 := quantile(samples, 0.5)
			p99 := quantile(samples, 0.99)
			fmt.Printf("point,%s,%d,,,,%.2f,%.2f,%.2f,%.2f,%.2f,,\n",
				pl.name, b>>10, p50, quantile(samples, 0.90), p99,
				quantile(samples, 0.999), p99/p50)
		}
	}

	// The load sweep runs the default point shape, Standard at 128 KiB,
	// against the in-flight cap. Rates derive from the measured mean
	// service so the cells are utilization bands, not absolute-rate bets.
	block := 128 << 10
	mp := &prng{s: 777}
	meanSvc := 0.0
	for range 200000 {
		meanSvc += coldPointMs(sim.S3Standard.Get, block, mp.normal())
	}
	meanSvc /= 200000
	svc := func(p *prng) float64 { return coldPointMs(sim.S3Standard.Get, block, p.normal()) }
	for _, cap := range []int{16, 64, 256} {
		for _, util := range []float64{0.3, 0.6, 0.8, 0.9, 0.95} {
			rate := util * float64(cap) / (meanSvc / 1000)
			p := &prng{s: uint64(cap)<<20 + uint64(util*100)}
			e2e, waits := runQueue(p, nQueue, cap, rate, svc)
			p50 := quantile(e2e, 0.5)
			p99 := quantile(e2e, 0.99)
			fmt.Printf("load,standard,%d,%d,%.2f,%.0f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
				block>>10, cap, util, rate, p50, quantile(e2e, 0.90), p99,
				quantile(e2e, 0.999), p99/p50, quantile(waits, 0.99), meanSvc)
		}
	}
}
