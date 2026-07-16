// Lab: strict-ack latency (spec 2064/obs1 doc 04 sections 3.2 and 12,
// doc 11 milestone O1b lab 03).
//
// The question: what latency distribution does a strict ack see under
// light, medium, and heavy strict load on the Standard and Express
// placement models, and which of doc 04 section 3.2's two statements
// wins: the flush-age/2 plus PUT plus append formula, or the barrier
// floor sentence that says a pending strict ack pulls the next flush
// to at most 5ms after the last one?
//
// Method: the flush-cadence lab's virtual-time flusher model (same
// triggers, 4-deep PUT pipeline, seq-ordered batched chain commits,
// swap only when a pipeline slot is free) with every write strict and
// barrier demand modeled per section 3.2: while any strict write is
// pending, the effective age trigger drops to the 5ms floor since the
// last swap. The ack is the arrival-to-covering-commit time, which is
// exactly the watermark a strict reply parks on. Standard draws from
// sim.S3Standard.Put; Express has no published tail, so the lab
// defines one from the doc 01 envelope (single-digit-ms first byte):
// p50 6ms with a p99 of 25ms, an assumption the O5 E-cloud refit
// replaces. Flush-age 50ms and flush-size 8 MiB stay at the defaults
// the cadence lab confirmed; the floor makes the age knob irrelevant
// for strict traffic, and the lab proves that rather than assuming it.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// z99 is the standard normal quantile at 0.99, as in sim/latency.go.
const z99 = 2.3263

// drawLat maps a standard normal z onto the lognormal through its p50
// and p99, the sim/latency.go mapping (pinned by TestDrawMatchesModel).
func drawLat(d sim.Dist, z float64) time.Duration {
	if d.P50 <= 0 {
		return 0
	}
	sigma := math.Log(float64(d.P99)/float64(d.P50)) / z99
	return time.Duration(float64(d.P50) * math.Exp(sigma*z))
}

// expressPut is the lab's Express One Zone PUT model: doc 01 states
// single-digit-millisecond first byte and publishes no tail, so the
// p99 is an envelope assumption, disclosed here and replaced at O5.
var expressPut = sim.Dist{P50: 6 * time.Millisecond, P99: 25 * time.Millisecond}

// expressPrices is the doc 01 section 2.3 Express One Zone sheet
// (April 2025 repricing); the sim only carries the Standard table, so
// the lab carries this one and the doc 09 rule applies to it too.
var expressPrices = sim.PriceTable{StorageGBMonth: 0.11, PutPerM: 1.13, GetPerM: 0.03}

const (
	flushAge     = 50 * time.Millisecond
	flushSize    = int64(8 << 20)
	barrierFloor = 5 * time.Millisecond
)

type loadShape struct {
	name       string
	interval   time.Duration
	frameBytes int64
}

type snap struct {
	seq      int64
	bytes    int64
	arrivals []time.Duration
	putDone  time.Duration
}

type cellResult struct {
	flushesPerS  float64
	acksPerFlush float64
	reqPerS      float64
	ackP50       time.Duration
	ackP90       time.Duration
	ackP99       time.Duration
	parks        int
	usdMonth     float64
}

const inf = time.Duration(math.MaxInt64)

// runCell simulates one placement/load cell; every write is strict and
// the returned percentiles are over arrival-to-covering-commit times
// for writes arriving inside the measured window.
func runCell(put sim.Dist, prices sim.PriceTable, ld loadShape, seed int64, warm, meas time.Duration) cellResult {
	rng := rand.New(rand.NewSource(seed))
	capBytes := 4 * flushSize
	end := warm + meas

	var (
		now          time.Duration
		delivered    int64
		bufBytes     int64
		bufArrivals  []time.Duration
		oldest       = inf
		lastSwap     time.Duration
		inflight     []*snap
		nextSeq      int64 = 1
		committed    int64
		completed    = map[int64]*snap{}
		appendDoneT  = inf
		appendBatch  []*snap
		pendingBytes int64
		blocked      bool
		stopped      bool
		parks        int
		acks         []time.Duration
		nPuts        int
		nAppends     int
		nAcked       int
	)

	inWindow := func(t time.Duration) bool { return t >= warm && t < end }

	snapshot := func() {
		if bufBytes == 0 || len(inflight) == 4 {
			return
		}
		s := &snap{seq: nextSeq, bytes: bufBytes, arrivals: bufArrivals}
		nextSeq++
		pendingBytes += bufBytes
		bufBytes, bufArrivals, oldest = 0, nil, inf
		lastSwap = now
		s.putDone = now + drawLat(put, rng.NormFloat64())
		if inWindow(now) {
			nPuts++
		}
		inflight = append(inflight, s)
	}
	// due reports whether a trigger holds right now; pending strict
	// writes are barrier demand, so a non-empty buffer is due once the
	// floor since the last swap has passed (doc 04 section 3.2).
	due := func() bool {
		if bufBytes == 0 {
			return false
		}
		if stopped || bufBytes >= flushSize || now-oldest >= flushAge {
			return true
		}
		return now-lastSwap >= barrierFloor
	}
	startAppend := func() {
		if appendDoneT != inf {
			return
		}
		for {
			s, ok := completed[committed+int64(len(appendBatch))+1]
			if !ok {
				break
			}
			appendBatch = append(appendBatch, s)
		}
		if len(appendBatch) == 0 {
			return
		}
		appendDoneT = now + drawLat(put, rng.NormFloat64())
		if inWindow(now) {
			nAppends++
		}
	}

	for {
		arrivalsLeft := time.Duration(delivered)*ld.interval < end
		if !arrivalsLeft && !stopped {
			stopped = true
			snapshot()
			continue
		}
		tArr := inf
		if arrivalsLeft && !blocked {
			tArr = max(time.Duration(delivered)*ld.interval, now)
		}
		// The barrier trigger: with strict writes pending, the next
		// swap fires at the floor after the last one (or immediately
		// if the floor already passed), pipeline slot permitting.
		tBar := inf
		if oldest != inf && len(inflight) < 4 {
			tBar = max(oldest, lastSwap+barrierFloor)
			tBar = min(tBar, oldest+flushAge)
		}
		tPut := inf
		for _, s := range inflight {
			tPut = min(tPut, s.putDone)
		}
		tApp := appendDoneT

		t := min(tArr, tBar, tPut, tApp)
		if t == inf {
			break
		}
		now = t
		switch t {
		case tApp:
			for _, s := range appendBatch {
				committed = s.seq
				delete(completed, s.seq)
				for _, a := range s.arrivals {
					if inWindow(a) {
						acks = append(acks, now-a)
						nAcked++
					}
				}
			}
			appendBatch, appendDoneT = nil, inf
			startAppend()
		case tPut:
			idx := 0
			for i := 1; i < len(inflight); i++ {
				if inflight[i].putDone < inflight[idx].putDone {
					idx = i
				}
			}
			s := inflight[idx]
			inflight = append(inflight[:idx], inflight[idx+1:]...)
			completed[s.seq] = s
			pendingBytes -= s.bytes
			blocked = false
			if due() {
				snapshot()
			}
			startAppend()
		case tBar:
			snapshot()
		default: // arrival
			if bufBytes+pendingBytes >= capBytes {
				blocked = true
				parks++
				continue
			}
			delivered++
			if bufBytes == 0 {
				oldest = now
			}
			bufBytes += ld.frameBytes
			bufArrivals = append(bufArrivals, now)
			if bufBytes >= flushSize {
				snapshot()
			}
		}
	}

	secs := meas.Seconds()
	r := cellResult{
		flushesPerS: float64(nPuts) / secs,
		ackP50:      percentile(acks, 0.50),
		ackP90:      percentile(acks, 0.90),
		ackP99:      percentile(acks, 0.99),
		parks:       parks,
	}
	if nPuts > 0 {
		r.acksPerFlush = float64(nAcked) / float64(nPuts)
	}
	r.reqPerS = float64(nPuts+nAppends) / secs
	monthly := int64(r.reqPerS * 730 * 3600)
	r.usdMonth = prices.Bill(sim.Usage{PutRequests: monthly}, 0).Puts
	return r
}

func percentile(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), xs...)
	slices.Sort(s)
	i := max(int(math.Ceil(p*float64(len(s))))-1, 0)
	return s[i]
}

func main() {
	warm := flag.Duration("warm", 10*time.Second, "virtual warmup discarded from every rate and ack")
	meas := flag.Duration("meas", 120*time.Second, "virtual measured window per cell")
	quick := flag.Bool("quick", false, "tiny windows, smoke only")
	flag.Parse()
	if *quick {
		*warm, *meas = 1*time.Second, 5*time.Second
	}

	placements := []struct {
		name   string
		put    sim.Dist
		prices sim.PriceTable
	}{
		{"standard", sim.S3Standard.Put, sim.S3StandardPrices},
		{"express", expressPut, expressPrices},
	}
	loads := []loadShape{
		{name: "light", interval: 100 * time.Millisecond, frameBytes: 100},
		{name: "medium", interval: time.Millisecond, frameBytes: 512},
		{name: "heavy", interval: 50 * time.Microsecond, frameBytes: 512},
	}
	fmt.Println("placement,load,flushes_s,acks_per_flush,req_s,ack_p50_ms,ack_p90_ms,ack_p99_ms,parks,req_usd_month")
	seed := int64(1)
	for _, pl := range placements {
		for _, ld := range loads {
			r := runCell(pl.put, pl.prices, ld, seed, *warm, *meas)
			seed++
			fmt.Printf("%s,%s,%.1f,%.1f,%.1f,%.0f,%.0f,%.0f,%d,%.0f\n",
				pl.name, ld.name,
				r.flushesPerS, r.acksPerFlush, r.reqPerS,
				float64(r.ackP50)/float64(time.Millisecond),
				float64(r.ackP90)/float64(time.Millisecond),
				float64(r.ackP99)/float64(time.Millisecond),
				r.parks, r.usdMonth)
		}
	}
}
