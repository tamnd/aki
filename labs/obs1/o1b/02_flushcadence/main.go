// Lab: flush cadence (spec 2064/obs1 doc 04 sections 4.1 and 12,
// doc 11 milestone O1b lab 02).
//
// The question: what request rate, monthly request bill, and realized
// commit lag do the flusher triggers produce across flush-age
// 5/50/250/1000ms and flush-size 1/8/64 MiB under trickle, steady, and
// saturating ingest, and where is the knee where the size trigger takes
// over from the age trigger?
//
// Method: virtual-time discrete-event model of the doc 04 section 4
// flusher, the same sim-only justification as the clock-skew lab: the
// quantities under test are trigger arithmetic and queueing, orthogonal
// to real store bytes, and real-time runs at flush-age 1000ms would
// need hours per cell. The model is not free-floating: PUT and chain
// append latencies draw from sim.S3Standard.Put (the doc 01 envelope
// the E-sim uses) and dollars go through sim.S3StandardPrices.Bill, so
// the engine's model stays the single source and the O5 E-cloud refit
// moves this lab automatically.
//
// Modeled per doc 04 section 4: owners append frames to the buffer;
// triggers are size (buffer reaches flush-size), age (oldest pending
// frame reaches flush-age), and shutdown; a snapshot swap hands the
// buffer to the PUT pipeline (4 deep); chain commit records append
// strictly in WAL-seq order and one append batches every commit whose
// PUT has completed; the WAL buffer cap (4x flush-size) parks ingest,
// block-not-drop, and parked writers deliver when a PUT frees bytes.
// A trigger only swaps when a pipeline slot is free: the flusher is
// swap-and-continue, so while four PUTs are in flight the buffer keeps
// accumulating and the next swap carries everything, which is the
// adaptive-cadence coalescing of section 4.1 (an early draft swapped
// on every age tick regardless and queued unbounded tiny objects the
// real flusher cannot produce).
// The barrier floor (5ms) is not modeled separately: a barrier storm
// degenerates to the age-5ms rows, which is why 5ms is in the grid.
// Known simplifications, stated: latency draws are size-independent
// exactly as the sim's model is (the bandwidth term on 64 MiB PUTs is
// an O5 refit item), and zstd on sections above 4 KiB moves bytes-up,
// not request rate, so it is out of scope here.
//
// The lag column is the commit lag, arrival to covering chain commit:
// in relaxed mode the ack already went out and this is the realized
// loss window the section 5 table bounds; a strict ack would park on
// exactly this watermark.
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
// and p99, the same two-line mapping sim/latency.go uses; draw there is
// unexported and TestDrawMatchesModel pins this copy to its quantiles.
func drawLat(d sim.Dist, z float64) time.Duration {
	if d.P50 <= 0 {
		return 0
	}
	sigma := math.Log(float64(d.P99)/float64(d.P50)) / z99
	return time.Duration(float64(d.P50) * math.Exp(sigma*z))
}

type loadShape struct {
	name       string
	interval   time.Duration // one frame per interval
	frameBytes int64
}

type cellResult struct {
	flushesPerS    float64
	appendsPerS    float64
	reqPerS        float64
	offeredMiBs    float64
	achievedMiBs   float64
	parks          int
	lagP50, lagP99 time.Duration
	usdMonth       float64
}

const inf = time.Duration(math.MaxInt64)

// snap is one snapshotted buffer: a WAL object waiting on, in, or past
// its PUT.
type snap struct {
	seq      int64
	bytes    int64
	arrivals []time.Duration
	putDone  time.Duration
}

// runCell simulates one grid cell for warm+meas of virtual time and
// reports rates over frames and requests inside the measured window.
func runCell(age time.Duration, flushSize int64, ld loadShape, put sim.Dist, seed int64, warm, meas time.Duration) cellResult {
	rng := rand.New(rand.NewSource(seed))
	capBytes := 4 * flushSize
	end := warm + meas

	var (
		now          time.Duration
		delivered    int64
		bufBytes     int64
		bufArrivals  []time.Duration
		oldest       = inf
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
		lags         []time.Duration
		nPuts        int
		nAppends     int
		measBytes    int64
	)

	inWindow := func(t time.Duration) bool { return t >= warm && t < end }

	// snapshot swaps the buffer straight into a pipeline slot; callers
	// only invoke it when a slot is free, so there is never a queue of
	// built objects, exactly like the swap-and-continue flusher.
	snapshot := func() {
		if bufBytes == 0 || len(inflight) == 4 {
			return
		}
		s := &snap{seq: nextSeq, bytes: bufBytes, arrivals: bufArrivals}
		nextSeq++
		pendingBytes += bufBytes
		bufBytes, bufArrivals, oldest = 0, nil, inf
		s.putDone = now + drawLat(put, rng.NormFloat64())
		if inWindow(now) {
			nPuts++
		}
		inflight = append(inflight, s)
	}
	// due reports whether a trigger condition holds right now.
	due := func() bool {
		return bufBytes >= flushSize || (oldest != inf && now-oldest >= age) || (stopped && bufBytes > 0)
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
			// Graceful stop: flush whatever is buffered (doc 04
			// section 4 shutdown trigger) and drain; if the pipeline
			// is full the putDone path picks the remainder up.
			stopped = true
			snapshot()
			continue
		}
		tArr := inf
		if arrivalsLeft && !blocked {
			tArr = max(time.Duration(delivered)*ld.interval, now)
		}
		tAge := inf
		if oldest != inf && len(inflight) < 4 {
			tAge = oldest + age
		}
		tPut := inf
		for _, s := range inflight {
			tPut = min(tPut, s.putDone)
		}
		tApp := appendDoneT

		t := min(tArr, tAge, tPut, tApp)
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
						lags = append(lags, now-a)
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
		case tAge:
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
			if inWindow(now) {
				measBytes += ld.frameBytes
			}
			if bufBytes >= flushSize {
				snapshot()
			}
		}
	}

	secs := meas.Seconds()
	r := cellResult{
		flushesPerS:  float64(nPuts) / secs,
		appendsPerS:  float64(nAppends) / secs,
		offeredMiBs:  float64(ld.frameBytes) / ld.interval.Seconds() / (1 << 20),
		achievedMiBs: float64(measBytes) / secs / (1 << 20),
		parks:        parks,
		lagP50:       percentile(lags, 0.50),
		lagP99:       percentile(lags, 0.99),
	}
	r.reqPerS = r.flushesPerS + r.appendsPerS
	monthly := int64(r.reqPerS * 730 * 3600)
	r.usdMonth = sim.S3StandardPrices.Bill(sim.Usage{PutRequests: monthly}, 0).Puts
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
	warm := flag.Duration("warm", 10*time.Second, "virtual warmup discarded from every rate and lag")
	meas := flag.Duration("meas", 120*time.Second, "virtual measured window per cell")
	quick := flag.Bool("quick", false, "tiny windows, smoke only")
	flag.Parse()
	if *quick {
		*warm, *meas = 1*time.Second, 5*time.Second
	}

	loads := []loadShape{
		// trickle realizes the age-trigger worst case for every age at
		// or above its interval: at least one frame per flush window.
		{name: "trickle", interval: 10 * time.Millisecond, frameBytes: 100},
		{name: "steady", interval: 500 * time.Microsecond, frameBytes: 512},
		// 200 MiB/s sits past the defaults' size-over-age knee
		// (flush-size/flush-age = 160 MiB/s) so the decoupling shows.
		{name: "heavy", interval: 156250 * time.Nanosecond, frameBytes: 32 << 10},
	}
	fmt.Println("age_ms,flush_mib,load,flushes_s,appends_s,req_s,offered_mib_s,achieved_mib_s,parks,lag_p50_ms,lag_p99_ms,req_usd_month")
	seed := int64(1)
	for _, ageMS := range []int{5, 50, 250, 1000} {
		for _, sizeMiB := range []int{1, 8, 64} {
			for _, ld := range loads {
				r := runCell(time.Duration(ageMS)*time.Millisecond, int64(sizeMiB)<<20, ld, sim.S3Standard.Put, seed, *warm, *meas)
				seed++
				fmt.Printf("%d,%d,%s,%.1f,%.1f,%.1f,%.2f,%.2f,%d,%.0f,%.0f,%.0f\n",
					ageMS, sizeMiB, ld.name,
					r.flushesPerS, r.appendsPerS, r.reqPerS,
					r.offeredMiBs, r.achievedMiBs, r.parks,
					float64(r.lagP50)/float64(time.Millisecond),
					float64(r.lagP99)/float64(time.Millisecond),
					r.usdMonth)
			}
		}
	}
}
