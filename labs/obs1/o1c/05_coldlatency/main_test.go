package main

import (
	"math"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// drawLatency must reproduce the sim constants exactly, so the O5 refit
// moves this lab by moving sim.
func TestDrawLatencyQuantiles(t *testing.T) {
	d := sim.S3Standard.Get
	if got := drawLatency(d, 0); got != d.P50 {
		t.Fatalf("z=0 draw = %v want %v", got, d.P50)
	}
	got := drawLatency(d, 2.3263)
	if diff := got - d.P99; diff < -time.Microsecond || diff > time.Microsecond {
		t.Fatalf("z=p99 draw = %v want %v", got, d.P99)
	}
}

// The transfer and decode terms are plain arithmetic on the disclosed
// constants; pin them so a constant change shows up as a diff here too.
func TestColdComponents(t *testing.T) {
	ms := coldPointMs(sim.S3Standard.Get, 128<<10, 0)
	base := 20.0
	transfer := 128.0 / 64 / 1024 * 1000 // 1.953125 ms at 64 MiB/s
	decode := 128.0 / 2048 / 1024 * 1000 // 0.061 ms at 2 GiB/s
	if math.Abs(ms-(base+transfer+decode)) > 1e-9 {
		t.Fatalf("cold point at z=0 = %.6f want %.6f", ms, base+transfer+decode)
	}
}

func TestNormalMoments(t *testing.T) {
	p := &prng{s: 42}
	var sum, sq float64
	n := 200000
	for range n {
		z := p.normal()
		sum += z
		sq += z * z
	}
	mean := sum / float64(n)
	varr := sq/float64(n) - mean*mean
	if math.Abs(mean) > 0.01 || math.Abs(varr-1) > 0.02 {
		t.Fatalf("normal moments mean=%.4f var=%.4f", mean, varr)
	}
}

func TestExpoMean(t *testing.T) {
	p := &prng{s: 7}
	var sum float64
	n := 200000
	for range n {
		sum += p.expo(10)
	}
	if mean := sum / float64(n); math.Abs(mean-10) > 0.2 {
		t.Fatalf("expo mean = %.3f want 10", mean)
	}
}

// With one server and constant service s, end-to-end minus wait equals s
// for every sample whatever the arrival jitter, and every wait is a
// multiple of the deficit structure the FCFS recursion produces; the
// identity is the jitter-proof proof that runQueue's bookkeeping is
// consistent. The idle arm then shows a 64-cap pool at a trickle rate
// never queues at all.
func TestQueueBacklogIdentity(t *testing.T) {
	fixed := func(*prng) float64 { return 10 }
	e2e, waits := runQueue(&prng{s: 9}, 1000, 1, 200, fixed)
	if len(e2e) != 800 || len(waits) != 800 {
		t.Fatalf("warm discard kept %d/%d of 1000", len(e2e), len(waits))
	}
	for i := range e2e {
		if math.Abs(e2e[i]-waits[i]-10) > 1e-9 {
			t.Fatalf("sample %d: e2e %.3f wait %.3f", i, e2e[i], waits[i])
		}
	}
	if waits[len(waits)-1] <= 0 {
		t.Fatal("a single server at 2x its capacity must queue")
	}
	e2e, waits = runQueue(&prng{s: 9}, 1000, 64, 1, fixed)
	if waits[len(waits)-1] != 0 || e2e[len(e2e)-1] != 10 {
		t.Fatalf("idle 64-cap pool queued: wait %.3f e2e %.3f",
			waits[len(waits)-1], e2e[len(e2e)-1])
	}
}

// Feeding the queue a constant-rate load at util 0.5 with exponential
// service must keep mean wait finite and small relative to service; this
// pins that the rate-to-interarrival conversion is per-second.
func TestQueueRateUnits(t *testing.T) {
	p := &prng{s: 11}
	svc := func(p *prng) float64 { return p.expo(10) }
	_, waits := runQueue(p, 50000, 1, 50, svc) // util 0.5 on one server
	var sum float64
	for _, w := range waits {
		sum += w
	}
	mean := sum / float64(len(waits))
	// M/M/1 mean wait at rho 0.5 is exactly the mean service time (10ms);
	// allow a generous band for finite-run noise.
	if mean < 5 || mean > 20 {
		t.Fatalf("M/M/1 rho=0.5 mean wait = %.2fms want ~10", mean)
	}
}

func TestQuantile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5}
	if quantile(s, 0.5) != 3 || quantile(s, 0.99) != 5 || quantile(nil, 0.5) != 0 {
		t.Fatal("quantile shape")
	}
}
