package main

import (
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/sim"
)

// drawLat is a copy of the unexported sim/latency.go draw; this pins it
// to the model's quantiles so the copies cannot drift apart silently.
func TestDrawMatchesModel(t *testing.T) {
	d := sim.S3Standard.Put
	if got := drawLat(d, 0); got != d.P50 {
		t.Fatalf("draw at z=0 = %v, want p50 %v", got, d.P50)
	}
	got := drawLat(d, z99)
	if diff := got - d.P99; diff < -time.Millisecond || diff > time.Millisecond {
		t.Fatalf("draw at z99 = %v, want p99 %v", got, d.P99)
	}
}

// With zero store latency the ack is pure barrier arithmetic: a lone
// strict write acks within the 5ms floor, never waiting out the 50ms
// age trigger, which is the section 3.2 sentence the lab exists to
// check. One write per 100ms means every flush carries one ack.
func TestBarrierFloorBeatsAgeTrigger(t *testing.T) {
	ld := loadShape{name: "l", interval: 100 * time.Millisecond, frameBytes: 100}
	r := runCell(sim.Dist{}, sim.S3StandardPrices, ld, 1, 2*time.Second, 60*time.Second)
	if r.ackP99 > barrierFloor {
		t.Fatalf("ack p99 = %v, want at most the %v floor at zero latency", r.ackP99, barrierFloor)
	}
	if r.acksPerFlush < 0.9 || r.acksPerFlush > 1.1 {
		t.Fatalf("acks/flush = %v, want ~1 at light load", r.acksPerFlush)
	}
}

// Strict writers share flushes: at zero latency a 1000/s strict stream
// against the 5ms floor rides ~200 flushes/s with ~5 acks each, not
// 1000 per-op PUTs, the section 3.2 degradation-to-cadence claim.
func TestStrictSharesFlushes(t *testing.T) {
	ld := loadShape{name: "m", interval: time.Millisecond, frameBytes: 512}
	r := runCell(sim.Dist{}, sim.S3StandardPrices, ld, 1, 2*time.Second, 60*time.Second)
	if r.flushesPerS < 190 || r.flushesPerS > 210 {
		t.Fatalf("flushes/s = %v, want ~200 at the floor", r.flushesPerS)
	}
	if r.acksPerFlush < 4 || r.acksPerFlush > 6 {
		t.Fatalf("acks/flush = %v, want ~5", r.acksPerFlush)
	}
	if r.ackP99 > barrierFloor+time.Millisecond {
		t.Fatalf("ack p99 = %v, want within the floor at zero latency", r.ackP99)
	}
}

func TestPercentile(t *testing.T) {
	xs := []time.Duration{4, 1, 3, 2}
	if p := percentile(xs, 0.50); p != 2 {
		t.Fatalf("p50 = %v, want 2", p)
	}
	if p := percentile(xs, 0.99); p != 4 {
		t.Fatalf("p99 = %v, want 4", p)
	}
	if p := percentile(nil, 0.99); p != 0 {
		t.Fatalf("empty p99 = %v, want 0", p)
	}
}
