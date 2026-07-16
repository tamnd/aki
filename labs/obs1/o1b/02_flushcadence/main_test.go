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
	if got := drawLat(sim.Dist{}, 1.5); got != 0 {
		t.Fatalf("zero model drew %v", got)
	}
}

// With zero latency the model is pure trigger arithmetic: trickle at one
// frame per 10ms under a 50ms age trigger flushes once per 50ms window
// (5 frames each), appends match flushes one to one, and no frame waits
// longer than the age trigger.
func TestAgeTriggerCadence(t *testing.T) {
	ld := loadShape{name: "t", interval: 10 * time.Millisecond, frameBytes: 100}
	r := runCell(50*time.Millisecond, 8<<20, ld, sim.Dist{}, 1, 2*time.Second, 60*time.Second)
	if r.flushesPerS < 19 || r.flushesPerS > 21 {
		t.Fatalf("flushes/s = %v, want ~20", r.flushesPerS)
	}
	if r.appendsPerS != r.flushesPerS {
		t.Fatalf("appends/s = %v, flushes/s = %v, want equal at zero latency", r.appendsPerS, r.flushesPerS)
	}
	if r.lagP99 > 51*time.Millisecond {
		t.Fatalf("lag p99 = %v, want at most the age trigger", r.lagP99)
	}
	if r.parks != 0 {
		t.Fatalf("parks = %d, want 0", r.parks)
	}
}

// When ingest outruns flush-size/flush-age the size trigger owns the
// cadence: 16 MiB/s into 1 MiB snapshots is 16 flushes/s regardless of
// the 1000ms age knob.
func TestSizeTriggerCadence(t *testing.T) {
	ld := loadShape{name: "s", interval: time.Millisecond, frameBytes: 16 << 10}
	r := runCell(1000*time.Millisecond, 1<<20, ld, sim.Dist{}, 1, 2*time.Second, 60*time.Second)
	if r.flushesPerS < 15 || r.flushesPerS > 17 {
		t.Fatalf("flushes/s = %v, want ~16", r.flushesPerS)
	}
	if r.achievedMiBs < 15.5 || r.achievedMiBs > 16.5 {
		t.Fatalf("achieved = %v MiB/s, want ~16", r.achievedMiBs)
	}
}

// The WAL cap parks ingest instead of dropping it: with PUTs pinned at
// 100ms and a 1 MiB flush-size the 4-deep pipeline tops out at 40 MiB/s,
// so an 80 MiB/s offer must park and achieved throughput must land at
// the pipeline ceiling, not the offer.
func TestCapParksIngest(t *testing.T) {
	ld := loadShape{name: "h", interval: 200 * time.Microsecond, frameBytes: 16 << 10}
	put := sim.Dist{P50: 100 * time.Millisecond, P99: 100 * time.Millisecond}
	r := runCell(1000*time.Millisecond, 1<<20, ld, put, 1, 2*time.Second, 60*time.Second)
	if r.parks == 0 {
		t.Fatalf("parks = 0, want ingest parked at the cap")
	}
	if r.achievedMiBs > 45 || r.achievedMiBs < 30 {
		t.Fatalf("achieved = %v MiB/s, want near the 40 MiB/s pipeline ceiling", r.achievedMiBs)
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
