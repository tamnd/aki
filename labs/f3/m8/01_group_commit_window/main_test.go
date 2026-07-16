package main

import (
	"math"
	"testing"
)

// the spec's worked machine parameters (doc 07 section 2).
const (
	specShards    = 16
	specWriteRate = 2_000_000.0
	specFlushSec  = 50e-6
	specHopSec    = 10e-6
)

func about(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s = %v, want ~%v", name, got, want)
	}
}

// TestDesignPointMatchesSpec pins the row the doc works by hand: a 1 ms window,
// 1000 group flushes/s at 5 percent device time, 16000 per-shard flushes/s at 80
// percent, a 1 percent hop tax.
func TestDesignPointMatchesSpec(t *testing.T) {
	r := measure(2000, specShards, specWriteRate, specFlushSec, specHopSec)
	about(t, "window ms", r.windowMS, 1.0, 1e-9)
	about(t, "group flush/s", r.groupFlushes, 1000, 1e-6)
	about(t, "group device frac", r.groupDeviceFrac, 0.05, 1e-9)
	about(t, "per-shard flush/s", r.shardFlushes, 16000, 1e-6)
	about(t, "per-shard device frac", r.shardDeviceFrac, 0.80, 1e-9)
	about(t, "hop frac", r.hopFrac, 0.01, 1e-9)
}

// TestGroupCommitCutsFlushesByShardCount is the whole point: the single writer
// flushes exactly N times less often than a writer per shard, at every group size.
func TestGroupCommitCutsFlushesByShardCount(t *testing.T) {
	for _, g := range []int{1, 64, 2000, 32768} {
		r := measure(g, specShards, specWriteRate, specFlushSec, specHopSec)
		ratio := r.shardFlushes / r.groupFlushes
		about(t, "flush ratio", ratio, specShards, 1e-9)
	}
}

// TestBiggerGroupLowersFlushAndHopCost proves the trade the window sizes: a larger
// group monotonically cuts both the flush rate and the amortized costs.
func TestBiggerGroupLowersFlushAndHopCost(t *testing.T) {
	groups := []int{1, 8, 64, 512, 2000, 8192, 32768}
	var prev row
	for i, g := range groups {
		r := measure(g, specShards, specWriteRate, specFlushSec, specHopSec)
		if i > 0 {
			if r.groupFlushes >= prev.groupFlushes {
				t.Fatalf("group %d flush/s %.0f did not fall below %.0f", g, r.groupFlushes, prev.groupFlushes)
			}
			if r.flushPerWriteNS >= prev.flushPerWriteNS {
				t.Fatalf("group %d flush ns/write %.3f did not fall", g, r.flushPerWriteNS)
			}
			if r.hopFrac >= prev.hopFrac {
				t.Fatalf("group %d hop tax %.4f did not fall", g, r.hopFrac)
			}
			if r.windowMS <= prev.windowMS {
				t.Fatalf("group %d window %.3f did not grow (the latency it trades)", g, r.windowMS)
			}
		}
		prev = r
	}
}

// TestHopTaxCrossesOnePercentAtDesignPoint shows where the ring-hop tax falls to
// the 1 percent the doc names: at or above the B=2000 design group, never below.
func TestHopTaxCrossesOnePercentAtDesignPoint(t *testing.T) {
	small := measure(512, specShards, specWriteRate, specFlushSec, specHopSec)
	if small.hopFrac <= 0.01 {
		t.Fatalf("a 512-record group already amortized the hop tax to %.4f, want above 1%%", small.hopFrac)
	}
	design := measure(2000, specShards, specWriteRate, specFlushSec, specHopSec)
	if design.hopFrac > 0.01+1e-9 {
		t.Fatalf("design group hop tax %.4f, want at or below 1%%", design.hopFrac)
	}
	big := measure(8192, specShards, specWriteRate, specFlushSec, specHopSec)
	if big.hopFrac >= design.hopFrac {
		t.Fatalf("a bigger group did not push the hop tax below the design point")
	}
}

// TestPerShardSaturatesWhereGroupDoesNot is the qualitative verdict: at the design
// window the rival is near device saturation while group commit has headroom.
func TestPerShardSaturatesWhereGroupDoesNot(t *testing.T) {
	r := measure(2000, specShards, specWriteRate, specFlushSec, specHopSec)
	if r.shardDeviceFrac < 0.75 {
		t.Fatalf("per-shard device fraction %.2f, want near saturation", r.shardDeviceFrac)
	}
	if r.groupDeviceFrac > 0.15 {
		t.Fatalf("group-commit device fraction %.2f, want comfortable headroom", r.groupDeviceFrac)
	}
}
