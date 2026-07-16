package main

import (
	"math"
	"testing"
)

func about(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s = %v, want ~%v", name, got, want)
	}
}

// TestGateDesignPointMeetsF21 pins the pre-registered recovery targets at the
// 16-core gate box: replay at or above 1.5 GB/s aggregate and under 1 s/GB of tail,
// resident load under 2 s/GB.
func TestGateDesignPointMeetsF21(t *testing.T) {
	r := measure(16, perCoreLoadGBs, perCoreReplayGBs, deviceGBs)
	if r.replayAggGBs < 1.5 {
		t.Fatalf("replay aggregate %.3f GB/s, want the 1.5 GB/s target", r.replayAggGBs)
	}
	if r.replaySecPerGB >= 1.0 {
		t.Fatalf("replay %.3f s/GB, want under the 1 s/GB bar", r.replaySecPerGB)
	}
	if r.loadSecPerGB >= 2.0 {
		t.Fatalf("resident load %.3f s/GB, want under the 2 s/GB bar", r.loadSecPerGB)
	}
	about(t, "replay aggregate", r.replayAggGBs, 1.6, 1e-9)
	about(t, "load aggregate", r.loadAggGBs, 0.64, 1e-9)
}

// TestReplayScalesLinearlyBelowDevice is the whole bet: while the shared device is
// not the ceiling, doubling the shard count doubles replay throughput, so recovery
// scales with cores exactly as the no-coordination per-shard chain promised.
func TestReplayScalesLinearlyBelowDevice(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8, 16} {
		lo := measure(n, perCoreLoadGBs, perCoreReplayGBs, deviceGBs)
		hi := measure(2*n, perCoreLoadGBs, perCoreReplayGBs, deviceGBs)
		if lo.replayDeviceBound || hi.replayDeviceBound {
			t.Fatalf("N=%d/%d should be CPU-scaled below the device ceiling", n, 2*n)
		}
		about(t, "doubling replay", hi.replayAggGBs, 2*lo.replayAggGBs, 1e-9)
	}
}

// TestDeviceCapsAtHighCoreCount confirms the model's honesty: past the crossover
// core count the shared device caps the aggregate, so the lab does not claim
// unbounded scaling. The crossover sits far above the 16-core gate box.
func TestDeviceCapsAtHighCoreCount(t *testing.T) {
	ceilN := int(math.Ceil(deviceGBs / perCoreReplayGBs))
	if ceilN <= 16 {
		t.Fatalf("device crossover N=%d, want far above the 16-core box", ceilN)
	}
	capped := measure(ceilN+64, perCoreLoadGBs, perCoreReplayGBs, deviceGBs)
	if !capped.replayDeviceBound || capped.replayAggGBs != deviceGBs {
		t.Fatalf("at N=%d replay %.3f bound=%v, want the device ceiling %.3f",
			ceilN+64, capped.replayAggGBs, capped.replayDeviceBound, deviceGBs)
	}
}

// TestOpenTimeUnderFalsifierLine pins the F21 abort line: a balanced open stays well
// under 5 s/GB at the gate box across a range of resident and tail mixes.
func TestOpenTimeUnderFalsifierLine(t *testing.T) {
	r := measure(16, perCoreLoadGBs, perCoreReplayGBs, deviceGBs)
	for _, mix := range []struct{ resident, tail float64 }{
		{8, 1}, {16, 2}, {1, 8}, {32, 4},
	} {
		open := openSec(r, mix.resident, mix.tail)
		if perGB := open / (mix.resident + mix.tail); perGB >= 5.0 {
			t.Fatalf("resident %.0f tail %.0f: open %.2f s/GB, want under the 5 s/GB abort line", mix.resident, mix.tail, perGB)
		}
	}
}

// TestMoreShardsNeverSlower is the monotonicity the design leans on: adding a worker
// never lengthens recovery, so a bigger box is always at least as fast.
func TestMoreShardsNeverSlower(t *testing.T) {
	shards := []int{1, 2, 4, 8, 16, 32, 64, 128}
	var prev float64
	for i, n := range shards {
		open := openSec(measure(n, perCoreLoadGBs, perCoreReplayGBs, deviceGBs), 8, 1)
		if i > 0 && open > prev+1e-9 {
			t.Fatalf("N=%d open %.3f s rose above the smaller box's %.3f s", n, open, prev)
		}
		prev = open
	}
}

// TestSharedBottleneckWouldFlatten is the falsifier made concrete: a device far
// below the CPU sum (the shared-bottleneck world F21 warns of) flattens replay to
// the device ceiling well before N=16, which is exactly the regression the gate row
// watches for.
func TestSharedBottleneckWouldFlatten(t *testing.T) {
	const slowDevice = 0.5 // a shared bottleneck: half a GB/s for the whole file
	a := measure(8, perCoreLoadGBs, perCoreReplayGBs, slowDevice)
	b := measure(16, perCoreLoadGBs, perCoreReplayGBs, slowDevice)
	if !a.replayDeviceBound || !b.replayDeviceBound {
		t.Fatalf("a shared bottleneck should bind before N=16")
	}
	if a.replayAggGBs != b.replayAggGBs {
		t.Fatalf("under a shared bottleneck replay should not grow with cores: %.3f vs %.3f", a.replayAggGBs, b.replayAggGBs)
	}
}
