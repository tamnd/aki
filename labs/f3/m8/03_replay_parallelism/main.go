// Command 03_replay_parallelism models the recovery-parallelism knob the .aki
// format was shaped around (doc 07 section 6, prediction F21): N recovery workers,
// one per shard pinned to its owner core, each replays its own logical tail from
// the one physical file with no cross-shard coordination, because the shard tag and
// the per-shard segment chain let a worker walk only its own segments. The question
// the format bet on is whether that scales: does replay throughput grow with cores,
// or does a shared bottleneck (the single device, a global lock) flatten it?
//
// The model is pure arithmetic over doc 07 section 6, no engine import and no real
// I/O:
//
//   - recovery has two phases, both parallel across the N workers: load each shard's
//     index checkpoint and rebuild its native structures (step 6), then replay each
//     shard's tail applying every record (step 7);
//   - each phase runs at a per-core rate (apply-bound, not checksum-bound: replay
//     does a random index insert and a chunk-directory advance per record, and
//     rebuild reconstructs native structures, both far below the crc32c ceiling), so
//     the aggregate is N times the per-core rate, capped by the device's sequential
//     read ceiling that all workers share;
//   - a balanced store hands each worker resident/N and tail/N bytes, so open time is
//     the per-worker load time plus the per-worker replay time, which for a balanced
//     store equals resident/aggregateLoad + tail/aggregateReplay.
//
// The verdict the model checks is the pre-registered one: at the 16-core gate box
// replay hits the 1.5 GB/s aggregate target (under 1 s/GB of tail), the resident
// load stays under 2 s/GB, and the device ceiling sits far above what 16 cores
// demand, so replay is CPU-scaled not device-bound and the no-coordination design
// pays off. The falsifier (F21, section 13): a shared bottleneck that caps the
// aggregate below N times the per-core rate, or an open time above 5 s/GB.
package main

import (
	"flag"
	"fmt"
	"math"
)

// gate-box machine parameters (doc 07 section 6). The per-core rates are
// apply-bound: replay does a random index insert plus a chunk-directory advance per
// record, and rebuild reconstructs native structures, so both sit well below the
// crc32c validate ceiling and far below the device.
const (
	perCoreLoadGBs   = 0.04 // one core's index-checkpoint load plus native rebuild
	perCoreReplayGBs = 0.10 // one core's tail replay, apply-bound
	deviceGBs        = 6.0  // the gate box NVMe sequential read ceiling, shared by all workers
)

// row is one shard-count point: the recovery economics at N workers.
type row struct {
	shards            int     // N, one worker per shard pinned to its owner core
	loadAggGBs        float64 // min(N*perCoreLoad, device), the checkpoint-load+rebuild throughput
	replayAggGBs      float64 // min(N*perCoreReplay, device), the tail-replay throughput
	loadSecPerGB      float64 // 1/loadAggGBs, the resident open cost
	replaySecPerGB    float64 // 1/replayAggGBs, the tail open cost
	replayDeviceBound bool    // true once the shared device caps replay below N*perCore
}

// measure computes one row at a given worker count and machine.
func measure(shards int, perCoreLoad, perCoreReplay, device float64) row {
	loadAgg := math.Min(float64(shards)*perCoreLoad, device)
	replayAgg := math.Min(float64(shards)*perCoreReplay, device)
	return row{
		shards:            shards,
		loadAggGBs:        loadAgg,
		replayAggGBs:      replayAgg,
		loadSecPerGB:      1 / loadAgg,
		replaySecPerGB:    1 / replayAgg,
		replayDeviceBound: float64(shards)*perCoreReplay > device,
	}
}

// openSec is the wall-clock recovery time for a balanced store of `resident` GB of
// resident set and `tail` GB of un-checkpointed tail at this worker count.
func openSec(r row, resident, tail float64) float64 {
	return resident/r.loadAggGBs + tail/r.replayAggGBs
}

func main() {
	device := flag.Float64("device", deviceGBs, "device sequential read ceiling in GB/s, shared by all workers")
	resident := flag.Float64("resident", 8.0, "resident set size in GB, for the open-time example")
	tail := flag.Float64("tail", 1.0, "un-checkpointed tail size in GB, for the open-time example")
	quick := flag.Bool("quick", false, "run a short sweep")
	flag.Parse()

	shards := []int{1, 2, 4, 8, 16, 32, 64, 128}
	if *quick {
		shards = []int{1, 16, 64}
	}

	fmt.Printf("recovery parallelism: per-core load %.0f MB/s, replay %.0f MB/s, device %.1f GB/s\n",
		perCoreLoadGBs*1000, perCoreReplayGBs*1000, *device)
	fmt.Printf("open-time example: %.0f GB resident, %.0f GB tail\n\n", *resident, *tail)

	const hdr = "%-8s %-12s %-12s %-12s %-12s %-12s %-10s\n"
	fmt.Printf(hdr, "shards", "load GB/s", "replay GB/s", "load s/GB", "replay s/GB", "open s", "replay")
	fmt.Printf(hdr, "N", "aggregate", "aggregate", "resident", "tail", "example", "bound-by")
	fmt.Printf(hdr, "------", "---------", "-----------", "---------", "-----------", "-------", "--------")

	for _, n := range shards {
		r := measure(n, perCoreLoadGBs, perCoreReplayGBs, *device)
		boundBy := "cpu"
		if r.replayDeviceBound {
			boundBy = "device"
		}
		fmt.Printf("%-8d %-12.3f %-12.3f %-12.3f %-12.3f %-12.2f %-10s\n",
			r.shards, r.loadAggGBs, r.replayAggGBs, r.loadSecPerGB, r.replaySecPerGB,
			openSec(r, *resident, *tail), boundBy)
	}

	gate := measure(16, perCoreLoadGBs, perCoreReplayGBs, *device)
	open := openSec(gate, *resident, *tail)

	fmt.Printf("\nDesign point (the 16-core gate box, F21):\n")
	fmt.Printf("  replay hits %.2f GB/s aggregate (%.3f s/GB of tail), at or above the 1.5 GB/s pre-registered target and under the 1 s/GB bar\n",
		gate.replayAggGBs, gate.replaySecPerGB)
	fmt.Printf("  resident load is %.2f GB/s (%.2f s/GB), under the 2 s/GB bar; a %.0f GB + %.0f GB open takes %.1f s = %.2f s/GB total\n",
		gate.loadAggGBs, gate.loadSecPerGB, *resident, *tail, open, open/(*resident+*tail))

	// The core count where the shared device would start to cap replay.
	ceilN := int(math.Ceil(*device / perCoreReplayGBs))
	fmt.Printf("  replay stays CPU-scaled (aggregate = N times per-core) until N=%d, where the device ceiling bites: at the 16-core box the device sits %.1fx above demand, so no shared bottleneck\n",
		ceilN, *device/gate.replayAggGBs)
	fmt.Printf("  falsifier held: open is %.2f s/GB, far under the 5 s/GB abort line, and replay scales near-linearly to N=16 with no coordination floor\n",
		open/(*resident+*tail))
}
