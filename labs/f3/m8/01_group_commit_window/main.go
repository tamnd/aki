// Command 01_group_commit_window models the group-commit window of the .aki
// append writer: how a single log-writer that funnels every shard's segments
// into one fsync per group beats a writer-per-shard layout on flush amplification,
// and where the group size stops paying a ring-hop-and-wakeup tax.
//
// It is the per-perf-change lab the durable append path owes (the append writer
// slice was the first to fsync a group). The model reproduces doc 07 section 2's
// arithmetic exactly, so its numbers are the design's numbers:
//
//   - a group holds B records; at an offered write rate W the group window is
//     T = B/W seconds, so the writer flushes 1/T = W/B times a second;
//   - each flush costs the device F seconds, so group commit spends F*W/B of the
//     device's second, while a writer-per-shard layout flushing at the same
//     latency budget spends N times that (N shards, N write heads, N flushes per
//     window), the amplification the single writer exists to avoid;
//   - the device flush amortizes over the whole group, F/B seconds per record,
//     so a bigger group buys a lower per-write flush cost at the price of latency;
//   - the group also pays one ring-hop-and-wakeup handoff per group, a fixed H
//     seconds spread over B records, so the hop tax per record is H/B, which as a
//     fraction of the writer's per-record service time 1/W is H*W/B.
//
// The spec's design point is section 2's worked case: N = 16, W = 2M acked
// writes/s, a 1 ms window, a 50 us device flush, which is 1000 group flushes/s
// (5 percent of device time) against 16000 per-shard flushes/s (80 percent, the
// saturation the doc names), a 16x cut, with B = 2000 records the point the doc
// says the hop and wakeup amortize below 1 percent. The verdict reports where the
// modeled numbers land against that.
//
// The lab imports no engine package: the append writer issues exactly one fsync
// per AppendGroup (akifile writer.go maybeSync under SyncAlways), which is the
// 1/T flush rate this model assumes; the device flush time and the ring-hop cost
// are machine parameters, so the model fixes them as flags and the test pins the
// derived arithmetic against the spec's table rather than timing a real disk.
package main

import (
	"flag"
	"fmt"
)

// row is one group-size point: the flush economics of committing B records per
// group at the offered write rate, for both the single-writer group commit and
// the writer-per-shard rival.
type row struct {
	group           int     // B, records per group
	windowMS        float64 // T = B/W in milliseconds, the added commit latency
	groupFlushes    float64 // 1/T = W/B flushes per second, the single writer
	groupDeviceFrac float64 // F*W/B, the device-time fraction group commit spends
	shardFlushes    float64 // N/T = N*W/B, the writer-per-shard rival
	shardDeviceFrac float64 // N*F*W/B, the rival's device-time fraction
	flushPerWriteNS float64 // F/B in nanoseconds, the amortized flush cost/record
	hopFrac         float64 // H*W/B, the ring-hop tax as a fraction of service time
}

// measure computes one row from the machine parameters.
func measure(group, shards int, writeRate, flushSec, hopSec float64) row {
	b := float64(group)
	windowSec := b / writeRate
	groupFlushes := 1 / windowSec // = writeRate / b
	return row{
		group:           group,
		windowMS:        windowSec * 1000,
		groupFlushes:    groupFlushes,
		groupDeviceFrac: flushSec * groupFlushes,
		shardFlushes:    float64(shards) * groupFlushes,
		shardDeviceFrac: float64(shards) * flushSec * groupFlushes,
		flushPerWriteNS: flushSec / b * 1e9,
		hopFrac:         hopSec * writeRate / b,
	}
}

func main() {
	shards := flag.Int("shards", 16, "shard count N funneling into one writer")
	writeRate := flag.Float64("writerate", 2_000_000, "offered acked writes per second W")
	flushUS := flag.Float64("flush", 50, "device flush time F in microseconds")
	hopUS := flag.Float64("hop", 10, "ring-hop plus wakeup handoff cost H per group in microseconds")
	quick := flag.Bool("quick", false, "run a short sweep")
	flag.Parse()

	groups := []int{1, 8, 64, 512, 2000, 8192, 32768}
	if *quick {
		groups = []int{1, 64, 2000, 32768}
	}

	flushSec := *flushUS * 1e-6
	hopSec := *hopUS * 1e-6

	fmt.Printf("group-commit window: N=%d shards, W=%.0f writes/s, F=%.0f us flush, H=%.0f us hop\n\n",
		*shards, *writeRate, *flushUS, *hopUS)

	const hdr = "%-8s %-9s %-11s %-9s %-12s %-10s %-11s %-8s\n"
	fmt.Printf(hdr, "group", "window", "group", "group", "per-shard", "per-shard", "flush amrt", "hop")
	fmt.Printf(hdr, "size B", "ms", "flush/s", "device%", "flush/s", "device%", "ns/write", "tax%")
	fmt.Printf(hdr, "------", "------", "-------", "-------", "---------", "---------", "----------", "----")

	var design row
	for _, g := range groups {
		r := measure(g, *shards, *writeRate, flushSec, hopSec)
		if g == 2000 {
			design = r
		}
		fmt.Printf("%-8d %-9.3f %-11.0f %-9s %-12.0f %-10s %-11.3f %-8s\n",
			r.group, r.windowMS, r.groupFlushes,
			fmt.Sprintf("%.1f%%", r.groupDeviceFrac*100),
			r.shardFlushes,
			fmt.Sprintf("%.1f%%", r.shardDeviceFrac*100),
			r.flushPerWriteNS,
			fmt.Sprintf("%.2f%%", r.hopFrac*100))
	}
	if design.group == 0 {
		design = measure(2000, *shards, *writeRate, flushSec, hopSec)
	}

	fmt.Printf("\nDesign point (B=2000 records, the W*T=1ms window of doc 07 section 2):\n")
	fmt.Printf("  group commit flushes %.0f/s at %.1f%% device time; a writer per shard flushes %.0f/s at %.1f%% (%.0fx more)\n",
		design.groupFlushes, design.groupDeviceFrac*100,
		design.shardFlushes, design.shardDeviceFrac*100,
		design.shardFlushes/design.groupFlushes)
	fmt.Printf("  the device flush amortizes to %.3f ns/write and the ring-hop tax is %.2f%% of per-record service, at or below the 1%% the doc names\n",
		design.flushPerWriteNS, design.hopFrac*100)
	fmt.Printf("  the trade the window sizes: this group adds %.3f ms of commit latency, and a bigger one cuts flush rate and hop tax further for more\n",
		design.windowMS)
}
