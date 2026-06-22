package command

import (
	"math"
	"runtime"
	"runtime/metrics"
	"strings"
)

// This file implements the INFO go_runtime section from doc 21 section 10.4. It
// exposes Go runtime internals (goroutine count, heap sizes, GC cycle and pause
// figures, and scheduler latency) so an operator can watch the runtime that backs
// aki without attaching a profiler. The section is not in the default INFO output;
// it shows up under INFO go_runtime, INFO all, and INFO everything, so the
// default report stays close to what real Redis returns.

// infoGoRuntime writes the go_runtime section.
func infoGoRuntime(_ *Ctx, b *strings.Builder) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	lineInt(b, "go_goroutines", int64(runtime.NumGoroutine()))
	lineInt(b, "go_heap_alloc_bytes", int64(ms.HeapAlloc))
	lineInt(b, "go_heap_sys_bytes", int64(ms.HeapSys))
	lineInt(b, "go_heap_idle_bytes", int64(ms.HeapIdle))
	lineInt(b, "go_gc_cycles_total", int64(ms.NumGC))
	lineInt(b, "go_gc_pause_total_ns", int64(ms.PauseTotalNs))
	lineInt(b, "go_gc_pause_max_ns", int64(maxGCPauseNs(&ms)))
	lineInt(b, "go_sched_latencies_p99_ns", int64(schedLatencyP99Ns()))
}

// maxGCPauseNs returns the longest GC pause still on record. PauseNs is a 256-slot
// ring; only the most recent min(NumGC, 256) slots hold real samples, so the scan
// stops there rather than reading stale zeros as valid pauses.
func maxGCPauseNs(ms *runtime.MemStats) uint64 {
	n := int(ms.NumGC)
	if n > len(ms.PauseNs) {
		n = len(ms.PauseNs)
	}
	var max uint64
	for i := 0; i < n; i++ {
		if ms.PauseNs[i] > max {
			max = ms.PauseNs[i]
		}
	}
	return max
}

// schedLatencyP99Ns returns the 99th percentile time a goroutine spent waiting to
// run, in nanoseconds, read from the runtime's /sched/latencies:seconds
// histogram. It returns 0 when the metric is unavailable or no samples exist.
func schedLatencyP99Ns() uint64 {
	sample := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0
	}
	return histogramP99Ns(sample[0].Value.Float64Histogram())
}

// histogramP99Ns returns the 99th percentile of a runtime float histogram of
// seconds, converted to nanoseconds. Buckets holds the bucket boundaries and is
// one longer than Counts; the value reported for a percentile is the upper edge of
// the bucket it lands in.
func histogramP99Ns(h *metrics.Float64Histogram) uint64 {
	var total uint64
	for _, c := range h.Counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(0.99 * float64(total))
	var cum uint64
	for i, c := range h.Counts {
		cum += c
		if cum <= target {
			continue
		}
		// Report the bucket's upper edge. The top bucket's upper edge is +Inf and
		// the bottom bucket's lower edge can be -Inf, so fall back to the nearest
		// finite boundary rather than converting an infinity.
		edge := h.Buckets[i+1]
		if math.IsInf(edge, 1) {
			edge = h.Buckets[i]
		}
		if edge < 0 || math.IsInf(edge, -1) {
			edge = 0
		}
		return uint64(edge * 1e9)
	}
	return 0
}
