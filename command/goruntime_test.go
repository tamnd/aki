package command

import (
	"math"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
)

// TestInfoGoRuntime checks INFO go_runtime carries the runtime fields and that
// they parse as integers, and that the section is not in the default report.
func TestInfoGoRuntime(t *testing.T) {
	r, c := startData(t)

	// The section is off by default, so plain INFO must not carry it.
	if def, _ := sendArgs(t, r, c, "INFO").(string); strings.Contains(def, "go_goroutines") {
		t.Fatal("go_runtime leaked into default INFO")
	}

	for _, k := range []string{
		"go_goroutines",
		"go_heap_alloc_bytes",
		"go_heap_sys_bytes",
		"go_heap_idle_bytes",
		"go_gc_cycles_total",
		"go_gc_pause_total_ns",
		"go_gc_pause_max_ns",
		"go_sched_latencies_p99_ns",
	} {
		v := infoField(t, r, c, "go_runtime", k)
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("%s = %q not an integer", k, v)
		}
		if n < 0 {
			t.Fatalf("%s = %d is negative", k, n)
		}
	}

	// A live server always has at least one goroutine.
	if g := infoField(t, r, c, "go_runtime", "go_goroutines"); g == "0" {
		t.Fatal("go_goroutines = 0 on a running server")
	}

	// INFO all must include the section too.
	if all, _ := sendArgs(t, r, c, "INFO", "all").(string); !strings.Contains(all, "go_goroutines") {
		t.Fatal("INFO all missing go_runtime")
	}
}

// TestHistogramP99Ns checks the percentile helper on a hand-built histogram and on
// the empty and infinite-edge cases.
func TestHistogramP99Ns(t *testing.T) {
	// Empty histogram returns zero.
	if got := histogramP99Ns(&metrics.Float64Histogram{Buckets: []float64{0, 1}, Counts: []uint64{0}}); got != 0 {
		t.Fatalf("empty histogram p99 = %d want 0", got)
	}

	// Counts concentrated in the second bucket [0.001, 0.002) seconds, so p99
	// reports the upper edge 0.002s = 2_000_000ns.
	h := &metrics.Float64Histogram{
		Buckets: []float64{0, 0.001, 0.002, 0.003},
		Counts:  []uint64{0, 100, 0},
	}
	if got := histogramP99Ns(h); got != 2_000_000 {
		t.Fatalf("p99 = %d want 2000000", got)
	}

	// A +Inf top edge must not blow up: the sample lands in the last bucket so the
	// helper falls back to the finite lower edge 0.005s = 5_000_000ns.
	inf := &metrics.Float64Histogram{
		Buckets: []float64{0, 0.005, math.Inf(1)},
		Counts:  []uint64{0, 1},
	}
	if got := histogramP99Ns(inf); got != 5_000_000 {
		t.Fatalf("p99 with inf edge = %d want 5000000", got)
	}
}
