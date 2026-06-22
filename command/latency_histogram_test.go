package command

import (
	"testing"
)

// TestLatencyHistogram runs a handful of commands and checks the per-command
// histogram reports their call counts with a cumulative microsecond histogram.
func TestLatencyHistogram(t *testing.T) {
	r, c := startData(t)
	for range 5 {
		if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
			t.Fatalf("SET = %q", got)
		}
	}
	for range 3 {
		_ = sendReply(t, r, c, "GET k")
	}

	top := flatMapAny(t, sendReply(t, r, c, "LATENCY HISTOGRAM"))
	for _, name := range []string{"set", "get"} {
		entry, ok := top[name]
		if !ok {
			t.Fatalf("LATENCY HISTOGRAM missing %q, got %v", name, top)
		}
		checkHistogramEntry(t, name, entry)
	}

	// SET ran 5 times.
	set := flatMapAny(t, top["set"])
	if calls, _ := set["calls"].(int64); calls != 5 {
		t.Fatalf("set calls = %v want 5", set["calls"])
	}

	// A name filter returns only the named command.
	only := flatMapAny(t, sendReply(t, r, c, "LATENCY HISTOGRAM set"))
	if len(only) != 1 || only["set"] == nil {
		t.Fatalf("filtered histogram = %v want only set", only)
	}

	// An unknown or never-run command yields an empty map.
	none := flatMapAny(t, sendReply(t, r, c, "LATENCY HISTOGRAM nosuchcommand"))
	if len(none) != 0 {
		t.Fatalf("unknown command histogram = %v want empty", none)
	}
}

// TestLatencyHistCumulative records a sample for every value across the linear
// and log regions and checks cumulative does not panic and stays monotonic. The
// value 7 lands in the last linear bucket, whose upper edge once crossed into the
// log region and shifted by a negative amount.
func TestLatencyHistCumulative(t *testing.T) {
	var h latencyHist
	for v := uint64(0); v < 4096; v++ {
		h.record(v)
	}
	points := h.cumulative()
	if len(points) == 0 {
		t.Fatal("cumulative returned no points")
	}
	var lastBound, lastCount uint64
	first := true
	for _, p := range points {
		if !first && p.bound <= lastBound {
			t.Fatalf("bound %d not increasing after %d", p.bound, lastBound)
		}
		if p.count < lastCount {
			t.Fatalf("count %d dropped below %d", p.count, lastCount)
		}
		lastBound, lastCount = p.bound, p.count
		first = false
	}
	if lastCount != 4096 {
		t.Fatalf("final cumulative count %d want 4096", lastCount)
	}
}

// checkHistogramEntry verifies one command entry: calls is positive, the
// histogram has points, the cumulative counts only grow, and the final count
// equals the call total.
func checkHistogramEntry(t *testing.T, name string, entry any) {
	t.Helper()
	m := flatMapAny(t, entry)
	calls, ok := m["calls"].(int64)
	if !ok || calls <= 0 {
		t.Fatalf("%s calls = %v", name, m["calls"])
	}
	hist := asArray(t, m["histogram_usec"])
	if len(hist) == 0 || len(hist)%2 != 0 {
		t.Fatalf("%s histogram_usec malformed: %v", name, hist)
	}
	var lastBound, lastCount int64 = -1, 0
	for i := 0; i < len(hist); i += 2 {
		bound, _ := hist[i].(int64)
		count, _ := hist[i+1].(int64)
		if bound <= lastBound {
			t.Fatalf("%s bound %d not increasing after %d", name, bound, lastBound)
		}
		if count < lastCount {
			t.Fatalf("%s count %d dropped below %d", name, count, lastCount)
		}
		lastBound, lastCount = bound, count
	}
	if lastCount != calls {
		t.Fatalf("%s final cumulative count %d != calls %d", name, lastCount, calls)
	}
}
