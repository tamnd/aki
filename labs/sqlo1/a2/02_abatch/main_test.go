package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRunAllSmoke runs every arm at tiny counts and checks the CSV: all
// workloads report, swept batch sizes appear, numeric fields parse.
func TestRunAllSmoke(t *testing.T) {
	cfg := config{
		dir:     t.TempDir(),
		keys:    2000,
		val:     32,
		ops:     1500,
		single:  100,
		batches: []int{64, 256},
		readers: 2,
		poolDur: 150 * time.Millisecond,
	}
	var out strings.Builder
	if err := runAll(cfg, &out); err != nil {
		t.Fatalf("runAll: %v", err)
	}

	want := map[string]int{
		"point-get": 0, "floor-get": 0, "drain-single": 0,
		"drain-solo": 0, "pool-read-r2": 0, "pool-drain-r2": 0,
	}
	batchesSeen := map[string]bool{}
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		fields := strings.Split(line, ",")
		if len(fields) != 11 {
			t.Fatalf("row has %d fields, want 11: %q", len(fields), line)
		}
		if fields[0] != "2000" || fields[1] != "32" {
			t.Fatalf("row does not carry the cell config: %q", line)
		}
		if _, ok := want[fields[2]]; !ok {
			t.Fatalf("unexpected workload %q in row %q", fields[2], line)
		}
		want[fields[2]]++
		if fields[2] == "drain-solo" {
			batchesSeen[fields[3]] = true
		}
		for i, f := range fields {
			if i == 2 {
				continue
			}
			if _, err := strconv.ParseFloat(f, 64); err != nil {
				t.Fatalf("field %d does not parse in row %q: %v", i, line, err)
			}
		}
	}
	for w, n := range want {
		if n == 0 {
			t.Fatalf("workload %q missing from output:\n%s", w, out.String())
		}
	}
	if !batchesSeen["64"] || !batchesSeen["256"] {
		t.Fatalf("drain-solo rows missing swept batch sizes: %v", batchesSeen)
	}
	if want["drain-solo"] != 2 || want["pool-read-r2"] != 2 {
		t.Fatalf("sweep did not visit both batch sizes: %v", want)
	}
}
