package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRunAllSmoke runs the whole arm sequence at tiny counts and checks
// the CSV holds together: every row carries the swept configuration, the
// expected workloads all report, and the numeric fields parse.
func TestRunAllSmoke(t *testing.T) {
	cfg := config{
		dir:      t.TempDir(),
		page:     4096,
		cacheKiB: 2048,
		ckpt:     2,
		val:      32,
		keys:     3000,
		ops:      2000,
		batch:    256,
		readers:  2,
		poolDur:  200 * time.Millisecond,
	}
	var out strings.Builder
	if err := runAll(cfg, &out); err != nil {
		t.Fatalf("runAll: %v", err)
	}

	want := map[string]bool{
		"load": false, "get-zipf": false, "get-cold": false,
		"pool-read-r2": false, "pool-drain-r2": false, "pool-ckpt": false,
	}
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		fields := strings.Split(line, ",")
		if len(fields) != 15 {
			t.Fatalf("row has %d fields, want 15: %q", len(fields), line)
		}
		if fields[0] != "4096" || fields[1] != "2048" || fields[2] != "2" {
			t.Fatalf("row does not carry the swept config: %q", line)
		}
		if _, ok := want[fields[5]]; !ok {
			t.Fatalf("unexpected workload %q in row %q", fields[5], line)
		}
		want[fields[5]] = true
		for i, f := range fields {
			if i == 5 {
				continue
			}
			if _, err := strconv.ParseFloat(f, 64); err != nil {
				t.Fatalf("field %d does not parse in row %q: %v", i, line, err)
			}
		}
	}
	for w, seen := range want {
		if !seen {
			t.Fatalf("workload %q missing from output:\n%s", w, out.String())
		}
	}
}

// TestPageSizeTakes catches the classic failure where PRAGMA page_size
// arrives after the file exists and silently does nothing.
func TestPageSizeTakes(t *testing.T) {
	for _, page := range []int{4096, 16384} {
		cfg := config{dir: t.TempDir(), page: page, cacheKiB: 2048, ckpt: 4,
			val: 16, keys: 100, ops: 100, batch: 32, readers: 1,
			poolDur: 50 * time.Millisecond}
		var out strings.Builder
		if err := runAll(cfg, &out); err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
	}
}
