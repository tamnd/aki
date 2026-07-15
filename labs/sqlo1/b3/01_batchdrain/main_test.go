package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRunAllSmoke runs the measured phase at tiny counts for both write
// distributions and checks the CSV shape.
func TestRunAllSmoke(t *testing.T) {
	for _, dist := range []string{"uniform", "zipf"} {
		cfg := config{
			dir:       t.TempDir(),
			keys:      3000,
			val:       32,
			threshold: 64 << 10,
			maxOps:    128,
			readers:   2,
			dist:      dist,
			dur:       150 * time.Millisecond,
			walSeg:    1 << 20,
			ckptBytes: 1 << 20,
		}
		var out strings.Builder
		if err := runAll(cfg, &out); err != nil {
			t.Fatalf("%s: runAll: %v", dist, err)
		}
		want := map[string]bool{
			"write": false, "drain": false, "lag": false, "ckpt": false, "pool-read-r2": false,
		}
		for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
			fields := strings.Split(line, ",")
			if len(fields) != 15 {
				t.Fatalf("%s: row has %d fields, want 15: %q", dist, len(fields), line)
			}
			if fields[1] != "128" || fields[2] != dist {
				t.Fatalf("%s: row does not carry the config: %q", dist, line)
			}
			if _, ok := want[fields[5]]; !ok {
				t.Fatalf("%s: unexpected workload %q in row %q", dist, fields[5], line)
			}
			want[fields[5]] = true
			for i, f := range fields {
				if i == 2 || i == 5 {
					continue
				}
				if _, err := strconv.ParseFloat(f, 64); err != nil {
					t.Fatalf("%s: field %d does not parse in row %q: %v", dist, i, line, err)
				}
			}
		}
		for w, seen := range want {
			if !seen {
				t.Fatalf("%s: workload %q missing:\n%s", dist, w, out.String())
			}
		}
	}
}

// TestQueueCoalesces pins the policy the harness mirrors from drain.go:
// one entry per dirty key however often it is rewritten, drained
// first-dirtied-first.
func TestQueueCoalesces(t *testing.T) {
	q := newQueue(10)
	q.write(3, 100)
	q.write(7, 100)
	q.write(3, 100)
	q.write(7, 100)
	if q.dirtyBytes != 200 {
		t.Fatalf("dirtyBytes %d after coalesced rewrites, want 200", q.dirtyBytes)
	}
	batch := q.pop(10)
	if len(batch) != 2 || batch[0] != 3 || batch[1] != 7 {
		t.Fatalf("pop returned %v, want [3 7]", batch)
	}
	q.dirty[3], q.dirty[7] = false, false
	q.write(3, 100)
	if got := q.pop(10); len(got) != 1 || got[0] != 3 {
		t.Fatalf("re-dirty after drain returned %v, want [3]", got)
	}
}
