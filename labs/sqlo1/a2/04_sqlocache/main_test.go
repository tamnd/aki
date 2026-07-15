package main

import (
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs both arms at tiny counts against one kept work
// dir (so the second arm exercises the preload-reuse path) and checks
// the CSV shape.
func TestRunAllSmoke(t *testing.T) {
	dir := t.TempDir()
	for _, arm := range []string{"hot", "page"} {
		cfg := config{
			dir: dir, arm: arm, budgetMiB: 1,
			keys: 3000, val: 32, ops: 4000,
			dist: "zipf", writePct: 10,
		}
		var out strings.Builder
		if err := runAll(cfg, &out); err != nil {
			t.Fatalf("%s: runAll: %v", arm, err)
		}
		want := map[string]bool{"mixed-read": false, "mixed-write": false}
		for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
			fields := strings.Split(line, ",")
			if len(fields) != 15 {
				t.Fatalf("%s: row has %d fields, want 15: %q", arm, len(fields), line)
			}
			if fields[1] != arm || fields[2] != "zipf" {
				t.Fatalf("%s: row does not carry the config: %q", arm, line)
			}
			if _, ok := want[fields[5]]; !ok {
				t.Fatalf("%s: unexpected workload %q", arm, fields[5])
			}
			want[fields[5]] = true
			for i, f := range fields {
				if i == 1 || i == 2 || i == 5 {
					continue
				}
				if _, err := strconv.ParseFloat(f, 64); err != nil {
					t.Fatalf("%s: field %d does not parse in row %q: %v", arm, i, line, err)
				}
			}
			if fields[5] == "mixed-read" {
				hit, _ := strconv.ParseFloat(fields[12], 64)
				if arm == "page" && hit != 0 {
					t.Fatalf("page arm reported cache hits: %q", line)
				}
				if arm == "hot" && hit == 0 {
					t.Fatalf("hot arm never hit its cache on a zipf mix: %q", line)
				}
			}
		}
		for w, seen := range want {
			if !seen {
				t.Fatalf("%s: workload %q missing:\n%s", arm, w, out.String())
			}
		}
	}
}

// TestRecCacheBudget pins the model: entries are charged at the unit
// cost, the cap holds, and dirty entries are pinned until cleaned.
func TestRecCacheBudget(t *testing.T) {
	c := newRecCache(300, 100)
	c.put(1, nil, false)
	c.put(2, nil, false)
	c.put(3, nil, false)
	if c.bytes != 300 || len(c.m) != 3 {
		t.Fatalf("bytes %d entries %d, want 300 and 3", c.bytes, len(c.m))
	}
	c.put(4, nil, false)
	if c.bytes != 300 || len(c.m) != 3 {
		t.Fatalf("cap did not hold: bytes %d entries %d", c.bytes, len(c.m))
	}

	d := newRecCache(200, 100)
	d.put(1, nil, true)
	d.put(2, nil, true)
	d.put(3, nil, true)
	if len(d.m) != 3 {
		t.Fatalf("dirty entries were evicted: %d left", len(d.m))
	}
	d.clean(1)
	d.clean(2)
	d.clean(3)
	d.put(4, nil, false)
	if len(d.m) > 2 {
		t.Fatalf("cleaned entries not evictable: %d left", len(d.m))
	}
}
