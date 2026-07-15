package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs one small cell per ladder tier end to end on
// both arms and checks the CSV shape: fifteen columns, the store
// column carrying the arm, the load and both point rows present, the
// mode column naming the expected tier, and the numeric fields
// parsing.
func TestRunAllSmoke(t *testing.T) {
	for _, arm := range []string{"a", "b"} {
		for _, tc := range []struct {
			fields int64
			mode   string
		}{
			{30, "inline"},
			{5000, "segmented"},
			{20000, "fence-paged"},
		} {
			t.Run(arm+"/"+tc.mode, func(t *testing.T) {
				cfg := config{
					dir: t.TempDir(), store: arm, fields: tc.fields,
					segMax: 4032, inlineMax: 2048, inlineCount: 128,
					fenceMax: 2048, pageEnts: 250,
					reps: 20, hotreps: 100, ckpt: 8,
				}
				var out bytes.Buffer
				if err := runAll(cfg, &out); err != nil {
					t.Fatalf("runAll: %v", err)
				}
				want := map[string]bool{"load": false, "point-cold": false, "point-hot": false}
				for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
					fields := strings.Split(line, ",")
					if len(fields) != 15 {
						t.Fatalf("row has %d fields, want 15: %q", len(fields), line)
					}
					if fields[0] != arm {
						t.Fatalf("row carries store %q, want %q: %q", fields[0], arm, line)
					}
					if fields[2] != tc.mode {
						t.Fatalf("mode column %q, want %q", fields[2], tc.mode)
					}
					if _, ok := want[fields[3]]; ok {
						want[fields[3]] = true
					}
					for _, idx := range []int{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14} {
						if _, err := strconv.ParseFloat(fields[idx], 64); err != nil {
							t.Fatalf("field %d not numeric in %q: %v", idx, line, err)
						}
					}
				}
				for w, seen := range want {
					if !seen {
						t.Fatalf("workload %s missing from output:\n%s", w, out.String())
					}
				}
			})
		}
	}
}

// TestLadderOracle shrinks every threshold so all three tiers appear at
// tiny field counts, then looks up every single preloaded field on both
// arms and requires the exact generated value at exactly the tier's
// record-read cost; a probe for an absent field must come back nil
// without blowing the ceiling, and a count past the one-level page
// index must refuse at plan time (doc 06 keeps a third level out of
// scope).
func TestLadderOracle(t *testing.T) {
	base := config{
		segMax:      segHdrSize + 4*entSize, // 4 fields per segment
		inlineMax:   rootHdrSize + 8*entSize,
		inlineCount: 8,
		fenceMax:    8 * fenceEntSize, // 8 segments before paging
		pageEnts:    16,
		ckpt:        8,
	}
	for _, arm := range []string{"a", "b"} {
		for _, tc := range []struct {
			fields    int64
			mode      string
			wantReads int
		}{
			{6, "inline", 1},
			{30, "segmented", 2},
			{200, "fence-paged", 3},
		} {
			t.Run(arm+"/"+tc.mode, func(t *testing.T) {
				cfg := base
				cfg.store = arm
				cfg.fields = tc.fields
				l, err := planLayout(cfg)
				if err != nil {
					t.Fatalf("planLayout: %v", err)
				}
				if l.mode != tc.mode {
					t.Fatalf("mode = %s, want %s for %d fields", l.mode, tc.mode, tc.fields)
				}
				path := filepath.Join(t.TempDir(), "oracle.db")
				st, err := openStore(cfg, path, []byte("h:oracle"))
				if err != nil {
					t.Fatalf("openStore: %v", err)
				}
				defer st.close()
				if _, _, err := preload(st, cfg, l); err != nil {
					t.Fatalf("preload: %v", err)
				}

				total := int64(0)
				for s := int64(0); s < l.segs; s++ {
					n := segFields(cfg, l, s)
					total += n
					for j := range n {
						f := fhAt(l, s, j)
						v, reads, err := lookup(st, f, fieldAt(f))
						if err != nil {
							t.Fatalf("lookup seg %d slot %d: %v", s, j, err)
						}
						if !bytes.Equal(v, valueAt(f)) {
							t.Fatalf("seg %d slot %d: wrong value (%d bytes)", s, j, len(v))
						}
						if reads != tc.wantReads {
							t.Fatalf("seg %d slot %d took %d reads, want exactly %d", s, j, reads, tc.wantReads)
						}
					}
				}
				if total != tc.fields {
					t.Fatalf("layout holds %d fields, want %d", total, tc.fields)
				}

				miss := fhAt(l, 0, 0) + 1
				v, reads, err := lookup(st, miss, fmt.Appendf(nil, "f%016x", miss))
				if err != nil {
					t.Fatalf("miss lookup: %v", err)
				}
				if v != nil {
					t.Fatalf("miss lookup returned %d bytes, want nil", len(v))
				}
				if reads > tc.wantReads {
					t.Fatalf("miss lookup took %d reads, ceiling %d", reads, tc.wantReads)
				}
			})
		}
	}

	cfg := base
	cfg.fields = 10000 // 2500 segments, 157 pages, over the 16-page index
	if _, err := planLayout(cfg); err == nil {
		t.Fatal("planLayout accepted a field count past the one-level page index")
	}
}
