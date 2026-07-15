package main

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs tiny counts on both arms (both layouts on a,
// the column-order question being SQLite-only) and checks the CSV
// shape: thirteen columns, load plus every shape-arm-temperature row,
// and the numeric fields parse. The answer assertions inside runAll do
// the correctness half.
func TestRunAllSmoke(t *testing.T) {
	for _, tc := range []struct{ store, layout string }{
		{"a", "pclast"}, {"a", "pcfirst"}, {"b", "pclast"},
	} {
		t.Run(tc.store+"/"+tc.layout, func(t *testing.T) {
			cfg := config{
				dir: t.TempDir(), store: tc.store, chunk: 8 << 10, layout: tc.layout,
				sizeMB: 1, reps: 2, hotReps: 3,
			}
			var out bytes.Buffer
			if err := runAll(cfg, &out); err != nil {
				t.Fatalf("runAll: %v", err)
			}
			want := map[string]bool{"load": false}
			for _, shape := range []string{"full", "small", "half"} {
				for _, arm := range []string{"cache", "scan"} {
					for _, temp := range []string{"cold", "hot"} {
						want[shape+"-"+arm+"-"+temp] = false
					}
				}
			}
			for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
				fields := strings.Split(line, ",")
				if len(fields) != 13 {
					t.Fatalf("row has %d fields, want 13: %q", len(fields), line)
				}
				if fields[0] != tc.store {
					t.Fatalf("row carries store %q, want %q: %q", fields[0], tc.store, line)
				}
				if _, ok := want[fields[4]]; ok {
					want[fields[4]] = true
				}
				for _, idx := range []int{6, 7, 8, 9, 10, 11, 12} {
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

// TestRangeOracle pins the range decomposition on both arms: a small
// bitmap kept flat in RAM, stored chunked with pc through the store's
// own flush, then hundreds of random byte ranges where cacheCount,
// scanCount, and a naive popcount over the flat copy must all agree,
// including single-chunk ranges, chunk-aligned edges, and spans ending
// at the last byte. cacheCount reads the stored pc state (pc column on
// a, kind 2 cache segments on b), so a wrong stored popcount fails here.
func TestRangeOracle(t *testing.T) {
	for _, arm := range []string{"a", "b"} {
		t.Run(arm, func(t *testing.T) { rangeOracle(t, arm) })
	}
}

func rangeOracle(t *testing.T, arm string) {
	const chunk = 4 << 10
	const size = int64(7*chunk + 123)
	cfg := config{store: arm, layout: "pclast", chunk: chunk}
	path := filepath.Join(t.TempDir(), "oracle.db")
	st, err := openStore(cfg, path, []byte("b:0000"), true)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	defer st.close()

	rng := rand.New(rand.NewSource(11))
	flat := make([]byte, size)
	rng.Read(flat)
	var fs flushSet
	for cid := int64(0); cid*chunk < size; cid++ {
		row := flat[cid*chunk : min((cid+1)*chunk, size)]
		fs.chunks = append(fs.chunks, chunkRow{cid: cid, row: row, pc: int64(popcount(row))})
		fs.seq = cid + 1
	}
	if err := st.flush(&fs); err != nil {
		t.Fatalf("flush: %v", err)
	}

	ranges := [][2]int64{
		{0, size}, {0, 1}, {size - 1, size},
		{chunk, 2 * chunk}, {chunk - 1, chunk + 1}, {0, chunk},
		{3*chunk + 5, 3*chunk + 6},
	}
	for range 300 {
		b0 := rng.Int63n(size)
		b1 := b0 + 1 + rng.Int63n(size-b0)
		ranges = append(ranges, [2]int64{b0, b1})
	}
	for _, rr := range ranges {
		b0, b1 := rr[0], rr[1]
		want := int64(popcount(flat[b0:b1]))
		got, err := cacheCount(st, chunk, b0, b1)
		if err != nil {
			t.Fatalf("cacheCount(%d, %d): %v", b0, b1, err)
		}
		if got != want {
			t.Fatalf("cacheCount(%d, %d) = %d, want %d", b0, b1, got, want)
		}
		got, err = scanCount(st, chunk, b0, b1)
		if err != nil {
			t.Fatalf("scanCount(%d, %d): %v", b0, b1, err)
		}
		if got != want {
			t.Fatalf("scanCount(%d, %d) = %d, want %d", b0, b1, got, want)
		}
	}
}
