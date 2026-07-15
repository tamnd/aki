package main

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs both layouts at tiny counts and checks the CSV
// shape: twelve columns, load plus every shape-arm-temperature row, and
// the numeric fields parse. The answer assertions inside runAll do the
// correctness half.
func TestRunAllSmoke(t *testing.T) {
	for _, layout := range []string{"pclast", "pcfirst"} {
		t.Run(layout, func(t *testing.T) {
			cfg := config{
				dir: t.TempDir(), chunk: 8 << 10, layout: layout,
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
				if len(fields) != 12 {
					t.Fatalf("row has %d fields, want 12: %q", len(fields), line)
				}
				if _, ok := want[fields[3]]; ok {
					want[fields[3]] = true
				}
				for _, idx := range []int{5, 6, 7, 8, 9, 10, 11} {
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

// TestRangeOracle pins the range decomposition: a small bitmap kept flat
// in RAM, stored chunked with pc, then hundreds of random byte ranges
// where cacheCount, scanCount, and a naive popcount over the flat copy
// must all agree, including single-chunk ranges, chunk-aligned edges,
// and spans ending at the last byte.
func TestRangeOracle(t *testing.T) {
	const chunk = 4 << 10
	const size = int64(7*chunk + 123)
	path := filepath.Join(t.TempDir(), "oracle.db")
	d, err := openDB(path, "pclast")
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer d.close()

	key := []byte("b:0000")
	rng := rand.New(rand.NewSource(11))
	flat := make([]byte, size)
	rng.Read(flat)
	txn, err := d.conn.BeginImmediate()
	if err != nil {
		t.Fatal(err)
	}
	for cid := int64(0); cid*chunk < size; cid++ {
		row := flat[cid*chunk : min((cid+1)*chunk, size)]
		if err := d.cput.BindBlob(1, key); err != nil {
			t.Fatal(err)
		}
		if err := d.cput.BindInt64(2, cid); err != nil {
			t.Fatal(err)
		}
		if err := d.cput.BindBlob(3, row); err != nil {
			t.Fatal(err)
		}
		if err := d.cput.BindInt64(4, int64(popcount(row))); err != nil {
			t.Fatal(err)
		}
		if _, err := stepReset(d.cput); err != nil {
			t.Fatal(err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
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
		got, err := d.cacheCount(key, chunk, b0, b1)
		if err != nil {
			t.Fatalf("cacheCount(%d, %d): %v", b0, b1, err)
		}
		if got != want {
			t.Fatalf("cacheCount(%d, %d) = %d, want %d", b0, b1, got, want)
		}
		got, err = d.scanCount(key, chunk, b0, b1)
		if err != nil {
			t.Fatalf("scanCount(%d, %d): %v", b0, b1, err)
		}
		if got != want {
			t.Fatalf("scanCount(%d, %d) = %d, want %d", b0, b1, got, want)
		}
	}
}
