package main

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs both arms at tiny counts and checks the CSV
// shape: twelve columns, load plus hot and cold incr and flush rows,
// and the numeric fields parse.
func TestRunAllSmoke(t *testing.T) {
	for _, arm := range []string{"shadow", "noshadow"} {
		t.Run(arm, func(t *testing.T) {
			cfg := config{
				dir: t.TempDir(), arm: arm, dist: "zipf",
				keys: 5000, hotKeys: 256, hotOps: 8000, coldOps: 2000,
				resident: 128, flushAt: 512,
			}
			var out bytes.Buffer
			if err := runAll(cfg, &out); err != nil {
				t.Fatalf("runAll: %v", err)
			}
			want := map[string]bool{
				"load": false, "hot-incr": false, "hot-flush": false,
				"cold-incr": false, "cold-flush": false,
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

// TestCountersOracle drives both arms over a capped resident model with
// random INCRs and flushes, mirrors every op on a reference map, then
// reads the store back: after the final flush both arms must have
// drained the exact reference counters as decimal strings. Eviction,
// dirty pinning, the shadow's drain-time format, and the noshadow
// reparse all sit on this path.
func TestCountersOracle(t *testing.T) {
	for _, arm := range []string{"shadow", "noshadow"} {
		t.Run(arm, func(t *testing.T) {
			const nKeys = 400
			path := filepath.Join(t.TempDir(), "oracle.db")
			d, err := openDB(path)
			if err != nil {
				t.Fatalf("openDB: %v", err)
			}
			defer d.close()

			keys := make([][]byte, nKeys)
			ref := make([]int64, nKeys)
			rng := rand.New(rand.NewSource(29))
			txn, err := d.conn.BeginImmediate()
			if err != nil {
				t.Fatal(err)
			}
			var buf []byte
			for i := range keys {
				keys[i] = []byte("n:" + strconv.Itoa(i))
				ref[i] = rng.Int63n(1_000_000_000)
				buf = strconv.AppendInt(buf[:0], ref[i], 10)
				if err := d.put1.BindBlob(1, keys[i]); err != nil {
					t.Fatal(err)
				}
				if err := d.put1.BindBlob(2, buf); err != nil {
					t.Fatal(err)
				}
				if _, err := stepReset(d.put1); err != nil {
					t.Fatal(err)
				}
			}
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}

			c := newCounters(d, arm, keys, 32, 100)
			for range 20000 {
				ki := rng.Intn(nKeys)
				if err := c.incr(ki); err != nil {
					t.Fatalf("incr: %v", err)
				}
				ref[ki]++
			}
			if err := c.flush(); err != nil {
				t.Fatalf("final flush: %v", err)
			}

			for i := range keys {
				v, found, err := d.get(keys[i])
				if err != nil {
					t.Fatalf("get %d: %v", i, err)
				}
				if !found {
					t.Fatalf("key %d missing after flush", i)
				}
				if got := string(v); got != strconv.FormatInt(ref[i], 10) {
					t.Fatalf("key %d: stored %q, reference %d", i, got, ref[i])
				}
			}
		})
	}
}
