package main

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestRunAllSmoke runs both shadow arms at tiny counts on both backends
// and checks the CSV shape: thirteen columns, load plus hot and cold
// incr and flush rows, and the numeric fields parse.
func TestRunAllSmoke(t *testing.T) {
	for _, st := range []string{"a", "b"} {
		for _, arm := range []string{"shadow", "noshadow"} {
			t.Run(st+"/"+arm, func(t *testing.T) {
				cfg := config{
					dir: t.TempDir(), store: st, arm: arm, dist: "zipf",
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
					if len(fields) != 13 {
						t.Fatalf("row has %d fields, want 13: %q", len(fields), line)
					}
					if fields[0] != st {
						t.Fatalf("row carries store %q, want %q: %q", fields[0], st, line)
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
}

// TestCountersOracle drives both shadow arms on both backends over a
// capped resident model with random INCRs and flushes, mirrors every
// op on a reference map, then reads the store back: after the final
// flush both arms must have drained the exact reference counters as
// decimal strings. Eviction, dirty pinning, the shadow's drain-time
// format, and the noshadow reparse all sit on this path.
func TestCountersOracle(t *testing.T) {
	for _, st := range []string{"a", "b"} {
		for _, arm := range []string{"shadow", "noshadow"} {
			t.Run(st+"/"+arm, func(t *testing.T) { countersOracle(t, st, arm) })
		}
	}
}

func countersOracle(t *testing.T, storeArm, arm string) {
	const nKeys = 400
	path := filepath.Join(t.TempDir(), "oracle.db")
	keys := make([][]byte, nKeys)
	for i := range keys {
		keys[i] = []byte("n:" + strconv.Itoa(i))
	}
	st, err := openStore(config{store: storeArm}, path, keys)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	defer st.close()

	ref := make([]int64, nKeys)
	rng := rand.New(rand.NewSource(29))
	fs := flushSet{seq: 1}
	for i := range keys {
		ref[i] = rng.Int63n(1_000_000_000)
		fs.vals = append(fs.vals, valRow{ki: i, val: strconv.AppendInt(nil, ref[i], 10)})
	}
	if err := st.flush(&fs); err != nil {
		t.Fatalf("preload flush: %v", err)
	}

	c := newCounters(st, arm, 32, 100, 1)
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
		v, found, err := st.get(i)
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
}
