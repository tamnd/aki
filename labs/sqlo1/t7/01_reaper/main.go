// Lab: sampling reaper budget and cadence (spec 2064/sqlo1 doc 11
// section 3.3, milestone T7 lab 01).
//
// Slice 3 wires a background task that samples cold-index chunks,
// skips chunks whose entries are all class none or far, and probes
// due-class entries exactly. Doc 11 caps its duty cycle at 1% of one
// core with 8 chunk reads per tick, both provisional. This lab
// prices the two constants against the real store: SampleExpiry runs
// under the store lock, so one pass's duration is exactly the stall
// a queued command can see, and the chunk budget is the only knob
// that trades that stall against lap time (how long until every
// bucket has been sampled once, which bounds expired-count staleness
// and index bloat).
//
// Three arms. The warm arm samples right after the build, index
// chunks still resident in the dirty map, which is the best case.
// The cold arm checkpoints, reopens, and samples against chains that
// page in from disk without caching, the doc 11 steady state for the
// huge-cold-set workload the reaper exists for; the frame cache
// warms across passes, matching a long-running reaper. The reap arm
// prices the batched tombstone side: ApplyBatch of DEL ops at
// several batch sizes, the frames slice 3 will emit for keys the
// probe finds dead.
//
// The keyspace splits by class from the write clock: near at 30
// minutes, mid at 24 hours, far at 30 days, the rest no expiry, and
// a fraction of the near keys already expired at build time so the
// probe has something real to find. Accuracy is structural (the
// probe checks exact deadlines), so the columns to read are pass
// percentiles, entries per pass, and the derived duty lines on
// stderr: at a given tick the budget sets duty, and lap time scales
// linearly in buckets over budget.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

type config struct {
	dir     string
	keys    int
	val     int
	nearPct int
	midPct  int
	farPct  int
	expPct  int // of the near keys, already expired at build
}

type built struct {
	s       *sqlo1b.Store
	path    string
	seq     int64
	expired []string // keys planted already-expired
	perCls  [4]int64 // ground truth entries per class
}

func classOf(cfg config, i int) int {
	switch p := i % 100; {
	case p < cfg.nearPct:
		return 1
	case p < cfg.nearPct+cfg.midPct:
		return 2
	case p < cfg.nearPct+cfg.midPct+cfg.farPct:
		return 3
	default:
		return 0
	}
}

// build creates the store and writes the class-mixed keyspace in
// 256-op batches. Deadlines come from the wall clock because the
// store's expiry bucketing does too.
func build(cfg config) (*built, error) {
	b := &built{path: filepath.Join(cfg.dir, "reaper.aki")}
	os.Remove(b.path)
	os.Remove(b.path + ".aki-wal")
	s, err := sqlo1b.CreateStore(b.path, 64<<20)
	if err != nil {
		return nil, err
	}
	b.s = s
	now := time.Now().UnixMilli()
	val := make([]byte, cfg.val)
	ctx := context.Background()
	var ops []sqlo1.Op
	nNear := 0
	for i := range cfg.keys {
		k := fmt.Sprintf("k%07d", i)
		var exp int64
		switch classOf(cfg, i) {
		case 1:
			exp = now + 30*60*1000
			if nNear%100 < cfg.expPct {
				exp = now - 1000
				b.expired = append(b.expired, k)
			}
			nNear++
			b.perCls[1]++
		case 2:
			exp = now + 24*60*60*1000
			b.perCls[2]++
		case 3:
			exp = now + 30*24*60*60*1000
			b.perCls[3]++
		default:
			b.perCls[0]++
		}
		ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: []byte(k), Value: val, ExpireMs: exp}})
		if len(ops) == 256 {
			b.seq++
			if err := s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: b.seq, Ops: ops}); err != nil {
				return nil, err
			}
			ops = ops[:0]
		}
	}
	if len(ops) > 0 {
		b.seq++
		if err := s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: b.seq, Ops: ops}); err != nil {
			return nil, err
		}
	}
	return b, nil
}

type lapResult struct {
	passes  int
	passNs  []int64
	entries int64
	probes  int64
	expired int64
	totalNs int64
}

// lap runs SampleExpiry passes at the budget until the cumulative
// entry tally covers the keyspace once. The cursor persists across
// laps, which does not matter: from any start, one lap visits every
// bucket exactly once before the tally reaches the key count.
func lap(s *sqlo1b.Store, budget, keys int) (lapResult, error) {
	var r lapResult
	cap := 64*keys/max(budget, 1) + 4096
	for r.entries < int64(keys) {
		if r.passes >= cap {
			return r, fmt.Errorf("lap did not converge: %d entries after %d passes", r.entries, r.passes)
		}
		t0 := time.Now()
		sm, err := s.SampleExpiry(budget)
		if err != nil {
			return r, err
		}
		dt := time.Since(t0).Nanoseconds()
		r.passes++
		r.passNs = append(r.passNs, dt)
		r.totalNs += dt
		for c := range sm {
			r.entries += sm[c].Entries
			r.probes += sm[c].Probed
			r.expired += sm[c].Expired
		}
	}
	return r, nil
}

func pct(ns []int64, p float64) int64 {
	if len(ns) == 0 {
		return 0
	}
	s := slices.Clone(ns)
	slices.Sort(s)
	i := int(p * float64(len(s)-1))
	return s[i]
}

func row(arm string, param int, cfg config, r lapResult, expTrue int) {
	fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f,%.0f,%d,%d,%d,%.1f\n",
		arm, param, cfg.keys, cfg.nearPct, cfg.midPct, cfg.farPct, cfg.expPct,
		r.passes,
		float64(pct(r.passNs, 0.50))/1e3, float64(pct(r.passNs, 0.99))/1e3, float64(pct(r.passNs, 1.0))/1e3,
		float64(r.entries)/float64(max(r.passes, 1)),
		r.probes, r.expired, expTrue,
		float64(r.totalNs)/1e6)
}

func parseInts(s string) []int {
	var out []int
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			fmt.Fprintf(os.Stderr, "reaper: bad sweep value %q: %v\n", f, err)
			os.Exit(1)
		}
		out = append(out, n)
	}
	return out
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.dir, "dir", os.TempDir(), "working directory for the store file")
	flag.IntVar(&cfg.keys, "keys", 200_000, "keyspace size")
	flag.IntVar(&cfg.val, "val", 64, "value bytes")
	flag.IntVar(&cfg.nearPct, "near", 25, "percent of keys with near TTLs")
	flag.IntVar(&cfg.midPct, "mid", 25, "percent of keys with mid TTLs")
	flag.IntVar(&cfg.farPct, "far", 25, "percent of keys with far TTLs")
	flag.IntVar(&cfg.expPct, "expired", 50, "percent of near keys already expired")
	budgets := flag.String("budgets", "1,2,4,8,16,32,64", "chunk budgets to sweep")
	reaps := flag.String("reaps", "8,64,256", "tombstone batch sizes to sweep")
	tick := flag.Duration("tick", 10*time.Millisecond, "tick period for the derived duty lines")
	flag.Parse()

	b, err := build(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reaper: build: %v\n", err)
		os.Exit(1)
	}
	expTrue := len(b.expired)

	for _, budget := range parseInts(*budgets) {
		r, err := lap(b.s, budget, cfg.keys)
		if err != nil {
			fmt.Fprintf(os.Stderr, "reaper: warm lap %d: %v\n", budget, err)
			os.Exit(1)
		}
		row("warm", budget, cfg, r, expTrue)
	}

	if err := b.s.Checkpoint(); err != nil {
		fmt.Fprintf(os.Stderr, "reaper: checkpoint: %v\n", err)
		os.Exit(1)
	}
	if err := b.s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "reaper: close: %v\n", err)
		os.Exit(1)
	}
	s, err := sqlo1b.OpenStore(b.path, 64<<20)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reaper: reopen: %v\n", err)
		os.Exit(1)
	}
	b.s = s

	for _, budget := range parseInts(*budgets) {
		r, err := lap(s, budget, cfg.keys)
		if err != nil {
			fmt.Fprintf(os.Stderr, "reaper: cold lap %d: %v\n", budget, err)
			os.Exit(1)
		}
		row("cold", budget, cfg, r, expTrue)
		mean := float64(r.totalNs) / float64(max(r.passes, 1))
		duty := mean / float64(tick.Nanoseconds()) * 100
		lapAt := time.Duration(r.passes) * *tick
		fmt.Fprintf(os.Stderr, "duty: budget %d cold mean %.0fus -> %.2f%% of a core at tick %v, lap %v for %d keys\n",
			budget, mean/1e3, duty, *tick, lapAt, cfg.keys)
	}

	// The reap arm: the tombstone frames slice 3 emits for dead keys,
	// in the batch sizes on the table.
	ctx := context.Background()
	remaining := b.expired
	for _, rb := range parseInts(*reaps) {
		n := min(len(remaining)/len(parseInts(*reaps))+1, len(remaining))
		batchKeys := remaining[:n]
		remaining = remaining[n:]
		var r lapResult
		for len(batchKeys) > 0 {
			k := min(rb, len(batchKeys))
			ops := make([]sqlo1.Op, k)
			for i := range k {
				ops[i] = sqlo1.Op{Del: true, Rec: sqlo1.Record{Key: []byte(batchKeys[i])}}
			}
			batchKeys = batchKeys[k:]
			b.seq++
			t0 := time.Now()
			if err := s.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: b.seq, Ops: ops}); err != nil {
				fmt.Fprintf(os.Stderr, "reaper: reap batch: %v\n", err)
				os.Exit(1)
			}
			dt := time.Since(t0).Nanoseconds()
			r.passes++
			r.passNs = append(r.passNs, dt)
			r.totalNs += dt
			r.entries += int64(k)
		}
		row("reap", rb, cfg, r, expTrue)
	}

	if err := s.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "reaper: close: %v\n", err)
		os.Exit(1)
	}
}
