// Lab: die-in-RAM fractions under the baseline drainer (spec
// 2064/sqlo1 doc 11 sections 6 and 8, milestone T7 lab 02).
//
// Slice 4 wants two levers: reap-cancel (a dirty record that expired
// while queued drains as nothing instead of a put) and drain
// reordering (volatile-near records drain last in the window, buying
// them time to die). This lab prices both against the baseline FIFO
// drainer before either exists: it runs a short-TTL write burst
// through the real Tiered-over-sqlo1b stack and meters every volatile
// record at the ApplyBatch door.
//
// The clock is simulated and advanced by the writer, so the drain
// interval is set by construction: the drainer fires on dirty bytes
// (8 MiB threshold), and the write rate converts that to simulated
// time. A record written now drains roughly one threshold's worth of
// newer bytes later, FIFO, so the interesting knob is the ratio of
// TTL to that interval, swept by run.sh.
//
// Per volatile record, exactly one of four fates:
//   - drained dead: already expired when its put reached the store.
//     Pure waste today; exactly what reap-cancel deletes.
//   - drained alive with slack (deadline minus drain time): the
//     reordering headroom. A record with slack under one interval
//     dies if ordering defers it one window; under two intervals,
//     two windows. Larger slack is unsavable and rightly so, the
//     key genuinely outlives its RAM stay.
//   - died in RAM: never drained and past deadline at run end, the
//     free win the baseline already gets from write coalescing.
//   - pending: never drained, still alive at run end (truncation
//     residue; the sweep runs long so this stays small).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// drainThreshold mirrors the engine's unexported dirty high-water
// mark (engine/sqlo1/drain.go); the interval math depends on it.
const drainThreshold = 8 << 20

const walSeg = 64 << 20

type config struct {
	dir       string
	arm       string
	val       int
	mbps      int // simulated dirty influx, MB per simulated second
	intervals int // run length in drain intervals
	ttlMs     int64
	uniform   bool // TTL uniform in (0, ttlMs] instead of fixed
	volPct    int  // 100 or 50: every other key plain
}

type row struct {
	written      int
	drainedAlive int
	drainedDead  int
	diedInRAM    int
	pending      int
	slackLe1     int
	slackLe2     int
	lagP50Ms     int64
}

// meter forwards to the real store and books every volatile put at
// the door, with the simulated time the batch applied. It hides the
// optional capabilities on purpose: the lab wants pure dirty-pressure
// drains, no checkpoint or compaction rungs firing mid-burst.
type meter struct {
	st    sqlo1.Store
	nowMs func() int64
	onPut func(key []byte, expMs, nowMs int64)
}

func (m *meter) Get(ctx context.Context, key []byte) (sqlo1.Record, error) {
	return m.st.Get(ctx, key)
}

func (m *meter) BatchGet(ctx context.Context, keys [][]byte) ([]sqlo1.Record, error) {
	return m.st.BatchGet(ctx, keys)
}

func (m *meter) ApplyBatch(ctx context.Context, b *sqlo1.DrainBatch) error {
	now := m.nowMs()
	for i := range b.Ops {
		op := &b.Ops[i]
		if op.Del || op.Rec.ExpireMs == 0 {
			continue
		}
		m.onPut(op.Rec.Key, op.Rec.ExpireMs, now)
	}
	return m.st.ApplyBatch(ctx, b)
}

func (m *meter) Scan(ctx context.Context, cur sqlo1.Cursor, fn func(sqlo1.Record) bool) (sqlo1.Cursor, error) {
	return m.st.Scan(ctx, cur, fn)
}

func (m *meter) Stats() sqlo1.StoreStats {
	return m.st.Stats()
}

func keyIndex(key []byte) int {
	n := 0
	for _, c := range key[1:] {
		n = n*10 + int(c-'0')
	}
	return n
}

func runOne(cfg config) (row, error) {
	ctx := context.Background()
	path := filepath.Join(cfg.dir, "dieinram.aki")
	db, err := sqlo1b.CreateStore(path, walSeg)
	if err != nil {
		return row{}, err
	}
	defer db.Close()

	// Simulated clock in microseconds: a 1 KiB record at 8 MB/s is a
	// 128us advance, which milliseconds would truncate to junk.
	base := int64(1) << 41
	usNow := base * 1000
	nowMs := func() int64 { return usNow / 1000 }

	nOps := cfg.intervals * drainThreshold / cfg.val
	deadlines := make([]int64, nOps)
	writeMs := make([]int64, nOps)
	drainMs := make([]int64, nOps)

	mt := &meter{st: db, nowMs: nowMs, onPut: func(key []byte, expMs, now int64) {
		i := keyIndex(key)
		if drainMs[i] == 0 {
			drainMs[i] = now
		}
	}}
	tr := sqlo1.NewTiered(mt, sqlo1.TieredConfig{
		// Room for the full dirty threshold plus resident slack, so
		// the drain trigger is the byte threshold, not tier pressure.
		Budget: sqlo1.Budget{Entries: 64 << 10, Arenas: 128 << 20},
		Seed:   7,
		NowMs:  nowMs,
	})

	rng := rand.New(rand.NewSource(42))
	val := make([]byte, cfg.val)
	advUs := int64(cfg.val) * 1_000_000 / int64(cfg.mbps<<20)
	lastTick := usNow
	for i := range nOps {
		key := fmt.Appendf(nil, "k%09d", i)
		if err := tr.Set(ctx, key, val, sqlo1.TagString); err != nil {
			return row{}, fmt.Errorf("set %d: %w", i, err)
		}
		if cfg.volPct == 100 || i%2 == 0 {
			ttl := cfg.ttlMs
			if cfg.uniform {
				ttl = 1 + rng.Int63n(cfg.ttlMs)
			}
			deadlines[i] = nowMs() + ttl
			writeMs[i] = nowMs()
			if _, err := tr.ExpireAt(ctx, key, deadlines[i]); err != nil {
				return row{}, fmt.Errorf("expire %d: %w", i, err)
			}
		}
		usNow += advUs
		// The server's maintenance ticker, scaled to simulated time.
		if usNow-lastTick >= 1_000_000 {
			lastTick = usNow
			if err := tr.Tick(ctx); err != nil {
				return row{}, fmt.Errorf("tick: %w", err)
			}
		}
	}

	intervalMs := int64(drainThreshold) / int64(cfg.mbps<<20) * 1000
	endMs := nowMs()
	var r row
	var lags []int64
	for i, dl := range deadlines {
		if dl == 0 {
			continue
		}
		r.written++
		dt := drainMs[i]
		if dt == 0 {
			if dl <= endMs {
				r.diedInRAM++
			} else {
				r.pending++
			}
			continue
		}
		lags = append(lags, dt-writeMs[i])
		if dl <= dt {
			r.drainedDead++
			continue
		}
		r.drainedAlive++
		if dl-dt <= intervalMs {
			r.slackLe1++
		}
		if dl-dt <= 2*intervalMs {
			r.slackLe2++
		}
	}
	if len(lags) > 0 {
		// Insertion-select the median without sorting the whole set.
		lo, hi := lags[0], lags[0]
		for _, l := range lags {
			if l < lo {
				lo = l
			}
			if l > hi {
				hi = l
			}
		}
		for target := len(lags) / 2; lo < hi; {
			mid := (lo + hi) / 2
			n := 0
			for _, l := range lags {
				if l <= mid {
					n++
				}
			}
			if n > target {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		r.lagP50Ms = lo
	}
	return r, nil
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.dir, "dir", os.TempDir(), "work directory")
	flag.StringVar(&cfg.arm, "arm", "fixed", "row label")
	flag.IntVar(&cfg.val, "val", 1024, "value bytes")
	flag.IntVar(&cfg.mbps, "mbps", 8, "simulated dirty influx, MB per simulated second")
	flag.IntVar(&cfg.intervals, "intervals", 30, "run length in drain intervals")
	ttl := flag.Int64("ttl", 1000, "TTL in simulated ms (fixed, or the uniform upper bound)")
	flag.BoolVar(&cfg.uniform, "uniform", false, "TTL uniform in (0, ttl] instead of fixed")
	flag.IntVar(&cfg.volPct, "vol", 100, "percent of keys written volatile (100 or 50)")
	flag.Parse()
	cfg.ttlMs = *ttl

	start := time.Now()
	r, err := runOne(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dieinram: %v\n", err)
		os.Exit(1)
	}
	intervalMs := int64(drainThreshold) / int64(cfg.mbps<<20) * 1000
	pct := func(n int) float64 { return 100 * float64(n) / float64(r.written) }
	fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%d\n",
		cfg.arm, cfg.ttlMs, cfg.volPct, intervalMs, r.written,
		r.drainedAlive, r.drainedDead, r.diedInRAM, r.pending,
		r.slackLe1, r.slackLe2, r.lagP50Ms,
		pct(r.drainedDead), pct(r.diedInRAM),
		pct(r.drainedDead+r.diedInRAM),
		pct(r.drainedDead+r.diedInRAM+r.slackLe1),
		pct(r.drainedDead+r.diedInRAM+r.slackLe2),
		int(time.Since(start).Seconds()))
}
