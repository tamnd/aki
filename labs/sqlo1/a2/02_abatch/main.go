// Lab: ApplyBatch sizing and the A2 stack-tax predictions (spec
// 2064/sqlo1 doc 02, milestone A2 lab 02).
//
// Three questions, one run. First, PRED-SQLO1-A2-POINT: a cache-hot
// point Get through the full sqlo1a stack against the raw prepared-read
// floor, with a same-run floor arm on the store's own file so the ratio
// isolates the stack tax from the pragma posture of the day. Second,
// PRED-SQLO1-A2-DRAIN: drained throughput at the batch-size knee against
// the same upsert committed one row per transaction, same store, same
// run. Third, the knee itself: ApplyBatch rows-per-transaction swept 256
// to 32k, solo and against a read pool, because the store serializes on
// one connection and a bigger batch holds that lock longer, so drain
// throughput buys reader p99 and the knee is where the trade stops
// paying.
//
// The stack under test is engine/sqlo1a exactly as shipped; the lab sets
// no pragmas on it and reports what the stack costs today.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1a"
)

type config struct {
	dir     string
	keys    int
	val     int
	ops     int
	single  int
	batches []int
	readers int
	poolDur time.Duration
}

type row struct {
	workload string
	batch    int
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	vmhwmMB  float64
}

func main() {
	var cfg config
	var batches string
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.IntVar(&cfg.keys, "keys", 200000, "loaded key count (the reference cell)")
	flag.IntVar(&cfg.val, "val", 128, "value size in bytes")
	flag.IntVar(&cfg.ops, "ops", 200000, "point ops per read arm, rows per drain arm")
	flag.IntVar(&cfg.single, "single", 10000, "rows for the one-row-per-transaction arm")
	flag.StringVar(&batches, "batches", "256,1024,4096,8192,16384,32768", "swept rows per transaction")
	flag.IntVar(&cfg.readers, "readers", 4, "read pool size")
	flag.DurationVar(&cfg.poolDur, "pooldur", 5*time.Second, "read-pool arm duration per batch size")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.ops, cfg.single = 20000, 20000, 500
		batches = "256,1024"
		cfg.poolDur = 200 * time.Millisecond
	}
	for b := range strings.SplitSeq(batches, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(b))
		if err != nil {
			fmt.Fprintln(os.Stderr, "abatch: bad -batches:", err)
			os.Exit(1)
		}
		cfg.batches = append(cfg.batches, n)
	}
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "abatch:", err)
		os.Exit(1)
	}
}

func runAll(cfg config, out io.Writer) error {
	ctx := context.Background()
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "abatch")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, "abatch.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		os.Remove(path + suffix)
	}

	keys := makeKeys(cfg.keys)
	val := makeVal(cfg.val)
	var seq int64

	// Load the reference cell through the stack, then measure the two
	// point arms on a warmed file.
	db, err := sqlo1a.Open(path)
	if err != nil {
		return err
	}
	if err := drainRange(ctx, db, keys, val, 0, cfg.keys, 4096, &seq); err != nil {
		db.Close()
		return err
	}

	// point: PRED-SQLO1-A2-POINT, uniform gets through the full stack.
	upick := uniformPicker(cfg.keys, 17)
	for range cfg.ops / 4 {
		if _, err := db.Get(ctx, keys[upick()]); err != nil {
			db.Close()
			return err
		}
	}
	start := time.Now()
	for range cfg.ops {
		if _, err := db.Get(ctx, keys[upick()]); err != nil {
			db.Close()
			return err
		}
	}
	pointDur := time.Since(start)
	emit(cfg, out, row{workload: "point-get", ops: cfg.ops, dur: pointDur, vmhwmMB: vmhwmMB()})
	if err := db.Close(); err != nil {
		return err
	}

	// floor: the same file, the raw driver, the catalog's get statement,
	// no store code. The point/floor ratio is the stack tax under
	// whatever pragma posture the stack ships today.
	floorDur, err := floorArm(path, keys, cfg.ops, 17)
	if err != nil {
		return err
	}
	emit(cfg, out, row{workload: "floor-get", ops: cfg.ops, dur: floorDur})

	db, err = sqlo1a.Open(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// single: one row per ApplyBatch, so every row pays the whole commit
	// path. This is the PRED-SQLO1-A2-DRAIN denominator.
	start = time.Now()
	for i := range cfg.single {
		if err := drainRange(ctx, db, keys, val, i%cfg.keys, 1, 1, &seq); err != nil {
			return err
		}
	}
	singleDur := time.Since(start)
	emit(cfg, out, row{workload: "drain-single", batch: 1, ops: cfg.single, dur: singleDur})

	// sweep: each batch size drains solo, then again under the read
	// pool. The store has one connection behind one mutex, so batch size
	// is also reader stall size.
	var bestSolo float64
	for _, batch := range cfg.batches {
		start = time.Now()
		if err := drainRange(ctx, db, keys, val, 0, cfg.ops, batch, &seq); err != nil {
			return err
		}
		soloDur := time.Since(start)
		emit(cfg, out, row{workload: "drain-solo", batch: batch, ops: cfg.ops, dur: soloDur})
		if rate := float64(cfg.ops) / soloDur.Seconds(); rate > bestSolo {
			bestSolo = rate
		}

		if err := runPool(ctx, cfg, db, keys, val, batch, &seq, out); err != nil {
			return err
		}
	}

	singleRate := float64(cfg.single) / singleDur.Seconds()
	fmt.Fprintf(os.Stderr, "PRED-SQLO1-A2-POINT: point %.0f ns/op, same-run floor %.0f ns/op, ratio %.2fx\n",
		float64(pointDur.Nanoseconds())/float64(cfg.ops),
		float64(floorDur.Nanoseconds())/float64(cfg.ops),
		float64(pointDur)/float64(floorDur))
	fmt.Fprintf(os.Stderr, "PRED-SQLO1-A2-DRAIN: best solo %.0f rows/s, single-transaction %.0f rows/s, ratio %.1fx\n",
		bestSolo, singleRate, bestSolo/singleRate)
	return nil
}

// drainRange pushes count rows through ApplyBatch starting at key offset
// off (wrapping), batch rows per transaction, advancing *seq.
func drainRange(ctx context.Context, db *sqlo1a.DB, keys [][]byte, val []byte, off, count, batch int, seq *int64) error {
	ops := make([]sqlo1.Op, 0, batch)
	for count > 0 {
		n := min(batch, count)
		ops = ops[:0]
		for i := range n {
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{
				Key:   keys[(off+i)%len(keys)],
				Value: val,
			}})
		}
		*seq++
		if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: *seq, Ops: ops}); err != nil {
			return err
		}
		off, count = (off+n)%len(keys), count-n
	}
	return nil
}

// floorArm reads the store's own file with a raw ncruces connection and
// the catalog's get statement. It sets no pragmas beyond what the file
// carries, matching the store connection's posture.
func floorArm(path string, keys [][]byte, ops int, seed int64) (time.Duration, error) {
	conn, err := sqlite3.Open(path)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	stmt, _, err := conn.Prepare(`SELECT t, exp, gen, v, crc FROM kv WHERE k = ?1`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	get := func(key []byte) error {
		if err := stmt.BindBlob(1, key); err != nil {
			return err
		}
		if stmt.Step() {
			_ = stmt.ColumnRawBlob(3)
		}
		if err := stmt.Err(); err != nil {
			return err
		}
		return stmt.Reset()
	}
	pick := uniformPicker(len(keys), seed)
	for range ops / 4 {
		if err := get(keys[pick()]); err != nil {
			return 0, err
		}
	}
	start := time.Now()
	for range ops {
		if err := get(keys[pick()]); err != nil {
			return 0, err
		}
	}
	return time.Since(start), nil
}

func runPool(ctx context.Context, cfg config, db *sqlo1a.DB, keys [][]byte, val []byte, batch int, seq *int64, out io.Writer) error {
	var stop atomic.Bool
	errs := make(chan error, cfg.readers)
	lats := make([][]time.Duration, cfg.readers)
	counts := make([]int, cfg.readers)
	var wg sync.WaitGroup
	for i := range cfg.readers {
		wg.Add(1)
		go func(slot int, seed int64) {
			defer wg.Done()
			pick := uniformPicker(cfg.keys, seed)
			n := 0
			var samples []time.Duration
			for !stop.Load() {
				t0 := time.Now()
				if _, err := db.Get(ctx, keys[pick()]); err != nil {
					errs <- err
					return
				}
				if n%8 == 0 {
					samples = append(samples, time.Since(t0))
				}
				n++
			}
			lats[slot], counts[slot] = samples, n
		}(i, int64(i)*131+41)
	}

	writes := 0
	deadline := time.Now().Add(cfg.poolDur)
	for time.Now().Before(deadline) {
		if err := drainRange(ctx, db, keys, val, writes%cfg.keys, batch, batch, seq); err != nil {
			stop.Store(true)
			wg.Wait()
			return err
		}
		writes += batch
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}

	reads := 0
	var all []time.Duration
	for i := range lats {
		reads += counts[i]
		all = append(all, lats[i]...)
	}
	p50, p99, maxLat := percentiles(all)
	emit(cfg, out, row{workload: fmt.Sprintf("pool-read-r%d", cfg.readers), batch: batch,
		ops: reads, dur: cfg.poolDur, p50: p50, p99: p99, maxLat: maxLat, vmhwmMB: vmhwmMB()})
	emit(cfg, out, row{workload: fmt.Sprintf("pool-drain-r%d", cfg.readers), batch: batch,
		ops: writes, dur: cfg.poolDur})
	return nil
}

func percentiles(all []time.Duration) (p50, p99, max time.Duration) {
	if len(all) == 0 {
		return 0, 0, 0
	}
	slices.Sort(all)
	return all[len(all)/2], all[len(all)*99/100], all[len(all)-1]
}

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k:%013d", i)
	}
	return keys
}

func makeVal(n int) []byte {
	val := make([]byte, n)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	return val
}

func uniformPicker(n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	return func() int { return rng.Intn(n) }
}

func vmhwmMB() float64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			fields := strings.Fields(rest)
			if len(fields) >= 1 {
				kb, err := strconv.ParseFloat(fields[0], 64)
				if err == nil {
					return kb / 1024
				}
			}
		}
	}
	return 0
}

func emit(cfg config, out io.Writer, r row) {
	nsPerOp := float64(r.dur.Nanoseconds()) / float64(max(r.ops, 1))
	opsPerS := float64(r.ops) / max(r.dur.Seconds(), 1e-9)
	fmt.Fprintf(out, "%d,%d,%s,%d,%d,%.0f,%.0f,%d,%d,%d,%.1f\n",
		cfg.keys, cfg.val, r.workload, r.batch, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(), r.vmhwmMB)
}
