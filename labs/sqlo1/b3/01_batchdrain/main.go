// Lab: drain window and batch cap over the B-format store (spec
// 2064/sqlo1 doc 04 section 7, milestone B3 lab 01).
//
// The shared queue's two constants, the dirty-bytes threshold that asks
// for a drain cycle (8 MiB) and the per-cycle op cap (1024), were priced
// against sqlo1a at A2. They do not carry silently: a Track B cycle is a
// WAL append run plus a RAM apply into group buffers and index chunks,
// not a SQL transaction, and the store lock point reads contend on is
// held for the apply, not for an fsynced commit. This lab re-prices
// window and cap against sqlo1b, with the checkpoint cadence running
// inline so its stalls land in the same rows they would land in
// production.
//
// The drainer is package-internal to engine/sqlo1 on purpose, so the
// harness mirrors its policy rather than importing it: a coalescing
// first-dirtied-first queue, one entry per dirty key however often it is
// rewritten, drained oldest-first up to the cap when dirty bytes cross
// the threshold, one DrainBatch per cycle with the sequence advancing.
// The writer loop runs its own drain and checkpoint cycles, matching the
// owner-loop shape where both are stolen from write time.
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

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

type config struct {
	dir       string
	keys      int
	val       int
	threshold int
	maxOps    int
	readers   int
	dist      string
	dur       time.Duration
	walSeg    int64
	ckptBytes int64
}

// queue is the drain.go policy restated: dirty keys in first-dirtied
// order, coalesced, with byte accounting.
type queue struct {
	fifo       []int
	dirty      []bool
	dirtiedAt  []time.Time
	dirtyBytes int
}

func newQueue(keys int) *queue {
	return &queue{dirty: make([]bool, keys), dirtiedAt: make([]time.Time, keys)}
}

func (q *queue) write(k, recBytes int) {
	if q.dirty[k] {
		return
	}
	q.dirty[k] = true
	q.dirtiedAt[k] = time.Now()
	q.fifo = append(q.fifo, k)
	q.dirtyBytes += recBytes
}

func (q *queue) pop(maxOps int) []int {
	n := min(maxOps, len(q.fifo))
	out := q.fifo[:n]
	q.fifo = q.fifo[n:]
	return out
}

func main() {
	var cfg config
	var thresholdMiB, ckptMiB float64
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.dir, "dir", "", "working directory (default: a temp dir)")
	flag.IntVar(&cfg.keys, "keys", 500000, "keyspace size")
	flag.IntVar(&cfg.val, "val", 128, "value size in bytes")
	flag.Float64Var(&thresholdMiB, "threshold", 8, "drain trigger in MiB of dirty bytes")
	flag.IntVar(&cfg.maxOps, "maxops", 1024, "per-cycle op cap")
	flag.IntVar(&cfg.readers, "readers", 4, "read pool size")
	flag.StringVar(&cfg.dist, "dist", "uniform", "write key distribution: uniform or zipf")
	flag.DurationVar(&cfg.dur, "dur", 8*time.Second, "measured phase duration")
	flag.Int64Var(&cfg.walSeg, "walseg", 64<<20, "WAL segment size in bytes")
	flag.Float64Var(&ckptMiB, "ckpt", 256, "checkpoint trigger in MiB of WAL growth")
	flag.Parse()
	if *quick {
		cfg.keys, cfg.dur = 20000, 300*time.Millisecond
		thresholdMiB, ckptMiB = 0.25, 4
		cfg.walSeg = 1 << 20
	}
	cfg.threshold = int(thresholdMiB * (1 << 20))
	cfg.ckptBytes = int64(ckptMiB * (1 << 20))
	if err := runAll(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "batchdrain:", err)
		os.Exit(1)
	}
}

type row struct {
	workload string
	ops      int
	dur      time.Duration
	p50, p99 time.Duration
	maxLat   time.Duration
	mbA, mbB float64
	vmhwmMB  float64
}

func runAll(cfg config, out io.Writer) error {
	ctx := context.Background()
	if cfg.dir == "" {
		dir, err := os.MkdirTemp("", "batchdrain")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		cfg.dir = dir
	}
	path := filepath.Join(cfg.dir, "batchdrain.aki")
	walpath := sqlo1.WALPath(path)
	os.Remove(path)
	os.Remove(walpath)

	keys := makeKeys(cfg.keys)
	val := makeVal(cfg.val)
	db, err := sqlo1b.CreateStore(path, cfg.walSeg)
	if err != nil {
		return err
	}
	defer db.Close()

	// Preload the whole keyspace so reader misses are store reads, not
	// not-founds, then checkpoint so the measured phase starts from a
	// cold-indexed base. Neither is measured.
	var seq int64
	ops := make([]sqlo1.Op, 0, 1024)
	for off := 0; off < cfg.keys; off += 1024 {
		n := min(1024, cfg.keys-off)
		ops = ops[:0]
		for i := range n {
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: keys[off+i], Value: val}})
		}
		seq++
		if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: ops}); err != nil {
			return err
		}
	}
	if err := db.Checkpoint(); err != nil {
		return err
	}

	// Read pool: uniform point gets against the store for the whole
	// measured phase, sampling latency.
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

	// Writer loop with inline drain and checkpoint cycles: the
	// owner-loop shape. The checkpoint cadence is the shipped policy
	// with the interval rung left at its default, which never fires in
	// a lab-length run, so the bytes rung is what gets priced.
	q := newQueue(cfg.keys)
	recBytes := len(keys[0]) + cfg.val
	var pick func() int
	switch cfg.dist {
	case "uniform":
		pick = uniformPicker(cfg.keys, 7)
	case "zipf":
		pick = zipfPicker(cfg.keys, 7)
	default:
		stop.Store(true)
		wg.Wait()
		return fmt.Errorf("unknown -dist %q", cfg.dist)
	}

	var writes, drained, batches int
	var lagSamples, ckptSamples []time.Duration
	var dirtyPeak, walPeak, walTriggerPeak float64
	drainCycle := func() error {
		batch := q.pop(cfg.maxOps)
		if len(batch) == 0 {
			return nil
		}
		lagSamples = append(lagSamples, time.Since(q.dirtiedAt[batch[0]]))
		ops = ops[:0]
		for _, k := range batch {
			q.dirty[k] = false
			ops = append(ops, sqlo1.Op{Rec: sqlo1.Record{Key: keys[k], Value: val}})
		}
		q.dirtyBytes -= len(batch) * recBytes
		seq++
		if err := db.ApplyBatch(ctx, &sqlo1.DrainBatch{Seq: seq, Ops: ops}); err != nil {
			return err
		}
		drained += len(batch)
		batches++
		if wm := fileMB(walpath); wm > walPeak {
			walPeak = wm
		}
		return nil
	}
	policy := sqlo1b.DefaultCheckpointPolicy()
	policy.Bytes = cfg.ckptBytes
	walBase := fileMB(walpath)
	lastCkpt := time.Now()
	ckptCycle := func() error {
		wm := fileMB(walpath)
		if !policy.Due(int64((wm-walBase)*(1<<20)), time.Since(lastCkpt)) {
			return nil
		}
		if wm > walTriggerPeak {
			walTriggerPeak = wm
		}
		t0 := time.Now()
		if err := db.Checkpoint(); err != nil {
			return err
		}
		ckptSamples = append(ckptSamples, time.Since(t0))
		walBase = fileMB(walpath)
		lastCkpt = time.Now()
		return nil
	}

	start := time.Now()
	deadline := start.Add(cfg.dur)
	for time.Now().Before(deadline) {
		for range 64 {
			q.write(pick(), recBytes)
			writes++
		}
		if float64(q.dirtyBytes)/(1<<20) > dirtyPeak {
			dirtyPeak = float64(q.dirtyBytes) / (1 << 20)
		}
		for q.dirtyBytes >= cfg.threshold {
			if err := drainCycle(); err != nil {
				stop.Store(true)
				wg.Wait()
				return err
			}
		}
		if err := ckptCycle(); err != nil {
			stop.Store(true)
			wg.Wait()
			return err
		}
	}
	measured := time.Since(start)
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	// Flush the tail so every accepted write is durable before the run
	// reports; flush time counts against nothing.
	for len(q.fifo) > 0 {
		if err := drainCycle(); err != nil {
			return err
		}
	}

	emit(cfg, out, row{workload: "write", ops: writes, dur: measured, mbA: dirtyPeak})
	fill := 0.0
	if batches > 0 {
		fill = float64(drained) / float64(batches)
	}
	emit(cfg, out, row{workload: "drain", ops: drained, dur: measured, mbA: fill, mbB: walPeak})
	p50, p99, maxLag := percentiles(lagSamples)
	emit(cfg, out, row{workload: "lag", ops: batches, dur: measured, p50: p50, p99: p99, maxLat: maxLag})
	p50, p99, maxCkpt := percentiles(ckptSamples)
	emit(cfg, out, row{workload: "ckpt", ops: len(ckptSamples), dur: measured,
		p50: p50, p99: p99, maxLat: maxCkpt, mbA: walTriggerPeak, mbB: fileMB(path)})
	reads := 0
	var all []time.Duration
	for i := range lats {
		reads += counts[i]
		all = append(all, lats[i]...)
	}
	p50, p99, maxLat := percentiles(all)
	emit(cfg, out, row{workload: fmt.Sprintf("pool-read-r%d", cfg.readers), ops: reads,
		dur: measured, p50: p50, p99: p99, maxLat: maxLat, vmhwmMB: vmhwmMB()})
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

func zipfPicker(n int, seed int64) func() int {
	rng := rand.New(rand.NewSource(seed))
	z := rand.NewZipf(rng, 1.1, 1, uint64(n-1))
	return func() int { return int(z.Uint64()) }
}

func fileMB(path string) float64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(fi.Size()) / (1 << 20)
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
	fmt.Fprintf(out, "%.2f,%d,%s,%d,%d,%s,%d,%.0f,%.0f,%d,%d,%d,%.1f,%.1f,%.1f\n",
		float64(cfg.threshold)/(1<<20), cfg.maxOps, cfg.dist, cfg.keys, cfg.val,
		r.workload, r.ops, nsPerOp, opsPerS,
		r.p50.Nanoseconds(), r.p99.Nanoseconds(), r.maxLat.Nanoseconds(),
		r.mbA, r.mbB, r.vmhwmMB)
}
