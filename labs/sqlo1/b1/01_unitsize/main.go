// unitsize prices the sqlo1b IO unit on the box that gates it.
//
// Doc 03 P2 bakes 4 KiB groups into the extent geometry on LeanStore's
// claim that 4 KiB has the lowest NVMe latency and random write
// amplification; the superblock stores io_unit as data, so this lab's
// verdict can amend the default without a version bump. The sweep
// times random unit reads and writes at 4/8/16 KiB across queue
// depths 1..32 against one big file opened for direct IO, which is
// the shape of a cold point lookup (one group read) and an in-place
// map page write.
//
// CSV columns:
//
//	op,unit_b,qd,secs,ops,iops,mb_per_s,p50_ns,p99_ns,max_ns,direct
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	dir    string
	fileMB int64
	secs   float64
	units  []int64
	qds    []int
	ops    []string
	seed   int64
	direct bool
}

func main() {
	var cfg config
	var units, qds, ops string
	flag.StringVar(&cfg.dir, "dir", ".", "directory holding the test file")
	flag.Int64Var(&cfg.fileMB, "filemb", 8192, "test file size in MiB")
	flag.Float64Var(&cfg.secs, "secs", 5, "seconds per cell")
	flag.StringVar(&units, "units", "4096,8192,16384", "unit sizes in bytes")
	flag.StringVar(&qds, "qds", "1,2,4,8,16,32", "queue depths")
	flag.StringVar(&ops, "ops", "randread,randwrite", "operations to sweep")
	flag.Int64Var(&cfg.seed, "seed", 4096, "rng seed")
	nodirect := flag.Bool("nodirect", false, "skip direct IO (troubleshooting only)")
	quick := flag.Bool("quick", false, "small smoke shape")
	flag.Parse()

	if *quick {
		cfg.fileMB, cfg.secs = 256, 1
		qds = "1,8"
	}
	cfg.direct = !*nodirect
	var err error
	if cfg.units, err = parseInts64(units); err != nil {
		fatal(err)
	}
	if cfg.qds, err = parseInts(qds); err != nil {
		fatal(err)
	}
	cfg.ops = strings.Split(ops, ",")
	if err := runAll(cfg, os.Stdout); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "unitsize:", err)
	os.Exit(1)
}

func parseInts64(s string) ([]int64, error) {
	var out []int64
	for p := range strings.SplitSeq(s, ",") {
		v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func parseInts(s string) ([]int, error) {
	v64, err := parseInts64(s)
	if err != nil {
		return nil, err
	}
	out := make([]int, len(v64))
	for i, v := range v64 {
		out[i] = int(v)
	}
	return out, nil
}

func runAll(cfg config, w io.Writer) error {
	for _, u := range cfg.units {
		if u <= 0 || u%4096 != 0 {
			return fmt.Errorf("unit %d not a positive 4096 multiple", u)
		}
	}
	path := cfg.dir + "/unitsize.dat"
	size := cfg.fileMB << 20
	if err := fillFile(path, size, cfg.seed); err != nil {
		return err
	}
	defer os.Remove(path)

	fmt.Fprintln(w, "op,unit_b,qd,secs,ops,iops,mb_per_s,p50_ns,p99_ns,max_ns,direct")
	for _, op := range cfg.ops {
		write := false
		switch op {
		case "randread":
		case "randwrite":
			write = true
		default:
			return fmt.Errorf("unknown op %q", op)
		}
		for _, unit := range cfg.units {
			for _, qd := range cfg.qds {
				f, direct, err := openIO(path, write, cfg.direct)
				if err != nil {
					return err
				}
				ops, lats, err := runCell(f, size, unit, qd, cfg.secs, write, cfg.seed)
				f.Close()
				if err != nil {
					return err
				}
				elapsed := cfg.secs
				p50, p99, mx := percentiles(lats)
				d := 0
				if direct {
					d = 1
				}
				fmt.Fprintf(w, "%s,%d,%d,%.1f,%d,%.0f,%.1f,%d,%d,%d,%d\n",
					op, unit, qd, elapsed, ops, float64(ops)/elapsed,
					float64(ops)*float64(unit)/elapsed/(1<<20), p50, p99, mx, d)
			}
		}
	}
	return nil
}

// fillFile writes size bytes of random data with plain buffered IO;
// the fill is not timed and random content keeps any device-side
// compression out of the numbers.
func fillFile(path string, size, seed int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, 1<<20)
	start := time.Now()
	for written := int64(0); written < size; written += int64(len(buf)) {
		rng.Read(buf)
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "fill: %d MiB in %.1fs\n", size>>20, time.Since(start).Seconds())
	return nil
}

// runCell drives qd workers against f for secs seconds, each issuing
// unit-sized preads or pwrites at random unit-aligned offsets, and
// returns the op count with all per-op latencies.
func runCell(f *os.File, size, unit int64, qd int, secs float64, write bool, seed int64) (int64, []int64, error) {
	slots := size / unit
	if slots == 0 {
		return 0, nil, fmt.Errorf("file smaller than one %d B unit", unit)
	}
	deadline := time.Now().Add(time.Duration(secs * float64(time.Second)))
	type res struct {
		lats []int64
		err  error
	}
	results := make([]res, qd)
	var wg sync.WaitGroup
	for wkr := range qd {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(seed + int64(wkr)*7919))
			buf := alignedBuf(int(unit), 4096)
			if write {
				rng.Read(buf)
			}
			lats := make([]int64, 0, 1<<16)
			for time.Now().Before(deadline) {
				off := rng.Int63n(slots) * unit
				t0 := time.Now()
				var err error
				if write {
					_, err = f.WriteAt(buf, off)
				} else {
					_, err = f.ReadAt(buf, off)
				}
				if err != nil {
					results[wkr].err = fmt.Errorf("worker %d off %d: %w", wkr, off, err)
					return
				}
				lats = append(lats, int64(time.Since(t0)))
			}
			results[wkr].lats = lats
		})
	}
	wg.Wait()
	var all []int64
	for _, r := range results {
		if r.err != nil {
			return 0, nil, r.err
		}
		all = append(all, r.lats...)
	}
	return int64(len(all)), all, nil
}

func percentiles(lats []int64) (p50, p99, max int64) {
	if len(lats) == 0 {
		return 0, 0, 0
	}
	slices.Sort(lats)
	return lats[len(lats)/2], lats[len(lats)*99/100], lats[len(lats)-1]
}
