// Lab: the visited-bit mark's price on the resident GET path (spec 2064/f3
// doc 09 section 8, lab 15).
//
// The question, from the m0-run3 report: run 3 measured aki's redis-benchmark
// GET rows 8-12% under the 514d57b re-run with rivals bit-identical, and the
// suspect on file was the residency slice's flagVisited mark turning a
// read-only GET into a header-byte store, a dirtied cache line per hit at a
// 1M-key footprint. This lab prices that mark directly: the same resident GET
// loop with the mark compiled to nothing (residency off, the 514d57b path),
// with the shipped check-then-set, and with the feared store-every-hit
// variant (TuneMarkAlways, the lab-only knob).
//
// Method: in-process, no server, no wire, one cell per invocation so maxrss
// is honest. 1M keys of 1032B separated values, about 1GiB, under a cap with
// headroom so everything stays resident: no spills, no promotion, no log
// traffic, the mark isolated from every other residency cost. The demotion
// hand still gets its boundary check at the worker cadence and declines every
// pass (live never crosses the low-water mark), which is exactly the headline
// GET cells' regime. Distribution swept uniform (the footprint adversary,
// every hit a fresh cache line) and zipfian s=0.99.
//
// Read: GET ops/s across the three marks. If the mark cost the run3 delta,
// always-store shows it and check-then-set recovers it; if the three tie, the
// mark was never the bill and the run3 delta lives elsewhere (the paired
// box A/B in the README settles where). See README.md for the numbers and
// the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	keys     = 1 << 20 // 1M keys, the headline cells' cardinality
	valSize  = 1032    // separated band, just past strInlineMax
	batchOps = 1024    // one emulated worker drain pass
	warmOps  = 2 << 20 // reads before the measured window opens
	measOps  = 6 << 20 // reads in the measured window
)

// xorshift is the shared PRNG: cheap, stateful, identical across cells.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func (x *xorshift) float() float64 {
	return float64(x.next()>>11) / (1 << 53)
}

// zipfian is the YCSB generator, the same code lab 13 runs; ranks are
// scrambled by a multiplicative hash so the hot set spreads over the
// keyspace.
type zipfian struct {
	n            uint64
	theta        float64
	alpha        float64
	zetan, zeta2 float64
	eta          float64
}

func newZipfian(n uint64, theta float64) *zipfian {
	z := &zipfian{n: n, theta: theta}
	for i := uint64(1); i <= n; i++ {
		z.zetan += 1 / math.Pow(float64(i), theta)
	}
	z.zeta2 = 1 + 1/math.Pow(2, theta)
	z.alpha = 1 / (1 - theta)
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - z.zeta2/z.zetan)
	return z
}

func (z *zipfian) next(r *xorshift) uint64 {
	u := r.float()
	uz := u * z.zetan
	if uz < 1 {
		return 0
	}
	if uz < z.zeta2 {
		return 1
	}
	rank := uint64(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	if rank >= z.n {
		rank = z.n - 1
	}
	return rank
}

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func rssMiB(ru *syscall.Rusage) float64 {
	if runtime.GOOS == "linux" {
		return float64(ru.Maxrss) / (1 << 10) // KiB
	}
	return float64(ru.Maxrss) / (1 << 20) // bytes on darwin
}

func main() {
	dist := flag.String("dist", "uniform", "uniform or zipfian")
	zs := flag.Float64("zipf-s", 0.99, "zipfian skew")
	mark := flag.String("mark", "check", "visited mark variant: off, check, always")
	flag.Parse()

	dir, err := os.MkdirTemp("", "lab15-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	s, err := store.Open(store.Options{
		ArenaBytes:       3 << 30,
		VlogPath:         filepath.Join(dir, "vlog"),
		ResidentCapBytes: 2 << 30, // headroom: the whole dataset stays resident
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = s.Close() }()
	switch *mark {
	case "off":
		s.TuneResidency(store.ResidOff) // the 514d57b read path: no mark at all
	case "check":
		// The shipped check-then-set.
	case "always":
		s.TuneMarkAlways(true)
	default:
		fmt.Fprintln(os.Stderr, "unknown mark", *mark)
		os.Exit(2)
	}

	// The worker's between-drains hook, verbatim; with the fill under the
	// low-water mark every MaybeDemote declines, like the headline cells.
	boundary := func() {
		if s.MaybeDemote() > 0 || s.ArenaTight() || s.ResidentOver() {
			s.CompactArena()
		}
	}

	val := make([]byte, valSize)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	var kb [16]byte
	for i := 0; i < keys; i++ {
		if err := s.SetString(makeKey(kb[:], uint64(i)), val, 0, 0, false); err != nil {
			fmt.Fprintf(os.Stderr, "fill: %v at key %d\n", err, i)
			os.Exit(1)
		}
		if i%batchOps == batchOps-1 {
			boundary()
		}
	}
	if s.Stats().LogRuns != 0 {
		fmt.Fprintln(os.Stderr, "fixture spilled; the cap no longer holds the dataset")
		os.Exit(1)
	}

	var z *zipfian
	if *dist == "zipfian" {
		z = newZipfian(keys, *zs)
	}
	rng := xorshift(0x9e3779b97f4a7c15)
	pick := func() uint64 {
		if z != nil {
			return (z.next(&rng) * 0x9e3779b97f4a7c15) & (keys - 1)
		}
		return rng.next() & (keys - 1)
	}

	run := func(n int) {
		var dst []byte
		for i := 0; i < n; i++ {
			v, ok := s.Get(makeKey(kb[:], pick()), dst)
			if !ok {
				fmt.Fprintln(os.Stderr, "lost key")
				os.Exit(1)
			}
			dst = v[:0]
			if i%batchOps == batchOps-1 {
				boundary()
			}
		}
	}

	run(warmOps)
	base := s.Resid()
	start := time.Now()
	run(measOps)
	el := time.Since(start)
	r := s.Resid()

	used, _ := s.ArenaBytes()
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	fmt.Printf("dist=%s s=%.2f mark=%s: %.2fM gets/s (%d gets in %v)\n",
		*dist, *zs, *mark,
		float64(measOps)/el.Seconds()/1e6, measOps, el.Round(time.Millisecond))
	fmt.Printf("  log reads=%d promotes=%d demotes=%d (all must be 0)\n",
		r.LogReads-base.LogReads, r.Promotes-base.Promotes, r.Demotes-base.Demotes)
	fmt.Printf("  arena fill=%.0fMiB maxrss=%.0fMiB\n",
		float64(used)/(1<<20), rssMiB(&ru))
}
