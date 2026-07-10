// Lab: the LTM spill write path (spec 2064/f3 doc 09 section 8, lab 17).
//
// The question: run 3 measured SET uniform on a full store (2M x 1032B, cap
// 512MiB) at 0.18x the best rival with p99 14ms, while the GET cells on the
// same setup win. Where does an over-cap SET's time actually go, and which
// lever fixes it: writing overwritten cold values straight to the log instead
// of arena-then-demote, batching the log appends, or both?
//
// Method: in-process, no server, no wire, one cell per invocation so maxrss is
// honest. The dataset and cap are the run 3 LTM shape: 2M keys of 1032B
// values (separated band), resident cap 512MiB, a quarter of the value bytes.
// The loop emulates the shard worker's boundaries exactly as lab 13 did:
// batches of 1024 SETs, then the between-drains demote-or-compact check.
// The sweep axes are the placement policy for an overwrite of a log-resident
// value (arena: admit then demote later, the pre-slice behavior; log: append
// the new bytes straight to the log) and the vlog append flush threshold
// (1 flushes every append, the pre-slice synchronous posture; larger values
// coalesce appends into one pwrite per threshold).
//
// Read: sets/s, per-batch p99 and max (the boundary pauses land here), log
// bytes appended per SET (the write amplification), demotions, arena fill,
// maxrss. See README.md for the numbers and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	keys     = 2 << 20 // 2M keys, power of two for the uniform mask
	valSize  = 1032    // separated band, the bench scenario's value size
	batchOps = 1024    // one emulated worker drain pass
	warmOps  = 1 << 20 // sets before the measured window opens
	measOps  = 4 << 20 // sets in the measured window
)

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

// zipfian is the YCSB generator, same shape as lab 13's.
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

func main() {
	dist := flag.String("dist", "uniform", "uniform or zipfian")
	zs := flag.Float64("zipf-s", 0.99, "zipfian skew")
	capMiB := flag.Int("cap", 512, "resident cap in MiB")
	place := flag.String("place", "log", "overwrite placement for a log-resident value: arena or log")
	flush := flag.Int("flush", 0, "vlog flush threshold in bytes; 1 is per-append, 0 keeps the shipped default")
	cpuprofile := flag.String("cpuprofile", "", "write a cpu profile of the measured window")
	flag.Parse()

	dir, err := os.MkdirTemp("", "lab17-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	s, err := store.Open(store.Options{
		ArenaBytes:       4 << 30,
		VlogPath:         filepath.Join(dir, "vlog"),
		ResidentCapBytes: uint64(*capMiB) << 20,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = s.Close() }()
	switch *place {
	case "arena":
		s.TuneSpillPlacement(false)
	case "log":
		s.TuneSpillPlacement(true)
	default:
		fmt.Fprintln(os.Stderr, "unknown place", *place)
		os.Exit(2)
	}
	if *flush > 0 {
		s.TuneVlogFlush(*flush)
	}

	// The worker's between-drains hook, verbatim.
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

	// run drives n SETs in worker-shaped batches and returns the per-batch
	// wall times (each batch is 1024 SETs plus the boundary check), the
	// figure the p99 column reads.
	run := func(n int) []time.Duration {
		lats := make([]time.Duration, 0, n/batchOps+1)
		for done := 0; done < n; {
			t0 := time.Now()
			for i := 0; i < batchOps; i++ {
				if err := s.SetString(makeKey(kb[:], pick()), val, 0, 0, false); err != nil {
					fmt.Fprintf(os.Stderr, "set: %v\n", err)
					os.Exit(1)
				}
			}
			boundary()
			lats = append(lats, time.Since(t0))
			done += batchOps
		}
		return lats
	}

	run(warmOps)
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	tail0, dead0 := s.LogBytes()
	base := s.Resid()
	start := time.Now()
	lats := run(measOps)
	el := time.Since(start)
	r := s.Resid()
	tail1, dead1 := s.LogBytes()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p99 := lats[len(lats)*99/100]
	max := lats[len(lats)-1]

	used, _ := s.ArenaBytes()
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	fmt.Printf("dist=%s cap=%dMiB place=%s flush=%d: %.2fM sets/s (%d sets in %v)\n",
		*dist, *capMiB, *place, *flush,
		float64(measOps)/el.Seconds()/1e6, measOps, el.Round(time.Millisecond))
	fmt.Printf("  batch(1024) p99=%v max=%v\n", p99.Round(time.Microsecond), max.Round(time.Microsecond))
	fmt.Printf("  log appended/set=%.0fB dead growth/set=%.0fB demotes=%d promotes=%d\n",
		float64(tail1-tail0)/float64(measOps), float64(dead1-dead0)/float64(measOps),
		r.Demotes-base.Demotes, r.Promotes-base.Promotes)
	fmt.Printf("  arena fill=%.0fMiB cap=%dMiB maxrss=%.0fMiB\n",
		float64(used)/(1<<20), *capMiB, float64(ru.Maxrss)/(1<<20))
}
