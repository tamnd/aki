// Lab: hot-set residency policy for the larger-than-memory regime (spec
// 2064/f3 doc 09 section 8, lab 13).
//
// The question: with the resident cap acting as a one-way valve (values spill
// to the log once the fill crosses the cap and never come back), a zipfian
// workload pays one synchronous log read per GET forever, because the resident
// set is the fill-order prefix, not the working set. The residency slice adds
// promotion on the read path and SIEVE-style clock-hand demotion at the owner
// boundaries. Which promotion policy earns its keep: promote on first touch,
// or the two-touch doorkeeper where the hand's pass over a log-resident record
// clears its mark (the ghost window)?
//
// Method: in-process, no server, no wire, one cell per invocation so maxrss is
// honest. The dataset is 2M keys of 1032B values, about 2.1GiB of value bytes,
// the same separated-band shape as the f3-ltm-strings bench scenario. The
// resident cap is swept at 512MiB (a quarter of the dataset) and 1GiB (half).
// The access distribution is swept between zipfian s=0.99 (the YCSB constant,
// scrambled so the hot ranks spread over the keyspace instead of landing on
// the resident fill prefix) and uniform (the adversary: no working set exists,
// so the right policy promotes almost nothing and the wrong one churns). The
// loop emulates the shard worker's boundaries: batches of 1024 ops, then the
// demote-or-compact check the run loop makes between drain passes.
//
// Read: GET ops/s, log reads per GET (the miss rate), promotions and
// demotions (the churn), arena fill against the cap, maxrss. See README.md
// for the numbers and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	keys     = 2 << 20 // 2M keys, power of two for the uniform mask
	valSize  = 1032    // separated band, the bench scenario's value size
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

// zipfian is the YCSB generator: rank 0 is the hottest key, s=0.99 the usual
// skew. Ranks are scrambled by a multiplicative hash before use so the hot
// set is spread across the keyspace; without the scramble the hot ranks are
// the first keys written, which are exactly the fill-order resident prefix,
// and the sweep would measure nothing.
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
	dist := flag.String("dist", "zipfian", "zipfian or uniform")
	zs := flag.Float64("zipf-s", 0.99, "zipfian skew")
	capMiB := flag.Int("cap", 512, "resident cap in MiB")
	mode := flag.String("mode", "two", "promotion policy: two, first, off")
	dkden := flag.Uint64("dkden", 0, "doorkeeper sampling denominator; 0 keeps the shipped default")
	flag.Parse()

	dir, err := os.MkdirTemp("", "lab13-")
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
	switch *mode {
	case "two":
		s.TuneResidency(store.ResidTwoTouch)
	case "first":
		s.TuneResidency(store.ResidFirstTouch)
	case "off":
		s.TuneResidency(store.ResidOff)
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", *mode)
		os.Exit(2)
	}
	if *dkden > 0 {
		s.TuneDoorkeeper(*dkden)
	}

	// The worker's between-drains hook, verbatim.
	boundary := func() {
		if s.MaybeDemote() > 0 || s.ArenaTight() || s.ResidentOver() {
			s.CompactArena()
		}
	}

	// Fill: 2M keys, boundary checks at the batch cadence like a loading
	// client would drive them.
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
			// Scramble the rank so heat is spread over the keyspace.
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

	reads := r.LogReads - base.LogReads
	used, _ := s.ArenaBytes()
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	fmt.Printf("dist=%s s=%.2f cap=%dMiB mode=%s dkden=%d: %.2fM gets/s (%d gets in %v)\n",
		*dist, *zs, *capMiB, *mode, *dkden,
		float64(measOps)/el.Seconds()/1e6, measOps, el.Round(time.Millisecond))
	fmt.Printf("  log reads/get=%.4f hit=%.2f%% promotes=%d demotes=%d\n",
		float64(reads)/float64(measOps), 100*(1-float64(reads)/float64(measOps)),
		r.Promotes-base.Promotes, r.Demotes-base.Demotes)
	fmt.Printf("  arena fill=%.0fMiB cap=%dMiB maxrss=%.0fMiB\n",
		float64(used)/(1<<20), *capMiB, float64(ru.Maxrss)/(1<<20))
}
