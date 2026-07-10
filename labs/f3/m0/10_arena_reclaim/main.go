// Lab: the arena reclaim threshold (spec 2064/f3, M0 gate follow-up, lab 10).
//
// The question: the arena never reclaimed a replaced or deleted record, so
// sustained SET at 4KiB walked it to ErrFull and killed five M0 gate cells
// (results/f3/m0-gate.md, issue #542). The reclaim slice adds in-place reuse
// on the separated band, per-segment dead accounting, and a dead-fraction
// compactor at the drain boundaries. What dead fraction should make a
// segment a victim: 1/8, 1/4, or 1/2?
//
// Method: in-process, no server, no wire, one cell per invocation so maxrss
// is honest. The gate workload, sustained 4KiB uniform overwrite over 1M
// keys, is pass one (-pass inplace): with in-place reuse landed it generates
// no dead bytes at all (a fresh run's reserved capacity is exactly its
// aligned length, so a same-size overwrite always fits), so it proves the
// steady state but cannot sweep a compaction threshold. Pass two (-pass
// churn -frac {off,1/8,1/4,1/2}) makes the compactor work: uniform-random
// key, value size a coin flip between 512B (embedded) and 4KiB (separated),
// so about half the writes change band and must republish the record,
// leaving dead bytes behind live neighbors in every segment. One key in
// eight is pinned, written at fill and never again, the long-lived residents
// every real cache holds: their records keep the fill segments from ever
// dying whole, so the fully-dead backstop alone cannot save this workload
// and relocation has to earn its keep. Both passes run to 3x arena turnover
// in written bytes.
//
// The loop emulates the shard worker's boundaries: batches of 1024 ops (the
// drainPassCap x batchCap drain-pass ceiling), the O(1) ArenaTight check
// between batches, and the idle trigger (ArenaReclaimable at the 1MiB floor)
// every 64 batches. The reported pause p99/max is the batch wall time,
// compaction included, the drain-to-drain gap a client would see.
//
// See README.md for the numbers and the verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	keys     = 1 << 20 // 1M
	batchOps = 1024    // drainPassCap * batchCap, one worker drain pass
	idleMod  = 64      // batches between emulated idle-boundary checks
	idleMin  = 1 << 20 // arenaCompactMinDead, the shard's idle floor
)

// xorshift is the key picker: cheap, stateful, uniform enough for a cache
// benchmark, and identical across cells.
type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

func main() {
	pass := flag.String("pass", "inplace", "inplace or churn")
	frac := flag.String("frac", "1/4", "victim dead fraction: off, 1/8, 1/4, 1/2")
	flag.Parse()

	switch *pass {
	case "inplace":
		run("inplace 4KiB overwrite", 4608<<20, *frac, false, func(r *xorshift) int { return 4096 })
	case "churn":
		run("churn 512B/4KiB pinned-eighth", 4096<<20, *frac, true, func(r *xorshift) int {
			if r.next()&1 == 0 {
				return 512
			}
			return 4096
		})
	default:
		fmt.Fprintln(os.Stderr, "unknown pass", *pass)
		os.Exit(2)
	}
}

func tune(s *store.Store, frac string) {
	switch frac {
	case "off":
		s.TuneArenaReclaim(1, 0) // dead*0 >= fill*1 never holds: no victims
	case "1/8":
		s.TuneArenaReclaim(1, 8)
	case "1/4":
		s.TuneArenaReclaim(1, 4)
	case "1/2":
		s.TuneArenaReclaim(1, 2)
	default:
		fmt.Fprintln(os.Stderr, "unknown frac", frac)
		os.Exit(2)
	}
}

func run(name string, arenaBytes int, frac string, pin bool, size func(*xorshift) int) {
	s := store.New(arenaBytes, 0)
	tune(s, frac)
	val := make([]byte, 4096)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	var kb [16]byte
	rng := xorshift(0x9e3779b97f4a7c15)
	for i := 0; i < keys; i++ {
		if err := s.SetString(makeKey(kb[:], uint64(i)), val[:size(&rng)], 0, 0, false); err != nil {
			fmt.Fprintf(os.Stderr, "fill: %v at key %d\n", err, i)
			os.Exit(1)
		}
	}
	_, total := s.ArenaBytes()
	target := 3 * total

	var (
		written   uint64
		ops       uint64
		compacts  int
		freed     int
		compactNs int64
		gaps      []time.Duration
	)
	compact := func() {
		t := time.Now()
		if n := s.CompactArena(); n > 0 {
			freed += n
			compacts++
		}
		compactNs += time.Since(t).Nanoseconds()
	}
	start := time.Now()
	for batch := 0; written < target; batch++ {
		t0 := time.Now()
		for i := 0; i < batchOps; i++ {
			k := rng.next() & (keys - 1)
			if pin && k&7 == 0 {
				k |= 1 // pinned keys are written at fill and never again
			}
			v := val[:size(&rng)]
			if err := s.SetString(makeKey(kb[:], k), v, 0, 0, false); err != nil {
				fmt.Printf("%s frac=%s: %v after %d ops, %.2fx turnover\n",
					name, frac, err, ops, float64(written)/float64(total))
				report(s, frac, name, ops, written, total, start, gaps, compacts, freed, compactNs)
				os.Exit(1)
			}
			written += uint64(len(v))
			ops++
		}
		if s.ArenaTight() {
			compact()
		}
		if batch%idleMod == idleMod-1 && s.ArenaReclaimable() >= idleMin {
			compact()
		}
		gaps = append(gaps, time.Since(t0))
	}
	// Sanity: every key still readable.
	var vb []byte
	for i := 0; i < 4096; i++ {
		k := rng.next() & (keys - 1)
		v, ok := s.Get(makeKey(kb[:], k), vb)
		if !ok || len(v) == 0 {
			fmt.Fprintf(os.Stderr, "sanity: key %d unreadable after run\n", k)
			os.Exit(1)
		}
		vb = v[:0]
	}
	report(s, frac, name, ops, written, total, start, gaps, compacts, freed, compactNs)
}

func report(s *store.Store, frac, name string, ops, written, total uint64,
	start time.Time, gaps []time.Duration, compacts, freed int, compactNs int64) {
	el := time.Since(start)
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	pct := func(p float64) time.Duration {
		if len(gaps) == 0 {
			return 0
		}
		i := int(p * float64(len(gaps)-1))
		return gaps[i]
	}
	used, tot := s.ArenaBytes()
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	fmt.Printf("%s frac=%s: %d ops in %v, %.2fM ops/s, %.2fx turnover\n",
		name, frac, ops, el.Round(time.Millisecond),
		float64(ops)/el.Seconds()/1e6, float64(written)/float64(total))
	fmt.Printf("  batch gap p50=%v p99=%v max=%v (%d batches)\n",
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond),
		pct(1.0).Round(time.Microsecond), len(gaps))
	fmt.Printf("  compact passes=%d segments freed=%d compact time=%v\n",
		compacts, freed, time.Duration(compactNs).Round(time.Millisecond))
	fmt.Printf("  arena used=%.2fGiB total=%.2fGiB, maxrss=%.2fGiB\n",
		float64(used)/(1<<30), float64(tot)/(1<<30), float64(ru.Maxrss)/(1<<30))
}
