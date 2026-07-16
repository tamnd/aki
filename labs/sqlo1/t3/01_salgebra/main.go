// Lab: set algebra probe batching and the probe-vs-merge switch
// (spec 2064/sqlo1 doc 08 sections 4 and 6, milestone T3 lab 01).
//
// SINTER drives from the smallest set and probes each driver member
// into the others; doc 08 makes probe batching the load-bearing
// detail on cold data (gather a driver segment's members, group by
// target segment, issue the segment reads as one IO batch). But
// probing is not free at every ratio: as the driver grows, the
// probed-into set's touched-segment share saturates at 1 and the
// probe rounds fragment (every gather window fetches its own batch),
// while a straight fh-order merge walk of both sets reads everything
// exactly once in fully packed 16-segment rounds. Somewhere the walk
// wins, and the switch must be checkable in O(1) from the roots:
// driver root count against target fence length.
//
// The lab prices both arms in segment reads, IO rounds, and payload
// bytes across size ratios and cache temperatures, reports effective
// probes per IO round (the milestone's headline cell), checks the
// gather window's insensitivity, and settles SUNION's dedupe digest
// width with collision math plus a live bytes-per-unique measurement.
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
)

// entBytes prices one set entry: the 3-byte valueless header plus a
// short member, the ID-set shape.
const entBytes = 3 + 8

// segHdrBytes is the segment payload header, doc 06's 12 bytes.
const segHdrBytes = 12

// batchSegs is the engine's LookupBatch round: iterSegEntries and the
// probe gather both fetch cold segments 16 at a time.
const batchSegs = 16

type config struct {
	tmembers int
	maxEnts  int
	churnPct int
	seed     int64
}

type ent struct {
	fh uint64
	id int32
}

type segment struct {
	lo   uint64
	ents []ent
}

// set is the same model the spop lab used: a fence of segments built
// through the engine's insert-split path, so occupancy spreads
// half-to-full the way median splits and lazy merges leave it.
type set struct {
	maxEnts int
	los     []uint64
	segs    []*segment
	live    int
}

func newSet(maxEnts int) *set {
	return &set{maxEnts: maxEnts, los: []uint64{0}, segs: []*segment{{lo: 0}}}
}

func (s *set) segFor(fh uint64) int {
	return sort.Search(len(s.los), func(i int) bool { return s.los[i] > fh }) - 1
}

func (s *set) insert(fh uint64, id int32) {
	i := s.segFor(fh)
	sg := s.segs[i]
	sg.ents = append(sg.ents, ent{fh: fh, id: id})
	s.live++
	if len(sg.ents) > s.maxEnts {
		s.split(i)
	}
}

func (s *set) split(i int) {
	sg := s.segs[i]
	sort.Slice(sg.ents, func(a, b int) bool { return sg.ents[a].fh < sg.ents[b].fh })
	mid := len(sg.ents) / 2
	hi := &segment{lo: sg.ents[mid].fh, ents: append([]ent(nil), sg.ents[mid:]...)}
	sg.ents = sg.ents[:mid]
	s.los = append(s.los, 0)
	copy(s.los[i+2:], s.los[i+1:])
	s.los[i+1] = hi.lo
	s.segs = append(s.segs, nil)
	copy(s.segs[i+2:], s.segs[i+1:])
	s.segs[i+1] = hi
}

func (s *set) remove(fh uint64, id int32) bool {
	sg := s.segs[s.segFor(fh)]
	for j, e := range sg.ents {
		if e.fh == fh && e.id == id {
			sg.ents[j] = sg.ents[len(sg.ents)-1]
			sg.ents = sg.ents[:len(sg.ents)-1]
			s.live--
			return true
		}
	}
	return false
}

// build inserts members then churns a slice out and back in, widening
// occupancy the way real delete traffic does.
func build(members, maxEnts, churnPct int, rng *rand.Rand) *set {
	s := newSet(maxEnts)
	fhs := make([]uint64, members)
	for i := range members {
		fhs[i] = rng.Uint64()
		s.insert(fhs[i], int32(i))
	}
	churn := members * churnPct / 100
	for i := range churn {
		s.remove(fhs[i], int32(i))
	}
	for i := range churn {
		fhs[i] = rng.Uint64()
		s.insert(fhs[i], int32(i))
	}
	return s
}

func (s *set) bytes() int {
	b := 0
	for _, sg := range s.segs {
		b += segHdrBytes + len(sg.ents)*entBytes
	}
	return b
}

// arm is one strategy's cost: cold segment reads (the IOPS bill), IO
// rounds (the latency bill, one LookupBatch each), and payload bytes
// off the store.
type arm struct {
	reads  int
	rounds int
	bytes  int
}

// coldWalk prices a full fence walk at temperature hot: each segment
// misses the tier with probability 1-hot, and the misses fetch in
// packed rounds of batchSegs.
func coldWalk(s *set, hot float64, rng *rand.Rand) arm {
	var a arm
	for _, sg := range s.segs {
		if rng.Float64() >= hot {
			a.reads++
			a.bytes += segHdrBytes + len(sg.ents)*entBytes
		}
	}
	a.rounds = (a.reads + batchSegs - 1) / batchSegs
	return a
}

// probeCost prices the probe arm of SINTER(driver, target): walk the
// driver fence window by window (windowSegs driver segments per gather),
// route every member to its target segment, and fetch the window's
// distinct not-yet-seen target segments as one batched round set. A
// segment fetched once stays hot for the rest of the op (the tier
// holds it; the model assumes the budget covers the touched set, the
// same assumption the merge arm gets). The driver's own walk is
// priced identically for both arms and included.
func probeCost(driver, target *set, windowSegs int, hot float64, rng *rand.Rand) (a arm, touched int) {
	a = coldWalk(driver, hot, rng)
	seen := make(map[int]bool)
	for base := 0; base < len(driver.segs); base += windowSegs {
		windowNew := 0
		for i := base; i < min(base+windowSegs, len(driver.segs)); i++ {
			for _, e := range driver.segs[i].ents {
				t := target.segFor(e.fh)
				if seen[t] {
					continue
				}
				seen[t] = true
				touched++
				if rng.Float64() >= hot {
					windowNew++
					a.bytes += segHdrBytes + len(target.segs[t].ents)*entBytes
				}
			}
		}
		a.reads += windowNew
		a.rounds += (windowNew + batchSegs - 1) / batchSegs
	}
	return a, touched
}

// mergeCost prices the fh-order zipper: both fences stream end to end
// in packed rounds, no routing, no rejection, and the intersection
// falls out of the walk.
func mergeCost(driver, target *set, hot float64, rng *rand.Rand) arm {
	d := coldWalk(driver, hot, rng)
	t := coldWalk(target, hot, rng)
	return arm{reads: d.reads + t.reads, rounds: d.rounds + t.rounds, bytes: d.bytes + t.bytes}
}

// ratioSweep prices both arms across driver sizes against one target,
// at two temperatures. The switch unit is driver members over target
// fence length, both O(1) from the roots.
func ratioSweep(target *set, cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed + 1))
	tsegs := len(target.segs)
	for _, dm := range []int{100, 400, 1600, 6400, 25600, 102400, 409600} {
		if dm > target.live {
			continue
		}
		driver := build(dm, cfg.maxEnts, cfg.churnPct, rng)
		for _, hot := range []float64{0, 0.9} {
			crng := rand.New(rand.NewSource(cfg.seed + int64(dm) + int64(hot*100)))
			p, touched := probeCost(driver, target, 1, hot, crng)
			m := mergeCost(driver, target, hot, crng)
			// The tag is dominance, not preference: near the boundary
			// both arms price within a few percent and the row reads
			// tradeoff; the README picks the constant from where the
			// tags flip cleanly.
			winner := "tradeoff"
			switch {
			case p.rounds <= m.rounds && p.bytes <= m.bytes*102/100:
				winner = "probe"
			case m.rounds <= p.rounds && m.bytes <= p.bytes*102/100:
				winner = "merge"
			}
			ppr := float64(driver.live)
			if p.rounds > 0 {
				ppr /= float64(p.rounds)
			}
			fmt.Fprintf(out, "ratio,%d,%d,%d,%d,%.2f,%.1f,%d,%d,%d,%d,%d,%d,%d,%.0f,%s\n",
				driver.live, len(driver.segs), target.live, tsegs,
				float64(driver.live)/float64(tsegs), hot,
				touched, p.reads, p.rounds, p.bytes,
				m.reads, m.rounds, m.bytes, ppr, winner)
		}
	}
}

// windowSweep checks the gather window's insensitivity at one mid
// ratio: wider windows can only merge partially filled rounds, and
// once the op cache dedupes repeat targets the win should be small.
func windowSweep(target *set, cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed + 2))
	driver := build(6400, cfg.maxEnts, cfg.churnPct, rng)
	for _, w := range []int{1, 2, 4, 8} {
		crng := rand.New(rand.NewSource(cfg.seed + 3))
		p, touched := probeCost(driver, target, w, 0, crng)
		fmt.Fprintf(out, "window,%d,%d,%d,%d,%d,%d\n",
			driver.live, target.live, w, touched, p.reads, p.rounds)
	}
}

// collisionP is the birthday bound for n uniform digests of the given
// bit width: P(any collision) <= n^2 / 2^(bits+1). A SUNION dedupe
// collision silently drops a distinct member, so the acceptable
// probability is corruption-grade, not cache-grade.
func collisionP(n float64, bits int) float64 {
	return n * n / math.Exp2(float64(bits+1))
}

// dedupeSink keeps the measured structure reachable across the GC
// and reads, so the collector cannot fold it away mid-measurement.
var dedupeSink any

// dedupeBytes measures live bytes per unique for a digest set of the
// given width, the SUNION dedupe structure before it spills. The
// structure is measured by the heap drop when it is released, which
// is robust against unrelated allocations between the two reads.
func dedupeBytes(uniques int, width int, rng *rand.Rand) float64 {
	if width == 16 {
		m := make(map[[2]uint64]struct{}, 16)
		for range uniques {
			m[[2]uint64{rng.Uint64(), rng.Uint64()}] = struct{}{}
		}
		dedupeSink = m
	} else {
		m := make(map[uint64]struct{}, 16)
		for range uniques {
			m[rng.Uint64()] = struct{}{}
		}
		dedupeSink = m
	}
	runtime.GC()
	var held runtime.MemStats
	runtime.ReadMemStats(&held)
	dedupeSink = nil
	runtime.GC()
	var dropped runtime.MemStats
	runtime.ReadMemStats(&dropped)
	return float64(int64(held.HeapAlloc)-int64(dropped.HeapAlloc)) / float64(uniques)
}

// dedupeSweep prints the digest decision inputs: collision odds per
// width at union sizes up to the PB-scale ambition, and measured
// resident bytes per unique member for both widths.
func dedupeSweep(cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed + 4))
	for _, n := range []float64{1e6, 1e8, 1e9} {
		fmt.Fprintf(out, "collide,%.0e,64,%.3g\n", n, collisionP(n, 64))
		fmt.Fprintf(out, "collide,%.0e,128,%.3g\n", n, collisionP(n, 128))
	}
	for _, width := range []int{8, 16} {
		fmt.Fprintf(out, "dedupe,%d,%d,%.1f\n", 1_000_000, width, dedupeBytes(1_000_000, width, rng))
	}
}

func runAll(cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed))
	target := build(cfg.tmembers, cfg.maxEnts, cfg.churnPct, rng)
	occMin, occMax := cfg.maxEnts, 0
	for _, sg := range target.segs {
		occMin, occMax = min(occMin, len(sg.ents)), max(occMax, len(sg.ents))
	}
	fmt.Fprintf(os.Stderr, "salgebra: target %d members, %d segments, occupancy %d..%d of %d\n",
		target.live, len(target.segs), occMin, occMax, cfg.maxEnts)
	ratioSweep(target, cfg, out)
	windowSweep(target, cfg, out)
	dedupeSweep(cfg, out)
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink sizes for smoke runs")
	flag.IntVar(&cfg.tmembers, "tmembers", 1000000, "target set members")
	flag.IntVar(&cfg.maxEnts, "maxents", 503, "entries per segment before a split")
	flag.IntVar(&cfg.churnPct, "churn", 25, "percent churned out and back in")
	flag.Int64Var(&cfg.seed, "seed", 97, "rng seed")
	flag.Parse()
	if *quick {
		cfg.tmembers = 50000
	}
	runAll(cfg, os.Stdout)
}
