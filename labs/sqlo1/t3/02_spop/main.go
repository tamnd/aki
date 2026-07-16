// Lab: SPOP count sampling and the whole-segment removal threshold
// (spec 2064/sqlo1 doc 08 sections 2 and 5, milestone T3 lab 02).
//
// SPOP count must remove a uniformly random distinct subset (SE-I4)
// and never rewrite more segments than it empties or edits. The
// planned allocator picks count distinct global positions over the
// fence's prefix counts (a partial Fisher-Yates over the implicit
// 0..N-1 array), which lands exact multivariate hypergeometric takes
// per segment for free; a segment whose take equals its live count is
// removed whole (one delete frame, no payload rewrite) and any other
// touched segment is rewritten minus its popped entries.
//
// Two questions decide slice 4's constants. First, uniformity: the
// position allocator must pass chi-square against uniform over
// members, and the cheap-looking null that spreads count evenly over
// segments must fail, since median splits and lazy merges leave
// occupancy anywhere between half and full. Second, the large-count
// strategy: editing every touched segment approaches a full rewrite of
// the set as the pop fraction grows, at which point emitting the
// popped members and bulk-rebuilding the remainder into fully packed
// fresh segments (the doc 09 section 6 pattern, old plane retired by
// one genbump) is cheaper. The sweep prices both arms in write frames
// and payload bytes per popped member across pop fractions; the
// crossover is the rebuild threshold slice 4 bakes.
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)

type config struct {
	members  int
	maxEnts  int
	churnPct int
	trials   int
	seed     int64
}

// entBytes prices one set entry in a segment payload: the 3-byte
// valueless header plus a short member, the ID-set shape the intset
// workload carries.
const entBytes = 3 + 8

// segHdrBytes is the segment payload header (count, reserved,
// min_expire), doc 06's 12 bytes unchanged for sets.
const segHdrBytes = 12

type ent struct {
	fh uint64
	id int32
}

type segment struct {
	lo   uint64
	ents []ent
}

// set is the sampling model: a fence of segments built through the
// same insert-split path the engine ships, entries carrying member ids
// so pops can be tallied per member.
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

// split cuts at the fh median like the engine: sort by fh, keep the
// low half, mint a segment at the median fh. Equal-fh runs stay
// together; member fhs are random 64-bit values here so ties are
// negligible and the model skips the run-preserving nudge.
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

// build inserts members then churns a slice of them out and back in,
// widening the occupancy spread the way real delete traffic does
// under lazy merges.
func build(cfg config, rng *rand.Rand) *set {
	s := newSet(cfg.maxEnts)
	fhs := make([]uint64, cfg.members)
	for i := range cfg.members {
		fhs[i] = rng.Uint64()
		s.insert(fhs[i], int32(i))
	}
	churn := cfg.members * cfg.churnPct / 100
	for i := range churn {
		s.remove(fhs[i], int32(i))
	}
	for i := range churn {
		fhs[i] = rng.Uint64()
		s.insert(fhs[i], int32(i))
	}
	return s
}

// prefix returns the running entry count before each segment, the
// fence-order coordinate system the allocator draws positions in.
func prefix(s *set) []int {
	pre := make([]int, len(s.segs)+1)
	for i, sg := range s.segs {
		pre[i+1] = pre[i] + len(sg.ents)
	}
	return pre
}

// allocate draws count distinct positions uniformly from [0, N) with
// a sparse partial Fisher-Yates (the swap map holds only touched
// slots, so the cost is O(count) regardless of N) and converts them to
// per-segment take counts plus the popped entry indexes. This is the
// allocator slice 4 ships: exact hypergeometric takes with no
// per-segment distribution sampling.
func allocate(pre []int, count int, rng *rand.Rand) (takes []int, picks [][]int) {
	n := pre[len(pre)-1]
	takes = make([]int, len(pre)-1)
	picks = make([][]int, len(pre)-1)
	swapped := make(map[int]int, count)
	at := func(i int) int {
		if v, ok := swapped[i]; ok {
			return v
		}
		return i
	}
	for d := range count {
		j := d + rng.Intn(n-d)
		pos := at(j)
		swapped[j] = at(d)
		seg := sort.SearchInts(pre, pos+1) - 1
		takes[seg]++
		picks[seg] = append(picks[seg], pos-pre[seg])
	}
	return takes, picks
}

// allocateEven is the null arm: count spread evenly over segments,
// uniform inside each. Occupancy varies half-to-full after churn, so
// members in thin segments are oversampled; the lab exists to show
// this fails chi-square.
func allocateEven(pre []int, count int, rng *rand.Rand) (takes []int, picks [][]int) {
	segs := len(pre) - 1
	takes = make([]int, segs)
	picks = make([][]int, segs)
	left := count
	for i := 0; left > 0; i = (i + 1) % segs {
		liveHere := pre[i+1] - pre[i]
		if takes[i] < liveHere {
			takes[i]++
			left--
		}
	}
	for i := range segs {
		liveHere := pre[i+1] - pre[i]
		seen := make(map[int]bool, takes[i])
		for len(seen) < takes[i] {
			seen[rng.Intn(liveHere)] = true
		}
		for j := range seen {
			picks[i] = append(picks[i], j)
		}
	}
	return takes, picks
}

type verdict struct {
	chi2PerDof float64
	z          float64
}

// judge chi-squares the per-member pop tallies against uniform. SPOP
// draws without replacement, so a member's tally across trials is a
// sum of Bernoulli(count/N) with variance deflated by the finite
// population factor (1 - count/N); the plain Poisson chi-square would
// sit that factor below its dof and read as impossibly even, so the
// statistic carries the correction.
func judge(obs []int64, trials, count, n int) verdict {
	exp := float64(trials) * float64(count) / float64(n)
	fpc := 1 - float64(count)/float64(n)
	chi2 := 0.0
	for _, o := range obs {
		d := float64(o) - exp
		chi2 += d * d / (exp * fpc)
	}
	dof := float64(n - 1)
	return verdict{chi2PerDof: chi2 / dof, z: (chi2 - dof) / math.Sqrt(2*dof)}
}

// uniformity tallies pops per member over trials without mutating the
// set, so every trial samples the same population.
func uniformity(s *set, cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed + 1))
	pre := prefix(s)
	count := s.live / 20
	for _, arm := range []string{"positions", "even"} {
		obs := make([]int64, s.live)
		start := time.Now()
		for range cfg.trials {
			var picks [][]int
			if arm == "positions" {
				_, picks = allocate(pre, count, rng)
			} else {
				_, picks = allocateEven(pre, count, rng)
			}
			for i, ps := range picks {
				for _, p := range ps {
					obs[s.segs[i].ents[p].id]++
				}
			}
		}
		dur := time.Since(start)
		v := judge(obs, cfg.trials, count, s.live)
		fmt.Fprintf(out, "uniform,%s,%d,%d,%d,%d,%.3f,%.2f,%.0f\n",
			arm, s.live, len(s.segs), cfg.trials, count,
			v.chi2PerDof, v.z,
			float64(dur.Nanoseconds())/float64(cfg.trials*count))
	}
}

type cost struct {
	frames int
	bytes  int
}

// editCost prices in-place removal: an emptied segment is one delete
// frame with no payload, an edited one is one rewrite frame carrying
// its remaining entries, and the root rides once.
func editCost(s *set, takes []int) (c cost, touched, emptied int) {
	for i, t := range takes {
		if t == 0 {
			continue
		}
		touched++
		remain := len(s.segs[i].ents) - t
		if remain == 0 {
			emptied++
			c.frames++
		} else {
			c.frames++
			c.bytes += segHdrBytes + remain*entBytes
		}
	}
	c.frames++ // root
	c.bytes += 64
	return c, touched, emptied
}

// rebuildCost prices the bulk-build arm: the remainder repacks into
// fully packed fresh segments in fh order, the root PUT commits, and
// one genbump retires the old plane.
func rebuildCost(s *set, popped int) cost {
	remain := s.live - popped
	if remain == 0 {
		return cost{frames: 1}
	}
	segs := (remain + s.maxEnts - 1) / s.maxEnts
	return cost{
		frames: segs + 2,
		bytes:  segs*segHdrBytes + remain*entBytes + 64,
	}
}

// sweep prices both arms across pop counts. The knee is not at a set
// fraction: with occupancy around 300 a pop of a few per segment
// already touches every segment, so the interesting region is counts
// of order the segment count and the sweep walks absolute counts up
// through the large fractions.
func sweep(s *set, cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed + 2))
	pre := prefix(s)
	counts := []int{1, 4, 16, 64, 256, 1024, 4096}
	for _, pct := range []int{5, 10, 25, 50, 75, 90, 99} {
		counts = append(counts, s.live*pct/100)
	}
	last := 0
	for _, count := range counts {
		if count <= last || count > s.live {
			continue
		}
		last = count
		takes, _ := allocate(pre, count, rng)
		edit, touched, emptied := editCost(s, takes)
		reb := rebuildCost(s, count)
		winner := "edit"
		if reb.bytes < edit.bytes {
			winner = "rebuild"
		}
		fmt.Fprintf(out, "cost,%d,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%s\n",
			count, 100*float64(count)/float64(s.live), s.live, len(s.segs),
			touched, emptied,
			edit.frames, edit.bytes, reb.frames, reb.bytes, winner)
	}
}

func runAll(cfg config, out io.Writer) {
	rng := rand.New(rand.NewSource(cfg.seed))
	s := build(cfg, rng)
	occMin, occMax := cfg.maxEnts, 0
	for _, sg := range s.segs {
		occMin, occMax = min(occMin, len(sg.ents)), max(occMax, len(sg.ents))
	}
	fmt.Fprintf(os.Stderr, "spop: %d live members, %d segments, occupancy %d..%d of %d\n",
		s.live, len(s.segs), occMin, occMax, cfg.maxEnts)
	uniformity(s, cfg, out)
	sweep(s, cfg, out)
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.IntVar(&cfg.members, "members", 200000, "members inserted before churn")
	flag.IntVar(&cfg.maxEnts, "maxents", 503, "entries per segment before a split (4032 B / 11 B short members)")
	flag.IntVar(&cfg.churnPct, "churn", 25, "percent of members deleted and reinserted before sampling")
	flag.IntVar(&cfg.trials, "trials", 200, "SPOP trials tallied for the chi-square")
	flag.Int64Var(&cfg.seed, "seed", 83, "rng seed")
	flag.Parse()
	if *quick {
		cfg.members, cfg.trials = 20000, 50
	}
	runAll(cfg, os.Stdout)
}
