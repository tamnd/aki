// Lab: which demotion policy the cold migrator runs, S3-FIFO versus SIEVE, both
// with ghost readmission, on a skewed trace with a heavy one-hit-wonder tail
// (spec 2064/f3/06 section 4.2, the choice the spec defers to a lab at line 337).
//
// The question: when a shard is above high water the migrator picks what sinks to
// the cold region. The f1 first cut sank on one recency bit, blind to a scan of
// one-hit-wonder keys that evicts the working set and then pays a pread per hot
// read forever. The spec fixes the family (FIFO with second chances, not an LRU
// chain and not a frequency sketch on the hot path) and leaves one open choice:
// literal S3-FIFO (a small probationary queue plus a main region plus a ghost
// ring) versus SIEVE-plus-ghost (one region with a hand and a visited bit). Both
// are lock-free on the hit path, both cost one visited bit per entry, both run as
// a sequential scan of the oldest segments the migrator already drains. The spec
// says the segment-organized realization accommodates either without a layout
// change, so the tie-breaker is empirical: hot-tier hit ratio and demotion CPU on
// a skewed trace with a heavy one-hit tail.
//
// This lab runs that trace and settles it. Plain FIFO is the third arm, the f1
// first cut, there to show the scan-resistance gap the second-chance policies
// close. The trace is a Zipfian working set (the hot keys that repeat) interleaved
// with a stream of unique keys that are touched once and never again (the tail
// that pollutes a naive cache). The metrics: working-set hit ratio, the share of
// repeat-key accesses served from the hot tier, which is the number that turns
// into preads avoided; and demotion CPU, the survivor-copy count the segment
// realization pays when a visited record is re-appended to the tail, the memcpy
// bandwidth the spec calls out (section 4.2, the survivor-cost note). A third
// sweep sizes the ghost ring at parity, 50%, and 25% per the spec's sizing
// paragraph, against readmission benefit.
//
// Method: in-process, no server, no wire, no engine import, the lab-local model
// the other f3 labs use. The policies are simulated over the trace with a
// container/list deque per region and a visited bit per entry, faithful to the
// segment realization: a survivor re-append is a MoveToBack and counts one copy,
// an unvisited entry sinks and its key fingerprint enters the ghost ring, a miss
// that hits the ghost readmits skipping probation. The trace is deterministic (a
// fixed seed), so CI pins the ordering the verdict rests on. This model measures
// policy quality, hit ratio and copy count, not wall-clock, so it needs no box.
package main

import (
	"container/list"
	"flag"
	"fmt"
	"math/rand"
	"time"
)

// ent is one cached record in a policy's region: its key and the visited bit a
// read sets and the demotion scan reads and clears. The bit is the whole
// second-chance mechanism, one store to a line the read already owns.
type ent struct {
	key     uint64
	visited bool
}

// policy is one demotion policy under test. access serves one key and reports
// whether it hit the hot tier; copies reports the running survivor-copy count,
// the demotion-CPU proxy.
type policy interface {
	name() string
	access(key uint64) bool
	copies() int64
}

// ghostRing is the per-shard readmission window: a circular array of key
// fingerprints, no data, sized to a fraction of the main region. A sunk record's
// key enters here; a later miss that finds its key here is a recent second
// sighting and readmits into the main region directly, skipping probation. The
// spec models membership as a linear probe over the ring; the set here is that
// membership, exact, because the lab measures readmission benefit against ring
// size, not the probe's constant. A zero-cap ring is the ghost-off arm.
type ghostRing struct {
	cap  int
	ring []uint64
	cur  int
	set  map[uint64]bool
}

func newGhost(cap int) *ghostRing {
	return &ghostRing{cap: cap, ring: make([]uint64, cap), set: make(map[uint64]bool, cap)}
}

// add records a sunk key's fingerprint, evicting the oldest slot when the ring
// wraps. A stale fingerprint a take already consumed is left in the ring as a
// tombstone and cleared when its slot is overwritten.
func (g *ghostRing) add(k uint64) {
	if g.cap == 0 {
		return
	}
	if old := g.ring[g.cur]; old != 0 {
		delete(g.set, old)
	}
	g.ring[g.cur] = k
	g.set[k] = true
	g.cur = (g.cur + 1) % g.cap
}

// take reports whether a key is a recent demotion and consumes the sighting, so a
// readmission does not fire twice off one ghost entry.
func (g *ghostRing) take(k uint64) bool {
	if g.set[k] {
		delete(g.set, k)
		return true
	}
	return false
}

// sieveGhost is SIEVE-plus-ghost: one region, a hand that scans from the oldest
// entry, a visited bit per entry. A read sets the bit. On overflow the hand walks
// the oldest end: a visited entry is a survivor, its bit cleared and the record
// re-appended to the tail (one copy), an unvisited entry sinks and its key enters
// the ghost. A miss that hits the ghost is admitted warm (visited set), so the
// recent second sighting survives the next hand pass; a fresh miss is admitted
// cold. Quick demotion of one-hit wonders falls out of the single region: an
// unvisited newcomer sinks the first time the hand reaches it.
type sieveGhost struct {
	capN  int
	q     *list.List
	at    map[uint64]*list.Element
	ghost *ghostRing
	copyN int64
}

func newSieve(capN, ghostCap int) *sieveGhost {
	return &sieveGhost{capN: capN, q: list.New(), at: make(map[uint64]*list.Element, capN), ghost: newGhost(ghostCap)}
}

func (s *sieveGhost) name() string  { return "sieve+ghost" }
func (s *sieveGhost) copies() int64 { return s.copyN }

func (s *sieveGhost) access(key uint64) bool {
	if e, ok := s.at[key]; ok {
		e.Value.(*ent).visited = true
		return true
	}
	warm := s.ghost.take(key)
	for s.q.Len() >= s.capN {
		s.demote()
	}
	s.at[key] = s.q.PushBack(&ent{key: key, visited: warm})
	return false
}

// demote frees exactly one slot: the hand re-appends visited survivors (clearing
// their bit) until it reaches the first unvisited entry, which sinks. If every
// entry is visited the hand clears the whole region once and then sinks the
// earliest, so the pass is bounded by the region size and terminates.
func (s *sieveGhost) demote() {
	for {
		front := s.q.Front()
		e := front.Value.(*ent)
		if e.visited {
			e.visited = false
			s.q.MoveToBack(front)
			s.copyN++
			continue
		}
		s.q.Remove(front)
		delete(s.at, e.key)
		s.ghost.add(e.key)
		return
	}
}

// s3fifo is literal S3-FIFO: a small probationary queue (~10% of the hot budget),
// a main region (the rest), and a ghost ring at main-region size. A fresh miss
// enters the small queue cold; a miss that hits the ghost enters the main region
// directly, skipping probation. A read sets the visited bit wherever the entry
// lives. The small queue quick-demotes: on overflow its oldest entry, if visited,
// is promoted to main (one copy), else sinks to the ghost, so a write-once key
// never touches the main region. The main region gives one second chance: a
// visited entry is re-appended (one copy, bit cleared), an unvisited one sinks.
type s3fifo struct {
	smallCap int
	mainCap  int
	small    *list.List
	main     *list.List
	at       map[uint64]*regionElem
	ghost    *ghostRing
	copyN    int64
}

// regionElem locates an entry: which region's list holds it and its element, so a
// hit can set the visited bit without scanning.
type regionElem struct {
	inMain bool
	el     *list.Element
}

func newS3FIFO(capN, ghostCap int) *s3fifo {
	smallCap := capN / 10
	if smallCap < 1 {
		smallCap = 1
	}
	return &s3fifo{
		smallCap: smallCap,
		mainCap:  capN - smallCap,
		small:    list.New(),
		main:     list.New(),
		at:       make(map[uint64]*regionElem, capN),
		ghost:    newGhost(ghostCap),
	}
}

func (s *s3fifo) name() string  { return "s3-fifo" }
func (s *s3fifo) copies() int64 { return s.copyN }

func (s *s3fifo) access(key uint64) bool {
	if loc, ok := s.at[key]; ok {
		loc.el.Value.(*ent).visited = true
		return true
	}
	if s.ghost.take(key) {
		s.insertMain(&ent{key: key})
	} else {
		s.at[key] = &regionElem{inMain: false, el: s.small.PushBack(&ent{key: key})}
		s.evictSmall()
	}
	return false
}

// insertMain pushes an entry into the main region and evicts down to cap, used by
// both a ghost readmission and a small-queue promotion.
func (s *s3fifo) insertMain(e *ent) {
	s.at[e.key] = &regionElem{inMain: true, el: s.main.PushBack(e)}
	s.evictMain()
}

// evictSmall drains the small queue to its cap: a visited head is promoted to the
// main region (one copy), an unvisited head sinks to the ghost.
func (s *s3fifo) evictSmall() {
	for s.small.Len() > s.smallCap {
		front := s.small.Front()
		e := front.Value.(*ent)
		s.small.Remove(front)
		delete(s.at, e.key)
		if e.visited {
			e.visited = false
			s.copyN++
			s.insertMain(e)
		} else {
			s.ghost.add(e.key)
		}
	}
}

// evictMain drains the main region to its cap with one second chance: a visited
// head is re-appended cleared (one copy), an unvisited head sinks to the ghost.
func (s *s3fifo) evictMain() {
	for s.main.Len() > s.mainCap {
		front := s.main.Front()
		e := front.Value.(*ent)
		if e.visited {
			e.visited = false
			s.main.MoveToBack(front)
			s.copyN++
			continue
		}
		s.main.Remove(front)
		delete(s.at, e.key)
		s.ghost.add(e.key)
	}
}

// fifo is plain FIFO, the f1 first cut without a second chance: a read is not
// tracked, and on overflow the oldest entry sinks unconditionally. It is the arm
// the one-hit tail defeats, the reason the second-chance policies exist.
type fifo struct {
	capN int
	q    *list.List
	at   map[uint64]*list.Element
}

func newFIFO(capN int) *fifo {
	return &fifo{capN: capN, q: list.New(), at: make(map[uint64]*list.Element, capN)}
}

func (f *fifo) name() string  { return "fifo" }
func (f *fifo) copies() int64 { return 0 }

func (f *fifo) access(key uint64) bool {
	if _, ok := f.at[key]; ok {
		return true
	}
	for f.q.Len() >= f.capN {
		front := f.q.Front()
		e := front.Value.(*ent)
		f.q.Remove(front)
		delete(f.at, e.key)
	}
	f.at[key] = f.q.PushBack(&ent{key: key})
	return false
}

// trace is a workload: a Zipfian working set of repeat keys interleaved with a
// stream of unique one-hit-wonder keys. Working-set keys are 1..W; tail keys are
// W+1 and up, each emitted once. keyIsWS reports whether an access is a repeat
// key, the only accesses that can hit.
type traceCfg struct {
	length   int
	wsKeys   int
	tailRate float64
	zipfS    float64
	seed     int64
}

func makeTrace(c traceCfg) []uint64 {
	r := rand.New(rand.NewSource(c.seed))
	zipf := rand.NewZipf(r, c.zipfS, 1, uint64(c.wsKeys-1))
	tr := make([]uint64, c.length)
	next := uint64(c.wsKeys) + 1
	for i := range tr {
		if r.Float64() < c.tailRate {
			tr[i] = next
			next++
		} else {
			tr[i] = zipf.Uint64() + 1 // 1..wsKeys
		}
	}
	return tr
}

// stats is one policy's run over one trace.
type stats struct {
	name   string
	wsHits int64
	wsAcc  int64
	hits   int64
	acc    int64
	copies int64
}

func (s stats) wsHitRatio() float64 {
	if s.wsAcc == 0 {
		return 0
	}
	return float64(s.wsHits) / float64(s.wsAcc)
}

func (s stats) copiesPerK() float64 { return float64(s.copies) * 1000 / float64(s.acc) }

// run drives one policy over a trace and tallies hits split by working-set versus
// tail. wsKeys is the working-set boundary: a key at or below it is a repeat key.
func run(p policy, tr []uint64, wsKeys int) stats {
	s := stats{name: p.name()}
	for _, k := range tr {
		ws := k <= uint64(wsKeys)
		hit := p.access(k)
		s.acc++
		if hit {
			s.hits++
		}
		if ws {
			s.wsAcc++
			if hit {
				s.wsHits++
			}
		}
	}
	s.copies = p.copies()
	return s
}

const (
	capN  = 8192  // hot-tier capacity in records
	wsN   = 20000 // working-set distinct keys, larger than the cap so the skew decides residency
	zipfS = 1.07  // Zipf exponent, a realistic web-cache skew
)

func main() {
	quick := flag.Bool("quick", false, "smaller trace for a fast check")
	flag.Parse()

	length := 2_000_000
	if *quick {
		length = 400_000
	}

	fmt.Printf("demotion policy, S3-FIFO vs SIEVE (both +ghost) vs plain FIFO, skewed trace with a one-hit tail, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("hot cap %d records, working set %d keys, Zipf s=%.2f, trace %d accesses\n", capN, wsN, zipfS, length)
	fmt.Printf("small queue %d (10%% of cap), ghost ring at main-region parity %d\n", capN/10, capN-capN/10)

	// Sweep A: rising one-hit-tail rate at a fixed cap. FIFO collapses as the tail
	// grows, evicting the working set on every scan; the second-chance policies
	// hold their working-set hit ratio because an unvisited one-hit key sinks
	// before it displaces a hot key. The copies columns are the demotion CPU.
	fmt.Println()
	fmt.Println("Sweep A: rising one-hit-tail rate (working-set hit ratio | copies/1k accesses)")
	fmt.Printf("%-10s %20s %20s %20s\n", "tailRate", "fifo", "sieve+ghost", "s3-fifo")
	for _, tr := range []float64{0.0, 0.2, 0.4, 0.6, 0.8} {
		t := makeTrace(traceCfg{length: length, wsKeys: wsN, tailRate: tr, zipfS: zipfS, seed: 1})
		ghostCap := capN - capN/10
		f := run(newFIFO(capN), t, wsN)
		sv := run(newSieve(capN, ghostCap), t, wsN)
		s3 := run(newS3FIFO(capN, ghostCap), t, wsN)
		fmt.Printf("%-10.1f %13.4f|%5.0f %13.4f|%5.0f %13.4f|%5.0f\n",
			tr, f.wsHitRatio(), f.copiesPerK(), sv.wsHitRatio(), sv.copiesPerK(), s3.wsHitRatio(), s3.copiesPerK())
	}

	// Sweep B: the head-to-head at a heavy tail, rising skew. A flatter working set
	// (lower s) is harder to hold, so the gap between the policies, if any, is
	// widest here. This is where the S3-FIFO-vs-SIEVE choice is decided.
	fmt.Println()
	fmt.Println("Sweep B: heavy tail (0.6), rising skew (working-set hit ratio | copies/1k)")
	fmt.Printf("%-8s %20s %20s %12s\n", "zipfS", "sieve+ghost", "s3-fifo", "hitDelta")
	for _, s := range []float64{1.05, 1.1, 1.2, 1.4} {
		t := makeTrace(traceCfg{length: length, wsKeys: wsN, tailRate: 0.6, zipfS: s, seed: 2})
		ghostCap := capN - capN/10
		sv := run(newSieve(capN, ghostCap), t, wsN)
		s3 := run(newS3FIFO(capN, ghostCap), t, wsN)
		fmt.Printf("%-8.2f %13.4f|%5.0f %13.4f|%5.0f %12.4f\n",
			s, sv.wsHitRatio(), sv.copiesPerK(), s3.wsHitRatio(), s3.copiesPerK(), s3.wsHitRatio()-sv.wsHitRatio())
	}

	// Sweep C: ghost ring sizing at a heavy tail, parity down to off. The spec
	// wants parity, 50%, and 25% against readmission benefit: past the knee the
	// ring just remembers keys that were correctly demoted, so the ring bytes buy
	// nothing. mainRegion is the parity size; the fractions are of it.
	fmt.Println()
	fmt.Println("Sweep C: ghost ring size (fraction of main region), heavy tail (0.6), s=1.05")
	fmt.Printf("%-10s %8s %20s %20s\n", "ghostFrac", "slots", "sieve+ghost wsHit", "s3-fifo wsHit")
	main := capN - capN/10
	for _, frac := range []float64{0.0, 0.25, 0.5, 1.0} {
		gc := int(float64(main) * frac)
		t := makeTrace(traceCfg{length: length, wsKeys: wsN, tailRate: 0.6, zipfS: 1.05, seed: 3})
		sv := run(newSieve(capN, gc), t, wsN)
		s3 := run(newS3FIFO(capN, gc), t, wsN)
		fmt.Printf("%-10.2f %8d %20.4f %20.4f\n", frac, gc, sv.wsHitRatio(), s3.wsHitRatio())
	}

	// Verdict figure: the head-to-head at the design point (heavy tail, realistic
	// skew), the hit-ratio delta and the copy-cost ratio the choice turns on.
	t := makeTrace(traceCfg{length: length, wsKeys: wsN, tailRate: 0.6, zipfS: zipfS, seed: 4})
	ghostCap := capN - capN/10
	sv := run(newSieve(capN, ghostCap), t, wsN)
	s3 := run(newS3FIFO(capN, ghostCap), t, wsN)
	fmt.Println()
	fmt.Printf("Verdict point (tail 0.6, s=%.2f): sieve wsHit %.4f at %.0f copies/1k, s3-fifo wsHit %.4f at %.0f copies/1k; hit delta %+.4f, sieve copy cost %.2fx s3-fifo.\n",
		zipfS, sv.wsHitRatio(), sv.copiesPerK(), s3.wsHitRatio(), s3.copiesPerK(), s3.wsHitRatio()-sv.wsHitRatio(), sv.copiesPerK()/s3.copiesPerK())
}
