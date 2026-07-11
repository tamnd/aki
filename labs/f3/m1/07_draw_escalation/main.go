// Lab: F13 hot-key draw escalation, the two-level draw versus the flat weighted
// draw (spec 2064/f3 doc 11 section 5.4, M1 lab 07).
//
// The question: the M1 gate read the hot-key SPOP row at 1.50M ops/s, inside the
// pre-registered 1.06M-2.1M band that makes the F13 escalation load-bearing
// rather than headroom (results/f3/m1-gate.md, PRED-F3-M1-SPOP). F13 splits one
// hot partitioned set's P sub-tables into k draw-groups so the draw fans out
// across k owning workers: a scatter layer picks a group weighted by its share of
// the total count, then the group draws within its own partitions by the same
// exactly-uniform weighted prefix-sum the flat band uses (section 4.3). This lab
// prices the single-thread cost of that two-level draw against the flat draw
// before the slice bakes escalateMinP. It is a design study for one constant and
// a no-regression check: escalation must not slow the single owner, because its
// payoff is the aggregate fan-out the execution model wires, not a single-thread
// speedup.
//
// Method: in-process, no server, no wire, no engine import. A lab-local
// partitioned set is P independent native sub-sets selected by member hash, each
// with its own draw vector and count, matching lab 04's shape. The escalation
// layer groups the P partitions into k contiguous draw-groups with a per-group
// subtotal, the scatter weight. Two draws are timed per cell:
//
//   - Draw (locate + vector read): the flat weighted prefix scan over P counts
//     versus the two-level scatter-then-group scan. This isolates the escalation
//     tax, the k-group scan the two-level draw adds around the same in-partition
//     walk.
//   - Pop (draw + swap-remove + reinsert): the SPOP kernel, the draw plus the
//     draw-vector swap-remove the pop performs, reinserting the same ordinal so
//     the cardinality holds steady across the measurement. The group-total
//     decrement is one add, excluded here to keep the set full; it is timed in
//     the engine benchmark, not this crossover study.
//
// Axes: cardinality {2M (P8), 4M (P16)}, the shapes the gate's hot-key row runs;
// k in {2, 4, 8} where k divides P. Read: ns/op for the flat and escalated draw
// and pop, the single-thread tax, and the projected aggregate at k workers. The
// projection is honest: the real P16 aggregate number comes from the GamingPC
// gate box with the execution-model fan-out live; this lab freezes only the
// single-thread verdict and the escalateMinP constant. See README.md.
package main

import (
	"flag"
	"fmt"
	"math/bits"
	"time"
)

// mix is the splitmix64 finalizer, the same strong hash labs 01, 03, and 04 use
// so every M1 lab prices the same routing shape.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// route selects a member's partition from the top bits of its hash, kept clear
// of the low bits the sub-table's own probe uses (section 4.1).
func route(h uint64, pmask uint32) uint32 { return uint32(h>>60) & pmask }

// subset is one native sub-set's draw state: the member ids and a dense draw
// vector of ordinals (section 2.1). The lab draws and pops over the vector, so it
// carries no control table; lab 04 already priced the table probe.
type subset struct {
	ids []uint64
	vec []uint32
}

func (s *subset) insert(id uint64) {
	ord := uint32(len(s.ids))
	s.ids = append(s.ids, id)
	s.vec = append(s.vec, ord)
}

// escalation groups a pset's partitions into k contiguous draw-groups, each with
// a subtotal count, the section-5.4 scatter weights.
type escalation struct {
	span   int
	totals []uint64
}

// pset is a partitioned set: P sub-sets selected by member hash, the per-partition
// counts that weight the flat draw, and an optional escalation layer.
type pset struct {
	P     int
	pmask uint32
	parts []*subset
	esc   *escalation
}

func newPSet(P, card int) *pset {
	ps := &pset{P: P, pmask: uint32(P - 1), parts: make([]*subset, P)}
	hint := card/P + 1
	for i := range ps.parts {
		ps.parts[i] = &subset{ids: make([]uint64, 0, hint), vec: make([]uint32, 0, hint)}
	}
	return ps
}

func (ps *pset) insert(id uint64) {
	ps.parts[route(mix(id), ps.pmask)].insert(id)
}

func (ps *pset) counts() []uint64 {
	c := make([]uint64, ps.P)
	for i, s := range ps.parts {
		c[i] = uint64(len(s.vec))
	}
	return c
}

func (ps *pset) total() uint64 {
	var t uint64
	for _, s := range ps.parts {
		t += uint64(len(s.vec))
	}
	return t
}

// escalate splits the partitions into k draw-groups, building the subtotals in one
// pass (section 5.4). k must divide P.
func (ps *pset) escalate(k int) {
	span := ps.P / k
	e := &escalation{span: span, totals: make([]uint64, k)}
	counts := ps.counts()
	for g := 0; g < k; g++ {
		for i := g * span; i < (g+1)*span; i++ {
			e.totals[g] += counts[i]
		}
	}
	ps.esc = e
}

// flatLocate is the section-4.3 flat weighted draw: walk the P counts to the
// owning partition by prefix sum, return the partition and the in-partition slot.
// The branchless form matches the engine (partition.go): end runs the full prefix
// while start accumulates only partitions before r.
func (ps *pset) flatLocate(r uint64) (part, local int) {
	var end, start uint64
	p := 0
	for i := 0; i < ps.P; i++ {
		end += uint64(len(ps.parts[i].vec))
		mask := ^uint64(int64(r-end) >> 63)
		p += int(mask & 1)
		start += uint64(len(ps.parts[i].vec)) & mask
	}
	return p, int(r - start)
}

// pickGroup is the F13 scatter layer: r lands in group g with probability
// totals[g]/total, and rLocal is uniform within the group (section 5.4).
func (e *escalation) pickGroup(r uint64) (group int, rLocal uint64) {
	var end, start uint64
	g := 0
	for i := 0; i < len(e.totals); i++ {
		end += e.totals[i]
		mask := ^uint64(int64(r-end) >> 63)
		g += int(mask & 1)
		start += e.totals[i] & mask
	}
	return g, r - start
}

// escLocate is the two-level draw: scatter to a group, then the group-local
// weighted walk over its span of partitions (section 5.4). Composed it is
// identical to flatLocate for every r.
func (ps *pset) escLocate(r uint64) (part, local int) {
	group, rLocal := ps.esc.pickGroup(r)
	base := group * ps.esc.span
	var end, start uint64
	p := base
	for i := base; i < base+ps.esc.span; i++ {
		end += uint64(len(ps.parts[i].vec))
		mask := ^uint64(int64(rLocal-end) >> 63)
		p += int(mask & 1)
		start += uint64(len(ps.parts[i].vec)) & mask
	}
	return p, int(rLocal - start)
}

// drawFlat and drawEsc read the drawn member id, so the compiler cannot elide the
// locate. They are the SRANDMEMBER-shaped draw, non-mutating.
func (ps *pset) drawFlat(r uint64) uint64 {
	p, l := ps.flatLocate(r)
	sub := ps.parts[p]
	return sub.ids[sub.vec[l]]
}

func (ps *pset) drawEsc(r uint64) uint64 {
	p, l := ps.escLocate(r)
	sub := ps.parts[p]
	return sub.ids[sub.vec[l]]
}

// popFlat and popEsc are the SPOP kernel: draw, then swap-remove the drawn ordinal
// from the draw vector and reinsert it so the cardinality holds steady across the
// measurement (the group-total decrement is excluded to keep the set full, see the
// method note). The swap-remove is identical work in both arms, so the delta is
// the locate.
func (ps *pset) popFlat(r uint64) uint64 {
	p, l := ps.flatLocate(r)
	return ps.popAt(p, l)
}

func (ps *pset) popEsc(r uint64) uint64 {
	p, l := ps.escLocate(r)
	return ps.popAt(p, l)
}

func (ps *pset) popAt(part, local int) uint64 {
	sub := ps.parts[part]
	ord := sub.vec[local]
	id := sub.ids[ord]
	last := len(sub.vec) - 1
	sub.vec[local] = sub.vec[last]
	sub.vec[last] = ord
	return id
}

// pcg is the per-op random source, the section-5.6 per-shard PCG discipline.
type pcg struct{ state, inc uint64 }

func newPCG(seed uint64) *pcg {
	p := &pcg{inc: (seed << 1) | 1}
	p.next()
	p.state += seed
	p.next()
	return p
}

func (p *pcg) next() uint32 {
	old := p.state
	p.state = old*6364136223846793005 + p.inc
	x := uint32(((old >> 18) ^ old) >> 27)
	rot := uint32(old >> 59)
	return bits.RotateLeft32(x, -int(rot))
}

func (p *pcg) below(n uint64) uint64 {
	v := (uint64(p.next()) << 32) | uint64(p.next())
	return v % n
}

// derivP mirrors the engine deriveP at the production threshold: the next power of
// two at or above ceil(card/262144), floored at 4.
func derivP(card int) int {
	const target = 1 << 18
	q := (card + target - 1) / target
	p := 1
	for p < q {
		p <<= 1
	}
	if p < 4 {
		p = 4
	}
	return p
}

type cell struct {
	card                                       int
	P, k                                       int
	flatDrawNs, escDrawNs, flatPopNs, escPopNs float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	flag.Parse()

	cards := []int{2_000_000, 4_000_000}
	if *quick {
		cards = []int{500_000, 1_000_000}
	}
	ks := []int{2, 4, 8}

	var cells []cell
	for _, card := range cards {
		P := derivP(card)
		ps := newPSet(P, card)
		for i := 0; i < card; i++ {
			ps.insert(uint64(i + 1))
		}
		total := ps.total()
		flatDraw := timeDraw(ps.drawFlat, total)
		flatPop := timeDraw(ps.popFlat, total)
		for _, k := range ks {
			if k > P || P%k != 0 {
				continue
			}
			ps.esc = nil
			ps.escalate(k)
			cells = append(cells, cell{
				card: card, P: P, k: k,
				flatDrawNs: flatDraw,
				escDrawNs:  timeDraw(ps.drawEsc, total),
				flatPopNs:  flatPop,
				escPopNs:   timeDraw(ps.popEsc, total),
			})
		}
	}
	report(cells)
}

// timeDraw runs fn over random draw positions for at least minDur and returns
// ns/op. sink defeats dead-code elimination.
func timeDraw(fn func(uint64) uint64, total uint64) float64 {
	const minDur = 60 * time.Millisecond
	const inner = 1 << 16
	rng := newPCG(0x9e3779b97f4a7c15)
	var sink uint64
	reps := 0
	start := time.Now()
	for {
		for i := 0; i < inner; i++ {
			sink ^= fn(rng.below(total))
		}
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	_ = sink
	return float64(time.Since(start).Nanoseconds()) / float64(reps*inner)
}

func report(cells []cell) {
	fmt.Printf("F13 draw-escalation sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("ns/op single-thread; draw is locate+read, pop is the SPOP kernel (draw+swap-remove)\n\n")
	fmt.Printf("%9s %3s %3s %10s %10s %9s %10s %9s %9s\n",
		"card", "P", "k", "flatDraw", "escDraw", "drawTax", "flatPop", "escPop", "popTax")
	lastCard := 0
	for _, c := range cells {
		if c.card != lastCard {
			fmt.Println()
			lastCard = c.card
		}
		fmt.Printf("%9d %3d %3d %10.2f %10.2f %9.2f %10.2f %9.2f %9.2f\n",
			c.card, c.P, c.k, c.flatDrawNs, c.escDrawNs, c.escDrawNs-c.flatDrawNs,
			c.flatPopNs, c.escPopNs, c.escPopNs-c.flatPopNs)
	}
	projectScaling(cells)
}

// projectScaling reads the single-thread escalated draw into a fan-out projection:
// k workers each draw within their own group with no cross-core read on the hot
// path, so the aggregate ceiling is k / escDrawNs draws per second, minus the
// epoch-refresh of the cross-group totals. The real number comes from the box;
// this is the arithmetic the gate row checks against.
func projectScaling(cells []cell) {
	fmt.Printf("\nfan-out projection (single owner -> k workers, section 5.4):\n")
	fmt.Printf("the single-owner ceiling is 1/escDraw; k workers lift it to ~k/escDraw before\n")
	fmt.Printf("the epoch-refresh of cross-group totals, the real number is the gate box\n")
	fmt.Printf("%9s %3s %14s %16s\n", "card", "k", "1-owner Mops/s", "k-worker Mops/s")
	for _, c := range cells {
		one := 1000.0 / c.escDrawNs
		fmt.Printf("%9d %3d %14.2f %16.2f\n", c.card, c.k, one, one*float64(c.k))
	}
}
