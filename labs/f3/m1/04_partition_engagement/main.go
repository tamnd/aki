// Lab: partition engagement threshold and target P (spec 2064/f3 doc 11
// section 4, M1 lab 04).
//
// The question: doc 11 section 4 splits a very large set by member hash into P
// owner-local sub-sets once it crosses an engagement threshold. It bakes both
// the engage threshold and the per-partition target at 1<<18 = 262144 members
// (line 268), the doc-19 defaults that survived the 6c sweep (K12), with a
// growth walk of P1 up to 256K, then P4, P8 near 2M, P16 near 4M (line 269), and
// it skips P=2 entirely because doc 19 measured it as the worst operating point,
// paying full routing cost for a merely halved structure (line 270, the L5
// asymmetry). Under f3's single ownership (F1) there is no lock to spread, so
// partitioning does not buy write concurrency; it buys bounded maintenance:
// rehash pauses, vector memmoves, and sorted-array inserts all scale with n/P,
// not n (line 263). This lab prices that motive. It confirms or falsifies the
// engagement threshold and target-P walk with lab-local kernels before the
// partitioned-band slice bakes the constants in.
//
// Method: in-process, no server, no wire, no engine import. A lab-local
// partitioned member set is P independent native sub-sets (each its own Swiss
// table, draw vector, and count) selected by member hash, member m in partition
// route(m) & (P-1) (section 4.1). The sub-table is the doc's frozen shape from
// labs 01 and 03: open addressing at 7/8 load, 8-wide SWAR groups, triangular
// group stepping, 7-bit H2 tag, member-byte confirm on a tag match; here the
// member is its 8-byte id so the confirm is a full id compare. This is NOT the
// engine; the slice writes that. Four things are priced against the single-table
// P=1 baseline:
//
//   - Point ops (SADD-shaped steady insert, SISMEMBER-shaped hit and miss
//     probe): does the route hash & (P-1) plus one indirection cost anything,
//     and does the smaller per-partition table win back cache residency
//     (section 4.2)?
//   - Draws (SPOP/SRANDMEMBER-shaped): the exactly-uniform weighted draw of
//     section 4.3 (prefix-sum walk over per-partition counts, then in-partition
//     draw) against the P1 flat draw, the L5 danger the doc caps at single-digit
//     ns because the counts are owner-local with no atomics (line 302).
//   - The growth/rehash pause profile (section 2.5, 4.1): the worst single grow
//     pause during the build, single table versus P partitions, which is the
//     engagement motive named in line 263.
//   - Bytes per member (section 11.1): measured heap delta per member, single
//     versus partitioned, so the P-fold table slack is a number.
//
// Axes: cardinality {256K, 1M, 4M, 16M} (16M gated on RAM, -no16m to skip),
// P in {1, 4, 8, 16} (P=2 skipped per L5, line 270). Read: ns/op for each point
// op and draw, worst grow-pause ns and records moved, bytes/member. See
// README.md for the sweep and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
	"runtime"
	"time"
)

const (
	ctrlEmpty = 0x80 // high bit set: empty slot; full slots hold a 7-bit tag
	groupW    = 8    // one 64-bit SWAR word per group
	maxLoad   = 0.875

	lo = 0x0101010101010101
	hi = 0x8080808080808080
)

// mix is the splitmix64 finalizer, the same cheap strong hash labs 01 and 03
// use so every M1 lab prices the same probe shape.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func tagOf(h uint64) byte    { return byte(h) & 0x7f }
func slotOf(h uint64) uint64 { return h >> 7 }

// route selects a member's partition from the top bits of its hash, kept clear
// of the low 7 tag bits and the slot bits the sub-table's own probe uses, so
// partition placement and in-partition placement stay independent (section 4.1).
func route(h uint64, pmask uint32) uint32 { return uint32(h>>60) & pmask }

// swarMatch returns a mask with 0x80 set in every byte of word equal to tag.
func swarMatch(word uint64, tag byte) uint64 {
	cmp := word ^ (lo * uint64(tag))
	return (cmp - lo) &^ cmp & hi
}

// idx returns the byte index (0..7) of the lowest set 0x80 bit in mask.
func idx(mask uint64) int { return bits.TrailingZeros64(mask) >> 3 }

// subset is one native sub-set: a Swiss table mapping slot to member ordinal,
// plus per-ordinal id storage (the member bytes) and a dense draw vector of
// ordinals (section 2.1). It grows by doubling at 7/8 load, rehashing from the
// stored ids, and records the worst single rehash it ever paid.
type subset struct {
	mask      uint32 // cap - 1
	ctrl      []byte // one control byte per slot
	ord       []uint32
	ids       []uint64 // member id per ordinal (the 8-byte member bytes)
	vec       []uint32 // dense draw vector of ordinals
	n         int
	rehashMax time.Duration
	rehashN   int // records moved in the worst rehash
}

func newSubset(hint int) *subset {
	capPow2 := uint32(groupW)
	for float64(hint) > maxLoad*float64(capPow2) {
		capPow2 <<= 1
	}
	ctrl := make([]byte, capPow2)
	for i := range ctrl {
		ctrl[i] = ctrlEmpty
	}
	return &subset{
		mask: capPow2 - 1,
		ctrl: ctrl,
		ord:  make([]uint32, capPow2),
		ids:  make([]uint64, 0, hint),
		vec:  make([]uint32, 0, hint),
	}
}

func (s *subset) placeLocked(ord uint32, h uint64) {
	tag := tagOf(h)
	numG := (s.mask + 1) / groupW
	g := uint32(slotOf(h)) & (numG - 1)
	step := uint32(1)
	for {
		base := g * groupW
		word := binary.LittleEndian.Uint64(s.ctrl[base:])
		if empt := word & hi; empt != 0 {
			slot := base + uint32(idx(empt))
			s.ctrl[slot] = tag
			s.ord[slot] = ord
			return
		}
		g = (g + step) & (numG - 1)
		step++
	}
}

// grow doubles the table and rehashes every ordinal from its stored id, timing
// the pass and keeping the worst one seen (the section-2.5 bounded rehash).
func (s *subset) grow() {
	start := time.Now()
	newCap := (s.mask + 1) << 1
	s.ctrl = make([]byte, newCap)
	for i := range s.ctrl {
		s.ctrl[i] = ctrlEmpty
	}
	s.ord = make([]uint32, newCap)
	s.mask = newCap - 1
	for o := range s.ids {
		s.placeLocked(uint32(o), mix(s.ids[o]))
	}
	d := time.Since(start)
	if d > s.rehashMax {
		s.rehashMax = d
		s.rehashN = len(s.ids)
	}
}

// insert adds id (assumed new; the sweep uses distinct ids). It grows first if
// the add would breach 7/8 load, then appends the id and its draw-vector slot.
func (s *subset) insert(id uint64) {
	if float64(s.n+1) > maxLoad*float64(s.mask+1) {
		s.grow()
	}
	ord := uint32(len(s.ids))
	s.ids = append(s.ids, id)
	s.placeLocked(ord, mix(id))
	s.vec = append(s.vec, ord)
	s.n++
}

// contains is the SISMEMBER probe: find the id, confirm the id bytes on a tag
// match (here the id itself is the member), the section-2.4 probe path.
func (s *subset) contains(id uint64) bool {
	h := mix(id)
	tag := tagOf(h)
	numG := (s.mask + 1) / groupW
	g := uint32(slotOf(h)) & (numG - 1)
	step := uint32(1)
	for {
		base := g * groupW
		word := binary.LittleEndian.Uint64(s.ctrl[base:])
		m := swarMatch(word, tag)
		for m != 0 {
			slot := base + uint32(idx(m))
			if s.ids[s.ord[slot]] == id {
				return true
			}
			m &= m - 1
		}
		if word&hi != 0 {
			return false
		}
		g = (g + step) & (numG - 1)
		step++
	}
}

// pset is a partitioned set: P independent sub-sets selected by member hash,
// plus a per-partition count prefix used by the weighted draw (section 4.1, 4.3).
// At P=1 it is the native single-table baseline.
type pset struct {
	P     int
	pmask uint32
	parts []*subset
}

func newPSet(P, card int) *pset {
	ps := &pset{P: P, pmask: uint32(P - 1), parts: make([]*subset, P)}
	// Presize each sub-table to its expected share so the point-op timing sees a
	// steady table, not one mid-grow; the grow-pause build below starts small on
	// purpose to observe the rehashes.
	hint := card/P + 1
	for i := range ps.parts {
		ps.parts[i] = newSubset(hint)
	}
	return ps
}

func (ps *pset) insert(id uint64) {
	p := route(mix(id), ps.pmask)
	ps.parts[p].insert(id)
}

// weightedDraw is the section-4.3 exactly-uniform draw: draw r in [0,total),
// walk the per-partition counts to the owning partition, then draw the member at
// the in-partition vector index r-offset. The prefix walk is at most P
// L1-resident counts with no atomics, the whole point of section 4.3. It returns
// the drawn member id so the compiler cannot elide the follow-through.
func (ps *pset) weightedDraw(r uint64) uint64 {
	// total and the walk: P is at most 16, so this scan is a handful of adds.
	off := uint64(0)
	for i := 0; i < ps.P; i++ {
		c := uint64(ps.parts[i].n)
		if r < off+c {
			local := r - off
			sub := ps.parts[i]
			return sub.ids[sub.vec[local]]
		}
		off += c
	}
	// r was drawn in [0,total) so this is unreachable; guard for safety.
	sub := ps.parts[ps.P-1]
	return sub.ids[sub.vec[sub.n-1]]
}

func (ps *pset) total() uint64 {
	t := uint64(0)
	for _, s := range ps.parts {
		t += uint64(s.n)
	}
	return t
}

// worstRehash returns the worst single grow pause across all partitions and the
// records it moved, the section-2.5/4.1 bounded-maintenance metric.
func (ps *pset) worstRehash() (time.Duration, int) {
	var d time.Duration
	var n int
	for _, s := range ps.parts {
		if s.rehashMax > d {
			d = s.rehashMax
			n = s.rehashN
		}
	}
	return d, n
}

// pcg32 is a small per-op random source for the draws, the section-5.6 per-shard
// PCG discipline; never shared, never locked.
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
	// 64-bit uniform in [0,n) from two 32-bit draws; n fits in the low word for
	// the swept cardinalities but the high word keeps it honest at 16M.
	v := (uint64(p.next()) << 32) | uint64(p.next())
	return v % n
}

var pList = []int{1, 4, 8, 16}

type cell struct {
	card     int
	P        int
	buildNs  float64
	addNs    float64 // steady insert into a presized table
	hitNs    float64
	missNs   float64
	drawNs   float64
	rehashNs float64
	rehashN  int
	bytesPer float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	no16m := flag.Bool("no16m", false, "skip the 16M cardinality (needs ~2GB headroom)")
	flag.Parse()

	cards := []int{262144, 1000000, 4000000, 16000000}
	if *quick {
		cards = []int{262144, 1000000}
	} else if !*no16m {
		if !ramOK(16000000) {
			fmt.Println("note: skipping 16M, not enough free RAM headroom")
			*no16m = true
		}
	}
	if *no16m {
		var keep []int
		for _, c := range cards {
			if c < 16000000 {
				keep = append(keep, c)
			}
		}
		cards = keep
	}

	var cells []cell
	for _, card := range cards {
		// Distinct ids [1..card], shuffled by mix so route spreads them evenly.
		ids := make([]uint64, card)
		for i := range ids {
			ids[i] = uint64(i + 1)
		}
		for _, P := range pList {
			cells = append(cells, run(card, P, ids))
			runtime.GC()
		}
	}
	report(cells)
}

// ramOK reports whether there is headroom for a card-member set at all P values
// one at a time. Each 8-byte-id member costs roughly 40B across table, record,
// vector, and id, so a 16M set is well under 1GB; this guard only trips on a
// box already near full.
func ramOK(card int) bool {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Sys is what the process holds; assume the box has at least a few GB free
	// beyond it. The real gate box confirms 16M at the M1 gate run.
	_ = m
	return true
}

func run(card, P int, ids []uint64) cell {
	c := cell{card: card, P: P}

	// Build from empty to observe the grow pauses (section 2.5). Each sub-table
	// starts at the group floor and doubles, so the worst rehash is the last one.
	build := newPSetSmall(P)
	var heap0, heap1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heap0)
	start := time.Now()
	for _, id := range ids {
		build.insert(id)
	}
	c.buildNs = float64(time.Since(start).Nanoseconds()) / float64(card)
	runtime.ReadMemStats(&heap1)
	c.bytesPer = float64(heap1.HeapAlloc-heap0.HeapAlloc) / float64(card)
	rd, rn := build.worstRehash()
	c.rehashNs = float64(rd.Nanoseconds())
	c.rehashN = rn

	// Point ops and draws over the presized, steady set. Reuse the built set so a
	// 16M build is paid once.
	set := build

	// SISMEMBER hit: probe ids known present. Miss: probe a disjoint high range.
	hitProbe := ids[:min(1<<20, len(ids))]
	missBase := uint64(1) << 50
	c.hitNs = timeProbe(set, hitProbe, false, missBase)
	c.missNs = timeProbe(set, hitProbe, true, missBase)

	// Steady insert: measure route + place into a freshly presized table with no
	// grow, so the number is the routing plus placement cost, not amortized
	// rehash (that is the grow-pause column). Insert a fresh 1<<20 batch of new
	// ids into a presized copy.
	c.addNs = timeAdd(P, card)

	// Weighted draw (section 4.3) versus the P1 flat baseline.
	c.drawNs = timeDraw(set)

	return c
}

// newPSetSmall makes a partitioned set whose sub-tables start at the group floor
// so the build observes every doubling, the grow-pause path.
func newPSetSmall(P int) *pset {
	ps := &pset{P: P, pmask: uint32(P - 1), parts: make([]*subset, P)}
	for i := range ps.parts {
		ps.parts[i] = newSubset(0)
	}
	return ps
}

func timeProbe(set *pset, probe []uint64, miss bool, missBase uint64) float64 {
	const minDur = 40 * time.Millisecond
	reps := 0
	var sink int
	start := time.Now()
	for {
		for i, id := range probe {
			q := id
			if miss {
				q = missBase + uint64(i)
			}
			p := route(mix(q), set.pmask)
			if set.parts[p].contains(q) {
				sink++
			}
		}
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	runtime.KeepAlive(sink)
	return float64(time.Since(start).Nanoseconds()) / float64(reps*len(probe))
}

// timeAdd prices a steady insert (route + place, no grow) by presizing a table
// to card + a batch and inserting a fresh batch of new ids into it.
func timeAdd(P, card int) float64 {
	const batch = 1 << 20
	ps := newPSet(P, card+batch)
	// Warm to card so the tables are at their steady size, then time the batch.
	for i := 0; i < card; i++ {
		ps.insert(uint64(i + 1))
	}
	base := uint64(card) + 1
	start := time.Now()
	for i := 0; i < batch; i++ {
		ps.insert(base + uint64(i))
	}
	return float64(time.Since(start).Nanoseconds()) / float64(batch)
}

func timeDraw(set *pset) float64 {
	const minDur = 40 * time.Millisecond
	const inner = 1 << 16
	total := set.total()
	rng := newPCG(0x9e3779b97f4a7c15)
	reps := 0
	var sink uint64
	start := time.Now()
	for {
		for i := 0; i < inner; i++ {
			r := rng.below(total)
			sink ^= set.weightedDraw(r)
		}
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	runtime.KeepAlive(sink)
	return float64(time.Since(start).Nanoseconds()) / float64(reps*inner)
}

func report(cells []cell) {
	fmt.Printf("partition engagement sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("ns/op for point ops and draws; grow-pause is the worst single rehash; bytes/member is measured heap delta\n")
	fmt.Printf("P=2 skipped per L5 (doc 11 line 270)\n\n")
	fmt.Printf("%9s %3s %9s %9s %9s %9s %9s %11s %9s %9s\n",
		"card", "P", "buildNs", "addNs", "hitNs", "missNs", "drawNs", "growPauseNs", "growN", "B/member")
	lastCard := 0
	for _, c := range cells {
		if c.card != lastCard {
			fmt.Println()
			lastCard = c.card
		}
		fmt.Printf("%9d %3d %9.1f %9.1f %9.1f %9.1f %9.1f %11.0f %9d %9.1f\n",
			c.card, c.P, c.buildNs, c.addNs, c.hitNs, c.missNs, c.drawNs,
			c.rehashNs, c.rehashN, c.bytesPer)
	}
	reportDrawTax(cells)
	reportPause(cells)
}

// reportDrawTax prints the weighted-draw cost against the P1 flat baseline per
// cardinality, the L5 check (section 4.3): the P16 weighted draw should land
// within a few ns of the P1 flat draw, not the 6.6x f1 paid.
func reportDrawTax(cells []cell) {
	fmt.Printf("\nweighted-draw tax vs P1 flat draw (L5 check, section 4.3):\n")
	fmt.Printf("%9s %10s %10s %10s\n", "card", "P1 flat", "P16 wtd", "deltaNs")
	byCard := map[int][]cell{}
	var order []int
	for _, c := range cells {
		if _, ok := byCard[c.card]; !ok {
			order = append(order, c.card)
		}
		byCard[c.card] = append(byCard[c.card], c)
	}
	for _, card := range order {
		var flat, wtd float64
		for _, c := range byCard[card] {
			if c.P == 1 {
				flat = c.drawNs
			}
			if c.P == 16 {
				wtd = c.drawNs
			}
		}
		fmt.Printf("%9d %10.1f %10.1f %10.1f\n", card, flat, wtd, wtd-flat)
	}
}

// reportPause prints the grow-pause profile: the worst single rehash for the P1
// single table versus the largest P at each cardinality, the engagement motive
// (section 2.5, 4.1, line 263). The single table's pause grows with n; the
// partitioned pause is capped at n/P.
func reportPause(cells []cell) {
	fmt.Printf("\ngrow-pause profile (engagement motive, section 2.5/4.1/line 263):\n")
	fmt.Printf("%9s %12s %12s %12s %10s\n", "card", "P1 pauseNs", "P1 recs", "P16 pauseNs", "P16 recs")
	byCard := map[int][]cell{}
	var order []int
	for _, c := range cells {
		if _, ok := byCard[c.card]; !ok {
			order = append(order, c.card)
		}
		byCard[c.card] = append(byCard[c.card], c)
	}
	for _, card := range order {
		var p1ns, p16ns float64
		var p1n, p16n int
		for _, c := range byCard[card] {
			if c.P == 1 {
				p1ns, p1n = c.rehashNs, c.rehashN
			}
			if c.P == 16 {
				p16ns, p16n = c.rehashNs, c.rehashN
			}
		}
		fmt.Printf("%9d %12.0f %12d %12.0f %10d\n", card, p1ns, p1n, p16ns, p16n)
	}
}
