// Lab: algebra maintenance cardinality floor (spec 2064/f3 doc 11 section 6,
// M1 lab 05).
//
// The question: doc 11 section 6.3 keeps each algebra-indexed set's sorted-hash
// arrays current inline at write time, in a bounded-tail form (a sorted run plus
// an unsorted tail of at most T entries, T=256). That makes SINTER-class algebra
// a sequential merge instead of a random probe (section 6.4, 6.6), which is the
// design's 2x lever (K16: 12ms merge against 40ms probe on a 1M-by-1M pair). But
// the maintenance taxes every write, and below some cardinality it cannot pay
// for itself: the setmergefloor lab already found the merge floor at 128 members
// on the smaller operand (line 428, the 1024 floor was wrong and left SINTER-256
// at 0.64x; floor 128 lifted it to 1.28x), which means below 128 the probe path
// wins the algebra outright and any maintenance is pure waste. This lab prices
// both sides of that trade, the write-time cost against the algebra-time win,
// and freezes the cardinality floor below which the arrays stay off, before the
// merge-kernel and algebra-driver slices bake the engagement rule in.
//
// Method: in-process, no server, no wire, no engine import. The merge and probe
// kernels are reused verbatim in shape from lab 03 (the frozen Swiss member
// table for the probe path, the section-6.6 two-pointer run-merge-tail cursor
// for the merge path). Two things are priced per cardinality:
//
//   - Write side: SADD-shaped build of a set's member table and draw vector,
//     with and without the section-6.3 sorted-run maintenance. The maintained
//     build appends each write to a bounded tail and, when the tail fills,
//     sorts the T entries and merges them into the sorted run (the tail-merge
//     amortization, line 419). The tax is maintained minus plain, ns per write.
//   - Algebra side: an equal-operand intersection at overlap 0.5 (the
//     equal-overlap gate shape, line 445), merge over the maintained arrays
//     against probe over the member table, the unordered fallback the driver
//     uses when no arrays exist. The win is probe minus merge, ns per op.
//
// From the two, break-even ops = tax * card / win: the number of intersections a
// set must take part in to repay maintaining its arrays across its whole build.
// Below the floor the win is zero or negative (probe wins) so no number of ops
// repays the tax; the floor is where the win turns positive and break-even
// becomes finite.
//
// Axes: cardinality {16 .. 65536} bracketing the 128 floor densely; member size
// {8, 16, 64} bytes (8 int-class, 16 the gate default, 64 the listpack-value cap
// line 234), since the setmergefloor result and the lab-03 crossover both move
// with member size. Read: plain and maintained write ns, the tax, merge and
// probe op ns, the win, and break-even ops. See README.md for the sweep and the
// frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
	"slices"
	"sort"
	"time"
)

const (
	ctrlEmpty = 0x80 // high bit set: empty slot; full slots hold a 7-bit tag
	groupW    = 8    // one 64-bit SWAR word per group
	maxLoad   = 0.875

	tailCap = 256 // doc 11 section 6.3: bounded unsorted tail T, knob 64-512

	lo = 0x0101010101010101
	hi = 0x8080808080808080
)

// mix is the splitmix64 finalizer, the same cheap strong hash labs 01, 03, and
// 04 use so every M1 lab prices the same probe shape.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// memberSet is a set of distinct members, each an id expanded to sz bytes in one
// slab so member i is slab[i*sz:i*sz+sz] with no per-member header (lab 03).
type memberSet struct {
	ids  []uint64
	sz   int
	slab []byte
}

func newMemberSet(ids []uint64, sz int) *memberSet {
	m := &memberSet{ids: ids, sz: sz, slab: make([]byte, len(ids)*sz)}
	for i, id := range ids {
		b := m.slab[i*sz : i*sz+sz]
		binary.LittleEndian.PutUint64(b, id)
		for j := 8; j < sz; j++ {
			b[j] = byte(id >> uint(j))
		}
	}
	return m
}

func (m *memberSet) key(i int) []byte { return m.slab[i*m.sz : i*m.sz+m.sz] }

func tagOf(h uint64) byte    { return byte(h) & 0x7f }
func slotOf(h uint64) uint64 { return h >> 7 }

func swarMatch(word uint64, tag byte) uint64 {
	cmp := word ^ (lo * uint64(tag))
	return (cmp - lo) &^ cmp & hi
}

func idx(mask uint64) int { return bits.TrailingZeros64(mask) >> 3 }

// table is the doc's frozen member table (lab 03), the probe path's structure.
// newEmptyTable presizes to n at 7/8 load and inserts nothing, so the write
// sweep can time the inserts themselves without a rehash confound.
type table struct {
	mask uint32
	ctrl []byte
	ord  []uint32
	set  *memberSet
}

func newEmptyTable(n int, set *memberSet) *table {
	capPow2 := uint32(groupW)
	for float64(n) > maxLoad*float64(capPow2) {
		capPow2 <<= 1
	}
	ctrl := make([]byte, capPow2)
	for i := range ctrl {
		ctrl[i] = ctrlEmpty
	}
	return &table{mask: capPow2 - 1, ctrl: ctrl, ord: make([]uint32, capPow2), set: set}
}

func newTable(set *memberSet) *table {
	t := newEmptyTable(len(set.ids), set)
	for i := range set.ids {
		t.insert(uint32(i))
	}
	return t
}

func (t *table) insert(ord uint32) {
	h := mix(t.set.ids[ord])
	tag := tagOf(h)
	numG := (t.mask + 1) / groupW
	g := uint32(slotOf(h)) & (numG - 1)
	step := uint32(1)
	for {
		base := g * groupW
		word := binary.LittleEndian.Uint64(t.ctrl[base:])
		if empt := word & hi; empt != 0 {
			slot := base + uint32(idx(empt))
			t.ctrl[slot] = tag
			t.ord[slot] = ord
			return
		}
		g = (g + step) & (numG - 1)
		step++
	}
}

func (t *table) contains(key []byte, h uint64) bool {
	tag := tagOf(h)
	numG := (t.mask + 1) / groupW
	g := uint32(slotOf(h)) & (numG - 1)
	step := uint32(1)
	sz := t.set.sz
	for {
		base := g * groupW
		word := binary.LittleEndian.Uint64(t.ctrl[base:])
		m := swarMatch(word, tag)
		for m != 0 {
			slot := base + uint32(idx(m))
			o := t.ord[slot]
			if bytesEqual(t.set.slab[int(o)*sz:int(o)*sz+sz], key) {
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

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// hpair is one sorted-hash-array entry: member hash and ordinal, 16B in the
// engine (lab 03, section 6.1). Sorted by hash.
type hpair struct {
	h   uint64
	ord uint32
}

// hset is the section-6.3 bounded-tail representation: a sorted run plus a small
// unsorted tail. buildHSet makes the arrays directly by sorting, the cheap way
// to get a large operand's arrays for the algebra timing (the incremental
// maintainer below is what the write sweep times).
type hset struct {
	run  []hpair
	tail []hpair
	set  *memberSet
}

func buildHSet(set *memberSet, tailFill int) *hset {
	n := len(set.ids)
	if tailFill > tailCap {
		tailFill = tailCap
	}
	if tailFill > n {
		tailFill = n
	}
	runN := n - tailFill
	run := make([]hpair, runN)
	for i := 0; i < runN; i++ {
		run[i] = hpair{h: mix(set.ids[i]), ord: uint32(i)}
	}
	sort.Slice(run, func(a, b int) bool { return run[a].h < run[b].h })
	tail := make([]hpair, tailFill)
	for i := 0; i < tailFill; i++ {
		tail[i] = hpair{h: mix(set.ids[runN+i]), ord: uint32(runN + i)}
	}
	return &hset{run: run, tail: tail, set: set}
}

// side is the run-merge-tail cursor over one hset (section 6.6, lab 03). It sorts
// a private copy of the tail once at init and yields the merged run+tail stream
// in ascending hash order without copying the run.
type side struct {
	run, tail []hpair
	set       *memberSet
	ri, ti    int
}

func (s *side) init(h *hset) {
	s.run = h.run
	s.set = h.set
	s.ri, s.ti = 0, 0
	s.tail = make([]hpair, len(h.tail))
	copy(s.tail, h.tail)
	sort.Slice(s.tail, func(a, b int) bool { return s.tail[a].h < s.tail[b].h })
}

func (s *side) done() bool { return s.ri >= len(s.run) && s.ti >= len(s.tail) }

func (s *side) pickTail() bool {
	if s.ri >= len(s.run) {
		return true
	}
	if s.ti >= len(s.tail) {
		return false
	}
	return s.tail[s.ti].h < s.run[s.ri].h
}

func (s *side) head() uint64 {
	if s.pickTail() {
		return s.tail[s.ti].h
	}
	return s.run[s.ri].h
}

func (s *side) headKey() []byte {
	var ord uint32
	if s.pickTail() {
		ord = s.tail[s.ti].ord
	} else {
		ord = s.run[s.ri].ord
	}
	return s.set.slab[int(ord)*s.set.sz : int(ord)*s.set.sz+s.set.sz]
}

func (s *side) advance() {
	if s.pickTail() {
		s.ti++
	} else {
		s.ri++
	}
}

// mergeIntersect is the section-6.6 two-pointer intersection, streaming both
// sides, byte-confirm folded into the emit (lab 03). Returns the match count.
func mergeIntersect(a, b *hset) int {
	var sa, sb side
	sa.init(a)
	sb.init(b)
	out := 0
	for !sa.done() && !sb.done() {
		ha, hb := sa.head(), sb.head()
		switch {
		case ha < hb:
			sa.advance()
		case ha > hb:
			sb.advance()
		default:
			if bytesEqual(sa.headKey(), sb.headKey()) {
				out++
			}
			sa.advance()
			sb.advance()
		}
	}
	return out
}

// probeIntersect is the section-6.4 probe path, the unordered fallback: iterate
// the small operand and probe the large operand's member table (lab 03).
func probeIntersect(small *memberSet, big *table) int {
	out := 0
	for i := range small.ids {
		if big.contains(small.key(i), mix(small.ids[i])) {
			out++
		}
	}
	return out
}

// maintainer is the section-6.3 inline write-time maintenance: a sorted run plus
// a bounded unsorted tail. add appends to the tail; when the tail fills, flush
// sorts the T entries and merges them into the run in one pass (the tail-merge
// amortization, line 419). The final partial tail is left unsorted, as the doc
// specifies, because the reader sorts it once on command entry.
//
// Two engine-faithful choices keep the measured tax honest rather than inflated
// by lab plumbing: the tail sort is slices.SortFunc (generic, no reflection),
// not sort.Slice, and the run merge double-buffers into a reused spare so a
// flush allocates nothing on the steady path. A naive sort.Slice-plus-alloc
// maintainer measures 3 to 5x higher and would misprice the floor.
type maintainer struct {
	run   []hpair
	tail  []hpair
	spare []hpair // reused merge target, grown once, so flush does not allocate
	cap   int     // tail cap; T=tailCap fixed, or n/16 scaled to hold run length
}

func (m *maintainer) add(h uint64, ord uint32) {
	m.tail = append(m.tail, hpair{h: h, ord: ord})
	if len(m.tail) >= m.cap {
		m.flush()
	}
}

func (m *maintainer) flush() {
	slices.SortFunc(m.tail, func(a, b hpair) int {
		switch {
		case a.h < b.h:
			return -1
		case a.h > b.h:
			return 1
		default:
			return 0
		}
	})
	need := len(m.run) + len(m.tail)
	if cap(m.spare) < need {
		m.spare = make([]hpair, 0, need)
	}
	merged := m.spare[:0]
	i, j := 0, 0
	for i < len(m.run) && j < len(m.tail) {
		if m.run[i].h <= m.tail[j].h {
			merged = append(merged, m.run[i])
			i++
		} else {
			merged = append(merged, m.tail[j])
			j++
		}
	}
	merged = append(merged, m.run[i:]...)
	merged = append(merged, m.tail[j:]...)
	// Swap run and spare so next flush reuses the old run's backing array.
	m.spare = m.run
	m.run = merged
	m.tail = m.tail[:0]
}

// timePlain times the SADD-shaped build of a set's member table and draw vector
// with no algebra maintenance, ns per write, built fresh each rep so the timing
// is a real build not a re-add.
func timePlain(set *memberSet) float64 {
	const minDur = 30 * time.Millisecond
	n := len(set.ids)
	reps := 0
	var sink uint32
	start := time.Now()
	for {
		tab := newEmptyTable(n, set)
		vec := make([]uint32, 0, n)
		for i := 0; i < n; i++ {
			tab.insert(uint32(i))
			vec = append(vec, uint32(i))
		}
		sink += vec[n-1] + uint32(tab.ctrl[0])
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	_ = sink
	return float64(time.Since(start).Nanoseconds()) / float64(reps*n)
}

// timeMaintained times the same build plus the section-6.3 sorted-run
// maintenance, ns per write. The delta against timePlain is the maintenance tax.
func timeMaintained(set *memberSet) float64 {
	const minDur = 30 * time.Millisecond
	n := len(set.ids)
	reps := 0
	var sink int
	start := time.Now()
	for {
		tab := newEmptyTable(n, set)
		vec := make([]uint32, 0, n)
		m := maintainer{cap: tailCap}
		for i := 0; i < n; i++ {
			tab.insert(uint32(i))
			vec = append(vec, uint32(i))
			m.add(mix(set.ids[i]), uint32(i))
		}
		sink += len(m.run) + len(m.tail) + int(vec[n-1])
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	_ = sink
	return float64(time.Since(start).Nanoseconds()) / float64(reps*n)
}

// scaledTail returns the tail cap that holds the run-length term of the flush at
// a constant, T = max(tailCap, n/16). At this cap the amortized flush is
// (n + T)/T ~= 17 element copies per write regardless of n, which is the doc's
// own line-420 arithmetic; the fixed tailCap does not hold it once the run grows
// past ~T*16 (section 6.3 tension).
func scaledTail(n int) int {
	t := n / 16
	if t < tailCap {
		t = tailCap
	}
	return t
}

// timeMaintainedScaled is timeMaintained with the tail cap scaled to n/16, to
// show the maintenance tax the slice can actually hold to the P9 <=15ns bound.
func timeMaintainedScaled(set *memberSet) float64 {
	const minDur = 30 * time.Millisecond
	n := len(set.ids)
	cap := scaledTail(n)
	reps := 0
	var sink int
	start := time.Now()
	for {
		tab := newEmptyTable(n, set)
		vec := make([]uint32, 0, n)
		m := maintainer{cap: cap}
		for i := 0; i < n; i++ {
			tab.insert(uint32(i))
			vec = append(vec, uint32(i))
			m.add(mix(set.ids[i]), uint32(i))
		}
		sink += len(m.run) + len(m.tail) + int(vec[n-1])
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
	}
	_ = sink
	return float64(time.Since(start).Nanoseconds()) / float64(reps*n)
}

func timeOp(op func()) float64 {
	const minDur = 30 * time.Millisecond
	reps := 0
	start := time.Now()
	for {
		op()
		reps++
		if reps >= 5 && time.Since(start) >= minDur {
			break
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

var cardList = []int{16, 32, 64, 96, 128, 192, 256, 512, 1024, 4096, 16384, 65536}

type cell struct {
	card      int
	sz        int
	plainNs   float64
	maintNs   float64
	tax       float64
	scaledNs  float64
	scaledTax float64
	scaledCap int
	mergeNs   float64
	probeNs   float64
	win       float64
	breakEv   float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinality ceiling for a fast check")
	flag.Parse()

	cards := cardList
	if *quick {
		cards = []int{16, 64, 128, 256, 1024, 4096}
	}
	sizes := []int{8, 16, 64}

	var cells []cell
	for _, sz := range sizes {
		for _, card := range cards {
			cells = append(cells, run(card, sz))
		}
	}
	report(cells)
	if !*quick {
		largeAlgebra()
	}
}

// largeAlgebra decouples the algebra-win half from the write tax and pushes the
// operands past the write-sweep ceiling to see whether merge crosses probe on
// this box at all. The sorted arrays are built directly with buildHSet (a sort,
// not the incremental maintainer), so cardinality is not bounded by the O(n^2/T)
// maintained build. K16 measured merge winning at 1M-by-1M on GamingPC (12ms vs
// 40ms); this sweep reports whether the M4's large caches keep probe ahead, the
// same residency effect lab 03 saw.
func largeAlgebra() {
	fmt.Printf("\nlarge-N algebra, merge vs probe (arrays built by direct sort, overlap 0.5):\n")
	fmt.Printf("%3s %9s %11s %11s %11s %8s\n", "sz", "card", "mergeNs", "probeNs", "winNs", "verdict")
	for _, sz := range []int{16, 64} {
		for _, card := range []int{262144, 1000000} {
			aIDs := make([]uint64, card)
			for i := range aIDs {
				aIDs[i] = uint64(i + 1)
			}
			a := newMemberSet(aIDs, sz)
			b := makeOperand(card, 0.5, sz)
			aHS := buildHSet(a, tailCap)
			bHS := buildHSet(b, tailCap)
			aTab := newTable(a)
			if mergeIntersect(aHS, bHS) != probeIntersect(b, aTab) {
				panic("large-N kernel disagreement")
			}
			mNs := timeOp(func() { mergeIntersect(aHS, bHS) })
			pNs := timeOp(func() { probeIntersect(b, aTab) })
			verdict := "probe"
			if pNs > mNs {
				verdict = "merge"
			}
			fmt.Printf("%3d %9d %11.0f %11.0f %11.0f %8s\n", sz, card, mNs, pNs, pNs-mNs, verdict)
		}
	}
}

func run(card, sz int) cell {
	c := cell{card: card, sz: sz}

	// Operand A: ids [1..card]. Operand B: card members at overlap 0.5 against A.
	aIDs := make([]uint64, card)
	for i := range aIDs {
		aIDs[i] = uint64(i + 1)
	}
	a := newMemberSet(aIDs, sz)
	b := makeOperand(card, 0.5, sz)

	c.plainNs = timePlain(a)
	c.maintNs = timeMaintained(a)
	c.tax = c.maintNs - c.plainNs
	c.scaledNs = timeMaintainedScaled(a)
	c.scaledTax = c.scaledNs - c.plainNs
	c.scaledCap = scaledTail(card)

	// Algebra: merge over both operands' arrays, probe over A's member table.
	aHS := buildHSet(a, tailCap)
	bHS := buildHSet(b, tailCap)
	aTab := newTable(a)

	om := mergeIntersect(aHS, bHS)
	op := probeIntersect(b, aTab)
	if om != op {
		panic(fmt.Sprintf("kernel disagreement card=%d sz=%d: merge=%d probe=%d", card, sz, om, op))
	}

	c.mergeNs = timeOp(func() { mergeIntersect(aHS, bHS) })
	c.probeNs = timeOp(func() { probeIntersect(b, aTab) })
	c.win = c.probeNs - c.mergeNs
	if c.win > 0 {
		c.breakEv = c.tax * float64(card) / c.win
	} else {
		c.breakEv = -1 // merge does not beat probe: maintenance never repays
	}
	return c
}

// makeOperand builds card distinct members, floor(ov*card) of them drawn from
// A's id space [1..card] so they are guaranteed present, the rest from a disjoint
// high range so they are guaranteed absent, fixing the intersection at
// floor(ov*card) exactly as lab 03's makeSmall does.
func makeOperand(card int, ov float64, sz int) *memberSet {
	inCount := int(ov*float64(card) + 0.5)
	ids := make([]uint64, card)
	stride := 1
	if inCount > 0 {
		stride = card / inCount
		if stride < 1 {
			stride = 1
		}
	}
	for i := 0; i < inCount; i++ {
		id := uint64(i*stride + 1)
		if id > uint64(card) {
			id = uint64(card)
		}
		ids[i] = id
	}
	base := uint64(card) + 1<<40
	for i := inCount; i < card; i++ {
		ids[i] = base + uint64(i)
	}
	return newMemberSet(ids, sz)
}

func report(cells []cell) {
	fmt.Printf("algebra maintenance floor sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("write ns/op plain vs maintained; tax is the delta; win is probe-merge; breakEv is tax*card/win (ops to repay)\n")
	fmt.Printf("breakEv -1 means merge does not beat probe, so maintenance never repays (below the floor)\n\n")
	fmt.Printf("tax is fixed-T=256 maintenance; scaledTax is T=n/16 maintenance (holds the run-length term)\n\n")
	fmt.Printf("%3s %7s %8s %7s %8s %8s %10s %10s %10s %9s\n",
		"sz", "card", "plainNs", "tax", "sclCap", "sclTax", "mergeNs", "probeNs", "win", "breakEv")
	lastSz := 0
	for _, c := range cells {
		if c.sz != lastSz {
			fmt.Println()
			lastSz = c.sz
		}
		be := fmt.Sprintf("%.0f", c.breakEv)
		if c.breakEv < 0 {
			be = "never"
		}
		fmt.Printf("%3d %7d %8.2f %7.2f %8d %8.2f %10.0f %10.0f %10.0f %9s\n",
			c.sz, c.card, c.plainNs, c.tax, c.scaledCap, c.scaledTax,
			c.mergeNs, c.probeNs, c.win, be)
	}
	reportFloor(cells)
}

// reportFloor prints, per member size, the least swept cardinality at which the
// algebra win turns positive and stays positive (merge beats probe for that card
// and every larger swept card), which is the maintenance floor.
func reportFloor(cells []cell) {
	fmt.Printf("\nmaintenance floor per member size (least card with merge > probe and staying so):\n")
	fmt.Printf("%3s %10s %14s\n", "sz", "floorCard", "breakEvAtFloor")
	sizes := []int{}
	seen := map[int]bool{}
	for _, c := range cells {
		if !seen[c.sz] {
			seen[c.sz] = true
			sizes = append(sizes, c.sz)
		}
	}
	for _, sz := range sizes {
		var rows []cell
		for _, c := range cells {
			if c.sz == sz {
				rows = append(rows, c)
			}
		}
		floor := 0
		var beAt float64
		for i, c := range rows {
			if c.win <= 0 {
				continue
			}
			all := true
			for _, cc := range rows[i:] {
				if cc.win <= 0 {
					all = false
					break
				}
			}
			if all {
				floor = c.card
				beAt = c.breakEv
				break
			}
		}
		label := fmt.Sprintf("%d", floor)
		if floor == 0 {
			label = ">65536"
		}
		fmt.Printf("%3d %10s %14.0f\n", sz, label, beAt)
	}
}
