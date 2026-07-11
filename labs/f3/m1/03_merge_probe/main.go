// Lab: merge-versus-probe crossover k (spec 2064/f3 doc 11 section 6, M1 lab
// 03).
//
// The question: doc 11's algebra driver (section 6.4) chooses between two ways
// to intersect two sets. Probe iterates the smaller operand and probes the
// larger operand's member table at ~40ns per DRAM-resident probe (line 393).
// Merge streams both operands' sorted-hash arrays as two sequential runs the
// prefetcher loves (section 6.1, 6.6). For comparable sizes merge wins; as the
// larger operand grows the merge's streaming cost grows with it while the probe
// cost stays pinned to the smaller operand, so past some size ratio k =
// |big|/|small| probe wins again. Doc 11 line 415 and section 6.4 pre-register
// that crossover at k ~= 7 from the f1 K16 lineage. This lab confirms or
// falsifies it with lab-local kernels before the algebra-driver slice bakes the
// constant in.
//
// Method: in-process, no server, no wire, no engine import. Two lab-local
// kernels model the two driver paths (this is NOT the engine; the driver slice
// writes that). The probe kernel is the doc's frozen table from lab 01: Swiss
// open addressing at 7/8 load, 8-wide SWAR groups, triangular group stepping,
// 7-bit H2 tag, confirm member bytes on a tag match (section 2.1, lab 01
// verdict). The merge kernel is the doc's section-6.6 two-pointer intersection
// over sorted-hash arrays in the section-6.3 bounded-tail form: a sorted run
// plus a small unsorted tail sorted once on command entry and three-way merged,
// byte-confirm folded into the emit (section 6.1, mergeresolve). The crossover
// is set by the doc's own model (section 6.4): the merge streams both arrays, so
// its cost is O(|big|+|small|) while probe is O(|small|), and the streaming
// merge is the primary kernel. A galloping-advance merge (section 6.6, the
// asymmetric-degradation refinement) is measured alongside as a second kernel so
// the verdict can say what the driver's flat size-ratio constant deliberately
// gives up.
//
// Axes: k = |big|/|small| in {1,2,4,7,8,16,32,64} (log-spaced, 7 pinned so the
// pre-registered value is a measured row); small-set cardinality {1k, 100k};
// member size class {8, 32, 64} bytes (8 int-class, 64 the listpack-value cap
// doc 11 line 234, 32 between); overlap fraction (fraction of the small set also
// in big) {0.10, 0.50, 0.90}, the equal-overlap and skewed shapes the gate names
// (line 445, PRED-F3-M1-SINTER). Read: total op ns for each kernel (what the
// driver compares) and ns per output element, plus the least swept k where probe
// total beats streaming-merge total. See README.md for the sweep and verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/bits"
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

// mix is the splitmix64 finalizer, the same cheap strong hash lab 01 uses so the
// two labs price the same probe shape.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// memberSet is a set of distinct members. Each member is an id expanded to sz
// bytes in a single slab, so member i is slab[i*sz : i*sz+sz] with no per-member
// slice header. The hash of a member is mix(id), so distinct ids give distinct
// hashes up to the 64-bit collision floor, and the confirm step compares the
// full member bytes exactly as the engine does (doc 11 line 189, section 6.1).
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

// table is the doc's frozen member table, group scheme only (lab 01 verdict):
// 8-wide SWAR groups, triangular group stepping, 7-bit H2 tag, 7/8 load. It maps
// a slot to a member ordinal in the owning memberSet, and confirms a tag match
// by comparing member bytes.
type table struct {
	mask uint32 // cap - 1
	ctrl []byte // one control byte per slot, cap long
	ord  []uint32
	set  *memberSet
}

func newTable(set *memberSet) *table {
	// Capacity is the next power of two that keeps load at or under 7/8.
	n := len(set.ids)
	capPow2 := uint32(groupW)
	for float64(n) > maxLoad*float64(capPow2) {
		capPow2 <<= 1
	}
	ctrl := make([]byte, capPow2)
	for i := range ctrl {
		ctrl[i] = ctrlEmpty
	}
	t := &table{mask: capPow2 - 1, ctrl: ctrl, ord: make([]uint32, capPow2), set: set}
	for i := range set.ids {
		t.insert(uint32(i))
	}
	return t
}

func tagOf(h uint64) byte    { return byte(h) & 0x7f }
func slotOf(h uint64) uint64 { return h >> 7 }

// swarMatch returns a mask with 0x80 set in every byte of word equal to tag.
func swarMatch(word uint64, tag byte) uint64 {
	cmp := word ^ (lo * uint64(tag))
	return (cmp - lo) &^ cmp & hi
}

// idx returns the byte index (0..7) of the lowest set 0x80 bit in mask.
func idx(mask uint64) int { return bits.TrailingZeros64(mask) >> 3 }

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

// contains is the SISMEMBER probe: find key, confirm bytes on a tag match. key
// is the probing member's bytes (from the other set), so the confirm is a full
// byte compare against this table's slab, the section-6.4 probe path.
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

// hpair is one sorted-hash-array entry: the member hash and its ordinal into the
// owning memberSet, 16B in the engine (doc 11 section 6.1). Sorted by hash.
type hpair struct {
	h   uint64
	ord uint32
}

// hset is the doc's section-6.3 bounded-tail representation: a sorted run plus a
// small unsorted tail of recent writes. The reader sorts the tail once on
// command entry and treats the pair as one logical sorted stream (section 6.6),
// which the side cursor below does with no copy of the run.
type hset struct {
	run  []hpair // sorted by h
	tail []hpair // unsorted, len <= tailCap
	set  *memberSet
}

// buildHSet makes a sorted run from all but the last tailFill members and leaves
// the last tailFill as an unsorted tail, modelling a set that took tailFill
// writes since its last tail merge.
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

// side is a run-merge-tail cursor over one hset (doc 11 section 6.6). It sorts a
// copy of the tail once at init (the "sorted on entry to the command" step) and
// then yields the merged run+tail stream in ascending hash order without copying
// the run.
type side struct {
	run, tail []hpair
	set       *memberSet
	ri, ti    int
}

func (s *side) init(h *hset) {
	s.run = h.run
	s.set = h.set
	s.ri, s.ti = 0, 0
	// Sort a private copy of the tail so the source hset is left untouched.
	s.tail = make([]hpair, len(h.tail))
	copy(s.tail, h.tail)
	sort.Slice(s.tail, func(a, b int) bool { return s.tail[a].h < s.tail[b].h })
}

func (s *side) done() bool { return s.ri >= len(s.run) && s.ti >= len(s.tail) }

// pickTail reports whether the next stream element comes from the tail.
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

// mergeIntersect is the doc's section-6.6 two-pointer intersection, streaming
// both sides fully (the section-6.4 "n/P streaming cost" the crossover is
// derived from). On a hash tie it confirms member bytes and emits in one step,
// the mergeresolve discipline (2 DRAM touches per matched member). It returns the
// number of matched members.
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

// probeIntersect is the section-6.4 probe path: iterate the small operand and
// probe the large operand's member table. It returns the number of matches.
func probeIntersect(small *memberSet, big *table) int {
	out := 0
	for i := range small.ids {
		if big.contains(small.key(i), mix(small.ids[i])) {
			out++
		}
	}
	return out
}

// galloping merge, the section-6.6 refinement. advance doubles its step then
// binary-searches to skip a locally sparse run toward the other side's head,
// which lets asymmetric-but-above-floor cases degrade toward the probe cost
// instead of paying a full linear scan. It works on the sorted run only (the
// tail is at most tailCap and merged linearly), which is faithful because the
// run carries all but the last few writes.
func gallopIntersect(a, b *hset) int {
	var sa, sb side
	sa.init(a)
	sb.init(b)
	out := 0
	for !sa.done() && !sb.done() {
		ha, hb := sa.head(), sb.head()
		switch {
		case ha < hb:
			sa.gallopTo(hb)
		case ha > hb:
			sb.gallopTo(ha)
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

// gallopTo advances the run pointer to the first entry with hash >= target by
// doubling then binary search; the tail is stepped linearly since it is tiny.
func (s *side) gallopTo(target uint64) {
	if s.ri < len(s.run) && s.run[s.ri].h < target {
		step := 1
		bound := s.ri
		for bound+step < len(s.run) && s.run[bound+step].h < target {
			bound += step
			step <<= 1
		}
		lo := bound
		hiIdx := bound + step
		if hiIdx > len(s.run) {
			hiIdx = len(s.run)
		}
		for lo < hiIdx {
			mid := (lo + hiIdx) / 2
			if s.run[mid].h < target {
				lo = mid + 1
			} else {
				hiIdx = mid
			}
		}
		s.ri = lo
	}
	for s.ti < len(s.tail) && s.tail[s.ti].h < target {
		s.ti++
	}
}

var kList = []int{1, 2, 4, 7, 8, 16, 32, 64}

type cellCard struct {
	name  string
	small int
}

type cell struct {
	card     string
	small    int
	k        int
	sz       int
	overlap  float64
	out      int
	probeNs  float64
	mergeNs  float64
	gallopNs float64
	// Mechanism metrics: ns per single probe (probeNs / |small|) and ns per
	// streamed merge element (mergeNs / (|small| + |big|)). Their ratio minus one
	// is the crossover k the driver bakes in (section 6.4).
	perProbe     float64
	perMergeElem float64
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities for a fast check")
	flag.Parse()

	cards := []cellCard{{"1k", 1000}, {"100k", 100000}}
	if *quick {
		cards = []cellCard{{"1k", 1000}, {"10k", 10000}}
	}
	sizes := []int{8, 32, 64}
	overlaps := []float64{0.10, 0.50, 0.90}

	var cells []cell
	for _, cd := range cards {
		for _, sz := range sizes {
			for _, k := range kList {
				bigN := cd.small * k
				// big holds ids [1 .. bigN]. Build once, reused across overlaps.
				bigIDs := make([]uint64, bigN)
				for i := range bigIDs {
					bigIDs[i] = uint64(i + 1)
				}
				big := newMemberSet(bigIDs, sz)
				bigTab := newTable(big)
				bigHS := buildHSet(big, tailCap)

				for _, ov := range overlaps {
					small := makeSmall(cd.small, bigN, ov, sz)
					smallHS := buildHSet(small, tailCap)

					// Correctness: all three kernels agree on the match count.
					op := probeIntersect(small, bigTab)
					om := mergeIntersect(smallHS, bigHS)
					og := gallopIntersect(smallHS, bigHS)
					if op != om || op != og {
						panic(fmt.Sprintf("kernel disagreement card=%s sz=%d k=%d ov=%.2f: probe=%d merge=%d gallop=%d",
							cd.name, sz, k, ov, op, om, og))
					}

					pNs := timeOp(func() { probeIntersect(small, bigTab) })
					mNs := timeOp(func() { mergeIntersect(smallHS, bigHS) })
					gNs := timeOp(func() { gallopIntersect(smallHS, bigHS) })

					cells = append(cells, cell{
						card: cd.name, small: cd.small, k: k, sz: sz, overlap: ov,
						out: op, probeNs: pNs, mergeNs: mNs, gallopNs: gNs,
						perProbe:     pNs / float64(cd.small),
						perMergeElem: mNs / float64(cd.small+bigN),
					})
				}
			}
		}
	}
	report(cells)
}

// makeSmall builds a small set of n distinct members. floor(ov*n) of them are
// drawn from big's id space [1..bigN] at an even stride so they are guaranteed
// present in big; the rest come from a disjoint high id range so they are
// guaranteed absent, which fixes the intersection size at floor(ov*n).
func makeSmall(n, bigN int, ov float64, sz int) *memberSet {
	inCount := int(ov*float64(n) + 0.5)
	if inCount > bigN {
		inCount = bigN
	}
	ids := make([]uint64, n)
	// Present members: strided ids across big so the merge sees a real spread.
	stride := 1
	if inCount > 0 {
		stride = bigN / inCount
		if stride < 1 {
			stride = 1
		}
	}
	for i := 0; i < inCount; i++ {
		id := uint64(i*stride + 1)
		if id > uint64(bigN) {
			id = uint64(bigN)
		}
		ids[i] = id
	}
	// Absent members: a disjoint high range that never overlaps [1..bigN].
	base := uint64(bigN) + 1<<40
	for i := inCount; i < n; i++ {
		ids[i] = base + uint64(i)
	}
	return newMemberSet(ids, sz)
}

// timeOp runs op until at least minDur has elapsed over at least minReps runs,
// then returns ns per run. Adaptive so tiny cells still get enough reps and huge
// cells do not overrun.
func timeOp(op func()) float64 {
	const minDur = 40 * time.Millisecond
	const minReps = 5
	reps := 0
	start := time.Now()
	for {
		op()
		reps++
		if reps >= minReps && time.Since(start) >= minDur {
			break
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps)
}

func report(cells []cell) {
	fmt.Printf("merge-versus-probe crossover sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("total op ns per kernel; perOut is ns per matched member; crossover = least k with probe < merge\n\n")
	fmt.Printf("%-5s %3s %4s %5s %9s %11s %11s %11s %8s %8s\n",
		"card", "sz", "k", "ov", "out", "probeNs", "mergeNs", "gallopNs", "ns/prb", "ns/elm")
	last := ""
	for _, c := range cells {
		key := fmt.Sprintf("%s/%d/%.2f", c.card, c.sz, c.overlap)
		if key != last {
			fmt.Println()
			last = key
		}
		fmt.Printf("%-5s %3d %4d %5.2f %9d %11.0f %11.0f %11.0f %8.1f %8.2f\n",
			c.card, c.sz, c.k, c.overlap, c.out,
			c.probeNs, c.mergeNs, c.gallopNs, c.perProbe, c.perMergeElem)
	}
	reportCrossover(cells)
}

// reportCrossover prints, per (card, sz, overlap), the least swept k at which
// probe total beats streaming-merge total, which is the driver constant.
func reportCrossover(cells []cell) {
	fmt.Printf("\ncrossover k (least k with probeNs < mergeNs), streaming merge:\n")
	fmt.Printf("%-5s %3s %5s %10s\n", "card", "sz", "ov", "crossK")
	type gk struct {
		card string
		sz   int
		ov   float64
	}
	seen := map[gk]bool{}
	var order []gk
	for _, c := range cells {
		g := gk{c.card, c.sz, c.overlap}
		if !seen[g] {
			seen[g] = true
			order = append(order, g)
		}
	}
	for _, g := range order {
		// Crossover is the smallest swept k past which probe beats streaming merge
		// and stays winning for every larger swept k. Requiring all higher k to win
		// surfaces the large-member high-overlap band where merge reclaims the lead
		// at high k instead of hiding it behind the first k where probe happens to
		// win.
		probeWins := map[int]bool{}
		for _, k := range kList {
			for _, c := range cells {
				if c.card == g.card && c.sz == g.sz && c.overlap == g.ov && c.k == k {
					probeWins[k] = c.probeNs < c.mergeNs
					break
				}
			}
		}
		cross := 0
		for i, k := range kList {
			all := true
			for _, kk := range kList[i:] {
				if !probeWins[kk] {
					all = false
					break
				}
			}
			if all {
				cross = k
				break
			}
		}
		label := fmt.Sprintf("%d", cross)
		if cross == 0 {
			label = ">64"
		}
		fmt.Printf("%-5s %3d %5.2f %10s\n", g.card, g.sz, g.ov, label)
	}
	reportModel(cells)
}

// reportModel prints the crossover the mechanism costs imply. The driver picks
// probe when |small|*perProbe < (|small|+|big|)*perMergeElem, i.e. when
// k > perProbe/perMergeElem - 1. It shows that ratio from the measured miss-path
// probe cost on this box and from the K16/line-393 40ns DRAM-resident probe floor
// the gate box reaches, so the pre-registered k~7 can be judged in both regimes.
func reportModel(cells []cell) {
	fmt.Printf("\nmodel crossover k = perProbe/perMergeElem - 1 (miss-path ov=0.10 rows):\n")
	fmt.Printf("%-5s %3s %8s %8s %10s %12s\n",
		"card", "sz", "ns/prb", "ns/elm", "kMeasured", "kAt40nsDRAM")
	type gk struct {
		card string
		sz   int
	}
	seen := map[gk]bool{}
	var order []gk
	for _, c := range cells {
		g := gk{c.card, c.sz}
		if !seen[g] {
			seen[g] = true
			order = append(order, g)
		}
	}
	for _, g := range order {
		var prb, elm float64
		var n int
		for _, c := range cells {
			// Average the mechanism costs over the miss-dominated k>=8 rows where
			// the big table is largest and residency is clearest.
			if c.card == g.card && c.sz == g.sz && c.overlap == 0.10 && c.k >= 8 {
				prb += c.perProbe
				elm += c.perMergeElem
				n++
			}
		}
		if n == 0 {
			continue
		}
		prb /= float64(n)
		elm /= float64(n)
		fmt.Printf("%-5s %3d %8.1f %8.2f %10.1f %12.1f\n",
			g.card, g.sz, prb, elm, prb/elm-1, 40.0/elm-1)
	}
}
