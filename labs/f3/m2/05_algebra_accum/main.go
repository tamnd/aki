// Lab: zset algebra accumulation threshold (spec 2064/f3 doc 12 section 6.12,
// M2 lab 05).
//
// The question: the STORE forms ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE build a
// destination sorted set from an aggregated result keyed by member. The union
// is where the accumulation structure matters, because every input member has
// to be folded together across all sources with weighted-score aggregation
// (SUM, MIN, MAX) before anything can be emitted. Doc 12 line 554 makes the
// k-way merge over per-source member-sorted runs the primary ZUNION plan and
// keeps hash accumulation over the largest source as the degradation path
// (Redis's own strategy). Doc 12 line 562 then says every STORE form sorts the
// aggregated result by (sortable score, member) once and bulk-loads the
// destination B+ tree at 0.9 fill. So there are two coupled choices to settle
// before the slice-7 driver bakes them:
//
//   - The accumulation structure: hash-accumulate then sort, versus a k-way
//     tournament merge over member-sorted runs then sort, versus accumulate
//     straight into a score-ordered tree (maintain-sorted, delete-reinsert per
//     aggregation update).
//   - The sort tax: the destination bulk load wants score order, but the
//     aggregation is keyed by member, so a final sort by score is forced unless
//     the structure keeps score order live. Sort-at-end versus maintain-sorted.
//
// This lab prices all three structures on the union path, sweeps the operand
// count and result cardinality, brackets with equal-overlap and disjoint
// shapes, and freezes the winner per regime plus the crossover for the driver.
// It mirrors the M1 lab-05 house style (in-process, no server, no wire, no
// engine import) and it carries the same darwin caveat: a winner within noise
// on this box gets its DRAM-regime confirmation at the M2 gate run.
//
// Method. Members are modeled by a uint64 id encoded big-endian into sz bytes,
// so byte order equals id order and member equality is id equality; this prices
// the 8-byte score-prefix compare that dominates routing (section 3.1), with
// long-member memcmp tails a separate lex concern (section 3.2) out of scope
// here. Scores are small integers held as float64 so SUM is associativity-exact
// and every kernel's aggregate agrees bit for bit, which is what lets the
// cross-check compare results exactly rather than within an epsilon.
//
// The three kernels all return the aggregated result as respairs sorted by
// (sortable score, member), the exact input the destination bulk load consumes:
//
//   - hashAccum: an open-addressed member->score accumulator (the M1 Swiss-table
//     lineage, keyed by id), aggregate every input, extract, sort by score.
//   - mergeAccum: one member-sorted run per source (the hash slot walk), a
//     tournament k-way merge folding equal members with aggregation, then sort
//     the member-ordered result by score.
//   - treeAccum: the same open-addressed accumulator to find a member's current
//     score, plus an AVL keyed by (sortable score, member) maintained live with
//     a delete-reinsert on every aggregation update, so its in-order walk is the
//     score-ordered result with no final sort. This is the maintain-sorted arm
//     and doubles as doc 12's "accumulate into a tree directly" candidate.
//
// Because hashAccum and treeAccum share the accumulator, the delta between them
// isolates the sort tax exactly: hashAccum pays one O(m log m) sort at the end,
// treeAccum pays O(total input) AVL delete-reinserts along the way. See
// README.md for the sweep and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"sort"
	"time"
	"unsafe"
)

// aggMode is the AGGREGATE form: SUM (default), MIN, MAX. Applied to post-weight
// scores per section 6.12.
type aggMode int

const (
	aggSum aggMode = iota
	aggMin
	aggMax
)

func (a aggMode) String() string {
	switch a {
	case aggSum:
		return "SUM"
	case aggMin:
		return "MIN"
	default:
		return "MAX"
	}
}

// combine folds an incoming post-weight score into a running aggregate.
func combine(mode aggMode, running, incoming float64) float64 {
	switch mode {
	case aggSum:
		return running + incoming
	case aggMin:
		if incoming < running {
			return incoming
		}
		return running
	default:
		if incoming > running {
			return incoming
		}
		return running
	}
}

// mix is the splitmix64 finalizer, the same cheap strong hash the M1 labs use so
// the accumulator prices the same probe shape.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// sortableScore is the section 3.1 order-preserving transform: memcmp on the
// 8-byte result equals zset score order, so sorting result pairs by this key is
// exactly what the destination bulk load consumes.
func sortableScore(score float64) uint64 {
	b := math.Float64bits(score)
	if b == 1<<63 { // collapse -0.0 onto +0.0, the zset treats them as one score
		b = 0
	}
	if b&(1<<63) == 0 {
		return b ^ (1 << 63)
	}
	return ^b
}

// source is one operand sorted set: distinct member ids with parallel scores and
// a per-source WEIGHTS multiplier, plus the member byte slab for the memory
// accounting (member i is slab[i*sz:i*sz+sz], big-endian id).
type source struct {
	ids    []uint64
	scores []float64
	weight float64
	sz     int
	slab   []byte
}

// scoreOf returns a stable pseudo-random integer score in [0,1000) for a member
// in a given source, held as float64 so it is exact. Distinct across sources for
// the same member so MIN and MAX are meaningful.
func scoreOf(srcIdx int, id uint64) float64 {
	return float64(mix(id^(uint64(srcIdx+1)*0x9e3779b97f4a7c15)) % 1000)
}

func newSource(srcIdx int, ids []uint64, weight float64, sz int) *source {
	s := &source{
		ids:    ids,
		scores: make([]float64, len(ids)),
		weight: weight,
		sz:     sz,
		slab:   make([]byte, len(ids)*sz),
	}
	for i, id := range ids {
		s.scores[i] = scoreOf(srcIdx, id)
		b := s.slab[i*sz : i*sz+sz]
		binary.BigEndian.PutUint64(b[sz-8:], id)
	}
	return s
}

// buildSources builds k operands of the given cardinality. equalOverlap makes
// every source share the same member set {1..card} (union m = card, every member
// aggregated k times, the collision-heavy shape); disjoint gives source s the
// members {s*card+1 .. s*card+card} (union m = k*card, no collisions, the
// sort-heavy shape). weighted toggles WEIGHTS: off is all 1.0, on is s+1.
func buildSources(k, card int, equalOverlap, weighted bool, sz int) []*source {
	srcs := make([]*source, k)
	for s := 0; s < k; s++ {
		ids := make([]uint64, card)
		if equalOverlap {
			for i := 0; i < card; i++ {
				ids[i] = uint64(i + 1)
			}
		} else {
			base := uint64(s)*uint64(card) + 1
			for i := 0; i < card; i++ {
				ids[i] = base + uint64(i)
			}
		}
		w := 1.0
		if weighted {
			w = float64(s + 1)
		}
		srcs[s] = newSource(s, ids, w, sz)
	}
	return srcs
}

// resultCard returns the union result cardinality for the shape, the m every
// per-member number normalizes by.
func resultCard(k, card int, equalOverlap bool) int {
	if equalOverlap {
		return card
	}
	return k * card
}

// respair is one aggregated result entry, ready for the destination bulk load:
// the sortable score key and the member id (member bytes follow the id one to
// one). Sorted by (key, id).
type respair struct {
	key uint64
	id  uint64
}

func lessPair(a, b respair) bool {
	if a.key != b.key {
		return a.key < b.key
	}
	return a.id < b.id
}

func sortPairs(p []respair) {
	sort.Slice(p, func(i, j int) bool { return lessPair(p[i], p[j]) })
}

// acc is the open-addressed member->score accumulator, keyed by id (the Swiss
// table lineage from the M1 labs, linear-probed). Presized to the result
// cardinality at 7/8 load so no rehash confounds the timing.
type acc struct {
	mask uint64
	ctrl []uint8 // 0 empty, 1 full
	ids  []uint64
	scr  []float64
	n    int
}

func newAcc(m int) *acc {
	capPow2 := uint64(8)
	for float64(m) > 0.875*float64(capPow2) {
		capPow2 <<= 1
	}
	return &acc{
		mask: capPow2 - 1,
		ctrl: make([]uint8, capPow2),
		ids:  make([]uint64, capPow2),
		scr:  make([]float64, capPow2),
	}
}

// add folds one post-weight score for a member into the accumulator, inserting
// on first sight. Returns the slot, the old aggregate, and whether the member
// was already present (the tree arm needs those to reindex the ordered view).
func (a *acc) add(mode aggMode, id uint64, wscore float64) (slot int, old float64, present bool) {
	i := mix(id) & a.mask
	for {
		if a.ctrl[i] == 0 {
			a.ctrl[i] = 1
			a.ids[i] = id
			a.scr[i] = wscore
			a.n++
			return int(i), 0, false
		}
		if a.ids[i] == id {
			old = a.scr[i]
			a.scr[i] = combine(mode, old, wscore)
			return int(i), old, true
		}
		i = (i + 1) & a.mask
	}
}

// slotBytes is the accumulator's scratch cost per capacity slot: control byte,
// id, score. The sort slice is counted separately.
const slotBytes = 1 + 8 + 8

// hashAccum aggregates every input into the member accumulator, then extracts
// and sorts by score. This is Redis's strategy and doc 12's degradation path.
func hashAccum(srcs []*source, mode aggMode) []respair {
	m := 0
	for _, s := range srcs {
		m += len(s.ids)
	}
	a := newAcc(m)
	for _, s := range srcs {
		for i, id := range s.ids {
			a.add(mode, id, s.weight*s.scores[i])
		}
	}
	out := make([]respair, 0, a.n)
	for i := range a.ctrl {
		if a.ctrl[i] == 1 {
			out = append(out, respair{key: sortableScore(a.scr[i]), id: a.ids[i]})
		}
	}
	sortPairs(out)
	return out
}

// accOnly runs the accumulate half of hashAccum without the final sort, so the
// sort tax can be isolated as hashAccum minus accOnly.
func accOnly(srcs []*source, mode aggMode) int {
	m := 0
	for _, s := range srcs {
		m += len(s.ids)
	}
	a := newAcc(m)
	for _, s := range srcs {
		for i, id := range s.ids {
			a.add(mode, id, s.weight*s.scores[i])
		}
	}
	return a.n
}

// runEntry is one entry of a per-source member-sorted run: member id and its
// post-weight score.
type runEntry struct {
	id  uint64
	scr float64
}

// buildRuns turns each source into a member-sorted run of (id, weighted score),
// the hash slot walk into a per-source sorted run (section 6.12). The per-source
// sort is part of the merge path's cost.
func buildRuns(srcs []*source) [][]runEntry {
	runs := make([][]runEntry, len(srcs))
	for s, src := range srcs {
		r := make([]runEntry, len(src.ids))
		for i, id := range src.ids {
			r[i] = runEntry{id: id, scr: src.weight * src.scores[i]}
		}
		sort.Slice(r, func(a, b int) bool { return r[a].id < r[b].id })
		runs[s] = r
	}
	return runs
}

// heapMerge is the section 6.12 tournament for a k-way merge, realized as a
// binary min-heap over run indices keyed by each run's current head member. Each
// pop is O(log k); the heap is the tournament tree in implicit form and prices
// the merge at the swept fan-in without the O(k) bias a linear cursor scan would
// carry against high k.
type heapMerge struct {
	h    []int
	runs [][]runEntry
	pos  []int
}

func (m *heapMerge) less(a, b int) bool {
	return m.runs[a][m.pos[a]].id < m.runs[b][m.pos[b]].id
}

func (m *heapMerge) siftDown(i int) {
	n := len(m.h)
	for {
		sm, l, r := i, 2*i+1, 2*i+2
		if l < n && m.less(m.h[l], m.h[sm]) {
			sm = l
		}
		if r < n && m.less(m.h[r], m.h[sm]) {
			sm = r
		}
		if sm == i {
			return
		}
		m.h[i], m.h[sm] = m.h[sm], m.h[i]
		i = sm
	}
}

// mergeAccum builds one member-sorted run per source, folds equal members with a
// tournament (min-heap) k-way merge, then sorts the member-ordered result by
// score for the destination bulk load.
func mergeAccum(srcs []*source, mode aggMode) []respair {
	runs := buildRuns(srcs)
	m := &heapMerge{runs: runs, pos: make([]int, len(runs))}
	for s := range runs {
		if len(runs[s]) > 0 {
			m.h = append(m.h, s)
		}
	}
	for i := len(m.h)/2 - 1; i >= 0; i-- {
		m.siftDown(i)
	}

	out := make([]respair, 0, len(runs[0]))
	haveCur := false
	var curID uint64
	var curScr float64

	for len(m.h) > 0 {
		w := m.h[0]
		e := runs[w][m.pos[w]]
		m.pos[w]++
		if m.pos[w] < len(runs[w]) {
			m.siftDown(0)
		} else {
			last := len(m.h) - 1
			m.h[0] = m.h[last]
			m.h = m.h[:last]
			if len(m.h) > 0 {
				m.siftDown(0)
			}
		}

		if haveCur && e.id == curID {
			curScr = combine(mode, curScr, e.scr)
			continue
		}
		if haveCur {
			out = append(out, respair{key: sortableScore(curScr), id: curID})
		}
		haveCur = true
		curID = e.id
		curScr = e.scr
	}
	if haveCur {
		out = append(out, respair{key: sortableScore(curScr), id: curID})
	}
	sortPairs(out)
	return out
}

// avlNode is one node of the score-ordered tree, keyed by (sortable score,
// member). Children and the free list are int indices into a slab so the memory
// is countable and there are no per-node GC pointers.
type avlNode struct {
	key         uint64
	id          uint64
	left, right int32
	height      int32
}

const nilNode = -1

// avl is a slab-backed AVL keyed by (key, id), the maintain-sorted structure.
// Freed nodes are recycled so the slab peaks at the result cardinality, not the
// total number of delete-reinserts.
type avl struct {
	nodes []avlNode
	root  int32
	free  []int32
}

func newAVL(m int) *avl {
	return &avl{nodes: make([]avlNode, 0, m), root: nilNode}
}

func (t *avl) height(n int32) int32 {
	if n == nilNode {
		return 0
	}
	return t.nodes[n].height
}

func (t *avl) fix(n int32) {
	l, r := t.height(t.nodes[n].left), t.height(t.nodes[n].right)
	if l > r {
		t.nodes[n].height = l + 1
	} else {
		t.nodes[n].height = r + 1
	}
}

func (t *avl) balance(n int32) int32 {
	return t.height(t.nodes[n].left) - t.height(t.nodes[n].right)
}

func lessKey(ak, aid, bk, bid uint64) bool {
	if ak != bk {
		return ak < bk
	}
	return aid < bid
}

func (t *avl) rotRight(y int32) int32 {
	x := t.nodes[y].left
	t2 := t.nodes[x].right
	t.nodes[x].right = y
	t.nodes[y].left = t2
	t.fix(y)
	t.fix(x)
	return x
}

func (t *avl) rotLeft(x int32) int32 {
	y := t.nodes[x].right
	t2 := t.nodes[y].left
	t.nodes[y].left = x
	t.nodes[x].right = t2
	t.fix(x)
	t.fix(y)
	return y
}

func (t *avl) rebalance(n int32) int32 {
	t.fix(n)
	b := t.balance(n)
	if b > 1 {
		if t.balance(t.nodes[n].left) < 0 {
			t.nodes[n].left = t.rotLeft(t.nodes[n].left)
		}
		return t.rotRight(n)
	}
	if b < -1 {
		if t.balance(t.nodes[n].right) > 0 {
			t.nodes[n].right = t.rotRight(t.nodes[n].right)
		}
		return t.rotLeft(n)
	}
	return n
}

func (t *avl) alloc(key, id uint64) int32 {
	if len(t.free) > 0 {
		i := t.free[len(t.free)-1]
		t.free = t.free[:len(t.free)-1]
		t.nodes[i] = avlNode{key: key, id: id, left: nilNode, right: nilNode, height: 1}
		return i
	}
	t.nodes = append(t.nodes, avlNode{key: key, id: id, left: nilNode, right: nilNode, height: 1})
	return int32(len(t.nodes) - 1)
}

func (t *avl) insert(n int32, key, id uint64) int32 {
	if n == nilNode {
		return t.alloc(key, id)
	}
	if lessKey(key, id, t.nodes[n].key, t.nodes[n].id) {
		t.nodes[n].left = t.insert(t.nodes[n].left, key, id)
	} else {
		t.nodes[n].right = t.insert(t.nodes[n].right, key, id)
	}
	return t.rebalance(n)
}

func (t *avl) minNode(n int32) int32 {
	for t.nodes[n].left != nilNode {
		n = t.nodes[n].left
	}
	return n
}

func (t *avl) delete(n int32, key, id uint64) int32 {
	if n == nilNode {
		return nilNode
	}
	switch {
	case lessKey(key, id, t.nodes[n].key, t.nodes[n].id):
		t.nodes[n].left = t.delete(t.nodes[n].left, key, id)
	case lessKey(t.nodes[n].key, t.nodes[n].id, key, id):
		t.nodes[n].right = t.delete(t.nodes[n].right, key, id)
	default:
		if t.nodes[n].left == nilNode || t.nodes[n].right == nilNode {
			child := t.nodes[n].left
			if child == nilNode {
				child = t.nodes[n].right
			}
			t.free = append(t.free, n)
			return child
		}
		succ := t.minNode(t.nodes[n].right)
		t.nodes[n].key = t.nodes[succ].key
		t.nodes[n].id = t.nodes[succ].id
		t.nodes[n].right = t.delete(t.nodes[n].right, t.nodes[succ].key, t.nodes[succ].id)
	}
	return t.rebalance(n)
}

func (t *avl) inorder(out []respair) []respair {
	if t.root == nilNode {
		return out
	}
	stack := make([]int32, 0, 48)
	n := t.root
	for n != nilNode || len(stack) > 0 {
		for n != nilNode {
			stack = append(stack, n)
			n = t.nodes[n].left
		}
		n = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, respair{key: t.nodes[n].key, id: t.nodes[n].id})
		n = t.nodes[n].right
	}
	return out
}

// treeAccum accumulates into the member table and maintains a score-ordered AVL
// live: on every aggregation update it deletes the member's old (score, member)
// and inserts the new one, so the in-order walk is the score-ordered result with
// no final sort. This is the maintain-sorted arm and doc 12's accumulate-into-a-
// tree candidate. The accumulator is shared with hashAccum so the delta between
// the two is exactly the sort tax.
func treeAccum(srcs []*source, mode aggMode) []respair {
	m := 0
	for _, s := range srcs {
		m += len(s.ids)
	}
	a := newAcc(m)
	t := newAVL(m)
	for _, s := range srcs {
		for i, id := range s.ids {
			_, old, present := a.add(mode, id, s.weight*s.scores[i])
			slot := findSlot(a, id)
			newScore := a.scr[slot]
			if present {
				oldKey := sortableScore(old)
				if oldKey != sortableScore(newScore) {
					t.root = t.delete(t.root, oldKey, id)
					t.root = t.insert(t.root, sortableScore(newScore), id)
				}
			} else {
				t.root = t.insert(t.root, sortableScore(newScore), id)
			}
		}
	}
	out := make([]respair, 0, a.n)
	return t.inorder(out)
}

func findSlot(a *acc, id uint64) int {
	i := mix(id) & a.mask
	for {
		if a.ctrl[i] == 1 && a.ids[i] == id {
			return int(i)
		}
		i = (i + 1) & a.mask
	}
}

func pairsEqual(a, b []respair) bool {
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

// timeKernel runs f adaptively and returns ns per result member. reps stay small
// for the large cells where one call already dominates minDur.
func timeKernel(m int, f func() int) float64 {
	const minDur = 40 * time.Millisecond
	reps := 0
	start := time.Now()
	for {
		f()
		reps++
		if reps >= 3 && time.Since(start) >= minDur {
			break
		}
		if reps >= 1 && time.Since(start) >= 4*minDur {
			break
		}
	}
	return float64(time.Since(start).Nanoseconds()) / float64(reps*m)
}

type cell struct {
	shape   string
	k, card int
	m       int
	hashNs  float64
	mergeNs float64
	treeNs  float64 // -1 when skipped
	winner  string
}

// treeInputCap bounds the total delete-reinsert work the maintain-sorted arm
// takes on: above it the arm is skipped in the sweep (it is already dominated at
// every smaller size) to keep the run bounded in time and memory.
const treeInputCap = 2_000_000

func runCell(shape string, k, card int, equalOverlap bool) cell {
	srcs := buildSources(k, card, equalOverlap, false, 16)
	m := resultCard(k, card, equalOverlap)
	totalInput := k * card

	ref := hashAccum(srcs, aggSum)
	if len(ref) != m {
		panic(fmt.Sprintf("%s k=%d card=%d: hash m=%d want %d", shape, k, card, len(ref), m))
	}
	if got := mergeAccum(srcs, aggSum); !pairsEqual(got, ref) {
		panic(fmt.Sprintf("%s k=%d card=%d: merge disagrees with hash", shape, k, card))
	}

	c := cell{shape: shape, k: k, card: card, m: m, treeNs: -1}
	c.hashNs = timeKernel(m, func() int { return len(hashAccum(srcs, aggSum)) })
	c.mergeNs = timeKernel(m, func() int { return len(mergeAccum(srcs, aggSum)) })
	if totalInput <= treeInputCap {
		if got := treeAccum(srcs, aggSum); !pairsEqual(got, ref) {
			panic(fmt.Sprintf("%s k=%d card=%d: tree disagrees with hash", shape, k, card))
		}
		c.treeNs = timeKernel(m, func() int { return len(treeAccum(srcs, aggSum)) })
	}

	c.winner = "hash"
	best := c.hashNs
	if c.mergeNs < best {
		best = c.mergeNs
		c.winner = "merge"
	}
	if c.treeNs >= 0 && c.treeNs < best {
		c.winner = "tree"
	}
	return c
}

func main() {
	quick := flag.Bool("quick", false, "smaller sweep for a fast check")
	flag.Parse()

	ks := []int{2, 4, 8}
	cards := []int{1000, 10000, 100000, 1000000}
	if *quick {
		ks = []int{2, 8}
		cards = []int{1000, 10000, 100000}
	}

	var cells []cell
	for _, shape := range []string{"equal", "disjoint"} {
		for _, k := range ks {
			for _, card := range cards {
				cells = append(cells, runCell(shape, k, card, shape == "equal"))
			}
		}
	}
	reportSweep(cells)
	reportSortTax(*quick)
	reportAggregate(*quick)
	reportMemory()
}

func reportSweep(cells []cell) {
	fmt.Printf("zset algebra accumulation sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("union path, AGGREGATE SUM, WEIGHTS off; ns per result member; tree is the maintain-sorted AVL arm\n")
	fmt.Printf("hash = accumulate then sort; merge = tournament k-way merge then sort; tree = accumulate into score tree\n\n")
	fmt.Printf("%-8s %3s %9s %10s %10s %10s %10s %8s\n",
		"shape", "k", "resultM", "hashNs", "mergeNs", "treeNs", "m/h", "winner")
	lastShape := ""
	for _, c := range cells {
		if c.shape != lastShape {
			fmt.Println()
			lastShape = c.shape
		}
		tree := "     skip"
		if c.treeNs >= 0 {
			tree = fmt.Sprintf("%10.2f", c.treeNs)
		}
		fmt.Printf("%-8s %3d %9d %10.2f %10.2f %s %10.2f %8s\n",
			c.shape, c.k, c.m, c.hashNs, c.mergeNs, tree, c.mergeNs/c.hashNs, c.winner)
	}
	fmt.Println()
}

// reportSortTax isolates the cost of getting the result into score order. accOnly
// is the accumulate half with no sort; hashAccum adds the O(m log m) sort at the
// end; treeAccum keeps score order live with per-update delete-reinserts. The
// gap between the last two is the whole point of question 3.
func reportSortTax(quick bool) {
	fmt.Printf("sort tax: accumulate-only vs sort-at-end vs maintain-sorted, ns per result member\n")
	fmt.Printf("shape equal (collision-heavy), AGGREGATE SUM; sortTax = hash - accOnly; maintainTax = tree - accOnly\n\n")
	fmt.Printf("%3s %9s %10s %10s %10s %10s %12s\n",
		"k", "resultM", "accOnly", "hash", "tree", "sortTax", "maintainTax")
	cards := []int{1000, 10000, 100000}
	if !quick {
		cards = append(cards, 1000000)
	}
	for _, card := range cards {
		k := 4
		srcs := buildSources(k, card, true, false, 16)
		m := resultCard(k, card, true)
		accNs := timeKernel(m, func() int { return accOnly(srcs, aggSum) })
		hashNs := timeKernel(m, func() int { return len(hashAccum(srcs, aggSum)) })
		treeStr := "      skip"
		treeVal := "        -"
		if k*card <= treeInputCap {
			treeNs := timeKernel(m, func() int { return len(treeAccum(srcs, aggSum)) })
			treeStr = fmt.Sprintf("%10.2f", treeNs)
			treeVal = fmt.Sprintf("%12.2f", treeNs-accNs)
		}
		fmt.Printf("%3d %9d %10.2f %10.2f %s %10.2f %s\n",
			k, m, accNs, hashNs, treeStr, hashNs-accNs, treeVal)
	}
	fmt.Println()
}

// reportAggregate confirms the AGGREGATE form and the WEIGHTS multiplication do
// not move the structural verdict: the winner is a property of the accumulation
// shape, not the fold operator.
func reportAggregate(quick bool) {
	fmt.Printf("aggregate and weights sensitivity, hash kernel, shape equal, k=4, ns per result member\n\n")
	fmt.Printf("%12s %10s %10s\n", "mode", "wOff", "wOn")
	card := 100000
	if quick {
		card = 10000
	}
	k := 4
	m := resultCard(k, card, true)
	for _, mode := range []aggMode{aggSum, aggMin, aggMax} {
		off := buildSources(k, card, true, false, 16)
		on := buildSources(k, card, true, true, 16)
		nsOff := timeKernel(m, func() int { return len(hashAccum(off, mode)) })
		nsOn := timeKernel(m, func() int { return len(hashAccum(on, mode)) })
		fmt.Printf("%12s %10.2f %10.2f\n", mode, nsOff, nsOn)
	}
	fmt.Println()
}

// reportMemory reports peak scratch bytes per result member for each structure,
// the F14 discipline applied to scratch. Numbers are the structure definitions,
// not a sampled heap, so they are exact and platform-independent.
func reportMemory() {
	fmt.Printf("peak scratch bytes per result member (F14 discipline, analytic)\n")
	fmt.Printf("m = result cardinality, tIn = total input (k*card); slot = %dB, respair = %dB, avlNode = %dB\n\n",
		slotBytes, int(unsafe.Sizeof(respair{})), int(unsafe.Sizeof(avlNode{})))
	nodeB := float64(unsafe.Sizeof(avlNode{}))
	pairB := float64(unsafe.Sizeof(respair{}))
	fmt.Printf("%-10s %s\n", "structure", "bytes per result member")
	// Accumulator cap at 7/8 load is ~1.14*m to 2.29*m depending on where m lands
	// against the next power of two; use the load-factor bound 1/0.875.
	accPerM := float64(slotBytes) / 0.875
	fmt.Printf("%-10s %.1f  (acc cap ~%.2f*m * %dB) + %.0f (sort slice) = %.1f\n",
		"hash", accPerM+pairB, 1/0.875, slotBytes, pairB, accPerM+pairB)
	fmt.Printf("%-10s runs 16B*(tIn/m) + %.0f (output+sort) ; collision-heavy tIn=k*m, disjoint tIn=m\n",
		"merge", 2*pairB)
	fmt.Printf("%-10s %.1f  (acc %.1f + AVL %.0f*1.0, no separate sort slice)\n",
		"tree", accPerM+nodeB, accPerM, nodeB)
	fmt.Printf("\nmerge worked: equal-overlap k=8 tIn=8m -> 16*8 + %.0f = %.0f B/member; disjoint tIn=m -> 16 + %.0f = %.0f B/member\n",
		2*pairB, 128+2*pairB, 2*pairB, 16+2*pairB)
}
