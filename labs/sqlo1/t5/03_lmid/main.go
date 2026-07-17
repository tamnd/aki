// Lab: adversarial middle-insert fence growth (spec 2064/sqlo1 doc 07
// sections 3 and 8, milestone T5 lab 03).
//
// LINSERT and LREM work in the middle of the list, where the deque
// path's edge amendment does not apply: a middle insert into a full
// node splits it, and middle removals shrink nodes that never empty.
// Without a counterweight an insert-remove churn at steady length
// grows the fence without bound as nodes erode toward slivers, which
// is the doc 14 kill-table row for the list type. The counterweight
// is lazy merge: after a shrink, adjacent nodes whose combined size
// fits a merge threshold coalesce into one, billing both images and
// a fence cut. This lab prices the threshold (merge_max, in encoded
// bytes) under an adversarial split storm and a steady churn, and
// checks the storm's occupancy floor: alternating inserts can pin
// occupancy near half, but never below it, and merge must hold the
// churn's steady state bounded.
//
// The model is the lnode lab's resident shape (doc 07 nodes behind an
// ordered fence, WAL billed per doc 06 W2/W4) plus the two middle
// operators: insertAt splits a full node at its byte midpoint, and
// removeAt drops emptied nodes or lazily merges a shrunken node with
// its smaller neighbor when the pair fits merge_max. An oracle test
// pins the model against a reference slice with merge on and off.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

// Encoded sizes, shared with the lnode lab (doc 07 section 2).
const (
	elemHdr        = 4
	nodeHdrBytes   = 12
	rootHdrBytes   = 28
	fenceEntBytes  = 12
	fenceInlineMax = 2048 - rootHdrBytes
	fencePageBytes = 4096
	fencePageEnts  = 330
	tombBytes      = 16
)

func elemSize(elen int) int { return elemHdr + elen }

type node struct {
	id    uint64
	elems [][]byte
	bytes int
}

type list struct {
	nodeMax  int
	ecap     int
	mergeMax int // 0 disables lazy merge
	nodes    []*node
	count    int
	nextID   uint64

	walBytes   int64
	walFrames  int64
	structural int64
	splits     int64
	merges     int64
	drops      int64
}

func newList(nodeMax, ecap, mergeMax int) *list {
	return &list{nodeMax: nodeMax, ecap: ecap, mergeMax: mergeMax}
}

func (l *list) paged() bool {
	return len(l.nodes)*fenceEntBytes > fenceInlineMax
}

func (l *list) rootBill() int {
	if !l.paged() {
		return rootHdrBytes + len(l.nodes)*fenceEntBytes
	}
	pages := (len(l.nodes) + fencePageEnts - 1) / fencePageEnts
	return fencePageBytes + rootHdrBytes + pages*fenceEntBytes
}

func (l *list) billNode(n *node) {
	l.walBytes += int64(n.bytes)
	l.walFrames++
}

func (l *list) billStructural() {
	l.walBytes += int64(l.rootBill())
	l.walFrames++
	l.structural++
}

func (l *list) newNode() *node {
	l.nextID++
	return &node{id: l.nextID, bytes: nodeHdrBytes}
}

func (l *list) fits(n *node, elen int) bool {
	return n.bytes+elemSize(elen) <= l.nodeMax && len(n.elems) < l.ecap
}

// seek finds the node holding index i; i == count lands after the
// last element of the last node for appends.
func (l *list) seek(i int) (ni, off int) {
	for ni = 0; ni < len(l.nodes)-1; ni++ {
		if i < len(l.nodes[ni].elems) {
			return ni, i
		}
		i -= len(l.nodes[ni].elems)
	}
	return len(l.nodes) - 1, i
}

// insertAt places e before index i, doc 07's LINSERT path: amend the
// covering node, or split it at the byte midpoint first when full.
func (l *list) insertAt(i int, e []byte) {
	if len(l.nodes) == 0 {
		n := l.newNode()
		l.nodes = []*node{n}
		l.billStructural()
		n.elems = [][]byte{e}
		n.bytes += elemSize(len(e))
		l.count++
		l.billNode(n)
		return
	}
	ni, off := l.seek(i)
	n := l.nodes[ni]
	if !l.fits(n, len(e)) {
		// Split at the byte midpoint; the insert then lands in the
		// half covering its offset.
		cut, b := 0, 0
		for cut < len(n.elems)-1 && b+elemSize(len(n.elems[cut])) <= (n.bytes-nodeHdrBytes)/2 {
			b += elemSize(len(n.elems[cut]))
			cut++
		}
		right := l.newNode()
		right.elems = append(right.elems, n.elems[cut:]...)
		for _, re := range right.elems {
			right.bytes += elemSize(len(re))
		}
		n.elems = n.elems[:cut]
		n.bytes = nodeHdrBytes + b
		l.nodes = append(l.nodes, nil)
		copy(l.nodes[ni+2:], l.nodes[ni+1:])
		l.nodes[ni+1] = right
		l.splits++
		l.billNode(right)
		l.billStructural()
		if off > cut {
			ni, off = ni+1, off-cut
			n = right
		}
	}
	n.elems = append(n.elems, nil)
	copy(n.elems[off+1:], n.elems[off:])
	n.elems[off] = e
	n.bytes += elemSize(len(e))
	l.count++
	l.billNode(n)
}

// removeAt deletes the element at index i, dropping emptied nodes and
// lazily merging a shrunken survivor with its smaller neighbor when
// the pair fits merge_max.
func (l *list) removeAt(i int) []byte {
	ni, off := l.seek(i)
	n := l.nodes[ni]
	e := n.elems[off]
	copy(n.elems[off:], n.elems[off+1:])
	n.elems = n.elems[:len(n.elems)-1]
	n.bytes -= elemSize(len(e))
	l.count--
	if len(n.elems) == 0 {
		l.nodes = append(l.nodes[:ni], l.nodes[ni+1:]...)
		l.drops++
		l.walBytes += tombBytes
		l.walFrames++
		l.billStructural()
		return e
	}
	l.billNode(n)
	l.maybeMerge(ni)
	return e
}

// maybeMerge coalesces the node at ni with whichever neighbor makes
// the smaller pair, when that pair fits merge_max under both caps.
func (l *list) maybeMerge(ni int) {
	if l.mergeMax == 0 {
		return
	}
	n := l.nodes[ni]
	best := -1
	bestBytes := 0
	for _, mi := range []int{ni - 1, ni + 1} {
		if mi < 0 || mi >= len(l.nodes) {
			continue
		}
		m := l.nodes[mi]
		pair := n.bytes + m.bytes - nodeHdrBytes
		if pair <= l.mergeMax && pair <= l.nodeMax && len(n.elems)+len(m.elems) <= l.ecap {
			if best == -1 || pair < bestBytes {
				best, bestBytes = mi, pair
			}
		}
	}
	if best == -1 {
		return
	}
	lo, hi := ni, best
	if hi < lo {
		lo, hi = hi, lo
	}
	left, right := l.nodes[lo], l.nodes[hi]
	left.elems = append(left.elems, right.elems...)
	left.bytes += right.bytes - nodeHdrBytes
	l.nodes = append(l.nodes[:hi], l.nodes[hi+1:]...)
	l.merges++
	l.billNode(left)
	l.walBytes += tombBytes
	l.walFrames++
	l.billStructural()
}

func (l *list) walk(emit func(e []byte)) {
	for _, n := range l.nodes {
		for _, e := range n.elems {
			emit(e)
		}
	}
}

// occupancy is live element bytes over node capacity, the erosion
// gauge the storm and churn rows report.
func (l *list) occupancy() float64 {
	if len(l.nodes) == 0 {
		return 0
	}
	total := 0
	for _, n := range l.nodes {
		total += n.bytes - nodeHdrBytes
	}
	return float64(total) / float64(len(l.nodes)*(l.nodeMax-nodeHdrBytes))
}

type config struct {
	mix      string
	nodeMax  int
	ecap     int
	mergeMax int
	length   int
	ops      int
	elen     int
	seed     int64
}

func elemBytes(rng *rand.Rand, elen int) []byte {
	b := make([]byte, elen)
	for i := range b {
		b[i] = 'a' + byte(rng.Intn(26))
	}
	return b
}

func row(cfg config, workload string, ops int, nsOp int64, framesOp, walBOp, nodesOp, x1, x2, x3 float64) {
	fmt.Printf("%s,%d,%d,%s,%d,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%.3f\n",
		cfg.mix, cfg.nodeMax, cfg.mergeMax, workload, ops, nsOp, framesOp, walBOp, nodesOp, x1, x2, x3)
}

func shapeRow(cfg config, l *list, label string) {
	paged := 0.0
	if l.paged() {
		paged = 1
	}
	row(cfg, label, l.count, 0, 0, 0, float64(len(l.nodes)), l.occupancy(), float64(len(l.nodes)*fenceEntBytes), paged)
}

func resetCounters(l *list) {
	l.walBytes, l.walFrames, l.structural, l.splits, l.merges, l.drops = 0, 0, 0, 0, 0, 0
}

// runStorm is the adversarial split storm: every insert lands at the
// same middle position, growing the list; the question is the
// occupancy floor, not the fence length (data really grows).
func runStorm(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap, cfg.mergeMax)
	for range cfg.length {
		l.insertAt(l.count, elemBytes(rng, cfg.elen))
	}
	shapeRow(cfg, l, "shape0")
	resetCounters(l)
	start := time.Now()
	for range cfg.ops {
		l.insertAt(cfg.length/2, elemBytes(rng, cfg.elen))
	}
	elapsed := time.Since(start)
	row(cfg, "storm", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops),
		float64(l.walFrames)/float64(cfg.ops), float64(l.walBytes)/float64(cfg.ops), float64(len(l.nodes)),
		float64(l.splits)*1000/float64(cfg.ops), float64(l.merges)*1000/float64(cfg.ops), l.occupancy())
	shapeRow(cfg, l, "shape")
}

// runChurn holds the length steady: every op is one LINSERT at a
// random position plus one LREM at a random position, the erosion
// shape lazy merge exists for. The row reports the end state; the
// growth column is nodes at the end over nodes at the start.
func runChurn(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap, cfg.mergeMax)
	for range cfg.length {
		l.insertAt(l.count, elemBytes(rng, cfg.elen))
	}
	nodes0 := len(l.nodes)
	shapeRow(cfg, l, "shape0")
	resetCounters(l)
	start := time.Now()
	for range cfg.ops {
		l.insertAt(rng.Intn(l.count+1), elemBytes(rng, cfg.elen))
		l.removeAt(rng.Intn(l.count))
	}
	elapsed := time.Since(start)
	row(cfg, "churn", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops),
		float64(l.walFrames)/float64(cfg.ops)/2, float64(l.walBytes)/float64(cfg.ops)/2, float64(len(l.nodes))/float64(nodes0),
		float64(l.splits)*1000/float64(cfg.ops), float64(l.merges)*1000/float64(cfg.ops), l.occupancy())
	shapeRow(cfg, l, "shape")
}

// runDecimate is the targeted adversary: each round removes every
// other element across the whole list, then refills at one fixed
// point, so eroded nodes are never backfilled and only decay further
// next round. Without merge this drives the sliver population the
// kill-table row worries about; with it the occupancy must floor.
func runDecimate(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap, cfg.mergeMax)
	for range cfg.length {
		l.insertAt(l.count, elemBytes(rng, cfg.elen))
	}
	nodes0 := len(l.nodes)
	shapeRow(cfg, l, "shape0")
	resetCounters(l)
	ops := 0
	start := time.Now()
	for ops < cfg.ops {
		removed := 0
		for i := l.count - 1; i >= 0; i -= 2 {
			l.removeAt(i)
			removed++
			ops++
		}
		at := l.count / 2
		for range removed {
			l.insertAt(at, elemBytes(rng, cfg.elen))
			ops++
		}
	}
	elapsed := time.Since(start)
	row(cfg, "decimate", ops, elapsed.Nanoseconds()/int64(ops),
		float64(l.walFrames)/float64(ops), float64(l.walBytes)/float64(ops), float64(len(l.nodes))/float64(nodes0),
		float64(l.splits)*1000/float64(ops), float64(l.merges)*1000/float64(ops), l.occupancy())
	shapeRow(cfg, l, "shape")
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.mix, "mix", "churn", "op mix: storm, churn, decimate")
	flag.IntVar(&cfg.nodeMax, "nodemax", 4032, "node split threshold in bytes")
	flag.IntVar(&cfg.ecap, "ecap", 128, "node element cap")
	flag.IntVar(&cfg.mergeMax, "mergemax", 2016, "lazy merge pair threshold in bytes; 0 disables")
	flag.IntVar(&cfg.length, "length", 100000, "list length")
	flag.IntVar(&cfg.ops, "ops", 200000, "measured ops (churn counts insert-remove pairs)")
	flag.IntVar(&cfg.elen, "elen", 100, "element length in bytes")
	flag.Int64Var(&cfg.seed, "seed", 47, "rng seed")
	flag.Parse()
	if *quick {
		cfg.length = 5000
		cfg.ops = 10000
	}
	switch cfg.mix {
	case "storm":
		runStorm(cfg)
	case "churn":
		runChurn(cfg)
	case "decimate":
		runDecimate(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown mix %q\n", cfg.mix)
		os.Exit(2)
	}
}
