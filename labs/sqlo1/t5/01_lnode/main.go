// Lab: list node size and element cap (spec 2064/sqlo1 doc 07
// sections 2, 3, and 7, milestone T5 lab 01).
//
// T5 slice 2 bakes the node split thresholds: node_max in encoded
// bytes and ecap in elements, whichever binds first. The trade is doc
// 06's W4 bandwidth knob on the queue type: every push or pop bills
// the amended head or tail node's full post-image in its frame group,
// so bigger nodes make steady queue traffic carry more WAL bytes per
// op, while smaller ones lengthen the fence, cut and drop nodes more
// often (each a fence-shape bill), and make an LRANGE window touch
// more nodes for the same output. The element cap only binds when
// elements are small (ID queues), which is why the sweep carries a
// 16-byte deque arm beside the payload-sized ones.
// PRED-SQLO1-T5-QUEUE and PRED-SQLO1-T5-FEED are priced here: WAL
// bytes per queue op at steady depth, and the amortized bill of the
// capped feed's LPUSH plus LTRIM.
//
// The model is the doc 07 shape resident, no store underneath (the
// hsegz pattern; the drain-substrate half of the trade was priced by
// T2's hseg lab on the real backends, and what T5 adds is the
// queue-shaped bill, which is arithmetic). Nodes hold contiguous
// element runs in list order behind an ordered fence of (segid,
// count) entries; positional math prefix-sums the fence. The WAL
// column is modeled arithmetic under rules W2 and W4: every mutating
// command bills the full post-images its frame group carries, a
// dropped node bills a tombstone, and a fence-shape change bills the
// whole root while the fence is inline or one fence page plus the
// root page index once it pages. Drain traffic accumulates dirty
// post-images against the engine's 8 MiB threshold for the WA column,
// which is where the queue's hot-tier coalescing shows up: a steady
// queue keeps only the head and tail nodes dirty, so drains almost
// never fire on node images. An oracle test pins the model against a
// reference slice through pushes, pops, trims, ranges, and counts.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"time"
)

// Encoded sizes, doc 07 section 2: nodes are n x (u32 elen, bytes)
// behind a 12 B segment header; the root header is 28 B and fence
// entries 12 B; fence pages hold up to 330 entries in a 4 KiB record.
const (
	elemHdr        = 4
	nodeHdrBytes   = 12
	rootHdrBytes   = 28
	fenceEntBytes  = 12
	fenceInlineMax = 2048 - rootHdrBytes
	fencePageBytes = 4096
	fencePageEnts  = 330
	tombBytes      = 16
	drainThreshold = 8 << 20
)

func elemSize(elen int) int { return elemHdr + elen }

// node is one contiguous run of elements in list order.
type node struct {
	id    uint64
	elems [][]byte
	bytes int // encoded size including the node header
}

// list is the doc 07 noded shape: fence order is list order.
type list struct {
	nodeMax int
	ecap    int
	nodes   []*node
	count   int
	nextID  uint64

	// The modeled WAL and drain columns.
	walBytes   int64
	walFrames  int64
	structural int64 // fence-shape bills
	cuts       int64
	drops      int64
	edgeRewr   int64          // trim edge rewrites
	dirty      map[uint64]int // node id to its dirty image size
	dirtyBytes int64
	drains     int64
	drainedB   int64
	logicalB   int64
}

func newList(nodeMax, ecap int) *list {
	return &list{nodeMax: nodeMax, ecap: ecap, dirty: map[uint64]int{}}
}

func (l *list) paged() bool {
	return len(l.nodes)*fenceEntBytes > fenceInlineMax
}

// rootBill is the fence-shape bill under W2: the inline root whole,
// or one fence page plus the root's page index once paged.
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
	l.dirtyAdd(n)
}

func (l *list) billStructural() {
	l.walBytes += int64(l.rootBill())
	l.walFrames++
	l.structural++
}

// dirtyAdd tracks the deduped dirty image set and drains when it
// crosses the threshold, the hot tier's write-behind modeled at its
// cheapest: a redirtied node replaces its pending image.
func (l *list) dirtyAdd(n *node) {
	l.dirtyBytes += int64(n.bytes - l.dirty[n.id])
	l.dirty[n.id] = n.bytes
	if l.dirtyBytes >= drainThreshold {
		l.drains++
		l.drainedB += l.dirtyBytes
		l.dirty = map[uint64]int{}
		l.dirtyBytes = 0
	}
}

// dirtyDrop retires a dropped node's pending image.
func (l *list) dirtyDrop(n *node) {
	l.dirtyBytes -= int64(l.dirty[n.id])
	delete(l.dirty, n.id)
}

func (l *list) newNode() *node {
	l.nextID++
	return &node{id: l.nextID, bytes: nodeHdrBytes}
}

// fits reports whether one more element of elen keeps the node under
// both caps.
func (l *list) fits(n *node, elen int) bool {
	return n.bytes+elemSize(elen) <= l.nodeMax && len(n.elems) < l.ecap
}

// push amends the head or tail node, cutting a fresh one when full,
// doc 07's O(1) deque path.
func (l *list) push(left bool, e []byte) {
	l.count++
	l.logicalB += int64(len(e))
	var n *node
	if len(l.nodes) > 0 {
		if left {
			n = l.nodes[0]
		} else {
			n = l.nodes[len(l.nodes)-1]
		}
	}
	if n == nil || !l.fits(n, len(e)) {
		n = l.newNode()
		if left {
			l.nodes = append([]*node{n}, l.nodes...)
		} else {
			l.nodes = append(l.nodes, n)
		}
		l.cuts++
		l.billStructural()
	}
	if left {
		n.elems = append([][]byte{e}, n.elems...)
	} else {
		n.elems = append(n.elems, e)
	}
	n.bytes += elemSize(len(e))
	l.billNode(n)
}

// pop shrinks the head or tail node, dropping it when emptied.
func (l *list) pop(left bool) ([]byte, bool) {
	if l.count == 0 {
		return nil, false
	}
	l.count--
	var n *node
	var e []byte
	if left {
		n = l.nodes[0]
		e = n.elems[0]
		n.elems = n.elems[1:]
	} else {
		n = l.nodes[len(l.nodes)-1]
		e = n.elems[len(n.elems)-1]
		n.elems = n.elems[:len(n.elems)-1]
	}
	n.bytes -= elemSize(len(e))
	if len(n.elems) == 0 {
		if left {
			l.nodes = l.nodes[1:]
		} else {
			l.nodes = l.nodes[:len(l.nodes)-1]
		}
		l.dirtyDrop(n)
		l.drops++
		l.walBytes += tombBytes
		l.walFrames++
		l.billStructural()
	} else {
		l.billNode(n)
	}
	return e, true
}

// ltrimHead keeps the first keep elements, doc 07's capped-feed path:
// whole tail nodes drop via fence cut and tombstones, and at most one
// edge node rewrites.
func (l *list) ltrimHead(keep int) {
	if l.count <= keep {
		return
	}
	drop := l.count - keep
	structural := false
	for drop > 0 {
		n := l.nodes[len(l.nodes)-1]
		if len(n.elems) <= drop {
			drop -= len(n.elems)
			l.count -= len(n.elems)
			l.nodes = l.nodes[:len(l.nodes)-1]
			l.dirtyDrop(n)
			l.drops++
			l.walBytes += tombBytes
			l.walFrames++
			structural = true
			continue
		}
		cut := len(n.elems) - drop
		for _, e := range n.elems[cut:] {
			n.bytes -= elemSize(len(e))
		}
		n.elems = n.elems[:cut]
		l.count -= drop
		drop = 0
		l.edgeRewr++
		l.billNode(n)
	}
	if structural {
		l.billStructural()
	}
}

// seek prefix-sums the fence to the node holding index i, two-level
// once paged; the returned latency term is the entries scanned.
func (l *list) seek(i int) (ni, off, scanned int) {
	if l.paged() {
		// Page totals first, then entries inside the covering page,
		// the doc 07 two-level shape.
		page := 0
		for ; page*fencePageEnts < len(l.nodes); page++ {
			lo := page * fencePageEnts
			hi := min(lo+fencePageEnts, len(l.nodes))
			total := 0
			for _, n := range l.nodes[lo:hi] {
				total += len(n.elems)
			}
			scanned++
			if i < total {
				break
			}
			i -= total
		}
		for ni = page * fencePageEnts; ; ni++ {
			scanned++
			if i < len(l.nodes[ni].elems) {
				return ni, i, scanned
			}
			i -= len(l.nodes[ni].elems)
		}
	}
	for ni = 0; ; ni++ {
		scanned++
		if i < len(l.nodes[ni].elems) {
			return ni, i, scanned
		}
		i -= len(l.nodes[ni].elems)
	}
}

func (l *list) lindex(i int) []byte {
	ni, off, _ := l.seek(i)
	return l.nodes[ni].elems[off]
}

// lrange streams count elements from index off: nodes touched and
// encoded bytes a cold read of them would pull.
func (l *list) lrange(off, count int, emit func(e []byte)) (nodesTouched, coldBytes int) {
	if off >= l.count || count <= 0 {
		return 0, 0
	}
	ni, o, _ := l.seek(off)
	for count > 0 && ni < len(l.nodes) {
		n := l.nodes[ni]
		nodesTouched++
		coldBytes += n.bytes
		for ; o < len(n.elems) && count > 0; o++ {
			emit(n.elems[o])
			count--
		}
		o = 0
		ni++
	}
	return nodesTouched, coldBytes
}

// walk hands every element to emit in list order, the oracle's view.
func (l *list) walk(emit func(e []byte)) {
	for _, n := range l.nodes {
		for _, e := range n.elems {
			emit(e)
		}
	}
}

// fenceBytes is the current fence footprint for the shape row.
func (l *list) fenceBytes() int {
	return len(l.nodes) * fenceEntBytes
}

type config struct {
	mix     string
	nodeMax int
	ecap    int
	depth   int
	feedCap int
	pageLen int
	ops     int
	elen    int
	seed    int64
}

func elemBytes(rng *rand.Rand, elen int) []byte {
	b := make([]byte, elen)
	for i := range b {
		b[i] = 'a' + byte(rng.Intn(26))
	}
	return b
}

func percentiles(all []time.Duration) (p50, p99 time.Duration) {
	if len(all) == 0 {
		return 0, 0
	}
	s := append([]time.Duration(nil), all...)
	slices.Sort(s)
	return s[len(s)/2], s[(len(s)*99)/100]
}

func row(cfg config, workload string, ops int, nsOp, p50, p99 int64, framesOp, walBOp, nodesOp, readBOp, x1, x2, x3 float64) {
	fmt.Printf("%s,%d,%d,%s,%d,%d,%d,%d,%.3f,%.1f,%.3f,%.1f,%.3f,%.3f,%.3f\n",
		cfg.mix, cfg.nodeMax, cfg.ecap, workload, ops, nsOp, p50, p99, framesOp, walBOp, nodesOp, readBOp, x1, x2, x3)
}

func shapeRow(cfg config, l *list) {
	elemsPerNode, bytesPerNode := 0.0, 0.0
	if len(l.nodes) > 0 {
		elemsPerNode = float64(l.count) / float64(len(l.nodes))
		total := 0
		for _, n := range l.nodes {
			total += n.bytes
		}
		bytesPerNode = float64(total) / float64(len(l.nodes))
	}
	paged := 0.0
	if l.paged() {
		paged = 1
	}
	row(cfg, "shape", l.count, 0, 0, 0, 0, 0, elemsPerNode, bytesPerNode, float64(len(l.nodes)), float64(l.fenceBytes()), paged)
}

func drainRow(cfg config, l *list) {
	wa := 0.0
	if l.logicalB > 0 {
		wa = float64(l.drainedB+l.dirtyBytes) / float64(l.logicalB)
	}
	row(cfg, "drain", int(l.drains), 0, 0, 0, float64(l.walFrames), wa, 0, 0, float64(l.dirtyBytes), 0, 0)
}

// runDeque is the queue marquee shape at steady depth: 50 LPUSH, 50
// RPOP, the doc 07 fast path.
func runDeque(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap)
	for range cfg.depth {
		l.push(true, elemBytes(rng, cfg.elen))
	}
	shapeRow(cfg, l)
	l.walBytes, l.walFrames, l.structural, l.cuts, l.drops = 0, 0, 0, 0, 0
	l.logicalB, l.drainedB, l.drains = 0, 0, 0
	l.dirty, l.dirtyBytes = map[uint64]int{}, 0

	start := time.Now()
	for i := 0; i < cfg.ops; i++ {
		if i%2 == 0 {
			l.push(true, elemBytes(rng, cfg.elen))
		} else {
			l.pop(false)
		}
	}
	elapsed := time.Since(start)
	row(cfg, "queue", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops), 0, 0,
		float64(l.walFrames)/float64(cfg.ops), float64(l.walBytes)/float64(cfg.ops), 0, 0,
		float64(l.cuts)*1000/float64(cfg.ops), float64(l.drops)*1000/float64(cfg.ops), float64(l.structural)*1000/float64(cfg.ops))
	drainRow(cfg, l)
}

// runFeed is the capped feed: every op is LPUSH one entry plus LTRIM
// to the cap, doc 07's O(1)-amortized claim.
func runFeed(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap)
	for range cfg.feedCap {
		l.push(true, elemBytes(rng, cfg.elen))
	}
	shapeRow(cfg, l)
	l.walBytes, l.walFrames, l.structural, l.cuts, l.drops, l.edgeRewr = 0, 0, 0, 0, 0, 0
	l.logicalB, l.drainedB, l.drains = 0, 0, 0
	l.dirty, l.dirtyBytes = map[uint64]int{}, 0

	start := time.Now()
	for range cfg.ops {
		l.push(true, elemBytes(rng, cfg.elen))
		l.ltrimHead(cfg.feedCap)
	}
	elapsed := time.Since(start)
	row(cfg, "feed", cfg.ops, elapsed.Nanoseconds()/int64(cfg.ops), 0, 0,
		float64(l.walFrames)/float64(cfg.ops), float64(l.walBytes)/float64(cfg.ops), 0, 0,
		float64(l.drops)*1000/float64(cfg.ops), float64(l.edgeRewr)*1000/float64(cfg.ops), float64(l.structural)*1000/float64(cfg.ops))
	drainRow(cfg, l)
}

// runPage is the pagination shape: a long static list walked in
// 100-element LRANGE windows at random offsets, with a trickle of
// RPUSH churn, plus an LINDEX latency arm.
func runPage(cfg config) {
	rng := rand.New(rand.NewSource(cfg.seed))
	l := newList(cfg.nodeMax, cfg.ecap)
	for range cfg.depth {
		l.push(false, elemBytes(rng, cfg.elen))
	}
	shapeRow(cfg, l)
	l.walBytes, l.walFrames = 0, 0

	var nodesTouched, coldBytes, windows int
	var rangeLat, indexLat []time.Duration
	sink := 0
	for i := 0; i < cfg.ops; i++ {
		if i%10 == 9 {
			l.push(false, elemBytes(rng, cfg.elen))
			continue
		}
		off := rng.Intn(l.count - cfg.pageLen)
		t0 := time.Now()
		nt, cb := l.lrange(off, cfg.pageLen, func(e []byte) { sink += len(e) })
		rangeLat = append(rangeLat, time.Since(t0))
		nodesTouched += nt
		coldBytes += cb
		windows++

		t0 = time.Now()
		sink += len(l.lindex(rng.Intn(l.count)))
		indexLat = append(indexLat, time.Since(t0))
	}
	p50, p99 := percentiles(rangeLat)
	row(cfg, "range", windows, 0, p50.Nanoseconds(), p99.Nanoseconds(), 0, 0,
		float64(nodesTouched)/float64(windows), float64(coldBytes)/float64(windows), 0, 0, 0)
	p50, p99 = percentiles(indexLat)
	row(cfg, "index", len(indexLat), 0, p50.Nanoseconds(), p99.Nanoseconds(), 0, 0, 0, 0, 0, 0, 0)
	if sink == 42 {
		fmt.Fprintln(os.Stderr, "sink")
	}
}

func main() {
	var cfg config
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.StringVar(&cfg.mix, "mix", "deque", "op mix: deque, dequeid, feed, page")
	flag.IntVar(&cfg.nodeMax, "nodemax", 4032, "node split threshold in bytes")
	flag.IntVar(&cfg.ecap, "ecap", 128, "node element cap")
	flag.IntVar(&cfg.depth, "depth", 10000, "queue depth or page-list length")
	flag.IntVar(&cfg.feedCap, "feedcap", 1000, "capped-feed length")
	flag.IntVar(&cfg.pageLen, "pagelen", 100, "LRANGE window")
	flag.IntVar(&cfg.ops, "ops", 200000, "ops in the measured mix")
	flag.IntVar(&cfg.elen, "elen", 200, "element length in bytes")
	flag.Int64Var(&cfg.seed, "seed", 47, "rng seed")
	flag.Parse()
	if *quick {
		cfg.ops = 5000
		cfg.depth = 2000
	}
	switch cfg.mix {
	case "deque":
		runDeque(cfg)
	case "dequeid":
		cfg.elen = 16
		runDeque(cfg)
	case "feed":
		cfg.elen = 600
		runFeed(cfg)
	case "page":
		cfg.elen = 100
		cfg.depth = 100000
		if *quick {
			cfg.depth = 20000
		}
		runPage(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown mix %q\n", cfg.mix)
		os.Exit(2)
	}
}
