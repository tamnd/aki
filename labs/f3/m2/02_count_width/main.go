// Lab: counted B+ tree subtree-count width (spec 2064/f3 doc 12 section 2.3 and
// 11 exit 1, M2 lab 02).
//
// The question: an interior node stores, beside each child ordinal, the count of
// live entries in that child's subtree; those counts are the whole order-
// statistic machinery for rank and select (doc 12 lines 166-201). The doc sizes
// that count at 4 bytes, "capping a subtree at ~4 billion entries, which covers
// the collection cap with headroom" (line 234). This lab sweeps the width, u16
// versus u32 versus u64, on lab 01's frozen node size (256-byte branch, 512-byte
// leaf) to settle two things the layout depends on: whether a narrower count
// overflows at the cardinalities the zset must hold, and what the width costs in
// arity, tree height and memory. The width sets the interior arity, because a
// 256-byte branch fits branchSz / (8 separator + 4 ordinal + w count) children,
// so u16 buys arity 18, u32 gives the doc's 16, and u64 drops it to 12.
//
// Method: in-process, no server, no wire, no engine import. The tree is the same
// lab-local counted B+ tree as lab 01, over fixed-size node blocks in a flat
// arena, with the count width parameterised. Keys are 8-byte sortable score
// prefixes, distinct so each stands for a distinct member. For each width and
// cardinality the lab builds a random-insert tree and a 0.9-fill bulk tree,
// measures the largest subtree count any interior node must store against the
// width's ceiling (65535, ~4.29e9, ~1.8e19), checks whether the stored counts
// stay consistent with the real subtree sizes (a narrow width that overflows
// silently truncates and breaks rank), and times rank, select and insert.
// Axes: width {u16, u32, u64}, cardinality {1k, 10k, 100k, 1M, 4M}.
//
// Read: max subtree count versus ceiling and the overflow verdict, interior
// bytes/entry overhead, arity and tree height, and ns/op for rank, select and
// insert. The bar is PRED-F3-M2-ZSETMEM: the tree overhead sits in the 2-3B/entry
// F14 band, and the count width barely moves that band (interior nodes are a
// small fraction of the tree), so the width is chosen on correctness first, then
// on arity and descent cost. See README.md for the sweep table and the frozen
// verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"runtime"
	"time"
)

// Node block layout, doc 12 section 2.3.
//
// Leaf (leafSz bytes):
//
//	0      1     tag
//	1      1     nentries
//	2      2     generation
//	4      4     next-leaf ordinal
//	8      e*16  entries: 8B sortable key, 4B offset, 2B length, 2B back-ordinal
//
// Interior (branchSz bytes), count width w bytes (4 here):
//
//	0      1        tag
//	1      1        nkeys k (k separators, k+1 children)
//	2      2        generation
//	4      4        reserved (parent hint)
//	8      (a-1)*8  separators (first key of each right child)
//	...    a*4      child ordinals
//	...    a*w      child subtree counts
//
// With w=4 the per-child cost is 8 (ordinal 4 plus count 4) and the header plus
// the one missing separator (a-1 not a) exactly fill the block, so arity a is
// branchSz/16.
const (
	tagLeaf   = 0x02
	tagBranch = 0x01

	leafHdr   = 8
	branchHdr = 8
	entrySz   = 16
	ordSz     = 4
)

// tree is a lab-local counted B+ tree over fixed-size node blocks in two flat
// arenas, one per node kind so the two kinds can carry different block sizes.
// Nodes are addressed by 4-byte ordinals into their kind's arena, never Go
// pointers, matching the engine's arena discipline (F7).
type tree struct {
	leaves   []byte
	branches []byte
	leafSz   int
	branchSz int
	countW   int
	leafCap  int
	arity    int
	sepMax   int

	leafFree   []uint32
	branchFree []uint32
	nLeaf      uint32
	nBranch    uint32

	root       uint32
	rootIsLeaf bool
	height     int // 1 == root is a leaf
}

func newTree(branchSz, leafSz, countW int) *tree {
	t := &tree{
		leafSz:   leafSz,
		branchSz: branchSz,
		countW:   countW,
		leafCap:  (leafSz - leafHdr) / entrySz,
		arity:    (branchSz - branchHdr + 8) / (8 + ordSz + countW),
	}
	t.sepMax = t.arity - 1
	t.root = t.allocLeaf()
	t.rootIsLeaf = true
	t.height = 1
	return t
}

func (t *tree) allocLeaf() uint32 {
	if n := len(t.leafFree); n > 0 {
		o := t.leafFree[n-1]
		t.leafFree = t.leafFree[:n-1]
		t.clearLeaf(o)
		return o
	}
	o := t.nLeaf
	t.nLeaf++
	t.leaves = append(t.leaves, make([]byte, t.leafSz)...)
	t.clearLeaf(o)
	return o
}

func (t *tree) allocBranch() uint32 {
	if n := len(t.branchFree); n > 0 {
		o := t.branchFree[n-1]
		t.branchFree = t.branchFree[:n-1]
		t.clearBranch(o)
		return o
	}
	o := t.nBranch
	t.nBranch++
	t.branches = append(t.branches, make([]byte, t.branchSz)...)
	t.clearBranch(o)
	return o
}

func (t *tree) leaf(o uint32) []byte {
	b := int(o) * t.leafSz
	return t.leaves[b : b+t.leafSz]
}
func (t *tree) branch(o uint32) []byte {
	b := int(o) * t.branchSz
	return t.branches[b : b+t.branchSz]
}

func (t *tree) clearLeaf(o uint32) {
	n := t.leaf(o)
	for i := range n {
		n[i] = 0
	}
	n[0] = tagLeaf
}
func (t *tree) clearBranch(o uint32) {
	n := t.branch(o)
	for i := range n {
		n[i] = 0
	}
	n[0] = tagBranch
}

// A node's kind is known from its level in the tree, never from a tag probe:
// the two arenas share ordinal ranges, so a byte read cannot tell a leaf from a
// branch. Level 1 is the leaf level; a node at level L has children at L-1.

// Leaf accessors.
func (t *tree) lNent(o uint32) int       { return int(t.leaf(o)[1]) }
func (t *tree) lSetNent(o uint32, n int) { t.leaf(o)[1] = byte(n) }
func (t *tree) lNext(o uint32) uint32    { return binary.LittleEndian.Uint32(t.leaf(o)[4:]) }
func (t *tree) lSetNext(o, v uint32)     { binary.LittleEndian.PutUint32(t.leaf(o)[4:], v) }
func (t *tree) lKey(o uint32, i int) uint64 {
	return binary.LittleEndian.Uint64(t.leaf(o)[leafHdr+i*entrySz:])
}

// Branch accessors.
func (t *tree) bNkeys(o uint32) int       { return int(t.branch(o)[1]) }
func (t *tree) bSetNkeys(o uint32, n int) { t.branch(o)[1] = byte(n) }
func (t *tree) bSep(o uint32, i int) uint64 {
	return binary.LittleEndian.Uint64(t.branch(o)[branchHdr+i*8:])
}
func (t *tree) bSetSep(o uint32, i int, k uint64) {
	binary.LittleEndian.PutUint64(t.branch(o)[branchHdr+i*8:], k)
}
func (t *tree) childOff() int { return branchHdr + t.sepMax*8 }
func (t *tree) countOff() int { return t.childOff() + t.arity*ordSz }
func (t *tree) bChild(o uint32, i int) uint32 {
	return binary.LittleEndian.Uint32(t.branch(o)[t.childOff()+i*ordSz:])
}
func (t *tree) bSetChild(o uint32, i int, v uint32) {
	binary.LittleEndian.PutUint32(t.branch(o)[t.childOff()+i*ordSz:], v)
}
func (t *tree) bCount(o uint32, i int) uint64 {
	p := t.branch(o)[t.countOff()+i*t.countW:]
	switch t.countW {
	case 2:
		return uint64(binary.LittleEndian.Uint16(p))
	case 8:
		return binary.LittleEndian.Uint64(p)
	default:
		return uint64(binary.LittleEndian.Uint32(p))
	}
}
func (t *tree) bSetCount(o uint32, i int, v uint64) {
	p := t.branch(o)[t.countOff()+i*t.countW:]
	switch t.countW {
	case 2:
		binary.LittleEndian.PutUint16(p, uint16(v))
	case 8:
		binary.LittleEndian.PutUint64(p, v)
	default:
		binary.LittleEndian.PutUint32(p, uint32(v))
	}
}

// count is the number of live entries under a node.
func (t *tree) count(o uint32, isLeaf bool) uint64 {
	if isLeaf {
		return uint64(t.lNent(o))
	}
	var s uint64
	k := t.bNkeys(o)
	for i := 0; i <= k; i++ {
		s += t.bCount(o, i)
	}
	return s
}

// route returns the child index for key: the number of separators <= key.
func (t *tree) route(o uint32, key uint64) int {
	k := t.bNkeys(o)
	lo, hi := 0, k
	for lo < hi {
		m := (lo + hi) / 2
		if t.bSep(o, m) <= key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}

// leafPos returns the index of the first entry >= key and whether key is present.
func (t *tree) leafPos(o uint32, key uint64) (int, bool) {
	n := t.lNent(o)
	lo, hi := 0, n
	for lo < hi {
		m := (lo + hi) / 2
		if t.lKey(o, m) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < n && t.lKey(o, lo) == key {
		return lo, true
	}
	return lo, false
}

// insert adds key (distinct keys only). Returns whether a new entry landed.
func (t *tree) insert(key uint64) bool {
	added, split, sep, right := t.insertInto(t.root, t.height, key)
	if split {
		leftLeaf := t.height == 1
		nr := t.allocBranch()
		t.bSetNkeys(nr, 1)
		t.bSetSep(nr, 0, sep)
		t.bSetChild(nr, 0, t.root)
		t.bSetChild(nr, 1, right)
		t.bSetCount(nr, 0, t.count(t.root, leftLeaf))
		t.bSetCount(nr, 1, t.count(right, leftLeaf))
		t.root = nr
		t.rootIsLeaf = false
		t.height++
	}
	return added
}

func (t *tree) insertInto(ord uint32, level int, key uint64) (added, split bool, sep uint64, right uint32) {
	if level == 1 {
		return t.leafInsert(ord, key)
	}
	c := t.route(ord, key)
	child := t.bChild(ord, c)
	childLeaf := level-1 == 1
	cadded, csplit, csep, cright := t.insertInto(child, level-1, key)
	if cadded {
		t.bSetCount(ord, c, t.bCount(ord, c)+1)
	}
	if !csplit {
		return cadded, false, 0, 0
	}
	t.bSetCount(ord, c, t.count(child, childLeaf))
	s, r := t.branchInsertChild(ord, c+1, csep, cright, t.count(cright, childLeaf))
	if r == noSplit {
		return cadded, false, 0, 0
	}
	return cadded, true, s, r
}

func (t *tree) leafInsert(o uint32, key uint64) (added, split bool, sep uint64, right uint32) {
	pos, present := t.leafPos(o, key)
	if present {
		return false, false, 0, 0
	}
	if t.lNent(o) < t.leafCap {
		t.leafShiftIn(o, pos, key)
		return true, false, 0, 0
	}
	right = t.splitLeaf(o)
	if key >= t.lKey(right, 0) {
		p, _ := t.leafPos(right, key)
		t.leafShiftIn(right, p, key)
	} else {
		p, _ := t.leafPos(o, key)
		t.leafShiftIn(o, p, key)
	}
	return true, true, t.lKey(right, 0), right
}

func (t *tree) leafShiftIn(o uint32, pos int, key uint64) {
	n := t.lNent(o)
	blk := t.leaf(o)
	src := leafHdr + pos*entrySz
	copy(blk[src+entrySz:leafHdr+(n+1)*entrySz], blk[src:leafHdr+n*entrySz])
	binary.LittleEndian.PutUint64(blk[src:], key)
	t.lSetNent(o, n+1)
}

func (t *tree) splitLeaf(o uint32) uint32 {
	n := t.lNent(o)
	half := n / 2
	right := t.allocLeaf()
	blk := t.leaf(o)
	rblk := t.leaf(right)
	copy(rblk[leafHdr:], blk[leafHdr+half*entrySz:leafHdr+n*entrySz])
	t.lSetNent(right, n-half)
	t.lSetNent(o, half)
	t.lSetNext(right, t.lNext(o))
	t.lSetNext(o, right)
	return right
}

const noSplit = ^uint32(0)

// branchInsertChild inserts child (with separator sep before it) at position ci.
// If the branch is full it splits, returning the promoted separator and the new
// right branch ordinal; otherwise it returns noSplit.
func (t *tree) branchInsertChild(o uint32, ci int, sep uint64, child uint32, cnt uint64) (uint64, uint32) {
	k := t.bNkeys(o)
	if k < t.sepMax {
		t.branchShiftIn(o, ci, sep, child, cnt)
		return 0, noSplit
	}
	// Gather k+2 children/counts and k+1 separators with the newcomer in place.
	kids := make([]uint32, 0, k+2)
	cnts := make([]uint64, 0, k+2)
	seps := make([]uint64, 0, k+1)
	for i := 0; i <= k; i++ {
		if i == ci {
			kids = append(kids, child)
			cnts = append(cnts, cnt)
		}
		kids = append(kids, t.bChild(o, i))
		cnts = append(cnts, t.bCount(o, i))
	}
	if ci == k+1 {
		kids = append(kids, child)
		cnts = append(cnts, cnt)
	}
	oi := 0 // index into original separators
	for i := 0; i < len(kids)-1; i++ {
		if i+1 == ci {
			seps = append(seps, sep)
		} else {
			seps = append(seps, t.bSep(o, oi))
			oi++
		}
	}
	total := len(kids) // k+2
	mid := total / 2
	upSep := seps[mid-1]
	right := t.allocBranch()
	t.bSetNkeys(o, mid-1)
	for i := 0; i < mid; i++ {
		t.bSetChild(o, i, kids[i])
		t.bSetCount(o, i, cnts[i])
	}
	for i := 0; i < mid-1; i++ {
		t.bSetSep(o, i, seps[i])
	}
	rk := total - mid
	t.bSetNkeys(right, rk-1)
	for i := 0; i < rk; i++ {
		t.bSetChild(right, i, kids[mid+i])
		t.bSetCount(right, i, cnts[mid+i])
	}
	for i := 0; i < rk-1; i++ {
		t.bSetSep(right, i, seps[mid+i])
	}
	return upSep, right
}

func (t *tree) branchShiftIn(o uint32, ci int, sep uint64, child uint32, cnt uint64) {
	k := t.bNkeys(o)
	for i := k + 1; i > ci; i-- {
		t.bSetChild(o, i, t.bChild(o, i-1))
		t.bSetCount(o, i, t.bCount(o, i-1))
	}
	t.bSetChild(o, ci, child)
	t.bSetCount(o, ci, cnt)
	for i := k; i > ci-1; i-- {
		t.bSetSep(o, i, t.bSep(o, i-1))
	}
	t.bSetSep(o, ci-1, sep)
	t.bSetNkeys(o, k+1)
}

// rank returns the number of entries sorting before key and whether present.
func (t *tree) rank(key uint64) (uint64, bool) {
	ord := t.root
	level := t.height
	var acc uint64
	for level > 1 {
		c := t.route(ord, key)
		for i := 0; i < c; i++ {
			acc += t.bCount(ord, i)
		}
		ord = t.bChild(ord, c)
		level--
	}
	pos, present := t.leafPos(ord, key)
	return acc + uint64(pos), present
}

// selectAt returns the key at rank k (0-based, k < cardinality).
func (t *tree) selectAt(k uint64) uint64 {
	ord := t.descendToRank(&k)
	return t.lKey(ord, int(k))
}

// descendToRank walks to the leaf holding rank *k and rewrites *k to the offset
// within that leaf.
func (t *tree) descendToRank(k *uint64) uint32 {
	ord := t.root
	level := t.height
	for level > 1 {
		nk := t.bNkeys(ord)
		c := 0
		for c <= nk {
			cc := t.bCount(ord, c)
			if *k < cc {
				break
			}
			*k -= cc
			c++
		}
		ord = t.bChild(ord, c)
		level--
	}
	return ord
}

func (t *tree) cardinality() uint64 { return t.count(t.root, t.rootIsLeaf) }

// The node size is frozen by lab 01: a 256-byte branch and a 512-byte leaf.
// This lab varies only the subtree-count width.
const (
	fixedBranchSz = 256
	fixedLeafSz   = 512
)

// arm is one count width.
type arm struct {
	name   string
	countW int
	ceil   uint64 // largest subtree count the width can store
}

type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// interiorStats walks the tree and returns the largest true subtree entry count
// any interior node must store beside a child, and whether every stored count
// already equals the true subtree size. A width too narrow for the tree fails
// the consistency check because bSetCount truncated the value on the way in,
// which is exactly the silent overflow that breaks rank and select.
func (t *tree) interiorStats() (maxCount uint64, consistent bool) {
	consistent = true
	var walk func(ord uint32, level int) uint64
	walk = func(ord uint32, level int) uint64 {
		if level == 1 {
			return uint64(t.lNent(ord))
		}
		k := t.bNkeys(ord)
		var sum uint64
		for i := 0; i <= k; i++ {
			real := walk(t.bChild(ord, i), level-1)
			if real > maxCount {
				maxCount = real
			}
			if t.bCount(ord, i) != real {
				consistent = false
			}
			sum += real
		}
		return sum
	}
	walk(t.root, t.height)
	return
}

// branchBytes is the live interior-node arena size, the only term the count
// width moves.
func (t *tree) branchBytes() int {
	return (int(t.nBranch) - len(t.branchFree)) * t.branchSz
}

func build(countW int, keys []uint64) *tree {
	t := newTree(fixedBranchSz, fixedLeafSz, countW)
	for _, k := range keys {
		t.insert(k)
	}
	return t
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities and op counts")
	flag.Parse()

	arms := []arm{
		{"u16", 2, 1<<16 - 1},
		{"u32", 4, 1<<32 - 1},
		{"u64", 8, 1<<64 - 1},
	}
	cards := []int{1_000, 10_000, 100_000, 1_000_000, 4_000_000}
	if *quick {
		cards = []int{1_000, 10_000, 100_000}
	}

	fmt.Printf("counted B+ tree subtree-count width sweep, 256B branch, 512B leaf, %s\n",
		time.Now().Format("2006-01-02"))
	fmt.Printf("%-6s %8s %4s %4s %10s %6s %5s %7s %7s %7s %8s\n",
		"width", "card", "ari", "lvl", "maxCount", "ovf", "ok", "rankNs", "selNs", "insNs", "ibpe")
	for _, cd := range cards {
		keys := make([]uint64, cd)
		rng := xorshift(0x9e3779b97f4a7c15 ^ uint64(cd))
		seen := make(map[uint64]struct{}, cd)
		for i := range keys {
			var k uint64
			for {
				k = mix(rng.next())
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					break
				}
			}
			keys[i] = k
		}

		// Op budget: enough samples for a stable ns/op, capped so the largest
		// cardinalities do not dominate the run.
		opN := 2_000_000
		if cd >= 1_000_000 {
			opN = 500_000
		}
		if *quick {
			opN = 200_000
		}
		if opN > cd*2 {
			opN = cd * 2
		}

		// fresh insert keys disjoint from the built set, shared across arms.
		insKeys := make([]uint64, opN)
		im := xorshift(0x123456789 ^ uint64(cd))
		for i := range insKeys {
			var k uint64
			for {
				k = mix(im.next())
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					break
				}
			}
			insKeys[i] = k
		}
		clear(seen) // release the dedup set before building the large trees.

		for _, a := range arms {
			t := build(a.countW, keys)
			card := int(t.cardinality())
			arity, height := t.arity, t.height
			maxCount, consistent := t.interiorStats()
			overflow := maxCount > a.ceil
			// Divide interior bytes by the true entry count, not the tree's own
			// cardinality: an overflowed u16 tree derives its cardinality from the
			// truncated root counts, so cardinality() lies for the overflow arm.
			ibpe := float64(t.branchBytes()) / float64(len(keys))
			r := xorshift(0xd1b54a32d192ed03 ^ uint64(cd))
			var sink uint64

			// Rank and select only mean anything when the counts are intact; a
			// truncated width returns wrong ranks and can walk select off the end,
			// so on an inconsistent tree we report the overflow and skip the timing.
			var rankNs, selNs float64 = -1, -1
			if consistent {
				s := time.Now()
				for i := 0; i < opN; i++ {
					rk, _ := t.rank(keys[r.next()%uint64(card)])
					sink += rk
				}
				rankNs = float64(time.Since(s).Nanoseconds()) / float64(opN)

				s = time.Now()
				for i := 0; i < opN; i++ {
					sink += t.selectAt(r.next() % uint64(card))
				}
				selNs = float64(time.Since(s).Nanoseconds()) / float64(opN)
			}
			t = nil

			// insert routes on separators and only bumps counts, so it never walks
			// off the end; it is safe to time even for the overflow arm.
			ti := build(a.countW, keys)
			s := time.Now()
			for _, k := range insKeys {
				ti.insert(k)
			}
			insNs := float64(time.Since(s).Nanoseconds()) / float64(opN)
			ti = nil
			runtime.GC()

			if sink == 0xdeadbeef {
				fmt.Println(sink)
			}
			ovf, ok := "no", "yes"
			if overflow {
				ovf = "YES"
			}
			if !consistent {
				ok = "NO"
			}
			rankS, selS := fmtNs(rankNs), fmtNs(selNs)
			fmt.Printf("%-6s %8d %4d %4d %10d %6s %5s %7s %7s %7.1f %8.2f\n",
				a.name, cd, arity, height, maxCount, ovf, ok, rankS, selS, insNs, ibpe)
		}
		fmt.Println()
	}
}

func fmtNs(v float64) string {
	if v < 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f", v)
}
