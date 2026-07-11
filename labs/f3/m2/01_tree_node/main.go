// Lab: counted B+ tree node size (spec 2064/f3 doc 12 section 2.3 and 11 exit
// 1, M2 lab 01).
//
// The question: doc 12 builds the native zset as an owner-local counted B+ tree
// and fixes the node block at 256 bytes, four cache lines, arity 16, with a
// leaf holding 15 entries of 16 bytes after an 8-byte header and an interior
// node holding 15 separators and 16 children with a 4-byte subtree count beside
// each child (lines 166-202). Section 11 exit 1 pre-registers the freeze: sweep
// 128/256/512-byte nodes on the rank and range microshapes, freeze the arity
// and the section 2.3 layouts. This lab is that sweep. It builds a real counted
// B+ tree over fixed-size node blocks in a flat arena (lab-local code, NOT the
// engine tree; the tree slice writes that against these constants) so the cache
// behavior of a descent is faithful: a node read touches exactly nodeSize
// contiguous bytes, so 128 versus 256 versus 512 differ in cache lines per
// level and in tree height, which is the whole point.
//
// Method: in-process, no server, no wire, no engine import. Keys are 8-byte
// sortable score prefixes, distinct so each stands for a distinct member; the
// leaf entry carries the full 16 bytes (8B key plus 8B offset/length/back-
// ordinal bookkeeping) the doc names, so bytes/entry accounting is honest even
// though the bench only compares the key. The count width is fixed at the
// doc's 4 bytes here; lab 02 sweeps that. We sweep node size over the branch
// and the leaf independently so the leaf-versus-branch asymmetry question the
// verdict must answer gets its own arms, not just the symmetric ones. Axes:
// node size {128, 256, 512} symmetric plus two asymmetric arms (256 branch with
// 512 and 1024 leaf), cardinality {1k, 10k, 100k, 1M, 4M}, operation {insert,
// delete, rank, select, range walk}.
//
// Read: ns/op per operation, tree height (levels), and structural bytes/entry
// overhead (arena bytes per entry minus the 16-byte leaf entry) at both bulk-
// load fill and random-insert steady-state fill. The bar is PRED-F3-M2-ZSETMEM:
// the tree share of overhead sits in the 2-3B/entry F14 band (doc 12 line 689),
// and the node size that holds rank and range fast without breaking that band
// wins. See README.md for the sweep table and the frozen verdict.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"runtime"
	"sort"
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

func (t *tree) freeLeaf(o uint32)   { t.leafFree = append(t.leafFree, o) }
func (t *tree) freeBranch(o uint32) { t.branchFree = append(t.branchFree, o) }

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
func (t *tree) lSetKey(o uint32, i int, k uint64) {
	binary.LittleEndian.PutUint64(t.leaf(o)[leafHdr+i*entrySz:], k)
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

// rangeWalk emits w keys starting at rank start, following leaf links. Returns
// the count emitted and a checksum.
func (t *tree) rangeWalk(start uint64, w int) (int, uint64) {
	k := start
	ord := t.descendToRank(&k)
	off := int(k)
	var sink uint64
	emitted := 0
	for emitted < w {
		n := t.lNent(ord)
		for off < n && emitted < w {
			sink += t.lKey(ord, off)
			off++
			emitted++
		}
		if emitted >= w {
			break
		}
		nx := t.lNext(ord)
		if nx == 0 {
			break
		}
		ord = nx
		off = 0
	}
	return emitted, sink
}

// delete removes key. Returns whether an entry left.
func (t *tree) delete(key uint64) bool {
	removed := t.deleteFrom(t.root, t.height, key)
	if !t.rootIsLeaf && t.bNkeys(t.root) == 0 {
		old := t.root
		child := t.bChild(t.root, 0)
		t.root = child
		t.height--
		t.rootIsLeaf = t.height == 1
		t.freeBranch(old)
	}
	return removed
}

func (t *tree) deleteFrom(ord uint32, level int, key uint64) bool {
	if level == 1 {
		pos, present := t.leafPos(ord, key)
		if !present {
			return false
		}
		t.leafRemove(ord, pos)
		return true
	}
	c := t.route(ord, key)
	child := t.bChild(ord, c)
	childLeaf := level-1 == 1
	if !t.deleteFrom(child, level-1, key) {
		return false
	}
	t.bSetCount(ord, c, t.bCount(ord, c)-1)
	if childLeaf {
		if t.lNent(child) < t.leafMin() {
			t.fixLeafUnderflow(ord, c)
		}
	} else {
		if t.bNkeys(child) < t.branchMin() {
			t.fixBranchUnderflow(ord, c)
		}
	}
	return true
}

func (t *tree) leafMin() int   { return t.leafCap / 4 }
func (t *tree) branchMin() int { return t.sepMax / 4 }

func (t *tree) leafRemove(o uint32, pos int) {
	n := t.lNent(o)
	blk := t.leaf(o)
	dst := leafHdr + pos*entrySz
	copy(blk[dst:leafHdr+(n-1)*entrySz], blk[dst+entrySz:leafHdr+n*entrySz])
	t.lSetNent(o, n-1)
}

func (t *tree) fixLeafUnderflow(parent uint32, c int) {
	k := t.bNkeys(parent)
	child := t.bChild(parent, c)
	if c < k {
		right := t.bChild(parent, c+1)
		if t.lNent(right) > t.leafMin() {
			t.leafAppend(child, t.lKey(right, 0))
			t.leafRemove(right, 0)
			t.bSetCount(parent, c, t.bCount(parent, c)+1)
			t.bSetCount(parent, c+1, t.bCount(parent, c+1)-1)
			t.bSetSep(parent, c, t.lKey(right, 0))
			return
		}
		t.mergeLeaves(parent, c)
		return
	}
	left := t.bChild(parent, c-1)
	if t.lNent(left) > t.leafMin() {
		ln := t.lNent(left)
		t.leafPrepend(child, t.lKey(left, ln-1))
		t.leafRemove(left, ln-1)
		t.bSetCount(parent, c, t.bCount(parent, c)+1)
		t.bSetCount(parent, c-1, t.bCount(parent, c-1)-1)
		t.bSetSep(parent, c-1, t.lKey(child, 0))
		return
	}
	t.mergeLeaves(parent, c-1)
}

func (t *tree) leafAppend(o uint32, key uint64) {
	n := t.lNent(o)
	t.lSetKey(o, n, key)
	t.lSetNent(o, n+1)
}
func (t *tree) leafPrepend(o uint32, key uint64) {
	n := t.lNent(o)
	blk := t.leaf(o)
	copy(blk[leafHdr+entrySz:leafHdr+(n+1)*entrySz], blk[leafHdr:leafHdr+n*entrySz])
	t.lSetKey(o, 0, key)
	t.lSetNent(o, n+1)
}

func (t *tree) mergeLeaves(parent uint32, c int) {
	left := t.bChild(parent, c)
	right := t.bChild(parent, c+1)
	ln := t.lNent(left)
	rn := t.lNent(right)
	lblk := t.leaf(left)
	rblk := t.leaf(right)
	copy(lblk[leafHdr+ln*entrySz:leafHdr+(ln+rn)*entrySz], rblk[leafHdr:leafHdr+rn*entrySz])
	t.lSetNent(left, ln+rn)
	t.lSetNext(left, t.lNext(right))
	t.freeLeaf(right)
	t.bDropChild(parent, c)
}

// bDropChild removes separator c and child c+1, folding count c+1 into c.
func (t *tree) bDropChild(parent uint32, c int) {
	k := t.bNkeys(parent)
	t.bSetCount(parent, c, t.bCount(parent, c)+t.bCount(parent, c+1))
	// children 0..k become 0..k-1 by shifting c+2..k down into c+1..k-1.
	for i := c + 1; i < k; i++ {
		t.bSetChild(parent, i, t.bChild(parent, i+1))
		t.bSetCount(parent, i, t.bCount(parent, i+1))
	}
	// separators 0..k-1 become 0..k-2 by shifting c+1..k-1 down into c..k-2.
	for i := c; i < k-1; i++ {
		t.bSetSep(parent, i, t.bSep(parent, i+1))
	}
	t.bSetNkeys(parent, k-1)
}

func (t *tree) fixBranchUnderflow(parent uint32, c int) {
	k := t.bNkeys(parent)
	child := t.bChild(parent, c)
	if c < k {
		right := t.bChild(parent, c+1)
		if t.bNkeys(right) > t.branchMin() {
			t.branchBorrowRight(parent, c, child, right)
			return
		}
		t.mergeBranches(parent, c)
		return
	}
	left := t.bChild(parent, c-1)
	if t.bNkeys(left) > t.branchMin() {
		t.branchBorrowLeft(parent, c-1, left, child)
		return
	}
	t.mergeBranches(parent, c-1)
}

func (t *tree) branchBorrowRight(parent uint32, c int, child, right uint32) {
	ck := t.bNkeys(child)
	t.bSetSep(child, ck, t.bSep(parent, c))
	t.bSetChild(child, ck+1, t.bChild(right, 0))
	t.bSetCount(child, ck+1, t.bCount(right, 0))
	t.bSetNkeys(child, ck+1)
	moved := t.bCount(right, 0)
	t.bSetSep(parent, c, t.bSep(right, 0))
	rk := t.bNkeys(right)
	for i := 0; i < rk; i++ {
		t.bSetChild(right, i, t.bChild(right, i+1))
		t.bSetCount(right, i, t.bCount(right, i+1))
	}
	for i := 0; i < rk-1; i++ {
		t.bSetSep(right, i, t.bSep(right, i+1))
	}
	t.bSetNkeys(right, rk-1)
	t.bSetCount(parent, c, t.bCount(parent, c)+moved)
	t.bSetCount(parent, c+1, t.bCount(parent, c+1)-moved)
}

func (t *tree) branchBorrowLeft(parent uint32, c int, left, child uint32) {
	lk := t.bNkeys(left)
	moved := t.bCount(left, lk)
	ck := t.bNkeys(child)
	for i := ck + 1; i > 0; i-- {
		t.bSetChild(child, i, t.bChild(child, i-1))
		t.bSetCount(child, i, t.bCount(child, i-1))
	}
	for i := ck; i > 0; i-- {
		t.bSetSep(child, i, t.bSep(child, i-1))
	}
	t.bSetSep(child, 0, t.bSep(parent, c))
	t.bSetChild(child, 0, t.bChild(left, lk))
	t.bSetCount(child, 0, moved)
	t.bSetNkeys(child, ck+1)
	t.bSetSep(parent, c, t.bSep(left, lk-1))
	t.bSetNkeys(left, lk-1)
	t.bSetCount(parent, c, t.bCount(parent, c)-moved)
	t.bSetCount(parent, c+1, t.bCount(parent, c+1)+moved)
}

func (t *tree) mergeBranches(parent uint32, c int) {
	left := t.bChild(parent, c)
	right := t.bChild(parent, c+1)
	lk := t.bNkeys(left)
	rk := t.bNkeys(right)
	t.bSetSep(left, lk, t.bSep(parent, c))
	for i := 0; i <= rk; i++ {
		t.bSetChild(left, lk+1+i, t.bChild(right, i))
		t.bSetCount(left, lk+1+i, t.bCount(right, i))
	}
	for i := 0; i < rk; i++ {
		t.bSetSep(left, lk+1+i, t.bSep(right, i))
	}
	t.bSetNkeys(left, lk+1+rk)
	t.freeBranch(right)
	t.bDropChild(parent, c)
}

// arenaBytes returns live arena bytes (allocated blocks minus free-listed).
func (t *tree) arenaBytes() int {
	liveLeaf := int(t.nLeaf) - len(t.leafFree)
	liveBranch := int(t.nBranch) - len(t.branchFree)
	return liveLeaf*t.leafSz + liveBranch*t.branchSz
}

func (t *tree) cardinality() uint64 { return t.count(t.root, t.rootIsLeaf) }

type arm struct {
	name     string
	branchSz int
	leafSz   int
}

type cell struct {
	arm       string
	card      int
	height    int
	arity     int
	leafCap   int
	insNs     float64
	delNs     float64
	rankNs    float64
	selNs     float64
	rangeNs   float64
	bpeBulk   float64
	bpeRandom float64
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

func build(a arm, keys []uint64) *tree {
	t := newTree(a.branchSz, a.leafSz, 4)
	for _, k := range keys {
		t.insert(k)
	}
	return t
}

// bulkLoad builds the tree from sorted keys with a right-edge fill of ~0.9,
// the doc's section 2.4 sorted-load path, so the memory reading is the best-case
// packing rather than the ~0.5 fill that ascending single-key inserts produce.
func bulkLoad(a arm, sorted []uint64) *tree {
	t := newTree(a.branchSz, a.leafSz, 4)
	t.nLeaf, t.nBranch = 0, 0
	t.leaves, t.branches = t.leaves[:0], t.branches[:0]
	t.leafFree, t.branchFree = nil, nil
	if len(sorted) == 0 {
		t.root = t.allocLeaf()
		t.rootIsLeaf = true
		t.height = 1
		return t
	}
	type node struct {
		ord      uint32
		firstKey uint64
		count    uint64
	}
	// spans slices n items into groups whose average size is fill*cap, spread as
	// evenly as possible so the whole level sits at a true ~0.9 fill instead of a
	// run of full nodes plus one near-empty tail, which is what a balanced right-edge
	// bulk loader produces and is the fair reading for the memory column.
	spans := func(n, cap int) [][2]int {
		target := 0.9 * float64(cap)
		groups := int(float64(n)/target + 0.5)
		// never fewer groups than the node capacity allows, else a group overflows.
		if min := (n + cap - 1) / cap; groups < min {
			groups = min
		}
		if groups < 1 {
			groups = 1
		}
		if groups > n {
			groups = n
		}
		base, extra := n/groups, n%groups
		out := make([][2]int, 0, groups)
		at := 0
		for g := 0; g < groups; g++ {
			sz := base
			if g < extra {
				sz++
			}
			out = append(out, [2]int{at, at + sz})
			at += sz
		}
		return out
	}
	var level []node
	for _, sp := range spans(len(sorted), t.leafCap) {
		o := t.allocLeaf()
		for j := sp[0]; j < sp[1]; j++ {
			t.lSetKey(o, j-sp[0], sorted[j])
		}
		t.lSetNent(o, sp[1]-sp[0])
		level = append(level, node{o, sorted[sp[0]], uint64(sp[1] - sp[0])})
	}
	for i := 0; i < len(level)-1; i++ {
		t.lSetNext(level[i].ord, level[i+1].ord)
	}
	height := 1
	for len(level) > 1 {
		var up []node
		for _, sp := range spans(len(level), t.arity) {
			o := t.allocBranch()
			t.bSetNkeys(o, sp[1]-sp[0]-1)
			var sum uint64
			for j := sp[0]; j < sp[1]; j++ {
				t.bSetChild(o, j-sp[0], level[j].ord)
				t.bSetCount(o, j-sp[0], level[j].count)
				sum += level[j].count
				if j > sp[0] {
					t.bSetSep(o, j-sp[0]-1, level[j].firstKey)
				}
			}
			up = append(up, node{o, level[sp[0]].firstKey, sum})
		}
		level = up
		height++
	}
	t.root = level[0].ord
	t.height = height
	t.rootIsLeaf = height == 1
	return t
}

func main() {
	quick := flag.Bool("quick", false, "smaller cardinalities and op counts")
	flag.Parse()

	arms := []arm{
		{"128", 128, 128},
		{"256", 256, 256},
		{"512", 512, 512},
		{"256b/512l", 256, 512},
		{"256b/1024l", 256, 1024},
	}
	cards := []int{1_000, 10_000, 100_000, 1_000_000, 4_000_000}
	if *quick {
		cards = []int{1_000, 10_000, 100_000}
	}

	fmt.Printf("counted B+ tree node-size sweep, %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("%-11s %8s %4s %4s %5s %7s %7s %7s %7s %7s %8s %8s\n",
		"arm", "card", "lvl", "ari", "lcap", "insNs", "delNs", "rankNs", "selNs", "rngNs", "bpeBulk", "bpeRand")
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
		sorted := make([]uint64, cd)
		copy(sorted, keys)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		// Op budget: enough samples for a stable ns/op, capped so the largest
		// cardinalities do not dominate the run. Rank/select/range are O(log n)
		// so a few hundred thousand samples already averages out box noise.
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
		// distinct present keys to delete, drawn once and shared across arms.
		// There are only cd present keys, so the delete bench is capped there.
		delN := opN
		if delN > cd {
			delN = cd
		}
		delKeys := make([]uint64, delN)
		dperm := make([]uint64, cd)
		copy(dperm, keys)
		dm := xorshift(0xabcdef ^ uint64(cd))
		for i := 0; i < delN; i++ {
			j := i + int(dm.next()%uint64(cd-i))
			dperm[i], dperm[j] = dperm[j], dperm[i]
			delKeys[i] = dperm[i]
		}
		dperm = nil
		seen = nil // release the dedup set before building the large trees.

		for _, a := range arms {
			// t serves rank/select/range and the random-fill memory reading.
			t := build(a, keys)
			card := int(t.cardinality())
			height, arity, leafCap := t.height, t.arity, t.leafCap
			bpeRandom := float64(t.arenaBytes())/float64(card) - 16
			r := xorshift(0xd1b54a32d192ed03 ^ uint64(cd))
			var sink uint64

			s := time.Now()
			for i := 0; i < opN; i++ {
				rk, _ := t.rank(keys[r.next()%uint64(card)])
				sink += rk
			}
			rankNs := float64(time.Since(s).Nanoseconds()) / float64(opN)

			s = time.Now()
			for i := 0; i < opN; i++ {
				sink += t.selectAt(r.next() % uint64(card))
			}
			selNs := float64(time.Since(s).Nanoseconds()) / float64(opN)

			rangeOps := opN / 20
			if rangeOps < 1 {
				rangeOps = 1
			}
			maxStart := uint64(card)
			if card > 100 {
				maxStart = uint64(card - 100)
			}
			s = time.Now()
			for i := 0; i < rangeOps; i++ {
				st := r.next() % maxStart
				_, sk := t.rangeWalk(st, 100)
				sink += sk
			}
			rangeNs := float64(time.Since(s).Nanoseconds()) / float64(rangeOps) / 100
			t = nil // free before the next build to keep peak memory down.

			ti := build(a, keys)
			s = time.Now()
			for _, k := range insKeys {
				ti.insert(k)
			}
			insNs := float64(time.Since(s).Nanoseconds()) / float64(opN)
			ti = nil

			td := build(a, keys)
			s = time.Now()
			for _, k := range delKeys {
				td.delete(k)
			}
			delNs := float64(time.Since(s).Nanoseconds()) / float64(len(delKeys))
			td = nil

			ts := bulkLoad(a, sorted)
			bpeBulk := float64(ts.arenaBytes())/float64(card) - 16
			ts = nil
			runtime.GC()

			if sink == 0xdeadbeef {
				fmt.Println(sink)
			}
			c := cell{
				arm: a.name, card: cd, height: height, arity: arity, leafCap: leafCap,
				insNs: insNs, delNs: delNs, rankNs: rankNs, selNs: selNs, rangeNs: rangeNs,
				bpeBulk: bpeBulk, bpeRandom: bpeRandom,
			}
			fmt.Printf("%-11s %8d %4d %4d %5d %7.1f %7.1f %7.1f %7.1f %7.2f %8.2f %8.2f\n",
				c.arm, c.card, c.height, c.arity, c.leafCap,
				c.insNs, c.delNs, c.rankNs, c.selNs, c.rangeNs, c.bpeBulk, c.bpeRandom)
		}
		fmt.Println()
	}
}
