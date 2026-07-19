package structs

import (
	"bytes"
	"encoding/binary"
)

// Tree is the owner-local counted B+ tree that serves every ordered zset view
// (spec 2064/f3/12 section 2): keyed by (score, member) in the zset total order,
// subtree counts beside every child so rank and select are O(log n) with no side
// structure, leaves singly linked for range walks. It is the shared package M5's
// directory and M6's geo reuse (issue #544), so it lands standalone here, not
// wired to any type; the zset dual-write slice swaps its placeholder store for
// this behind the same method set.
//
// Arena discipline (F7): nodes are fixed-size blocks in two flat byte arenas,
// one per kind so a leaf and a branch can carry different block sizes, addressed
// by 4-byte ordinals, never Go pointers, so a scan is invisible to the collector.
// The kind of a node is known from its level in the tree (level 1 is the leaf
// level), never from a tag probe, because the two arenas share ordinal ranges.
// It is owner-local (F1): one shard goroutine reads and writes it, so nothing
// here is atomic and nothing locks.
//
// The node size and count width are the frozen verdicts of labs 01 and 02
// (labs/f3/m2): a 256-byte branch at arity 16, a 512-byte leaf at 31 entries as
// the leaf-versus-branch asymmetry that clears the 2-3B/entry F14 bar, and a
// 4-byte (u32) subtree count, the only width both correct at million-entry scale
// and not paying an extra tree level for range it can never use. NewTree bakes
// them; the size and width stay parameters so the pinning tests can prove the
// arity is a pure function of them and that a narrower count truncates.
//
// Score is the order key: an 8-byte sortable prefix (section 3.1's order-
// preserving transform of the IEEE double, or a geohash for GEO), opaque and
// ordered at this layer. Ties on score break on the raw member bytes ascending,
// which is what makes the order total and lex range meaningful. Separators route
// on the 8-byte score alone when scores differ and fall through to a member
// compare only inside a tied band spanning a node boundary; the member bytes
// themselves live in the caller's arena, addressed here by a 4-byte reference the
// Members callback turns back into bytes, so a separator costs 8 bytes in the
// block plus a 4-byte reference beside it, never the member length.
type Tree struct {
	leaves   []byte
	branches []byte
	// srefs holds, beside each in-block score separator, the member reference of
	// its boundary key, so a tied-band routing decision compares members exactly
	// without widening the separator or descending to fetch the boundary member.
	srefs []uint32

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
	entries    uint64

	// Edge-leaf ordinal cache (lab 04, labs/f3/m2): the leftmost and rightmost
	// leaf ordinals, 8 bytes for the whole tree, so a read that walks from an end
	// skips the root-to-leaf descent. It is read-path only, never steering a
	// mutation, and is lazily refilled (a fresh descent is 9-17ns), so it is
	// invalidated wholesale on any split or merge that could allocate or free a
	// leaf and left leafNone until the next end read repopulates it. edgeCache is
	// the off switch the byte-identical cache-parity test flips; production always
	// leaves it on.
	minLeaf   uint32
	maxLeaf   uint32
	edgeCache bool
}

// Members turns a member reference back into its bytes for a tie-break compare.
// The tree stores references, never member bytes, so a full scan touches no
// member payload; the callback is invoked only when two keys share a score, so a
// distinct-score workload never pays it. Passing a pointer that implements it
// allocates nothing.
type Members interface {
	// Member returns the raw member bytes stored at ref. It runs only on a score
	// tie, the confirm-before-compare of the ordered path.
	Member(ref uint32) []byte
}

// Frozen node geometry, labs 01 and 02.
const (
	// BranchSize is the interior block: four cache lines, the descent sweet spot.
	BranchSize = 256
	// LeafSize is the leaf block: the leaf-versus-branch asymmetry lab 01 froze to
	// clear the 2-3B/entry bar, 31 entries against the 256-byte leaf's 15.
	LeafSize = 512
	// CountWidth is the subtree-count width: u32, correct to ~4.29e9 without the
	// extra tree level u64 costs at 4M.
	CountWidth = 4

	tagLeaf   = 0x02
	tagBranch = 0x01
	leafHdr   = 8 // tag, nentries, generation, next-leaf ordinal
	branchHdr = 8 // tag, nkeys, generation, parent hint
	entrySz   = 16
	ordSz     = 4
	noSplit   = ^uint32(0)
	// leafNone marks the edge-leaf cache as stale: no ordinal is known and the
	// next end read must descend to refill it.
	leafNone = ^uint32(0)
)

// ArityFor is the interior fanout as a pure function of the branch block size and
// the count width: a child costs an 8-byte separator, a 4-byte ordinal and a
// count of countW bytes, and the header plus the one unused separator slot fill
// the block, so the fanout is branchSz/(8+4+countW). Pinned by the tests so a
// layout change cannot silently move it: 256-byte branch is 18 at u16, 16 at u32,
// 12 at u64.
func ArityFor(branchSz, countW int) int {
	return (branchSz - branchHdr + 8) / (8 + ordSz + countW)
}

// LeafCapFor is the leaf entry capacity for a leaf block size: the header plus
// n 16-byte entries fill the block, so 512 bytes holds 31.
func LeafCapFor(leafSz int) int { return (leafSz - leafHdr) / entrySz }

// NewTree returns an empty tree at the frozen geometry: a 256-byte branch at
// arity 16, a 512-byte leaf at 31 entries, u32 counts.
func NewTree() *Tree { return newTreeSized(BranchSize, LeafSize, CountWidth) }

// newTreeSized builds a tree at an explicit geometry, the seam the pinning and
// width tests drive to prove arity and the count ceiling against the constants.
func newTreeSized(branchSz, leafSz, countW int) *Tree {
	t := &Tree{
		leafSz:   leafSz,
		branchSz: branchSz,
		countW:   countW,
		leafCap:  LeafCapFor(leafSz),
		arity:    ArityFor(branchSz, countW),
	}
	t.sepMax = t.arity - 1
	t.edgeCache = true
	t.minLeaf = leafNone
	t.maxLeaf = leafNone
	t.root = t.allocLeaf()
	t.rootIsLeaf = true
	t.height = 1
	return t
}

// Arity is the interior fanout this tree was built at.
func (t *Tree) Arity() int { return t.arity }

// LeafCap is the leaf entry capacity this tree was built at.
func (t *Tree) LeafCap() int { return t.leafCap }

// Len is the live entry count, the ZCARD source.
func (t *Tree) Len() int { return int(t.entries) }

func (t *Tree) allocLeaf() uint32 {
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

func (t *Tree) allocBranch() uint32 {
	if n := len(t.branchFree); n > 0 {
		o := t.branchFree[n-1]
		t.branchFree = t.branchFree[:n-1]
		t.clearBranch(o)
		return o
	}
	o := t.nBranch
	t.nBranch++
	t.branches = append(t.branches, make([]byte, t.branchSz)...)
	t.srefs = append(t.srefs, make([]uint32, t.sepMax)...)
	t.clearBranch(o)
	return o
}

func (t *Tree) freeLeaf(o uint32)   { t.leafFree = append(t.leafFree, o) }
func (t *Tree) freeBranch(o uint32) { t.branchFree = append(t.branchFree, o) }

func (t *Tree) leaf(o uint32) []byte {
	b := int(o) * t.leafSz
	return t.leaves[b : b+t.leafSz]
}
func (t *Tree) branch(o uint32) []byte {
	b := int(o) * t.branchSz
	return t.branches[b : b+t.branchSz]
}

func (t *Tree) clearLeaf(o uint32) {
	n := t.leaf(o)
	for i := range n {
		n[i] = 0
	}
	n[0] = tagLeaf
}
func (t *Tree) clearBranch(o uint32) {
	n := t.branch(o)
	for i := range n {
		n[i] = 0
	}
	n[0] = tagBranch
	base := int(o) * t.sepMax
	for i := 0; i < t.sepMax; i++ {
		t.srefs[base+i] = 0
	}
}

// Leaf accessors.
func (t *Tree) lNent(o uint32) int       { return int(t.leaf(o)[1]) }
func (t *Tree) lSetNent(o uint32, n int) { t.leaf(o)[1] = byte(n) }
func (t *Tree) lNext(o uint32) uint32    { return binary.LittleEndian.Uint32(t.leaf(o)[4:]) }
func (t *Tree) lSetNext(o, v uint32)     { binary.LittleEndian.PutUint32(t.leaf(o)[4:], v) }
func (t *Tree) lScore(o uint32, i int) uint64 {
	return binary.LittleEndian.Uint64(t.leaf(o)[leafHdr+i*entrySz:])
}
func (t *Tree) lRef(o uint32, i int) uint32 {
	return binary.LittleEndian.Uint32(t.leaf(o)[leafHdr+i*entrySz+8:])
}
func (t *Tree) lSetEnt(o uint32, i int, score uint64, ref uint32) {
	p := t.leaf(o)[leafHdr+i*entrySz:]
	binary.LittleEndian.PutUint64(p, score)
	binary.LittleEndian.PutUint32(p[8:], ref)
	// bytes 12..16 stay zero: reserved for the member length and hash back-ordinal
	// the zset seam fills, unused by the ordered path.
}

// Branch accessors.
func (t *Tree) bNkeys(o uint32) int       { return int(t.branch(o)[1]) }
func (t *Tree) bSetNkeys(o uint32, n int) { t.branch(o)[1] = byte(n) }
func (t *Tree) bSepScore(o uint32, i int) uint64 {
	return binary.LittleEndian.Uint64(t.branch(o)[branchHdr+i*8:])
}
func (t *Tree) bSepRef(o uint32, i int) uint32 { return t.srefs[int(o)*t.sepMax+i] }
func (t *Tree) bSetSep(o uint32, i int, score uint64, ref uint32) {
	binary.LittleEndian.PutUint64(t.branch(o)[branchHdr+i*8:], score)
	t.srefs[int(o)*t.sepMax+i] = ref
}
func (t *Tree) bMoveSep(o uint32, dst, src int) {
	t.bSetSep(o, dst, t.bSepScore(o, src), t.bSepRef(o, src))
}
func (t *Tree) childOff() int { return branchHdr + t.sepMax*8 }
func (t *Tree) countOff() int { return t.childOff() + t.arity*ordSz }
func (t *Tree) bChild(o uint32, i int) uint32 {
	return binary.LittleEndian.Uint32(t.branch(o)[t.childOff()+i*ordSz:])
}
func (t *Tree) bSetChild(o uint32, i int, v uint32) {
	binary.LittleEndian.PutUint32(t.branch(o)[t.childOff()+i*ordSz:], v)
}
func (t *Tree) bCount(o uint32, i int) uint64 {
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
func (t *Tree) bSetCount(o uint32, i int, v uint64) {
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

// bCountPrefix sums the subtree counts of children [0,c) of branch o, the prefix
// Rank accumulates at every interior level. It hoists the block slice and the
// count-array base out of the per-child loop, and for the production u32 width it
// sums two counts per u64 load in split 32-bit lanes, halving the loop trips on a
// path that touches up to arity-1 counts per level. Lab 06 (labs/f3/m2) measured
// the plain scalar hoist as noise (the compiler already lifts the offset
// arithmetic) and the packed form as the real lever: 1.5x on the accumulation
// kernel on a cache-resident descent, the zipf-ZRANK shape whose hot members keep
// their descent blocks in L1 and turn the op compute-bound.
//
// The lane sum is exact for any valid tree, not just bounded counts: the low lane
// is the sum of the even-index child counts and the high lane the odd-index ones,
// each a sum of disjoint subtrees, so each lane is at most the node's subtree
// total, at most t.entries, at most the count-width maximum (below 2^32 for u32,
// the ceiling countW=4 exists to hold). The low lane therefore never carries out
// of bit 31 into the high lane, so the packed add stays byte-identical to the
// per-element sum. u16 and u64 keep the scalar loop; they are off the frozen
// production path. Read-only, no layout change.
func (t *Tree) bCountPrefix(o uint32, c int) uint64 {
	p := t.branch(o)[t.countOff():]
	var acc uint64
	switch t.countW {
	case 2:
		for i := 0; i < c; i++ {
			acc += uint64(binary.LittleEndian.Uint16(p[i*2:]))
		}
	case 8:
		for i := 0; i < c; i++ {
			acc += binary.LittleEndian.Uint64(p[i*8:])
		}
	default:
		var packed uint64
		i := 0
		for ; i+2 <= c; i += 2 {
			packed += binary.LittleEndian.Uint64(p[i*4:])
		}
		acc = (packed & 0xFFFFFFFF) + (packed >> 32)
		if i < c {
			acc += uint64(binary.LittleEndian.Uint32(p[i*4:]))
		}
	}
	return acc
}

// subtreeCount is the number of live entries under a node.
func (t *Tree) subtreeCount(o uint32, isLeaf bool) uint64 {
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

// cmpSep orders separator i of branch o against (score, member): the score
// decides, and only a tie descends to the member bytes.
func (t *Tree) cmpSep(o uint32, i int, score uint64, member []byte, m Members) int {
	ss := t.bSepScore(o, i)
	if ss != score {
		if ss < score {
			return -1
		}
		return 1
	}
	return bytes.Compare(m.Member(t.bSepRef(o, i)), member)
}

// cmpEntry orders leaf entry i of leaf o against (score, member), same rule.
func (t *Tree) cmpEntry(o uint32, i int, score uint64, member []byte, m Members) int {
	es := t.lScore(o, i)
	if es != score {
		if es < score {
			return -1
		}
		return 1
	}
	return bytes.Compare(m.Member(t.lRef(o, i)), member)
}

// route returns the child index for (score, member): the number of separators at
// or before the key, so a key equal to a separator (the first key of the right
// child) routes right into the child that holds it.
func (t *Tree) route(o uint32, score uint64, member []byte, m Members) int {
	lo, hi := 0, t.bNkeys(o)
	for lo < hi {
		mid := (lo + hi) / 2
		if t.cmpSep(o, mid, score, member, m) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// leafPos returns the index of the first entry at or after (score, member) and
// whether the key is present.
func (t *Tree) leafPos(o uint32, score uint64, member []byte, m Members) (int, bool) {
	n := t.lNent(o)
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if t.cmpEntry(o, mid, score, member, m) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < n && t.cmpEntry(o, lo, score, member, m) == 0 {
		return lo, true
	}
	return lo, false
}

// Insert places (score, member) carrying reference ref. It reports whether a new
// entry landed; a key already present is left untouched and reports false, so an
// idempotent re-add writes nothing (the no-op the churn path relies on).
func (t *Tree) Insert(score uint64, member []byte, ref uint32, m Members) bool {
	added, split, ss, sr, right := t.insertInto(t.root, t.height, score, member, ref, m)
	if added {
		t.entries++
	}
	if split {
		leftLeaf := t.height == 1
		nr := t.allocBranch()
		t.bSetNkeys(nr, 1)
		t.bSetSep(nr, 0, ss, sr)
		t.bSetChild(nr, 0, t.root)
		t.bSetChild(nr, 1, right)
		t.bSetCount(nr, 0, t.subtreeCount(t.root, leftLeaf))
		t.bSetCount(nr, 1, t.subtreeCount(right, leftLeaf))
		t.root = nr
		t.rootIsLeaf = false
		t.height++
	}
	return added
}

func (t *Tree) insertInto(ord uint32, level int, score uint64, member []byte, ref uint32, m Members) (added, split bool, sepScore uint64, sepRef, right uint32) {
	if level == 1 {
		return t.leafInsert(ord, score, member, ref, m)
	}
	c := t.route(ord, score, member, m)
	child := t.bChild(ord, c)
	childLeaf := level-1 == 1
	cadded, csplit, css, csr, cright := t.insertInto(child, level-1, score, member, ref, m)
	if cadded {
		t.bSetCount(ord, c, t.bCount(ord, c)+1)
	}
	if !csplit {
		return cadded, false, 0, 0, 0
	}
	t.bSetCount(ord, c, t.subtreeCount(child, childLeaf))
	ss, sr, r := t.branchInsertChild(ord, c+1, css, csr, cright, t.subtreeCount(cright, childLeaf))
	if r == noSplit {
		return cadded, false, 0, 0, 0
	}
	return cadded, true, ss, sr, r
}

func (t *Tree) leafInsert(o uint32, score uint64, member []byte, ref uint32, m Members) (added, split bool, sepScore uint64, sepRef, right uint32) {
	pos, present := t.leafPos(o, score, member, m)
	if present {
		return false, false, 0, 0, 0
	}
	if t.lNent(o) < t.leafCap {
		t.leafShiftIn(o, pos, score, ref)
		return true, false, 0, 0, 0
	}
	right = t.splitLeaf(o)
	if t.cmpEntry(right, 0, score, member, m) <= 0 {
		p, _ := t.leafPos(right, score, member, m)
		t.leafShiftIn(right, p, score, ref)
	} else {
		p, _ := t.leafPos(o, score, member, m)
		t.leafShiftIn(o, p, score, ref)
	}
	return true, true, t.lScore(right, 0), t.lRef(right, 0), right
}

func (t *Tree) leafShiftIn(o uint32, pos int, score uint64, ref uint32) {
	n := t.lNent(o)
	blk := t.leaf(o)
	src := leafHdr + pos*entrySz
	copy(blk[src+entrySz:leafHdr+(n+1)*entrySz], blk[src:leafHdr+n*entrySz])
	t.lSetNent(o, n+1)
	t.lSetEnt(o, pos, score, ref)
}

func (t *Tree) splitLeaf(o uint32) uint32 {
	t.invalidateEdges()
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

// branchInsertChild inserts child (with separator (sepScore, sepRef) before it)
// at position ci. A full branch splits, returning the promoted separator and the
// new right branch; otherwise it returns noSplit.
func (t *Tree) branchInsertChild(o uint32, ci int, sepScore uint64, sepRef, child uint32, cnt uint64) (uint64, uint32, uint32) {
	k := t.bNkeys(o)
	if k < t.sepMax {
		t.branchShiftIn(o, ci, sepScore, sepRef, child, cnt)
		return 0, 0, noSplit
	}
	// Gather k+2 children/counts and k+1 separators with the newcomer in place.
	kids := make([]uint32, 0, k+2)
	cnts := make([]uint64, 0, k+2)
	sepScores := make([]uint64, 0, k+1)
	sepRefs := make([]uint32, 0, k+1)
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
	oi := 0
	for i := 0; i < len(kids)-1; i++ {
		if i+1 == ci {
			sepScores = append(sepScores, sepScore)
			sepRefs = append(sepRefs, sepRef)
		} else {
			sepScores = append(sepScores, t.bSepScore(o, oi))
			sepRefs = append(sepRefs, t.bSepRef(o, oi))
			oi++
		}
	}
	total := len(kids) // k+2
	mid := total / 2
	upScore, upRef := sepScores[mid-1], sepRefs[mid-1]
	right := t.allocBranch()
	t.bSetNkeys(o, mid-1)
	for i := 0; i < mid; i++ {
		t.bSetChild(o, i, kids[i])
		t.bSetCount(o, i, cnts[i])
	}
	for i := 0; i < mid-1; i++ {
		t.bSetSep(o, i, sepScores[i], sepRefs[i])
	}
	rk := total - mid
	t.bSetNkeys(right, rk-1)
	for i := 0; i < rk; i++ {
		t.bSetChild(right, i, kids[mid+i])
		t.bSetCount(right, i, cnts[mid+i])
	}
	for i := 0; i < rk-1; i++ {
		t.bSetSep(right, i, sepScores[mid+i], sepRefs[mid+i])
	}
	return upScore, upRef, right
}

func (t *Tree) branchShiftIn(o uint32, ci int, sepScore uint64, sepRef, child uint32, cnt uint64) {
	k := t.bNkeys(o)
	for i := k + 1; i > ci; i-- {
		t.bSetChild(o, i, t.bChild(o, i-1))
		t.bSetCount(o, i, t.bCount(o, i-1))
	}
	t.bSetChild(o, ci, child)
	t.bSetCount(o, ci, cnt)
	for i := k; i > ci-1; i-- {
		t.bMoveSep(o, i, i-1)
	}
	t.bSetSep(o, ci-1, sepScore, sepRef)
	t.bSetNkeys(o, k+1)
}

// Append places (score, member) that the caller guarantees sorts after every
// current entry, the band-promotion and STORE right-edge path (section 2.4). It
// descends the right spine and appends, so it never compares members and never
// invokes the callback; a caller that breaks the ordering guarantee corrupts the
// order, exactly as a misused bulk load would.
func (t *Tree) Append(score uint64, ref uint32) {
	split, ss, sr, right := t.appendInto(t.root, t.height, score, ref)
	t.entries++
	if split {
		leftLeaf := t.height == 1
		nr := t.allocBranch()
		t.bSetNkeys(nr, 1)
		t.bSetSep(nr, 0, ss, sr)
		t.bSetChild(nr, 0, t.root)
		t.bSetChild(nr, 1, right)
		t.bSetCount(nr, 0, t.subtreeCount(t.root, leftLeaf))
		t.bSetCount(nr, 1, t.subtreeCount(right, leftLeaf))
		t.root = nr
		t.rootIsLeaf = false
		t.height++
	}
}

func (t *Tree) appendInto(ord uint32, level int, score uint64, ref uint32) (split bool, sepScore uint64, sepRef, right uint32) {
	if level == 1 {
		n := t.lNent(ord)
		if n < t.leafCap {
			t.lSetEnt(ord, n, score, ref)
			t.lSetNent(ord, n+1)
			return false, 0, 0, 0
		}
		right = t.splitLeaf(ord)
		rn := t.lNent(right)
		t.lSetEnt(right, rn, score, ref)
		t.lSetNent(right, rn+1)
		return true, t.lScore(right, 0), t.lRef(right, 0), right
	}
	c := t.bNkeys(ord) // the right-most child
	child := t.bChild(ord, c)
	childLeaf := level-1 == 1
	csplit, css, csr, cright := t.appendInto(child, level-1, score, ref)
	t.bSetCount(ord, c, t.bCount(ord, c)+1)
	if !csplit {
		return false, 0, 0, 0
	}
	t.bSetCount(ord, c, t.subtreeCount(child, childLeaf))
	ss, sr, r := t.branchInsertChild(ord, c+1, css, csr, cright, t.subtreeCount(cright, childLeaf))
	if r == noSplit {
		return false, 0, 0, 0
	}
	return true, ss, sr, r
}

// Rank returns the number of entries sorting before (score, member) and whether
// the key is present: descend to it, summing every left-sibling child count on
// the way, plus the offset in the leaf, O(log n) with nothing but the counts.
func (t *Tree) Rank(score uint64, member []byte, m Members) (uint64, bool) {
	ord := t.root
	level := t.height
	var acc uint64
	for level > 1 {
		c := t.route(ord, score, member, m)
		acc += t.bCountPrefix(ord, c)
		ord = t.bChild(ord, c)
		level--
	}
	pos, present := t.leafPos(ord, score, member, m)
	return acc + uint64(pos), present
}

// Find returns the reference stored at (score, member) and whether the key is
// present: descend to its leaf and probe, O(log n) reading nothing but the one
// leaf it lands in, never mutating and never allocating. It is the point lookup
// XCLAIM and XNACK ride to reach a pending slab by ID without removing it, the
// read twin of Delete: same descent, no motion.
func (t *Tree) Find(score uint64, member []byte, m Members) (uint32, bool) {
	ord := t.root
	level := t.height
	for level > 1 {
		ord = t.bChild(ord, t.route(ord, score, member, m))
		level--
	}
	pos, present := t.leafPos(ord, score, member, m)
	if !present {
		return 0, false
	}
	return t.lRef(ord, pos), true
}

// SelectAt returns the (score, ref) at rank r, 0-based, and whether r is in range:
// descend subtracting child counts from r until it falls inside a child, the leaf
// remainder is the offset, O(log n) with no member compare. This is what makes a
// far ZRANGE a seek, not a walk.
func (t *Tree) SelectAt(r uint64) (score uint64, ref uint32, ok bool) {
	if r >= t.entries {
		return 0, 0, false
	}
	ord := t.descendToRank(&r)
	return t.lScore(ord, int(r)), t.lRef(ord, int(r)), true
}

// descendToRank walks to the leaf holding rank *r and rewrites *r to the offset
// within it.
func (t *Tree) descendToRank(r *uint64) uint32 {
	ord := t.root
	level := t.height
	for level > 1 {
		nk := t.bNkeys(ord)
		c := 0
		for c <= nk {
			cc := t.bCount(ord, c)
			if *r < cc {
				break
			}
			*r -= cc
			c++
		}
		ord = t.bChild(ord, c)
		level--
	}
	return ord
}

// Each iterates every entry in order, following the leaf chain with no re-descent,
// stopping early if fn returns false.
func (t *Tree) Each(fn func(score uint64, ref uint32) bool) {
	ord := t.leftmostLeaf()
	for {
		n := t.lNent(ord)
		for i := 0; i < n; i++ {
			if !fn(t.lScore(ord, i), t.lRef(ord, i)) {
				return
			}
		}
		nx := t.lNext(ord)
		if nx == 0 || n == 0 {
			return
		}
		ord = nx
	}
}

// WalkFromRank iterates from rank start in order, the seek-then-leaf-walk a
// ZRANGE by index rides, stopping early if fn returns false.
func (t *Tree) WalkFromRank(start uint64, fn func(score uint64, ref uint32) bool) {
	if start >= t.entries {
		return
	}
	k := start
	ord := t.descendToRank(&k)
	off := int(k)
	for {
		n := t.lNent(ord)
		for off < n {
			if !fn(t.lScore(ord, off), t.lRef(ord, off)) {
				return
			}
			off++
		}
		nx := t.lNext(ord)
		if nx == 0 {
			return
		}
		ord = nx
		off = 0
	}
}

// WalkFromRankRev iterates from rank start down to rank 0 in descending order,
// the seek-then-walk a ZRANGE REV or ZREVRANGE by index rides. Leaves are singly
// linked (section 6.4), so a leaf runs its entries backward in place and the
// crossing to the previous leaf re-descends by rank once per leaf boundary, the
// small ancestor re-seek the lab measured against 4-byte back-links. It stops
// early if fn returns false.
func (t *Tree) WalkFromRankRev(start uint64, fn func(score uint64, ref uint32) bool) {
	if t.entries == 0 {
		return
	}
	if start >= t.entries {
		start = t.entries - 1
	}
	k := start
	ord := t.descendToRank(&k)
	off := int(k)
	base := start - k // global rank of this leaf's entry 0
	for {
		for off >= 0 {
			if !fn(t.lScore(ord, off), t.lRef(ord, off)) {
				return
			}
			off--
		}
		if base == 0 {
			return
		}
		prev := base - 1 // global rank of the last entry in the previous leaf
		k = prev
		ord = t.descendToRank(&k)
		off = int(k)
		base = prev - k
	}
}

// RankCursor walks a forward-rank window one entry at a time without a
// per-element callback, so a range read drives the loop itself and the member
// append stays inlined. It is the callback-free twin of WalkFromRank: the same
// counted seek and leaf-chain walk, but the caller pulls each ref instead of
// pushing through a closure, which deletes the two indirect calls per element
// the closure path pays and lets the walk skip the score read entirely when the
// caller does not want scores (labs/f3/m2/08 prices both). It aliases the tree's
// leaf storage, so it stays valid only until the next write, the same
// single-command lifetime the walk carried.
type RankCursor struct {
	t   *Tree
	ord uint32 // current leaf (0 is a valid leaf ordinal, hence the ok flag)
	off int    // entry offset within the current leaf
	n   int    // live entries in the current leaf
	ok  bool   // false once the window is exhausted
}

// SeekRank positions a forward cursor at rank start. A start at or past the end
// yields an exhausted cursor (Valid reports false).
func (t *Tree) SeekRank(start uint64) RankCursor {
	if start >= t.entries {
		return RankCursor{}
	}
	k := start
	ord := t.descendToRank(&k)
	return RankCursor{t: t, ord: ord, off: int(k), n: t.lNent(ord), ok: true}
}

// Valid reports whether the cursor still points at an entry.
func (c *RankCursor) Valid() bool { return c.ok }

// Ref returns the record ref at the cursor. The score lives on the record, so a
// plain ZRANGE never reads the leaf's score array.
func (c *RankCursor) Ref() uint32 { return c.t.lRef(c.ord, c.off) }

// Next advances one entry, crossing to the next leaf at a boundary. A cross past
// the last leaf exhausts the cursor. lNext is 0 at the tail; leaf ordinal 0 is
// only ever the head, never a next target, so 0 unambiguously means end.
func (c *RankCursor) Next() {
	c.off++
	if c.off < c.n {
		return
	}
	nx := c.t.lNext(c.ord)
	if nx == 0 {
		c.ok = false
		return
	}
	c.ord = nx
	c.off = 0
	c.n = c.t.lNent(nx)
}

// RevRankCursor walks a forward-rank window high-to-low, the ZRANGE REV and
// ZREVRANGE order, the callback-free twin of WalkFromRankRev. Leaves are singly
// linked (section 6.4), so a leaf runs its entries backward in place and the
// crossing to the previous leaf re-descends by rank once per leaf boundary, the
// same small ancestor re-seek the walk pays.
type RevRankCursor struct {
	t    *Tree
	ord  uint32 // current leaf
	off  int    // entry offset within the current leaf, walked down to 0
	base uint64 // global rank of this leaf's entry 0
	ok   bool   // false once the window is exhausted
}

// SeekRankRev positions a reverse cursor at rank start, walking down toward rank
// 0. A start past the end clamps to the last entry.
func (t *Tree) SeekRankRev(start uint64) RevRankCursor {
	if t.entries == 0 {
		return RevRankCursor{}
	}
	if start >= t.entries {
		start = t.entries - 1
	}
	k := start
	ord := t.descendToRank(&k)
	return RevRankCursor{t: t, ord: ord, off: int(k), base: start - k, ok: true}
}

// Valid reports whether the cursor still points at an entry.
func (c *RevRankCursor) Valid() bool { return c.ok }

// Ref returns the record ref at the cursor.
func (c *RevRankCursor) Ref() uint32 { return c.t.lRef(c.ord, c.off) }

// Next steps one entry toward rank 0, re-descending to the previous leaf at a
// boundary. A step below rank 0 exhausts the cursor.
func (c *RevRankCursor) Next() {
	c.off--
	if c.off >= 0 {
		return
	}
	if c.base == 0 {
		c.ok = false
		return
	}
	prev := c.base - 1 // global rank of the last entry in the previous leaf
	k := prev
	c.ord = c.t.descendToRank(&k)
	c.off = int(k)
	c.base = prev - k
}

// WalkFrom iterates from the first entry at or after (score, member) in order,
// the seek a ZRANGEBYSCORE or ZRANGEBYLEX low bound rides, stopping early if fn
// returns false.
func (t *Tree) WalkFrom(score uint64, member []byte, m Members, fn func(score uint64, ref uint32) bool) {
	ord := t.root
	level := t.height
	for level > 1 {
		ord = t.bChild(ord, t.route(ord, score, member, m))
		level--
	}
	off, _ := t.leafPos(ord, score, member, m)
	for {
		n := t.lNent(ord)
		for off < n {
			if !fn(t.lScore(ord, off), t.lRef(ord, off)) {
				return
			}
			off++
		}
		nx := t.lNext(ord)
		if nx == 0 {
			return
		}
		ord = nx
		off = 0
	}
}

// invalidateEdges drops the cached end-leaf ordinals; called on any split or
// merge that could move which leaf sits at an end.
func (t *Tree) invalidateEdges() {
	t.minLeaf = leafNone
	t.maxLeaf = leafNone
}

func (t *Tree) leftmostLeaf() uint32 {
	if t.edgeCache && t.minLeaf != leafNone {
		return t.minLeaf
	}
	ord := t.root
	for level := t.height; level > 1; level-- {
		ord = t.bChild(ord, 0)
	}
	if t.edgeCache {
		t.minLeaf = ord
	}
	return ord
}

func (t *Tree) rightmostLeaf() uint32 {
	if t.edgeCache && t.maxLeaf != leafNone {
		return t.maxLeaf
	}
	ord := t.root
	for level := t.height; level > 1; level-- {
		ord = t.bChild(ord, t.bNkeys(ord))
	}
	if t.edgeCache {
		t.maxLeaf = ord
	}
	return ord
}

// Delete removes (score, member), returning the freed reference and whether an
// entry left. A miss changes nothing and touches no count.
func (t *Tree) Delete(score uint64, member []byte, m Members) (uint32, bool) {
	ref, removed := t.deleteFrom(t.root, t.height, score, member, m)
	if removed {
		t.entries--
	}
	t.collapseRoot()
	return ref, removed
}

// collapseRoot drops a branch root that a merge left with a single child, the
// height-shrinking step shared by every removal path (Delete and the fused
// pops). A leaf root, or a branch root that still routes, is left alone.
func (t *Tree) collapseRoot() {
	if !t.rootIsLeaf && t.bNkeys(t.root) == 0 {
		old := t.root
		t.root = t.bChild(t.root, 0)
		t.height--
		t.rootIsLeaf = t.height == 1
		t.freeBranch(old)
	}
}

func (t *Tree) deleteFrom(ord uint32, level int, score uint64, member []byte, m Members) (uint32, bool) {
	if level == 1 {
		pos, present := t.leafPos(ord, score, member, m)
		if !present {
			return 0, false
		}
		ref := t.lRef(ord, pos)
		t.leafRemove(ord, pos)
		return ref, true
	}
	c := t.route(ord, score, member, m)
	child := t.bChild(ord, c)
	childLeaf := level-1 == 1
	ref, removed := t.deleteFrom(child, level-1, score, member, m)
	if !removed {
		return 0, false
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
	return ref, true
}

// DeleteAt removes the entry at rank r, 0-based, and returns its score, its
// member reference, and false when r is out of range. It is the bounded removal
// the ZREMRANGEBY* window surgery loops over (spec 2064/f3/12 section 6.9): one
// rank-routed descent takes the entry, and the unwind decrements the single child
// count on the path and rebalances the one child that may have underflowed, the
// same count-fixup and merge machinery Delete and the fused pops share, so the
// tree stays a valid counted B+ tree after every call. The descent routes on the
// subtree counts alone, exactly as SelectAt and the pops do, so it reads no member
// bytes and never invokes the Members callback.
func (t *Tree) DeleteAt(r uint64) (score uint64, ref uint32, ok bool) {
	if r >= t.entries {
		return 0, 0, false
	}
	score, ref = t.deleteAtFrom(t.root, t.height, r)
	t.entries--
	t.collapseRoot()
	return score, ref, true
}

func (t *Tree) deleteAtFrom(ord uint32, level int, r uint64) (uint64, uint32) {
	if level == 1 {
		score, ref := t.lScore(ord, int(r)), t.lRef(ord, int(r))
		t.leafRemove(ord, int(r))
		return score, ref
	}
	nk := t.bNkeys(ord)
	c := 0
	for c <= nk {
		cc := t.bCount(ord, c)
		if r < cc {
			break
		}
		r -= cc
		c++
	}
	child := t.bChild(ord, c)
	score, ref := t.deleteAtFrom(child, level-1, r)
	t.bSetCount(ord, c, t.bCount(ord, c)-1)
	if level-1 == 1 {
		if t.lNent(child) < t.leafMin() {
			t.fixLeafUnderflow(ord, c)
		}
	} else if t.bNkeys(child) < t.branchMin() {
		t.fixBranchUnderflow(ord, c)
	}
	return score, ref
}

func (t *Tree) leafMin() int   { return t.leafCap / 4 }
func (t *Tree) branchMin() int { return t.sepMax / 4 }

func (t *Tree) leafRemove(o uint32, pos int) {
	n := t.lNent(o)
	blk := t.leaf(o)
	dst := leafHdr + pos*entrySz
	copy(blk[dst:leafHdr+(n-1)*entrySz], blk[dst+entrySz:leafHdr+n*entrySz])
	t.lSetNent(o, n-1)
}

func (t *Tree) fixLeafUnderflow(parent uint32, c int) {
	k := t.bNkeys(parent)
	child := t.bChild(parent, c)
	if c < k {
		right := t.bChild(parent, c+1)
		if t.lNent(right) > t.leafMin() {
			t.lSetEnt(child, t.lNent(child), t.lScore(right, 0), t.lRef(right, 0))
			t.lSetNent(child, t.lNent(child)+1)
			t.leafRemove(right, 0)
			t.bSetCount(parent, c, t.bCount(parent, c)+1)
			t.bSetCount(parent, c+1, t.bCount(parent, c+1)-1)
			t.bSetSep(parent, c, t.lScore(right, 0), t.lRef(right, 0))
			return
		}
		t.mergeLeaves(parent, c)
		return
	}
	left := t.bChild(parent, c-1)
	if t.lNent(left) > t.leafMin() {
		ln := t.lNent(left)
		t.leafPrepend(child, t.lScore(left, ln-1), t.lRef(left, ln-1))
		t.leafRemove(left, ln-1)
		t.bSetCount(parent, c, t.bCount(parent, c)+1)
		t.bSetCount(parent, c-1, t.bCount(parent, c-1)-1)
		t.bSetSep(parent, c-1, t.lScore(child, 0), t.lRef(child, 0))
		return
	}
	t.mergeLeaves(parent, c-1)
}

func (t *Tree) leafPrepend(o uint32, score uint64, ref uint32) {
	n := t.lNent(o)
	blk := t.leaf(o)
	copy(blk[leafHdr+entrySz:leafHdr+(n+1)*entrySz], blk[leafHdr:leafHdr+n*entrySz])
	t.lSetEnt(o, 0, score, ref)
	t.lSetNent(o, n+1)
}

func (t *Tree) mergeLeaves(parent uint32, c int) {
	t.invalidateEdges()
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
func (t *Tree) bDropChild(parent uint32, c int) {
	k := t.bNkeys(parent)
	t.bSetCount(parent, c, t.bCount(parent, c)+t.bCount(parent, c+1))
	for i := c + 1; i < k; i++ {
		t.bSetChild(parent, i, t.bChild(parent, i+1))
		t.bSetCount(parent, i, t.bCount(parent, i+1))
	}
	for i := c; i < k-1; i++ {
		t.bMoveSep(parent, i, i+1)
	}
	t.bSetNkeys(parent, k-1)
}

func (t *Tree) fixBranchUnderflow(parent uint32, c int) {
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

func (t *Tree) branchBorrowRight(parent uint32, c int, child, right uint32) {
	ck := t.bNkeys(child)
	t.bSetSep(child, ck, t.bSepScore(parent, c), t.bSepRef(parent, c))
	t.bSetChild(child, ck+1, t.bChild(right, 0))
	t.bSetCount(child, ck+1, t.bCount(right, 0))
	t.bSetNkeys(child, ck+1)
	moved := t.bCount(right, 0)
	t.bSetSep(parent, c, t.bSepScore(right, 0), t.bSepRef(right, 0))
	rk := t.bNkeys(right)
	for i := 0; i < rk; i++ {
		t.bSetChild(right, i, t.bChild(right, i+1))
		t.bSetCount(right, i, t.bCount(right, i+1))
	}
	for i := 0; i < rk-1; i++ {
		t.bMoveSep(right, i, i+1)
	}
	t.bSetNkeys(right, rk-1)
	t.bSetCount(parent, c, t.bCount(parent, c)+moved)
	t.bSetCount(parent, c+1, t.bCount(parent, c+1)-moved)
}

func (t *Tree) branchBorrowLeft(parent uint32, c int, left, child uint32) {
	lk := t.bNkeys(left)
	moved := t.bCount(left, lk)
	ck := t.bNkeys(child)
	for i := ck + 1; i > 0; i-- {
		t.bSetChild(child, i, t.bChild(child, i-1))
		t.bSetCount(child, i, t.bCount(child, i-1))
	}
	for i := ck; i > 0; i-- {
		t.bMoveSep(child, i, i-1)
	}
	t.bSetSep(child, 0, t.bSepScore(parent, c), t.bSepRef(parent, c))
	t.bSetChild(child, 0, t.bChild(left, lk))
	t.bSetCount(child, 0, moved)
	t.bSetNkeys(child, ck+1)
	t.bSetSep(parent, c, t.bSepScore(left, lk-1), t.bSepRef(left, lk-1))
	t.bSetNkeys(left, lk-1)
	t.bSetCount(parent, c, t.bCount(parent, c)-moved)
	t.bSetCount(parent, c+1, t.bCount(parent, c+1)+moved)
}

func (t *Tree) mergeBranches(parent uint32, c int) {
	left := t.bChild(parent, c)
	right := t.bChild(parent, c+1)
	lk := t.bNkeys(left)
	rk := t.bNkeys(right)
	t.bSetSep(left, lk, t.bSepScore(parent, c), t.bSepRef(parent, c))
	for i := 0; i <= rk; i++ {
		t.bSetChild(left, lk+1+i, t.bChild(right, i))
		t.bSetCount(left, lk+1+i, t.bCount(right, i))
	}
	for i := 0; i < rk; i++ {
		t.bSetSep(left, lk+1+i, t.bSepScore(right, i), t.bSepRef(right, i))
	}
	t.bSetNkeys(left, lk+1+rk)
	t.freeBranch(right)
	t.bDropChild(parent, c)
}

// Entry is one (score, ref) pair for the bulk loader; the caller supplies them in
// the zset total order.
type Entry struct {
	Score uint64
	Ref   uint32
}

// BulkLoad builds a tree from entries already in order with a right-edge fill of
// ~0.9 (section 2.4), the sorted-load path band promotion, ZRANGESTORE and the
// algebra STORE outputs ride, so a bulk-built tree starts at the 2-3B/entry bar
// instead of the ~0.5 fill ascending single-key inserts leave. The entries carry
// their own member order, so no member callback is needed.
func BulkLoad(entries []Entry) *Tree {
	t := newTreeSized(BranchSize, LeafSize, CountWidth)
	return t.bulkLoad(entries)
}

func (t *Tree) bulkLoad(entries []Entry) *Tree {
	// Reset to a clean single-leaf tree, discarding the allocation NewTree made.
	t.leaves = t.leaves[:0]
	t.branches = t.branches[:0]
	t.srefs = t.srefs[:0]
	t.leafFree, t.branchFree = nil, nil
	t.nLeaf, t.nBranch = 0, 0
	t.invalidateEdges()
	if len(entries) == 0 {
		t.root = t.allocLeaf()
		t.rootIsLeaf = true
		t.height = 1
		t.entries = 0
		return t
	}
	type node struct {
		ord   uint32
		score uint64
		ref   uint32
		count uint64
	}
	var level []node
	for _, sp := range fillSpans(len(entries), t.leafCap) {
		o := t.allocLeaf()
		for j := sp[0]; j < sp[1]; j++ {
			t.lSetEnt(o, j-sp[0], entries[j].Score, entries[j].Ref)
		}
		t.lSetNent(o, sp[1]-sp[0])
		level = append(level, node{o, entries[sp[0]].Score, entries[sp[0]].Ref, uint64(sp[1] - sp[0])})
	}
	for i := 0; i < len(level)-1; i++ {
		t.lSetNext(level[i].ord, level[i+1].ord)
	}
	height := 1
	for len(level) > 1 {
		var up []node
		for _, sp := range fillSpans(len(level), t.arity) {
			o := t.allocBranch()
			t.bSetNkeys(o, sp[1]-sp[0]-1)
			var sum uint64
			for j := sp[0]; j < sp[1]; j++ {
				t.bSetChild(o, j-sp[0], level[j].ord)
				t.bSetCount(o, j-sp[0], level[j].count)
				sum += level[j].count
				if j > sp[0] {
					t.bSetSep(o, j-sp[0]-1, level[j].score, level[j].ref)
				}
			}
			up = append(up, node{o, level[sp[0]].score, level[sp[0]].ref, sum})
		}
		level = up
		height++
	}
	t.root = level[0].ord
	t.height = height
	t.rootIsLeaf = height == 1
	t.entries = uint64(len(entries))
	return t
}

// fillSpans slices n items into groups whose average size is ~0.9 of cap, spread
// evenly so the whole level sits at a true 0.9 fill instead of a run of full nodes
// and one near-empty tail, the balanced right-edge build the memory bar is read
// against.
func fillSpans(n, capacity int) [][2]int {
	target := 0.9 * float64(capacity)
	groups := int(float64(n)/target + 0.5)
	if lo := (n + capacity - 1) / capacity; groups < lo {
		groups = lo
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

// Bytes is the live node-arena size: allocated blocks minus free-listed ones,
// plus the separator-reference side array, the memory the bytes-per-entry column
// reads against the F14 bar.
func (t *Tree) Bytes() int {
	liveLeaf := int(t.nLeaf) - len(t.leafFree)
	liveBranch := int(t.nBranch) - len(t.branchFree)
	return liveLeaf*t.leafSz + liveBranch*(t.branchSz+t.sepMax*ordSz)
}
