package structs

// Fused single-descent pops (spec 2064/f3/12 section 6.7, lab 04 labs/f3/m2).
// A ZPOPMIN or ZPOPMAX walks one edge of the tree, and the frozen lab verdict is
// that the win is the fused pop, not the end-leaf cache: the naive "find the
// extreme, then delete it" is two API descents (109ns at 1M, the delete leg
// alone 95-100ns), so PopMin/PopMax do it in one pass. They descend the edge,
// take the extreme leaf entry, and on the way back up decrement the one child
// count on the path and rebalance the one child that may have underflowed, the
// same count-fixup and merge machinery Delete uses, so the tree stays a valid
// counted B+ tree after every pop. No member bytes are read and the Members
// callback is never touched: the edge is child 0 (or the last child) all the way
// down, so there is no compare to make.

// PopMin removes the smallest entry and returns its score, its member reference,
// and false on an empty tree. One descent: the recursion takes the leftmost leaf
// entry and unwinds fixing counts and underflow, then the root collapses if the
// last merge emptied it.
func (t *Tree) PopMin() (score uint64, ref uint32, ok bool) {
	if t.entries == 0 {
		return 0, 0, false
	}
	score, ref = t.popMinFrom(t.root, t.height)
	t.entries--
	t.collapseRoot()
	return score, ref, true
}

// PopMax removes the largest entry, the mirror of PopMin down the right edge.
func (t *Tree) PopMax() (score uint64, ref uint32, ok bool) {
	if t.entries == 0 {
		return 0, 0, false
	}
	score, ref = t.popMaxFrom(t.root, t.height)
	t.entries--
	t.collapseRoot()
	return score, ref, true
}

func (t *Tree) popMinFrom(ord uint32, level int) (uint64, uint32) {
	if level == 1 {
		score, ref := t.lScore(ord, 0), t.lRef(ord, 0)
		t.leafRemove(ord, 0)
		return score, ref
	}
	child := t.bChild(ord, 0)
	score, ref := t.popMinFrom(child, level-1)
	t.bSetCount(ord, 0, t.bCount(ord, 0)-1)
	if level-1 == 1 {
		if t.lNent(child) < t.leafMin() {
			t.fixLeafUnderflow(ord, 0)
		}
	} else if t.bNkeys(child) < t.branchMin() {
		t.fixBranchUnderflow(ord, 0)
	}
	return score, ref
}

func (t *Tree) popMaxFrom(ord uint32, level int) (uint64, uint32) {
	if level == 1 {
		n := t.lNent(ord)
		score, ref := t.lScore(ord, n-1), t.lRef(ord, n-1)
		t.lSetNent(ord, n-1) // a max pop is a trailing truncation, no shift
		return score, ref
	}
	c := t.bNkeys(ord)
	child := t.bChild(ord, c)
	score, ref := t.popMaxFrom(child, level-1)
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

// PopMinN pops up to want entries from the min edge in ascending order, handing
// each (score, ref) to emit, and returns how many left. This is the drain the
// ZPOPMIN-count and ZMPOP forms ride. Lab 04 froze the batch win as saturating
// at one leaf (c=31): "chasing larger per-seek batches buys under 5 percent and
// is not worth a second code path", so the drain is a loop of the fused single
// pop rather than a separate bulk-trim path. The fused pop already collapses the
// two-descent the lab priced, and the spine it re-walks each step stays hot in
// cache, so the loop lands the saturated per-element number without a second way
// for the count fixup and rebalance to be wrong.
func (t *Tree) PopMinN(want int, emit func(score uint64, ref uint32)) int {
	n := 0
	for n < want {
		score, ref, ok := t.PopMin()
		if !ok {
			break
		}
		emit(score, ref)
		n++
	}
	return n
}

// PopMaxN is the mirror drain down the max edge, emitting in descending order.
func (t *Tree) PopMaxN(want int, emit func(score uint64, ref uint32)) int {
	n := 0
	for n < want {
		score, ref, ok := t.PopMax()
		if !ok {
			break
		}
		emit(score, ref)
		n++
	}
	return n
}
