package structs

import (
	"bytes"
	"fmt"
)

// Check verifies every structural invariant the fuzz exit of lab 01 names, run
// inside the property tests after each mutation batch: every interior count
// equals the live entry count of its child subtree, separators are sorted and
// each equals the first key of the child it fronts, node fill stays inside the
// bounds (min fill except the root, never over capacity), and the leaf chain is a
// single strictly ascending run of every entry. It returns the first violation or
// nil, and cross-checks the cached cardinality against the walk.
func (t *Tree) Check(m Members) error {
	if t.height < 1 {
		return fmt.Errorf("height %d below 1", t.height)
	}
	if t.rootIsLeaf != (t.height == 1) {
		return fmt.Errorf("rootIsLeaf %v disagrees with height %d", t.rootIsLeaf, t.height)
	}
	total, _, _, _, _, err := t.checkNode(t.root, t.height, true, m)
	if err != nil {
		return err
	}
	if total != t.entries {
		return fmt.Errorf("subtree total %d != cardinality %d", total, t.entries)
	}
	return t.checkLeafChain(total, m)
}

// cmpKeys orders two (score, ref) keys, the score first and the member bytes only
// on a tie.
func (t *Tree) cmpKeys(sA uint64, rA uint32, sB uint64, rB uint32, m Members) int {
	if sA != sB {
		if sA < sB {
			return -1
		}
		return 1
	}
	return bytes.Compare(m.Member(rA), m.Member(rB))
}

// checkNode recurses a subtree, returning its live entry count and its first and
// last keys for the parent's separator and ordering checks. isRoot relaxes the
// min-fill floor, which the root alone is allowed to break. The separator
// invariant it enforces is the routing range, not equality: a separator must sit
// strictly above the last key of the child it follows and at or below the first
// key of the child it fronts, because a plain insert or delete legitimately
// leaves a separator below a child's minimum without any restructuring.
func (t *Tree) checkNode(ord uint32, level int, isRoot bool, m Members) (count, firstS uint64, firstRef uint32, lastS uint64, lastRef uint32, err error) {
	if level == 1 {
		n := t.lNent(ord)
		if n > t.leafCap {
			return 0, 0, 0, 0, 0, fmt.Errorf("leaf %d over capacity: %d > %d", ord, n, t.leafCap)
		}
		if !isRoot && n < t.leafMin() {
			return 0, 0, 0, 0, 0, fmt.Errorf("leaf %d underfull: %d < %d", ord, n, t.leafMin())
		}
		for i := 1; i < n; i++ {
			if t.cmpKeys(t.lScore(ord, i), t.lRef(ord, i), t.lScore(ord, i-1), t.lRef(ord, i-1), m) <= 0 {
				return 0, 0, 0, 0, 0, fmt.Errorf("leaf %d entries %d,%d out of order", ord, i-1, i)
			}
		}
		if n == 0 {
			return 0, 0, 0, 0, 0, nil
		}
		return uint64(n), t.lScore(ord, 0), t.lRef(ord, 0), t.lScore(ord, n-1), t.lRef(ord, n-1), nil
	}

	k := t.bNkeys(ord)
	if k > t.sepMax {
		return 0, 0, 0, 0, 0, fmt.Errorf("branch %d over capacity: %d > %d", ord, k, t.sepMax)
	}
	if !isRoot && k < t.branchMin() {
		return 0, 0, 0, 0, 0, fmt.Errorf("branch %d underfull: %d < %d", ord, k, t.branchMin())
	}
	if isRoot && k < 1 {
		return 0, 0, 0, 0, 0, fmt.Errorf("root branch %d has %d separators", ord, k)
	}
	var sum, subFirstS, subLastS uint64
	var subFirstRef, subLastRef uint32
	for i := 0; i <= k; i++ {
		child := t.bChild(ord, i)
		cCount, cFirstS, cFirstRef, cLastS, cLastRef, cErr := t.checkNode(child, level-1, false, m)
		if cErr != nil {
			return 0, 0, 0, 0, 0, cErr
		}
		if got := t.bCount(ord, i); got != cCount {
			return 0, 0, 0, 0, 0, fmt.Errorf("branch %d child %d count %d != subtree %d", ord, i, got, cCount)
		}
		if i == 0 {
			subFirstS, subFirstRef = cFirstS, cFirstRef
		} else {
			ss, sr := t.bSepScore(ord, i-1), t.bSepRef(ord, i-1)
			// max(previous child) < separator: routing sends keys < sep left.
			if t.cmpKeys(subLastS, subLastRef, ss, sr, m) >= 0 {
				return 0, 0, 0, 0, 0, fmt.Errorf("branch %d separator %d not above left child max", ord, i-1)
			}
			// separator <= min(this child): routing sends keys >= sep right.
			if t.cmpKeys(ss, sr, cFirstS, cFirstRef, m) > 0 {
				return 0, 0, 0, 0, 0, fmt.Errorf("branch %d separator %d above child %d min", ord, i-1, i)
			}
			if i >= 2 {
				pS, pR := t.bSepScore(ord, i-2), t.bSepRef(ord, i-2)
				if t.cmpKeys(ss, sr, pS, pR, m) <= 0 {
					return 0, 0, 0, 0, 0, fmt.Errorf("branch %d separators %d,%d out of order", ord, i-2, i-1)
				}
			}
		}
		subLastS, subLastRef = cLastS, cLastRef
		sum += cCount
	}
	return sum, subFirstS, subFirstRef, subLastS, subLastRef, nil
}

// auditCounts walks the tree and returns the largest true subtree entry count any
// interior slot must hold and whether every stored count still equals its real
// subtree size. A count width too narrow truncated the value on the way in, so a
// too-large tree fails the consistency check, which is the silent overflow the
// width choice guards against. Used by the overflow-at-scale test.
func (t *Tree) auditCounts() (maxCount uint64, consistent bool) {
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

// checkLeafChain follows the singly linked leaf chain from the leftmost leaf and
// asserts it is one strictly ascending run of exactly want entries, the range-walk
// backbone the ZRANGE family depends on.
func (t *Tree) checkLeafChain(want uint64, m Members) error {
	ord := t.leftmostLeaf()
	var seen uint64
	haveLast := false
	var lastS uint64
	var lastRef uint32
	for {
		n := t.lNent(ord)
		for i := 0; i < n; i++ {
			s, r := t.lScore(ord, i), t.lRef(ord, i)
			if haveLast {
				if s < lastS || (s == lastS && bytes.Compare(m.Member(r), m.Member(lastRef)) <= 0) {
					return fmt.Errorf("leaf chain not ascending across leaf %d entry %d", ord, i)
				}
			}
			lastS, lastRef, haveLast = s, r, true
			seen++
		}
		nx := t.lNext(ord)
		if nx == 0 {
			break
		}
		ord = nx
	}
	if seen != want {
		return fmt.Errorf("leaf chain has %d entries, tree holds %d", seen, want)
	}
	return nil
}
