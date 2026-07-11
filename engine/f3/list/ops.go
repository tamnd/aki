package list

import "bytes"

// The band-dispatching list operations the command handlers call. Each one
// routes to the inline blob while the list is small and to the native
// placeholder once a write has crossed the budget. The inline mutating ops that
// can grow the list (pushes, LSET, LINSERT) promote to native when the result
// breaches the budget, the one-way F4 transition; the inline read ops never
// promote.

// pushBack appends v, promoting to native first when v would breach the budget.
func (l *list) pushBack(v []byte) {
	if l.nat != nil {
		l.nat.pushBack(v)
		return
	}
	if l.wouldExceed(lpEntrySize(v)) {
		l.toNative()
		l.nat.pushBack(v)
		return
	}
	l.inlinePushBack(v)
}

// pushFront prepends v, promoting to native first when v would breach the
// budget.
func (l *list) pushFront(v []byte) {
	if l.nat != nil {
		l.nat.pushFront(v)
		return
	}
	if l.wouldExceed(lpEntrySize(v)) {
		l.toNative()
		l.nat.pushFront(v)
		return
	}
	l.inlinePushFront(v)
}

// popFront removes and returns the head element. The list must be non-empty.
func (l *list) popFront() []byte {
	if l.nat != nil {
		return l.nat.popFront()
	}
	return l.inlinePopFront()
}

// popBack removes and returns the tail element. The list must be non-empty.
func (l *list) popBack() []byte {
	if l.nat != nil {
		return l.nat.popBack()
	}
	return l.inlinePopBack()
}

// get returns the element at index i, which the caller has already normalized
// into [0, length). The bytes alias internal storage and are valid until the
// next write.
func (l *list) get(i int) []byte {
	if l.nat != nil {
		return l.nat.elems[i]
	}
	return l.inlineAt(i)
}

// setAt overwrites the element at index i.
func (l *list) setAt(i int, v []byte) {
	if l.nat != nil {
		l.nat.elems[i] = cloneBytes(v)
		return
	}
	elems := l.inlineDecode()
	elems[i] = v
	l.reencodeInline(elems)
}

// insert places v before or after the first pivot match and reports whether the
// pivot was present.
func (l *list) insert(before bool, pivot, v []byte) bool {
	if l.nat != nil {
		return l.nat.insert(before, pivot, v)
	}
	elems := l.inlineDecode()
	idx := -1
	for i, e := range elems {
		if bytes.Equal(e, pivot) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	at := idx
	if !before {
		at++
	}
	elems = append(elems, nil)
	copy(elems[at+1:], elems[at:])
	elems[at] = v
	l.reencodeInline(elems)
	return true
}

// remove deletes matches of v under the LREM count-sign rule and reports how
// many it dropped. A positive count removes up to count from head to tail, a
// negative count removes up to -count from tail to head, and zero removes every
// match.
func (l *list) remove(count int, v []byte) int {
	if l.nat != nil {
		kept, removed := removeMatches(l.nat.elems, count, v)
		l.nat.elems = kept
		return removed
	}
	elems := l.inlineDecode()
	kept, removed := removeMatches(elems, count, v)
	if removed > 0 {
		l.reencodeInline(kept)
	}
	return removed
}

// removeMatches applies the LREM count-sign rule to elems and returns the kept
// elements and the number removed. It marks victims by index so the head and
// tail directions share one filter pass.
func removeMatches(elems [][]byte, count int, v []byte) ([][]byte, int) {
	limit := count
	if limit < 0 {
		limit = -limit
	}
	drop := make([]bool, len(elems))
	removed := 0
	if count >= 0 {
		for i := 0; i < len(elems); i++ {
			if (limit == 0 || removed < limit) && bytes.Equal(elems[i], v) {
				drop[i] = true
				removed++
			}
		}
	} else {
		for i := len(elems) - 1; i >= 0; i-- {
			if removed < limit && bytes.Equal(elems[i], v) {
				drop[i] = true
				removed++
			}
		}
	}
	if removed == 0 {
		return elems, 0
	}
	kept := elems[:0:0]
	for i, e := range elems {
		if !drop[i] {
			kept = append(kept, e)
		}
	}
	return kept, removed
}

// trim keeps the elements in the normalized inclusive range [start, stop] and
// reports the surviving length. start and stop are already clamped by the
// caller; an empty range clears the list.
func (l *list) trim(start, stop int) int {
	if l.nat != nil {
		if start > stop {
			l.nat.elems = l.nat.elems[:0]
			return 0
		}
		kept := make([][]byte, stop-start+1)
		copy(kept, l.nat.elems[start:stop+1])
		l.nat.elems = kept
		return len(l.nat.elems)
	}
	if start > stop {
		l.reset()
		l.n = 0
		return 0
	}
	elems := l.inlineDecode()[start : stop+1]
	l.reencodeInline(elems)
	return l.length()
}

// each visits every element in order, the bytes aliasing internal storage. Used
// by the whole-list replies (LRANGE 0 -1) and the differential harness.
func (l *list) each(fn func(v []byte)) {
	if l.nat != nil {
		for _, e := range l.nat.elems {
			fn(e)
		}
		return
	}
	for _, e := range l.inlineDecode() {
		fn(e)
	}
}
