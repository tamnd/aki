package list

import "bytes"

// native is the placeholder for the list's native band (spec 2064/f3/13 section
// 2, encoding quicklist). The shipped design is an owner-local ring-backed byte
// deque of contiguous chunks with a Fenwick chunk directory; that lands in the
// chunked-deque slice, which replaces this file. Until then a list that crosses
// the inline budget lives in a plain element slice that answers every command
// correctly, so the band-transition differential holds across the promotion
// while the resident deque is still being built. Every method here is the
// obvious O(n) slice operation; none of it is the gate-measured path.
//
// The elements are owned copies: toNative and the push paths clone the argument
// bytes in, so the slice never aliases a blob or a request buffer.
type native struct {
	elems [][]byte
}

func (nt *native) pushBack(v []byte) { nt.elems = append(nt.elems, cloneBytes(v)) }
func (nt *native) pushFront(v []byte) {
	nt.elems = append(nt.elems, nil)
	copy(nt.elems[1:], nt.elems)
	nt.elems[0] = cloneBytes(v)
}

func (nt *native) popFront() []byte {
	v := nt.elems[0]
	nt.elems = nt.elems[1:]
	return v
}

func (nt *native) popBack() []byte {
	last := len(nt.elems) - 1
	v := nt.elems[last]
	nt.elems = nt.elems[:last]
	return v
}

// insert places v before or after the first pivot match and reports whether the
// pivot was found.
func (nt *native) insert(before bool, pivot, v []byte) bool {
	idx := -1
	for i, e := range nt.elems {
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
	nt.elems = append(nt.elems, nil)
	copy(nt.elems[at+1:], nt.elems[at:])
	nt.elems[at] = cloneBytes(v)
	return true
}
