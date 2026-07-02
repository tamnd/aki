package f1srv

import "bytes"

// listSig folds an element down to one byte, the signature a resident scan (LPOS, LINSERT pivot
// search, LREM match collection) filters on before it pays for a full compare. It samples the
// length and the first, middle, and last bytes rather than hashing the whole element, so it stays
// O(1) on the push path that fills it. It only has to spread well enough that a scan's target
// signature rules out the vast majority of non-matching positions: a collision costs one wasted
// full compare, never a wrong answer, so a weak signature degrades speed, not correctness. Sampling
// the ends and the length separates the "member_1", "member_2", ... shape a real list carries,
// where a first-byte-only signature would collapse every element onto one value.
func listSig(v []byte) byte {
	n := len(v)
	if n == 0 {
		return 0
	}
	h := uint64(n)*0x9E3779B1 + uint64(v[0])
	h = h*131 + uint64(v[n-1])
	h = h*131 + uint64(v[n>>1])
	return byte(h ^ (h >> 11) ^ (h >> 23))
}

// listRing is the resident element-byte deque for a hot list (spec 2064/f1_rewrite_ltm/impl/34). It
// holds the raw element bytes for the positions currently resident in a listWindow, indexed by
// position modulo a power-of-two capacity, so a pop reads bytes straight from a slot with no f1raw
// hash probe. That probe is the measured bottleneck of the pop path under saturation (find is 36% of
// server CPU at 10M pops/s on one key), and one row per element gives no locality to batch it away,
// so the only way past it is to stop probing and keep the hot bytes resident.
//
// The window guarantees the live span (committedTail - committedHead) never exceeds cap by spilling
// the far end to f1raw rows before it would wrap, so a position p and p+cap are never both live and
// the p & mask index is collision-free. The mask indexing is correct for negative positions too: a
// list grows its head below zero (LPUSH decrements), and for any int64 p and mask = 2^k - 1, p & mask
// lands in [0, 2^k) because the mask clears the sign and high bits.
type listRing struct {
	slots [][]byte // ring of element byte slices, len == cap, a power of two
	sig   []byte   // per-slot element signature, len == cap, kept in lockstep with slots
	mask  int64    // cap - 1, so slot(p) == p & mask
}

// newListRing builds a ring of capPow2 slots. The capacity must be a power of two so the position to
// slot map is a single mask, and it bounds the resident span: the caller spills before the span would
// reach cap.
func newListRing(capPow2 int64) *listRing {
	if capPow2 <= 0 || capPow2&(capPow2-1) != 0 {
		panic("f1srv: listRing capacity must be a positive power of two")
	}
	return &listRing{slots: make([][]byte, capPow2), sig: make([]byte, capPow2), mask: capPow2 - 1}
}

// cap returns the ring's slot count, the resident-span ceiling.
func (r *listRing) capacity() int64 { return int64(len(r.slots)) }

// put stores the element bytes for position pos, copying because v aliases the connection read
// buffer that the next command reuses. It reuses the slot's backing array when it is large enough, so
// the steady churn of a queue (push tail, pop head, slot i reused every cap positions) allocates
// nothing after the ring warms up.
func (r *listRing) put(pos int64, v []byte) {
	i := pos & r.mask
	r.slots[i] = append(r.slots[i][:0], v...)
	r.sig[i] = listSig(v)
}

// get returns the bytes stored at pos. The returned slice aliases the ring slot and stays valid until
// a later put at a colliding position overwrites it, which the resident-cap invariant keeps at least
// cap positions away, well outside any single command's view.
func (r *listRing) get(pos int64) []byte {
	return r.slots[pos&r.mask]
}

// reset releases the logical content at pos while keeping the backing array for reuse, so a popped
// position does not report stale bytes to a later resident-first read that races the slot's reuse. It
// is length-only, not a nil, because the array will be refilled by the next push that reaches this
// slot and keeping it avoids an allocation there.
func (r *listRing) reset(pos int64) {
	i := pos & r.mask
	if r.slots[i] != nil {
		r.slots[i] = r.slots[i][:0]
	}
}

// takeSlot detaches the byte slice at pos and returns it, leaving the slot nil so nothing else can
// alias or overwrite the bytes. A pop uses this to claim an element under the window commit mutex and
// then frame it onto the wire after releasing the lock: because the slot is nil'd, the returned slice
// is sole-owned and stays valid even though later pushes and a grow run concurrently. It is safe
// against those two writers precisely because a claimed position sits behind committedHead (or ahead
// of committedTail), outside the live [head, tail) span: a concurrent push only touches slots inside
// the span, and grow only rehashes the live span by reference, so neither reads a detached slot. The
// one cost is that the next push reaching a nil slot allocates a fresh backing instead of reusing one,
// which a pop-then-push churn never triggers on the same slot within a single window generation.
func (r *listRing) takeSlot(pos int64) []byte {
	i := pos & r.mask
	v := r.slots[i]
	r.slots[i] = nil
	return v
}

// grow doubles the ring and rehashes the live positions [head, tail) into their new slots. It is
// called by the window when a push would drive the live span up to capacity, so the collision-free
// invariant (span < cap) keeps holding as the list gets longer. The rehash is required, not optional:
// after doubling, a live position p that sat at p & oldmask must move to p & newmask, and because two
// positions cap apart shared the old slot the copy has to be driven by the live position list, not by
// walking the old slot array. It moves the byte slices by reference, so no element bytes are copied,
// only the pointers, and growth is O(span) and happens log(n) times over a list's life, off the
// per-element push path (under the window's commit mutex).
func (r *listRing) grow(head, tail int64) {
	oldCap := int64(len(r.slots))
	newCap := oldCap << 1
	newSlots := make([][]byte, newCap)
	newSig := make([]byte, newCap)
	newMask := newCap - 1
	for p := head; p < tail; p++ {
		newSlots[p&newMask] = r.slots[p&r.mask]
		newSig[p&newMask] = r.sig[p&r.mask]
	}
	r.slots = newSlots
	r.sig = newSig
	r.mask = newMask
}

// move relocates the element at src to dst, the element pointer and its signature together, and
// detaches src by leaving its slot nil. The detach is the point: moving a slice header alone would
// leave dst and src aliasing one backing array, and a later put reusing src's array in place would
// then corrupt the element now living at dst. Nilling src keeps every live slot owning a distinct
// array, so the interior edits can slide a run by pointer at ring speed and still let put reuse
// backings on the steady push path. dst and src must differ; the caller (a one-position shift or a
// compaction that skips the no-op case) never passes them equal, and the guard makes an equal call a
// harmless no-op rather than a self-nil.
func (r *listRing) move(dst, src int64) {
	i, j := dst&r.mask, src&r.mask
	if i == j {
		return
	}
	r.slots[i] = r.slots[j]
	r.sig[i] = r.sig[j]
	r.slots[j] = nil
}

// shiftDown slides the live run [lo, hi) one position lower, so the element at p lands at p-1 for
// every p in [lo, hi). It walks ascending, lowest source first, so each source is read before a
// higher move overwrites it. move detaches each source, so after the walk the top slot hi-1 is nil
// and the caller puts the new element there fresh. It is the left-run open an interior insert makes
// when it grows the window head by one.
func (r *listRing) shiftDown(lo, hi int64) {
	for p := lo; p < hi; p++ {
		r.move(p-1, p)
	}
}

// shiftUp slides the live run [lo, hi) one position higher, so the element at p lands at p+1. It
// walks descending, highest source first, so each source is read before a lower move overwrites it.
// move detaches each source, so after the walk the bottom slot lo is nil and the caller puts the new
// element there fresh. It is the right-run open an interior insert makes when it grows the window
// tail by one.
func (r *listRing) shiftUp(lo, hi int64) {
	for p := hi - 1; p >= lo; p-- {
		r.move(p+1, p)
	}
}

// scanSigForward calls visit with each live position in [lo, hi) whose signature equals want, in
// ascending position order, and stops early when visit returns true. The range must be resident and
// collision-free (span < cap), which the window guarantees for a committed span, so the slot indices
// for consecutive positions increment by one modulo cap: the range maps to one contiguous run of sig
// bytes, or two when it wraps the ring end. bytes.IndexByte walks each run at memory-bandwidth speed
// (it is AVX2 on amd64), so a scan pays a byte compare per position and a full element compare only
// at the rare signature hit. want is the target's signature; visit does the authoritative full
// compare, since the signature only filters.
func (r *listRing) scanSigForward(lo, hi int64, want byte, visit func(pos int64) bool) {
	for lo < hi {
		startSlot := lo & r.mask
		run := int64(len(r.sig)) - startSlot
		if run > hi-lo {
			run = hi - lo
		}
		seg := r.sig[startSlot : startSlot+run]
		off := 0
		for {
			i := bytes.IndexByte(seg[off:], want)
			if i < 0 {
				break
			}
			if visit(lo + int64(off+i)) {
				return
			}
			off += i + 1
			if off >= len(seg) {
				break
			}
		}
		lo += run
	}
}

// scanSigBackward mirrors scanSigForward in descending position order, walking the range's sig runs
// from the high end with bytes.LastIndexByte. It is the direction a negative-rank LPOS wants.
func (r *listRing) scanSigBackward(lo, hi int64, want byte, visit func(pos int64) bool) {
	for hi > lo {
		// The topmost live position is hi-1; walk the contiguous run of sig bytes ending at its slot.
		endSlot := (hi - 1) & r.mask
		run := endSlot + 1
		if run > hi-lo {
			run = hi - lo
		}
		seg := r.sig[endSlot+1-run : endSlot+1] // len == run; seg[i] is position base+i
		base := hi - run
		end := int(run)
		for {
			i := bytes.LastIndexByte(seg[:end], want)
			if i < 0 {
				break
			}
			if visit(base + int64(i)) {
				return
			}
			end = i
			if end == 0 {
				break
			}
		}
		hi -= run
	}
}
