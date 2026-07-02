package f1srv

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
	mask  int64    // cap - 1, so slot(p) == p & mask
}

// newListRing builds a ring of capPow2 slots. The capacity must be a power of two so the position to
// slot map is a single mask, and it bounds the resident span: the caller spills before the span would
// reach cap.
func newListRing(capPow2 int64) *listRing {
	if capPow2 <= 0 || capPow2&(capPow2-1) != 0 {
		panic("f1srv: listRing capacity must be a positive power of two")
	}
	return &listRing{slots: make([][]byte, capPow2), mask: capPow2 - 1}
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
