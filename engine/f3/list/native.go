package list

import "encoding/binary"

// native is the list's native band (spec 2064/f3/13 section 2, encoding
// quicklist): an owner-local ring-backed byte deque. It replaces the slice
// placeholder slice 1 shipped. Only the shard goroutine touches it, so nothing
// here locks (section 2.6).
//
// The list is a ring of fixed-capacity chunks, head chunk first, tail chunk
// last. Each chunk packs a contiguous run of positions as uvarint-length-
// prefixed frames in a byte blob, with a uint16 offset directory for O(1)
// in-chunk random access and a live-window cursor pair (lo, hi) that lets a pop
// advance a cursor instead of memmoving (section 2.2, 2.5). Position is carried
// by layout: external index k resolves to (chunk, ordinal) through the per-chunk
// counts, never through a per-element key (section 2.4).
//
// Slab representation choice: a chunk is a Go struct with a []byte blob, a
// []uint16 dir, and the header fields as struct fields, rather than a hand-packed
// CAP-byte slab. The doc allows either (section 2.2); the struct form keeps the
// blob and the directory as separate typed slices so the frame walk and the
// directory read are each obvious, it recycles cleanly onto a per-list freelist
// (the backing arrays survive a recycle), and it maps one-to-one onto the doc's
// header so slice 3 (the Fenwick directory) and later cold-tiering bolt on
// without reshaping the chunk. The blob is kept at full capacity length and
// written by offset with bytesUsed as the high-water cursor, so a push is a
// uvarint write plus a copy with no reslicing.
//
// Constants are frozen by the merged labs: chunkBlobCap and chunkElemCap by lab
// 01 (labs/f3/m3/01_chunk_capacity). The flat-versus-Fenwick crossover frozen by
// lab 02 (02_directory_crossover) lands with its consumer in slice 3; this slice
// runs the flat scan at every ring size, which is correct everywhere.
const (
	// chunkBlobCap is the per-chunk blob byte budget, 4 KiB. Lab 01 froze it as
	// the smallest budget that clears the 3-4B per-element memory bar at the 64B
	// band while holding the interior memmove to half the 8 KiB cost.
	chunkBlobCap = 4096
	// chunkElemCap is the element cap per chunk, 128, whichever binds first with
	// the byte budget. It keeps the uint16 directory at or under 256 bytes and
	// every header field honestly uint16 at any legal geometry (lab 01, doc 2.3).
	chunkElemCap = 128
	// freeCap bounds the per-list slab freelist. Edge churn recycles at most a
	// chunk or two per boundary crossing, so a small freelist keeps the steady
	// push/pop path allocation-free without pinning memory on a drained list.
	freeCap = 8
)

// chunk is one fixed-capacity slab. Live elements occupy the directory window
// [lo, hi); element j inside the chunk is dir[lo+j], whose frame starts at that
// blob offset. bytesUsed is the append high-water into blob and includes the
// dead bytes a pop leaves behind, which stay until the chunk is recycled whole
// (section 2.2). A tail chunk grows hi upward on RPUSH; a head chunk grows lo
// downward on LPUSH, so a push-heavy end never memmoves.
type chunk struct {
	blob      []byte   // packed frames; cap is the slab budget (chunkBlobCap, larger for a lone oversized frame)
	dir       []uint16 // per-element blob offsets; the live window is dir[lo:hi]
	lo, hi    int      // live directory window
	bytesUsed int      // append high-water into blob, dead bytes included until recycle
}

func (c *chunk) count() int { return c.hi - c.lo }

// budget is the chunk's blob byte ceiling: cap(blob), which is chunkBlobCap for
// a normal chunk and the frame length for a lone oversized chunk, so the same
// canAppend test seals both without a special case.
func (c *chunk) budget() int { return cap(c.blob) }

// canAppendTail reports whether a frame of f bytes fits on the tail side.
func (c *chunk) canAppendTail(f int) bool {
	return c.hi < chunkElemCap && c.bytesUsed+f <= c.budget()
}

// canPrependHead reports whether a frame of f bytes fits on the head side.
func (c *chunk) canPrependHead(f int) bool {
	return c.lo > 0 && c.bytesUsed+f <= c.budget()
}

// writeFrame packs v at blob offset off and returns the frame length.
func (c *chunk) writeFrame(off int, v []byte) int {
	w := binary.PutUvarint(c.blob[off:], uint64(len(v)))
	copy(c.blob[off+w:], v)
	return w + len(v)
}

// frameAt decodes the frame at blob offset off into its value bytes (aliasing
// the blob) and the total frame length.
func (c *chunk) frameAt(off int) (v []byte, flen int) {
	vlen, w := binary.Uvarint(c.blob[off:])
	start := off + w
	end := start + int(vlen)
	return c.blob[start:end], end - off
}

// chunkRing is a growable circular buffer of chunk handles with O(1) amortized
// push and pop at both ends, so a fresh head chunk on a push-heavy head never
// pays an O(chunks) pointer shift (section 2.1, the ring of chunk handles).
type chunkRing struct {
	buf  []*chunk
	head int // index of the head chunk in buf
	n    int // live chunk count
}

func (r *chunkRing) at(i int) *chunk { return r.buf[(r.head+i)%len(r.buf)] }
func (r *chunkRing) tail() *chunk    { return r.buf[(r.head+r.n-1)%len(r.buf)] }
func (r *chunkRing) front() *chunk   { return r.buf[r.head] }

func (r *chunkRing) grow() {
	nb := make([]*chunk, max(4, len(r.buf)*2))
	for i := 0; i < r.n; i++ {
		nb[i] = r.at(i)
	}
	r.buf = nb
	r.head = 0
}

func (r *chunkRing) pushTail(c *chunk) {
	if r.n == len(r.buf) {
		r.grow()
	}
	r.buf[(r.head+r.n)%len(r.buf)] = c
	r.n++
}

func (r *chunkRing) pushHead(c *chunk) {
	if r.n == len(r.buf) {
		r.grow()
	}
	r.head = (r.head - 1 + len(r.buf)) % len(r.buf)
	r.buf[r.head] = c
	r.n++
}

func (r *chunkRing) popHead() {
	r.buf[r.head] = nil
	r.head = (r.head + 1) % len(r.buf)
	r.n--
}

func (r *chunkRing) popTail() {
	r.n--
	r.buf[(r.head+r.n)%len(r.buf)] = nil
}

// native is the ring header (spec 2064/f3/13 section 2.1). count is LLEN in
// O(1); bytes is the live payload total that feeds F14 accounting; the sticky
// everLarge quicklist bit lives on the list, not here.
type native struct {
	ring  chunkRing
	count int
	bytes int
	free  []*chunk // recycled slabs for reuse, bounded by freeCap
}

// --- slab allocation and recycling ---------------------------------------

// getChunk returns a slab able to hold a frame of f bytes. A frame within the
// blob budget reuses a recycled slab when one is free; a lone oversized frame
// (f beyond the budget) always gets a fresh right-sized slab and never a
// freelist one, which keeps the freelist all-normal-sized for clean reuse.
func (nt *native) getChunk(f int) *chunk {
	if f <= chunkBlobCap && len(nt.free) > 0 {
		c := nt.free[len(nt.free)-1]
		nt.free = nt.free[:len(nt.free)-1]
		c.lo, c.hi, c.bytesUsed = 0, 0, 0
		return c
	}
	blobCap := chunkBlobCap
	if f > blobCap {
		blobCap = f
	}
	return &chunk{blob: make([]byte, blobCap), dir: make([]uint16, chunkElemCap)}
}

// recycle returns a drained slab to the freelist for reuse. Oversized slabs and
// overflow past freeCap are dropped to the GC instead of pinned.
func (nt *native) recycle(c *chunk) {
	if cap(c.blob) != chunkBlobCap || len(nt.free) >= freeCap {
		return
	}
	nt.free = append(nt.free, c)
}

// --- ends (section 2.5) ---------------------------------------------------

// pushBack appends v to the tail, sealing and linking a fresh slab when the tail
// chunk is full (RPUSH). A lone value wider than the blob budget takes a chunk
// to itself.
func (nt *native) pushBack(v []byte) {
	f := frameLen(len(v))
	var c *chunk
	if nt.ring.n > 0 {
		if t := nt.ring.tail(); t.canAppendTail(f) {
			c = t
		}
	}
	if c == nil {
		c = nt.getChunk(f)
		c.lo, c.hi = 0, 0 // a fresh tail chunk grows hi upward
		nt.ring.pushTail(c)
	}
	off := c.bytesUsed
	c.bytesUsed += c.writeFrame(off, v)
	c.dir[c.hi] = uint16(off)
	c.hi++
	nt.count++
	nt.bytes += len(v)
}

// pushFront prepends v to the head, filling a fresh head chunk back to front so
// a push-heavy head never memmoves (LPUSH).
func (nt *native) pushFront(v []byte) {
	f := frameLen(len(v))
	var c *chunk
	if nt.ring.n > 0 {
		if h := nt.ring.front(); h.canPrependHead(f) {
			c = h
		}
	}
	if c == nil {
		c = nt.getChunk(f)
		c.lo, c.hi = chunkElemCap, chunkElemCap // a fresh head chunk grows lo downward
		nt.ring.pushHead(c)
	}
	off := c.bytesUsed
	c.bytesUsed += c.writeFrame(off, v)
	c.lo--
	c.dir[c.lo] = uint16(off)
	nt.count++
	nt.bytes += len(v)
}

// popFront removes and returns the head element (LPOP). The bytes alias the blob
// and stay valid until the next write; the pop advances the head cursor and
// leaves the frame dead in the slab, reclaiming it only when it is the last
// frame physically written (the steady LPUSH+LPOP shape), so churn at the head
// does not grow the blob (section 2.2, 2.5).
func (nt *native) popFront() []byte {
	c := nt.ring.front()
	off := int(c.dir[c.lo])
	v, flen := c.frameAt(off)
	c.lo++
	if off+flen == c.bytesUsed {
		c.bytesUsed = off
	}
	nt.count--
	nt.bytes -= len(v)
	if c.count() == 0 {
		nt.recycle(c)
		nt.ring.popHead()
	}
	return v
}

// popBack removes and returns the tail element (RPOP), symmetric to popFront.
func (nt *native) popBack() []byte {
	c := nt.ring.tail()
	c.hi--
	off := int(c.dir[c.hi])
	v, flen := c.frameAt(off)
	if off+flen == c.bytesUsed {
		c.bytesUsed = off
	}
	nt.count--
	nt.bytes -= len(v)
	if c.count() == 0 {
		nt.recycle(c)
		nt.ring.popTail()
	}
	return v
}

// --- positional access (section 2.4) -------------------------------------

// locate resolves a dense index k, already in [0, count), to a (chunk index,
// in-chunk ordinal) pair through the flat per-chunk count directory.
//
// SLICE 3 SEAM: above flatMax chunks the Fenwick rank descent replaces the flat
// scan here (doc 2.4). The branch point is marked below; until slice 3 lands the
// flat scan runs at every ring size. It is correct everywhere, only O(chunks)
// instead of O(log chunks) on the seek above the crossover, which no edge or
// LLEN op pays because none of them call locate.
func (nt *native) locate(k int) (ci, ord int) {
	// if nt.ring.n > flatMax { return nt.fenwick.rank(k) } // slice 3
	for i := 0; i < nt.ring.n; i++ {
		c := nt.ring.at(i)
		if n := c.count(); k < n {
			return i, k
		} else {
			k -= n
		}
	}
	panic("list: locate index out of range")
}

// at returns the element at dense index i (LINDEX), aliasing the blob.
func (nt *native) at(i int) []byte {
	ci, ord := nt.locate(i)
	c := nt.ring.at(ci)
	v, _ := c.frameAt(int(c.dir[c.lo+ord]))
	return v
}

// setAt overwrites the element at dense index i (LSET). A same-length value is
// written in place over the frame; a length change re-packs the deque, the
// O(CAP) surgery the doc prices at parity and slice 3 tightens (section 5.6).
func (nt *native) setAt(i int, v []byte) {
	ci, ord := nt.locate(i)
	c := nt.ring.at(ci)
	off := int(c.dir[c.lo+ord])
	old, _ := c.frameAt(off)
	if len(old) == len(v) {
		copy(c.blob[off+uvarintLen(uint64(len(v))):], v)
		return
	}
	vals := nt.toSlice()
	vals[i] = cloneBytes(v)
	nt.rebuild(vals)
}

// insert places v before or after the first pivot match (LINSERT) and reports
// whether the pivot was found.
func (nt *native) insert(before bool, pivot, v []byte) bool {
	vals := nt.toSlice()
	idx := -1
	for i, e := range vals {
		if bytesEqual(e, pivot) {
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
	vals = append(vals, nil)
	copy(vals[at+1:], vals[at:])
	vals[at] = cloneBytes(v)
	nt.rebuild(vals)
	return true
}

// remove deletes matches of v under the LREM count-sign rule and reports how
// many it dropped.
func (nt *native) remove(count int, v []byte) int {
	vals := nt.toSlice()
	kept, removed := removeMatches(vals, count, v)
	if removed > 0 {
		nt.rebuild(kept)
	}
	return removed
}

// trim keeps the inclusive dense range [start, stop] and clears the deque when
// the range is empty (LTRIM). start and stop are already clamped by the caller.
func (nt *native) trim(start, stop int) {
	if start > stop {
		nt.rebuild(nil)
		return
	}
	vals := nt.toSlice()
	nt.rebuild(vals[start : stop+1])
}

// each visits every element in order, the bytes aliasing the blob, walking the
// ring head to tail and each chunk's live window low to high.
func (nt *native) each(fn func(v []byte)) {
	for i := 0; i < nt.ring.n; i++ {
		c := nt.ring.at(i)
		for p := c.lo; p < c.hi; p++ {
			v, _ := c.frameAt(int(c.dir[p]))
			fn(v)
		}
	}
}

// --- rebuild support ------------------------------------------------------

// toSlice materializes every element as an owned copy, independent of the chunk
// blobs, so a rebuild can recycle the current chunks without clobbering the
// values it is repacking.
func (nt *native) toSlice() [][]byte {
	out := make([][]byte, 0, nt.count)
	nt.each(func(v []byte) { out = append(out, cloneBytes(v)) })
	return out
}

// rebuild recycles the current chunks and repacks vals from empty through the
// push path, so the interior ops reuse the tested append machinery and the
// freelist. vals must be independent of the chunk blobs (toSlice guarantees it).
func (nt *native) rebuild(vals [][]byte) {
	for i := 0; i < nt.ring.n; i++ {
		nt.recycle(nt.ring.at(i))
	}
	nt.ring.head, nt.ring.n = 0, 0
	nt.count, nt.bytes = 0, 0
	for _, v := range vals {
		nt.pushBack(v)
	}
}

// bytesEqual is a small dependency-free byte compare for the pivot scan.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
