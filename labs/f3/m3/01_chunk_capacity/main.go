// Lab: chunk capacity and element cap for the list chunked deque (spec 2064/f3
// doc 13 section 2.2-2.3 and section 10 exit 1, M3 lab 01).
//
// The question: doc 13 builds the native list as an owner-local ring of
// fixed-capacity chunk slabs, each holding a contiguous run of positions packed
// as uvarint-length-prefixed frames with a u16 offset directory (section 2.2).
// Section 2.3 fixes the default at a 4KiB blob budget with a 128-element cap,
// whichever binds first, mirroring Redis quicklist's list-max-listpack-size -2
// node sizing, and in the same breath pre-registers the sweep: 2/4/8 KiB blob
// budgets, caps 64/128, scored on end-push throughput, interior memmove cost,
// and bytes-per-element overhead, one geometry frozen for all bands unless a
// gated row cannot clear at any single value (section 2.3, section 10 exit 1).
// This lab runs that sweep and freezes the chunk geometry and the element-inline
// cap the numbers ask for.
//
// The memory bar is PRED-F3-M3-LISTMEM, which F14 states as 3 to 4 bytes of
// overhead per element at or under quicklist for the native band (doc 13
// section 8.2: one uvarint prefix plus one u16 directory entry is 3-4B/element,
// plus the 8-byte chunk header and slab slack). Over ~4.5B/element trips the
// lean-directory lab item (section 8.2, gate row 9.3-6).
//
// Method: in-process, no server, no wire, no engine import. The chunk deque here
// is lab-local code that models the doc's structure so the geometry can be priced
// before the chunked-deque slice writes it. Each chunk is a fixed-capacity slab
// (the blob budget) carrying its live-ordered frames in a byte blob plus a u16
// offset directory, exactly the section 2.2 layout, so a memmove on interior
// surgery moves real bytes and the byte accounting is honest. A frame whose value
// meets the reference threshold stores a 16-byte value-log reference instead of
// the bytes (F5), which is the element-inline cap this lab settles. Resident cost
// is counted as chunks times the slab budget, so slab slack is on the ledger the
// way F14 requires, not hidden.
//
// Read: RPUSH ns/op (edge append), LPOP ns/op (edge pop), interior-insert ns/op
// (LINSERT-shaped memmove plus directory shift), elements per chunk, and bytes of
// overhead per element beyond the payload, at the 64B and 1KiB value bands. A
// second sweep walks element size against a fixed budget to find where inlining
// stops paying and the reference form takes over. See README.md for the sweep
// tables and the frozen verdict.
package main

import (
	"flag"
	"fmt"
	"time"
)

const (
	chunkHdr = 8  // count u16, bytesUsed u16, firstLive u16, flags u16
	dirEntry = 2  // one u16 offset per element
	refFrame = 16 // value-log reference: 8B offset + 8B length
	logHdr   = 8  // per-value framing in the value log (F5 side)
)

// uvarintLen returns the number of bytes an unsigned varint of n occupies.
func uvarintLen(n int) int {
	l := 1
	for n >= 0x80 {
		n >>= 7
		l++
	}
	return l
}

// chunk is one fixed-capacity slab in the ring. Frames live in blob in live
// order; dir[i] is the byte offset of frame i within blob. firstLive is the pop
// cursor: live ordinals are [firstLive, len(dir)). The slab occupies capBytes of
// arena regardless of fill, so resident accounting counts capBytes per chunk.
type chunk struct {
	blob      []byte
	dir       []int
	firstLive int
	capBytes  int
	elemCap   int
	refThresh int // values >= this are stored as a 16B reference, not inline
}

func newChunk(capBytes, elemCap, refThresh int) *chunk {
	return &chunk{
		blob:      make([]byte, 0, capBytes),
		capBytes:  capBytes,
		elemCap:   elemCap,
		refThresh: refThresh,
	}
}

func (c *chunk) live() int { return len(c.dir) - c.firstLive }

// frameLen is the in-chunk byte cost of storing a value: a reference frame for
// oversized values, an inline uvarint-prefixed frame otherwise.
func (c *chunk) frameLen(vlen int) int {
	if c.refThresh > 0 && vlen >= c.refThresh {
		return uvarintLen(refFrame) + refFrame
	}
	return uvarintLen(vlen) + vlen
}

// used is the live byte footprint inside the slab: header, directory, blob.
func (c *chunk) used() int {
	return chunkHdr + len(c.dir)*dirEntry + len(c.blob)
}

// fits reports whether a value of vlen bytes can still append to this chunk
// under both the byte budget and the element cap.
func (c *chunk) fits(vlen int) bool {
	if len(c.dir) >= c.elemCap {
		return false
	}
	return c.used()+dirEntry+c.frameLen(vlen) <= c.capBytes
}

// appendFrame appends a value at the tail (RPUSH), assuming fits() is true.
func (c *chunk) appendFrame(vlen int) {
	off := len(c.blob)
	fl := c.frameLen(vlen)
	c.dir = append(c.dir, off)
	// grow blob by the frame length; the actual bytes are irrelevant to the
	// cost model, only the memcpy volume is, which the byte slice reproduces.
	c.blob = append(c.blob, make([]byte, fl)...)
}

// insertAt inserts a value before live position p (0 <= p <= live), doing the
// in-chunk blob memmove and directory shift the doc's LINSERT surgery pays.
// Caller guarantees the chunk has room. This is the O(capBytes) interior term.
func (c *chunk) insertAt(p, vlen int) {
	ord := c.firstLive + p
	fl := c.frameLen(vlen)
	var off int
	if ord < len(c.dir) {
		off = c.dir[ord]
	} else {
		off = len(c.blob)
	}
	// grow blob and memmove the tail up by fl bytes.
	c.blob = append(c.blob, make([]byte, fl)...)
	copy(c.blob[off+fl:], c.blob[off:len(c.blob)-fl])
	// insert the directory entry and bump every offset after it.
	c.dir = append(c.dir, 0)
	copy(c.dir[ord+1:], c.dir[ord:len(c.dir)-1])
	c.dir[ord] = off
	for j := ord + 1; j < len(c.dir); j++ {
		c.dir[j] += fl
	}
}

// listNative is the ring of chunks plus the flat per-chunk live-count directory
// (the Fenwick crossover is lab 02; this lab holds the flat counts and prices
// their maintenance). headChunk is the ring index of the chunk holding position
// 0; pops advance it as head chunks drain.
type listNative struct {
	chunks    []*chunk
	counts    []int // per-chunk live count, index-aligned with chunks
	headChunk int
	count     int
	capBytes  int
	elemCap   int
	refThresh int
	// payload is the sum of value bytes stored, for the overhead accounting.
	payload int64
	logByte int64 // bytes pushed to the value log for reference frames
}

func newList(capBytes, elemCap, refThresh int) *listNative {
	return &listNative{capBytes: capBytes, elemCap: elemCap, refThresh: refThresh}
}

func (l *listNative) tail() *chunk {
	if len(l.chunks) == 0 {
		c := newChunk(l.capBytes, l.elemCap, l.refThresh)
		l.chunks = append(l.chunks, c)
		l.counts = append(l.counts, 0)
	}
	return l.chunks[len(l.chunks)-1]
}

// rpush appends one value; O(1) amortized, seals and links a fresh slab when the
// tail chunk is full. Directory maintenance is the counts[] bump plus a slice
// append on a chunk boundary.
func (l *listNative) rpush(vlen int) {
	c := l.tail()
	if !c.fits(vlen) {
		c = newChunk(l.capBytes, l.elemCap, l.refThresh)
		l.chunks = append(l.chunks, c)
		l.counts = append(l.counts, 0)
	}
	if l.refThresh > 0 && vlen >= l.refThresh {
		l.logByte += int64(logHdr + vlen)
	}
	c.appendFrame(vlen)
	l.counts[len(l.counts)-1]++
	l.count++
	l.payload += int64(vlen)
}

// lpop drops position 0; O(1), advances firstLive and recycles a drained head
// chunk. Returns false on empty.
func (l *listNative) lpop() bool {
	if l.count == 0 {
		return false
	}
	c := l.chunks[l.headChunk]
	c.firstLive++
	l.counts[l.headChunk]--
	l.count--
	if l.counts[l.headChunk] == 0 {
		l.headChunk++
	}
	return true
}

// locate maps external position k to (ring index, live ordinal within chunk).
func (l *listNative) locate(k int) (int, int) {
	for i := l.headChunk; i < len(l.chunks); i++ {
		if k < l.counts[i] {
			return i, k
		}
		k -= l.counts[i]
	}
	return -1, -1
}

// insert places a value before external position k, running the in-chunk surgery
// with a split on overflow. This is the LINSERT interior term at a known offset;
// the pivot scan that precedes it in the real command is a contiguous walk priced
// separately (note 29) and is budget-independent, so this lab prices only the
// geometry-sensitive surgery half.
func (l *listNative) insert(k, vlen int) {
	if l.count == 0 || k >= l.count {
		l.rpush(vlen)
		return
	}
	ci, p := l.locate(k)
	c := l.chunks[ci]
	if c.used()+dirEntry+c.frameLen(vlen) <= c.capBytes && len(c.dir) < c.elemCap {
		c.insertAt(p, vlen)
		l.counts[ci]++
	} else {
		l.splitInsert(ci, p, vlen)
	}
	if l.refThresh > 0 && vlen >= l.refThresh {
		l.logByte += int64(logHdr + vlen)
	}
	l.count++
	l.payload += int64(vlen)
}

// splitInsert splits chunk ci around live position p and inserts into whichever
// half owns the slot, one slab allocation plus two bounded memcpys.
func (l *listNative) splitInsert(ci, p, vlen int) {
	c := l.chunks[ci]
	live := c.live()
	half := live / 2
	right := newChunk(l.capBytes, l.elemCap, l.refThresh)
	// move live ordinals [half, live) into the right chunk, repacked from 0.
	for j := half; j < live; j++ {
		ord := c.firstLive + j
		var fl int
		if ord+1 < len(c.dir) {
			fl = c.dir[ord+1] - c.dir[ord]
		} else {
			fl = len(c.blob) - c.dir[ord]
		}
		off := len(right.blob)
		right.dir = append(right.dir, off)
		right.blob = append(right.blob, make([]byte, fl)...)
	}
	// truncate the left chunk to [firstLive, firstLive+half).
	if c.firstLive+half < len(c.dir) {
		cut := c.dir[c.firstLive+half]
		c.blob = c.blob[:cut]
		c.dir = c.dir[:c.firstLive+half]
	}
	// splice the right chunk into the ring after ci.
	l.chunks = append(l.chunks, nil)
	copy(l.chunks[ci+2:], l.chunks[ci+1:])
	l.chunks[ci+1] = right
	l.counts = append(l.counts, 0)
	copy(l.counts[ci+2:], l.counts[ci+1:])
	l.counts[ci] = half
	l.counts[ci+1] = live - half
	// insert into the half that owns p; the caller adds the one l.count bump.
	if p <= half {
		c.insertAt(p, vlen)
		l.counts[ci]++
	} else {
		right.insertAt(p-half, vlen)
		l.counts[ci+1]++
	}
}

// resident is the arena footprint: one slab budget per chunk. Value-log bytes for
// reference frames are added by the caller when the reference band is measured.
func (l *listNative) resident() int64 {
	return int64(len(l.chunks)) * int64(l.capBytes)
}

// overheadPerElem is resident-plus-log bytes minus payload, per element: the
// F14 bar term.
func (l *listNative) overheadPerElem() float64 {
	if l.count == 0 {
		return 0
	}
	return float64(l.resident()+l.logByteTotal()-l.payload) / float64(l.count)
}

func (l *listNative) logByteTotal() int64 { return l.logByte }

func (l *listNative) elemsPerChunk() float64 {
	if len(l.chunks) == 0 {
		return 0
	}
	return float64(l.count) / float64(len(l.chunks))
}

// buildSeq fills a list with n values of vlen bytes by RPUSH.
func buildSeq(capBytes, elemCap, refThresh, n, vlen int) *listNative {
	l := newList(capBytes, elemCap, refThresh)
	for i := 0; i < n; i++ {
		l.rpush(vlen)
	}
	return l
}

type geoArm struct {
	name     string
	capBytes int
	elemCap  int
}

func main() {
	quick := flag.Bool("quick", false, "smaller op counts for a fast check")
	flag.Parse()

	fmt.Printf("chunk capacity and element-cap sweep, %s\n", time.Now().Format("2006-01-02"))

	arms := []geoArm{
		{"512/128", 512, 128},
		{"1024/128", 1024, 128},
		{"2048/128", 2048, 128},
		{"4096/128", 4096, 128},
		{"8192/128", 8192, 128},
		{"4096/64", 4096, 64},
	}
	valBands := []int{64, 1024}

	fmt.Println()
	fmt.Println("Sweep A: chunk geometry, per value band")
	fmt.Println("(sub-budget arms whose value cannot pack >=2 inline are marked must-ref)")
	fmt.Printf("%-10s %5s %8s %8s %8s %8s %9s %9s\n",
		"arm", "val", "chunks", "el/chunk", "pushNs", "popNs", "surgNs", "ovhd/el")
	for _, vb := range valBands {
		// op counts scale down with value size so peak memory stays bounded; the
		// overhead columns are structural and stable at these element counts.
		memN, opN, insBase := 200_000, 2_000_000, 200_000
		if vb > 256 {
			memN, opN, insBase = 60_000, 300_000, 60_000
		}
		if *quick {
			memN, opN, insBase = memN/10, opN/10, insBase/10
		}
		for _, a := range arms {
			// an arm that cannot pack at least two values of this band inline is a
			// reference case in the real design (F5), not a chunk-geometry point.
			if 2*(chunkHdr+dirEntry+uvarintLen(vb)+vb) > a.capBytes {
				fmt.Printf("%-10s %5d %8s %8s %8s %8s %9s %9s\n",
					a.name, vb, "must-ref", "-", "-", "-", "-", "-")
				continue
			}

			// memory reading: a build large enough for a stable overhead figure.
			lm := buildSeq(a.capBytes, a.elemCap, 0, memN, vb)
			chunks := len(lm.chunks)
			epc := lm.elemsPerChunk()
			ovhd := lm.overheadPerElem()
			lm = nil

			// push timing: fresh list, time opN rpushes.
			lp := newList(a.capBytes, a.elemCap, 0)
			s := time.Now()
			for i := 0; i < opN; i++ {
				lp.rpush(vb)
			}
			pushNs := float64(time.Since(s).Nanoseconds()) / float64(opN)

			// pop timing: pop the list we just built back down.
			s = time.Now()
			popped := 0
			for lp.lpop() {
				popped++
			}
			popNs := float64(time.Since(s).Nanoseconds()) / float64(popped)
			lp = nil

			// interior surgery timing: the pure in-chunk memmove plus directory
			// shift the byte budget bounds (doc 5.6/5.7). This is isolated from the
			// pivot scan and the directory select (both budget-independent and
			// lab-02 concerns), so the number is the geometry-sensitive term alone.
			surgOps := insBase / 10
			if surgOps > 40_000 {
				surgOps = 40_000
			}
			surgNs := surgeryNs(a.capBytes, a.elemCap, vb, surgOps)

			fmt.Printf("%-10s %5d %8d %8.1f %8.1f %8.1f %9.1f %9.2f\n",
				a.name, vb, chunks, epc, pushNs, popNs, surgNs, ovhd)
		}
		fmt.Println()
	}

	// Sweep B: element size against a fixed budget, inline vs reference.
	fmt.Println("Sweep B: element-inline cap at 4096/128, inline vs 16B reference")
	fmt.Printf("%-8s %8s %10s %10s %10s\n",
		"elem", "el/chunk", "inl ovhd", "ref ovhd", "verdict")
	sizes := []int{16, 64, 256, 512, 1024, 1536, 2048, 3072, 4096, 8192}
	// overhead is structural, so a modest build gives a stable figure while
	// keeping peak memory small at the large element sizes (1 frame per chunk).
	nB := 40_000
	if *quick {
		nB = 8_000
	}
	for _, sz := range sizes {
		// inline arm: no reference threshold.
		var epc, inl, ref float64
		var note string
		if sz+uvarintLen(sz)+chunkHdr+dirEntry <= 4096 {
			li := buildSeq(4096, 128, 0, nB, sz)
			epc = li.elemsPerChunk()
			inl = li.overheadPerElem()
			li = nil
		} else {
			epc = 0
			inl = -1 // does not fit inline at all
			note = "must-ref"
		}
		// reference arm: force every element to a reference (threshold below sz).
		lr := buildSeq(4096, 128, 1, nB, sz) // refThresh 1 => all references
		ref = lr.overheadPerElem()
		lr = nil
		if note == "" {
			if epc < 2 {
				note = "ref-wins"
			} else {
				note = "inline"
			}
		}
		if inl < 0 {
			fmt.Printf("%-8d %8s %10s %10.2f %10s\n", sz, "-", "n/a", ref, note)
		} else {
			fmt.Printf("%-8d %8.2f %10.2f %10.2f %10s\n", sz, epc, inl, ref, note)
		}
	}
	fmt.Println()

	// Sweep C: per-chunk count maintenance, isolated from the frame write.
	fmt.Println("Sweep C: per-chunk count maintenance cost at 4096/128, 64B values")
	cOps := 2_000_000
	if *quick {
		cOps = 200_000
	}
	countMaintNs(cOps)
}

// countMaintNs prices the directory-count maintenance the deque pays per push at
// the chosen geometry: the counts[] bump on every push plus the slice append on
// a chunk boundary. It times push with maintenance against push with the frame
// write only, so the difference is the maintenance term the Fenwick lab (02)
// builds on.
func countMaintNs(opN int) {
	// push with full maintenance.
	l := newList(4096, 128, 0)
	s := time.Now()
	for i := 0; i < opN; i++ {
		l.rpush(64)
	}
	full := float64(time.Since(s).Nanoseconds()) / float64(opN)

	// push the frame write only: same chunk lifecycle (fresh slab every time the
	// budget or cap binds) but without the listNative counts[] bump and payload
	// bookkeeping, so the difference is the directory-maintenance term.
	c := newChunk(4096, 128, 0)
	s = time.Now()
	for i := 0; i < opN; i++ {
		if !c.fits(64) {
			c = newChunk(4096, 128, 0)
		}
		c.appendFrame(64)
	}
	frameOnly := float64(time.Since(s).Nanoseconds()) / float64(opN)
	c = nil
	l = nil

	fmt.Printf("push w/ count maintenance: %.2f ns/op\n", full)
	fmt.Printf("frame write only:          %.2f ns/op\n", frameOnly)
	fmt.Printf("maintenance + chunk-link share: %.2f ns/op\n", full-frameOnly)
}

// surgeryNs prices the pure in-chunk interior edit at a geometry: it pre-builds
// ops chunks each filled to about half the byte budget (so one insert fits
// without a split, isolating the memmove and directory shift), then times one
// insertAt at a random interior ordinal into each. The cost is O(bytes moved),
// which grows with the byte budget, so this column is what makes the byte-budget
// tradeoff visible: a bigger slab bounds a bigger interior memmove.
func surgeryNs(capBytes, elemCap, vb, ops int) float64 {
	if ops < 1 {
		ops = 1
	}
	chunks := make([]*chunk, ops)
	for i := range chunks {
		c := newChunk(capBytes, elemCap, 0)
		for c.used()+dirEntry+c.frameLen(vb) <= capBytes/2 && len(c.dir) < elemCap {
			c.appendFrame(vb)
		}
		chunks[i] = c
	}
	r := uint64(0xda3e39cb94b95bdb ^ uint64(capBytes) ^ uint64(vb))
	s := time.Now()
	for i := 0; i < ops; i++ {
		c := chunks[i]
		p := 0
		if live := c.live(); live > 0 {
			r = xorshift(r)
			p = int(r % uint64(live))
		}
		c.insertAt(p, vb)
	}
	el := time.Since(s).Nanoseconds()
	return float64(el) / float64(ops)
}

func xorshift(x uint64) uint64 {
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	return x
}
