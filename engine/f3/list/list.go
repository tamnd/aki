package list

import (
	"encoding/binary"

	"github.com/tamnd/aki/engine/f3/store"
)

// The inline list band (spec 2064/f3/13 section 4): a list born small is one
// packed element blob stored in the key's record, the listpack idea. The blob
// packs frames from the low end as uvarint-length-prefixed values, and a head
// cursor marks the first live frame so a pop advances a cursor instead of
// memmoving the tail (section 2.5). Point ops walk the frames; at inline sizes a
// linear frame walk beats maintaining a directory, and Redis's listpack makes
// the same call.
//
// A write that would breach the byte budget converts the band one way to the
// native form (F4, never backward). The native form is the owner-local
// ring-backed chunked deque in native.go (spec 2064/f3/13 section 2): the inline
// blob's elements move into the deque's chunks on promotion, and every command
// reaches the deque through the encoding() and length() seam this file owns.

const (
	// listpackBudget is Redis's list-max-listpack-size -2 default: an 8 KiB
	// budget on the packed listpack bytes. Verified live against redis-server
	// 8.8.0, the inline->quicklist flip tracks this byte budget, not an element
	// count and not a per-element size cap. A list stays listpack while its
	// packed bytes stay within budget and promotes to quicklist on the write
	// that crosses it. A positive list-max-listpack-size (an element count cap)
	// and the other negative byte budgets are config surface a later slice wires
	// through; this slice pins the shipped default.
	listpackBudget = 8192

	// lpOverhead is the fixed listpack framing charged on top of the packed
	// entry bytes for the budget check (the 6-byte header and terminator). The
	// value is the empirical fit against redis-server 8.8.0's flip point across
	// element sizes from 1 to ~8 KiB: the boundary is packed-entry-sum plus
	// lpOverhead crossing listpackBudget. A lone element within a few bytes of
	// the budget is the one shape Redis keeps inline where this model promotes;
	// the differential stays off that razor edge and matches everywhere else.
	lpOverhead = 5
)

// encoding is the list's storage shape and the string OBJECT ENCODING reports.
type encoding uint8

const (
	encListpack  encoding = iota // the inline packed blob
	encQuicklist                 // the native form (placeholder in this slice)
)

func (e encoding) String() string {
	if e == encQuicklist {
		return "quicklist"
	}
	return "listpack"
}

// list is one key's list. It is owner-local: only the shard goroutine touches
// it, so nothing here locks. While nat is nil the inline blob is live; once a
// write crosses the budget nat carries the list and the inline fields are
// released. everLarge is the sticky quicklist bit: it sets on promotion and
// never clears while the key lives, matching Redis's sticky conversion, so a
// list that shrinks back under the budget still reports quicklist.
type list struct {
	// inline band.
	blob []byte // packed frames; the live region is blob[head:]
	head int    // byte offset of the first live frame
	n    int    // live element count
	lpsz int    // packed listpack byte estimate, lpOverhead + sum of entry sizes

	// native band (encQuicklist): the owner-local ring-backed chunked deque in
	// native.go once a write crosses the budget.
	nat *native

	everLarge bool

	// clock is the per-key access clock OBJECT IDLETIME reads back: the batch
	// second-resolution time (store.LRUClock) stamped on every read and write the
	// way Redis stamps robj.lru, folded to sixteen bits. It rides the alignment
	// padding after everLarge, so a list carries a real idle clock at zero added
	// bytes, the same free-header trick the string cell uses (store record
	// offKindBits). It wraps every ~18.2h, the fidelity price of spending no bytes.
	clock uint16

	// expireAt is the key-level TTL deadline in ms since the epoch, 0 when the list
	// has no expiry (spec 2064/f3/16 section 2). It rides inline on the list rather
	// than in a side "expires" dict the way Redis keeps one: a second dict would be a
	// second htable plus a copy of every volatile key plus a pointer per entry, the
	// opposite of the memory bar this build holds against redis and valkey. The lazy
	// live funnel in reg.go drops the whole list once cx.NowMs passes this. It is not
	// counted in residentBytes: an int64 on a struct the registry already sizes is
	// below the estimate's granularity, the same call set/zset/hash make.
	expireAt int64

	// acct is the footprint this list last posted into the registry's running
	// resident-byte total (reg.note). It lets note post a delta instead of a fresh
	// sum, so the total stays exact across a mutation without rewalking the
	// registry. It is meaningful only when the store runs a cold tier; a store
	// with no cold region never accounts and leaves it zero.
	acct uint64
}

// residentBytes estimates the list's resident-byte footprint, the figure the
// registry sums to weigh the shard's list heap against the resident cap (spec
// 2064/f3/06 section 6). An inline listpack list is its packed blob's capacity; a
// native list is its ring of chunk slabs plus the ring, directory, and scratch
// overhead. A native chunk's blob is always allocated at the full chunk budget
// regardless of fill, so counting a fixed budget per resident chunk is exact for
// the common all-standard-chunk list and a small conservative estimate when a lone
// oversized element takes a wider slab, the same estimate shape the set and zset
// registries carry. Zero preads, O(1). Owner goroutine only.
func (l *list) residentBytes() uint64 {
	if l.nat != nil {
		return l.nat.residentBytes()
	}
	return uint64(cap(l.blob))
}

// newList builds an empty inline list. The first push decides nothing about the
// band the way the set's first member does; a list is always born listpack and
// only the byte budget moves it.
func newList() *list { return &list{lpsz: lpOverhead} }

// encoding reports the band OBJECT ENCODING answers.
func (l *list) encoding() encoding {
	if l.everLarge {
		return encQuicklist
	}
	return encListpack
}

// length is LLEN in O(1) on both bands.
func (l *list) length() int {
	if l.nat != nil {
		return l.nat.count
	}
	return l.n
}

// backlenSize is the listpack back-length field width for an entry of entryLen
// bytes, matching Redis's lpEncodeBacklen.
func backlenSize(entryLen int) int {
	switch {
	case entryLen < 1<<7:
		return 1
	case entryLen < 1<<14:
		return 2
	case entryLen < 1<<21:
		return 3
	case entryLen < 1<<28:
		return 4
	default:
		return 5
	}
}

// lpEntrySize is the packed listpack size of one element, matching Redis's
// listpack encoder: an integer-shaped value packs into an integer encoding, any
// other value into a string encoding, and both carry a back-length field. It
// allocates nothing so the push path stays zero-alloc.
func lpEntrySize(v []byte) int {
	if iv, ok := store.ParseInt(v); ok {
		enc := lpIntEncodingSize(iv)
		return enc + backlenSize(enc)
	}
	n := len(v)
	var hdr int
	switch {
	case n < 64:
		hdr = 1
	case n < 4096:
		hdr = 2
	default:
		hdr = 5
	}
	sz := hdr + n
	return sz + backlenSize(sz)
}

// lpIntEncodingSize is the encoding-plus-data width of a listpack integer, the
// same ladder Redis's lpEncodeIntegerGetType walks.
func lpIntEncodingSize(v int64) int {
	switch {
	case v >= 0 && v <= 127:
		return 1
	case v >= -4096 && v <= 4095:
		return 2
	case v >= -32768 && v <= 32767:
		return 3
	case v >= -(1<<23) && v <= (1<<23)-1:
		return 4
	case v >= -(1<<31) && v <= (1<<31)-1:
		return 5
	default:
		return 9
	}
}

// wouldExceed reports whether adding an element of packed size add to the inline
// blob would cross the budget and force promotion.
func (l *list) wouldExceed(add int) bool { return l.lpsz+add > listpackBudget }

// --- inline band frame ops ------------------------------------------------

// inlineGrow extends the blob by n bytes, reusing spare capacity when it is
// there so the steady push path allocates nothing.
func (l *list) inlineGrow(n int) {
	if cap(l.blob)-len(l.blob) >= n {
		l.blob = l.blob[:len(l.blob)+n]
		return
	}
	l.blob = append(l.blob, make([]byte, n)...)
}

// inlinePushBack appends v as a frame. The caller has already decided v fits the
// budget.
func (l *list) inlinePushBack(v []byte) {
	off := len(l.blob)
	l.inlineGrow(frameLen(len(v)))
	w := binary.PutUvarint(l.blob[off:], uint64(len(v)))
	copy(l.blob[off+w:], v)
	l.n++
	l.lpsz += lpEntrySize(v)
}

// inlinePushFront prepends v as a frame, writing into the dead prefix left by
// earlier pops when it fits and shifting the live region right otherwise.
func (l *list) inlinePushFront(v []byte) {
	f := frameLen(len(v))
	if l.head >= f {
		l.head -= f
		w := binary.PutUvarint(l.blob[l.head:], uint64(len(v)))
		copy(l.blob[l.head+w:], v)
		l.n++
		l.lpsz += lpEntrySize(v)
		return
	}
	need := f - l.head
	old := len(l.blob)
	l.inlineGrow(need)
	// Shift the live region right by need so the new frame fits at the front.
	copy(l.blob[l.head+need:], l.blob[l.head:old])
	w := binary.PutUvarint(l.blob[0:], uint64(len(v)))
	copy(l.blob[w:], v)
	l.head = 0
	l.n++
	l.lpsz += lpEntrySize(v)
}

// inlinePopFront returns the first element and advances the head cursor. The
// returned bytes alias the blob and stay valid until the next write, which is
// after the reply is copied out; the pop never memmoves.
func (l *list) inlinePopFront() []byte {
	vlen, w := binary.Uvarint(l.blob[l.head:])
	start := l.head + w
	end := start + int(vlen)
	v := l.blob[start:end]
	l.head = end
	l.n--
	l.lpsz -= lpEntrySize(v)
	if l.n == 0 {
		l.reset()
	}
	return v
}

// inlinePopBack returns the last element and truncates the blob to drop its
// frame. The returned bytes stay live in the backing array past the new length
// until the next write, which is after the reply is copied out.
func (l *list) inlinePopBack() []byte {
	pos := l.head
	lastStart, lastW, lastLen := pos, 0, uint64(0)
	for pos < len(l.blob) {
		vlen, w := binary.Uvarint(l.blob[pos:])
		lastStart, lastW, lastLen = pos, w, vlen
		pos += w + int(vlen)
	}
	v := l.blob[lastStart+lastW : lastStart+lastW+int(lastLen)]
	l.blob = l.blob[:lastStart]
	l.n--
	l.lpsz -= lpEntrySize(v)
	if l.n == 0 {
		l.reset()
	}
	return v
}

// reset empties the inline blob and its cursor. The capacity is kept so a
// churning list reuses its backing array.
func (l *list) reset() {
	l.blob = l.blob[:0]
	l.head = 0
	l.lpsz = lpOverhead
}

// inlineAt returns the element at index i, aliasing the blob. It walks i frames
// from the head cursor; inline lists are budget-bounded so the walk is short.
func (l *list) inlineAt(i int) []byte {
	pos := l.head
	for k := 0; k < i; k++ {
		vlen, w := binary.Uvarint(l.blob[pos:])
		pos += w + int(vlen)
	}
	vlen, w := binary.Uvarint(l.blob[pos:])
	return l.blob[pos+w : pos+w+int(vlen)]
}

// inlineDecode returns a view of every element, each aliasing the blob. The
// slice header is fresh; the element bytes are not copied, so the caller reads
// them before the next inline write. Used by the cold rebuild ops.
func (l *list) inlineDecode() [][]byte {
	out := make([][]byte, 0, l.n)
	pos := l.head
	for pos < len(l.blob) {
		vlen, w := binary.Uvarint(l.blob[pos:])
		start := pos + w
		end := start + int(vlen)
		out = append(out, l.blob[start:end])
		pos = end
	}
	return out
}

// reencodeInline rebuilds the inline blob from elems and promotes to native when
// the result crosses the budget. elems may alias the current blob: the new blob
// is a separate allocation, so the reads finish before the swap. This is the
// cold path shared by LSET, LINSERT, LREM, and LTRIM on the inline band, where
// an allocation is fine.
func (l *list) reencodeInline(elems [][]byte) {
	nb := make([]byte, 0, blobCap(elems))
	lpsz := lpOverhead
	for _, v := range elems {
		nb = binary.AppendUvarint(nb, uint64(len(v)))
		nb = append(nb, v...)
		lpsz += lpEntrySize(v)
	}
	l.blob = nb
	l.head = 0
	l.n = len(elems)
	l.lpsz = lpsz
	if lpsz > listpackBudget {
		l.toNative()
	}
}

func blobCap(elems [][]byte) int {
	total := 0
	for _, v := range elems {
		total += frameLen(len(v))
	}
	return total
}

// frameLen is the packed size of one inline frame: the uvarint length prefix
// plus the value bytes.
func frameLen(vlen int) int { return uvarintLen(uint64(vlen)) + vlen }

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// toNative promotes the inline band to the native chunked deque, the one-way F4
// transition (spec 2064/f3/13 section 4.3). It pushes every element into the
// deque through the tail path (adopting the inline order), sets the sticky
// quicklist bit, and releases the inline blob. A single oversized element lands
// as a lone chunk through the same push path.
func (l *list) toNative() {
	if l.nat != nil {
		return
	}
	nt := &native{}
	for _, v := range l.inlineDecode() {
		nt.pushBack(v)
	}
	l.nat = nt
	l.blob = nil
	l.head = 0
	l.n = 0
	l.everLarge = true
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
