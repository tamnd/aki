// Package btree implements a paged, ordered byte-string map on top of the
// pager. It is the storage structure the keyspace and the aggregate sub-trees
// sit on (doc 02 §10-12, doc 05 §4).
//
// Keys and values are arbitrary byte slices. The tree is type-agnostic: the
// keyspace layer composes its composite keys and serializes its value headers
// into these opaque bytes. The on-disk page layout follows doc 02: a 16-byte
// common header, a per-type header (a right-sibling pointer for leaves, a
// rightmost-child pointer for interior pages), a slot array of 2-byte cell
// offsets, and variable-length cells packed from the end of the page.
//
// What this first version narrows from the spec: pages are rewritten in full on
// every modification instead of doing in-place slot shuffles and incremental
// compaction, deletes do not merge underfull pages, and a cell that cannot fit
// in an empty page is rejected rather than spilled to an overflow chain. The
// keyspace only ever stores small inline cells here (a composite key plus a
// value header), so the overflow path is not needed yet. Large value bodies
// live in their own pages, referenced from the value header.
package btree

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/pager"
)

// arenaNodeCap is the maximum number of node structs that can be pre-allocated
// in one nodeArena without falling back to the heap. A Get traverses at most
// one node per tree level; Put/Delete traverse and may rewrite one per level.
// A 4-level tree covering billions of keys still fits in 4 entries.
const arenaNodeCap = 8

// arenaNodeKeyCap is the maximum number of cells per page that the pre-allocated
// backing arrays cover without heap fallback. Pages hold at most 255 cells with
// the default 4096-byte page size.
const arenaNodeKeyCap = 256

// arenaNodeEntry holds one pre-allocated node struct and its backing arrays.
// Embedding the arrays directly in the struct keeps them on the same cache line
// group as the node header, which matters for sequential key scans.
type arenaNodeEntry struct {
	n        node
	keysBuf  [arenaNodeKeyCap][]byte
	valsBuf  [arenaNodeKeyCap][]byte
	childBuf [arenaNodeKeyCap + 1]uint32
}

// nodeArena is a per-operation scratch buffer that holds the decoded key and
// value bytes for all B-tree nodes visited during one Get, Put, or Delete call.
// All bytes.Clone calls in the hot path are replaced by arena copies, which
// append into the arena's backing slice rather than allocating individual heap
// objects for each key and value. After the operation completes, the arena is
// reset (its length set back to zero) and returned to the pool. The backing
// slice is reused on the next operation that gets this arena from the pool.
//
// entries pre-allocates arenaNodeCap node structs along with their keys/vals
// backing arrays. decodeNodeAr calls allocNode instead of &node{} + make(),
// eliminating 3 heap allocations per page decode.
//
// tmp is a separate scratch buffer for building encoded cells in encodeNode.
// It is also reset between cells and reset at the start of each writeNode call.
// Because both buf and tmp are only used within the single-threaded hot path
// (one goroutine holds e.mu for writes; reads take RLock but each gets its own
// arena from the pool), there are no concurrency hazards.
type nodeArena struct {
	buf     []byte
	tmp     []byte
	entries [arenaNodeCap]arenaNodeEntry
	entryN  int
}

var nodeArenaPool = sync.Pool{
	New: func() any {
		return &nodeArena{
			buf: make([]byte, 0, 8192),
			tmp: make([]byte, 0, 256),
		}
	},
}

func (a *nodeArena) copy(src []byte) []byte {
	start := len(a.buf)
	a.buf = append(a.buf, src...)
	return a.buf[start:]
}

// allocNode returns a node backed by pre-allocated arrays from the arena when
// possible, falling back to heap allocation for deep trees or oversized pages.
func (a *nodeArena) allocNode(leaf bool, count int) *node {
	if a.entryN < arenaNodeCap && count <= arenaNodeKeyCap {
		e := &a.entries[a.entryN]
		a.entryN++
		n := &e.n
		n.leaf = leaf
		n.rightSibling = 0
		// counts must be cleared on every reuse: the arena pool is global, so an
		// order-statistic tree can leave a stale counts slice on this entry, and a
		// plain tree decoding into it would otherwise be misread as augmented.
		n.counts = nil
		if leaf {
			n.keys = e.keysBuf[:count]
			n.vals = e.valsBuf[:count]
			n.children = nil
		} else {
			n.keys = e.keysBuf[:count]
			n.vals = nil
			n.children = e.childBuf[:count+1]
		}
		return n
	}
	n := &node{leaf: leaf}
	if leaf {
		n.keys = make([][]byte, count)
		n.vals = make([][]byte, count)
	} else {
		n.keys = make([][]byte, count)
		n.children = make([]uint32, count+1)
	}
	return n
}

func (a *nodeArena) reset() {
	a.buf = a.buf[:0]
	a.tmp = a.tmp[:0]
	a.entryN = 0
}

// slotsStart is the byte offset where the slot array begins on both leaf and
// interior pages. The common header is 16 bytes; the per-type header (sibling
// or rightmost-child pointer plus slot_count and slot_array_start) fills bytes
// 16..24.
const slotsStart = 24

// noSibling marks a leaf with no right neighbour.
const noSibling = format.NullPage

// ErrCellTooLarge is returned when a single key/value pair cannot fit in an
// empty page. Callers that need to store a value this large must keep the body
// out of the tree and store a reference instead.
var ErrCellTooLarge = errors.New("aki/btree: entry too large for a page")

// Tree is a B-tree rooted at a single page. The root page number changes when
// the root splits, so callers that persist the root must read Root after a
// mutation and store the new value.
type Tree struct {
	pgr  *pager.Pager
	root uint32
	// orderStat makes this tree maintain per-child subtree row counts on its
	// interior pages, so Rank and SelectAt run in O(log n). Only opt-in trees
	// (the zset score sub-tree) set it; the keyspace tree and the other coll
	// sub-trees leave it false and write plain interior pages.
	orderStat bool
}

// Create allocates an empty leaf and returns a tree rooted at it.
func Create(pgr *pager.Pager) (*Tree, error) {
	t := &Tree{pgr: pgr}
	root, err := t.writeNewNode(&node{leaf: true, rightSibling: noSibling})
	if err != nil {
		return nil, err
	}
	t.root = root
	return t, nil
}

// CreateOrderStat allocates an empty order-statistic tree. Its interior pages
// carry per-child subtree row counts, so it can answer Rank and SelectAt in
// O(log n) once it grows past a single leaf.
func CreateOrderStat(pgr *pager.Pager) (*Tree, error) {
	t, err := Create(pgr)
	if err != nil {
		return nil, err
	}
	t.orderStat = true
	return t, nil
}

// Open returns a tree rooted at the given page. The page is not read until the
// first operation.
func Open(pgr *pager.Pager, root uint32) *Tree {
	return &Tree{pgr: pgr, root: root}
}

// OpenOrderStat opens an existing order-statistic tree at root. The caller is
// responsible for only opening a tree this way that was created with
// CreateOrderStat; a plain tree opened this way maintains no counts on the pages
// it already has and Rank/SelectAt would be wrong, so the keyspace pairs the flag
// with the tree at creation and never mixes the two.
func OpenOrderStat(pgr *pager.Pager, root uint32) *Tree {
	return &Tree{pgr: pgr, root: root, orderStat: true}
}

// OrderStat reports whether this tree maintains order-statistic counts.
func (t *Tree) OrderStat() bool { return t.orderStat }

// Root returns the current root page number.
func (t *Tree) Root() uint32 { return t.root }

// node is the decoded form of a page. Decoding copies all key and value bytes
// out of the page buffer, so the page can be unpinned as soon as it is decoded.
type node struct {
	leaf bool
	keys [][]byte
	// vals is set only for leaves: vals[i] is the value for keys[i].
	vals [][]byte
	// children is set only for interior nodes: children[i] is the subtree whose
	// keys are all less than keys[i], and children[len(keys)] is the rightmost
	// subtree. So len(children) == len(keys)+1.
	children []uint32
	// counts is set only for interior nodes of an order-statistic (augmented)
	// tree: counts[i] is the number of leaf rows in the subtree rooted at
	// children[i], so len(counts) == len(children). A nil counts marks a plain
	// interior node, which encodes and decodes exactly as before and pays nothing.
	counts []uint32
	// rightSibling links leaves left to right for range scans.
	rightSibling uint32
}

// Get returns the value stored under key and whether it was found.
// Intermediate node data is decoded into a pooled arena so that only one
// allocation (the returned value copy) escapes per call.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	pgno := t.root
	for {
		pg, err := t.pgr.Get(pgno)
		if err != nil {
			return nil, false, err
		}
		if pg.Data[0] == format.PageTypeBTreeLeaf {
			// leafLookup returns a heap-owned copy, so the page can be unpinned
			// right after. No arena is touched on the read path at all.
			v, ok, err := leafLookup(pg.Data, key)
			t.pgr.Unpin(pg, false)
			return v, ok, err
		}
		_, child, err := descendOnPage(pg.Data, key)
		t.pgr.Unpin(pg, false)
		if err != nil {
			return nil, false, err
		}
		pgno = child
	}
}

// Put inserts or replaces the value stored under key. It may split pages and
// grow the tree, in which case Root changes.
func (t *Tree) Put(key, val []byte) error {
	if err := t.put(key, val); err != nil {
		return err
	}
	assertInvariants(t)
	return nil
}

// Upsert inserts or replaces key/val and returns the previous value stored
// under key, or nil if key was not present. A single traversal finds the leaf
// and captures the old value there, saving one Get traversal compared to a
// separate lookup followed by Put.
func (t *Tree) Upsert(key, val []byte) ([]byte, error) {
	ar := nodeArenaPool.Get().(*nodeArena)
	defer func() { ar.reset(); nodeArenaPool.Put(ar) }()
	oldVal, _, sp, err := t.insertAr(t.root, key, val, ar)
	if err != nil {
		return nil, err
	}
	if sp != nil {
		newRoot, err := t.writeNewNodeAr(t.newRootNode(sp), ar)
		if err != nil {
			return nil, err
		}
		t.root = newRoot
	}
	assertInvariants(t)
	return oldVal, nil
}

// newRootNode builds the interior root that replaces the old root after it split.
// On an order-statistic tree it carries the two halves' counts so the new root's
// counts are correct without a recount.
func (t *Tree) newRootNode(sp *splitResult) *node {
	n := &node{
		leaf:     false,
		keys:     [][]byte{sp.sepKey},
		children: []uint32{t.root, sp.right},
	}
	if t.orderStat {
		n.counts = []uint32{sp.leftCount, sp.rightCount}
	}
	return n
}

func (t *Tree) put(key, val []byte) error {
	ar := nodeArenaPool.Get().(*nodeArena)
	defer func() { ar.reset(); nodeArenaPool.Put(ar) }()
	_, _, sp, err := t.insertAr(t.root, key, val, ar)
	if err != nil {
		return err
	}
	if sp == nil {
		return nil
	}
	// The root split: build a new interior root over the two halves.
	newRoot, err := t.writeNewNodeAr(t.newRootNode(sp), ar)
	if err != nil {
		return err
	}
	t.root = newRoot
	return nil
}

// splitResult reports that inserting into a child page split it: sepKey is the
// separator to insert into the parent, and right is the new page holding the
// upper half. On an order-statistic tree leftCount and rightCount carry the row
// counts of the two halves, so the parent can set its count for the old child to
// leftCount and insert rightCount for the new one without recounting.
type splitResult struct {
	sepKey     []byte
	right      uint32
	leftCount  uint32
	rightCount uint32
}

// insertAr traverses the tree from pgno and inserts key/val, using ar to avoid
// per-cell heap allocations. It returns the previous value stored under key
// (or nil if key was absent) so callers can inspect or free the old value in
// a single traversal without a separate Get.
func (t *Tree) insertAr(pgno uint32, key, val []byte, ar *nodeArena) (oldVal []byte, inserted bool, sp *splitResult, err error) {
	// Peek the page type without decoding the whole node. A leaf is decoded and
	// modified as before; an interior node is descended by searching its
	// separators directly on the page, so the common no-split case never decodes
	// the interior levels at all.
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return nil, false, nil, err
	}
	if pg.Data[0] == format.PageTypeBTreeLeaf {
		// Fast path: mutate the leaf bytes in place when the change fits the page's
		// free gap, so a single-key write does O(cell) work instead of decoding and
		// re-encoding the whole page. It cannot split, so a full page falls through
		// to the decode path below, which compacts dead cells and splits if needed.
		if ov, ins, done := leafUpsertInPlace(pg.Data, key, val); done {
			t.pgr.Unpin(pg, true)
			return ov, ins, nil, nil
		}
		n, derr := decodeNodeAr(pg.Data, ar)
		t.pgr.Unpin(pg, false)
		if derr != nil {
			return nil, false, nil, derr
		}
		oldVal, inserted = n.upsertAr(key, val, ar)
		sp, err = t.writeOrSplitAr(pgno, n, ar)
		return
	}

	ci, child, derr := descendOnPage(pg.Data, key)
	t.pgr.Unpin(pg, false)
	if derr != nil {
		return nil, false, nil, derr
	}
	oldVal, inserted, sp, err = t.insertAr(child, key, val, ar)
	if err != nil {
		return
	}
	if sp == nil {
		// No child split. On an order-statistic tree a genuine insert grows the
		// descended child's subtree by one, so bump this node's count for that
		// child in place. The bump is the only interior write the common insert
		// does, and it touches four bytes without decoding the page.
		if inserted && t.orderStat {
			if berr := t.bumpChildCount(pgno, ci, 1); berr != nil {
				return oldVal, inserted, nil, berr
			}
		}
		return
	}
	// The child split. Only now decode this interior node so the new separator
	// and child pointer can be inserted at slot ci (the same slot childIndex
	// would have returned for key), then write it back or split it in turn.
	// sepKey lives in ar.buf; appending to the arena during this decode may grow
	// ar.buf into a new backing array, but sp.sepKey keeps pointing at the old
	// one and stays valid.
	n, derr := t.readNodeAr(pgno, ar)
	if derr != nil {
		return oldVal, inserted, nil, derr
	}
	n.keys = insertBytes(n.keys, ci, sp.sepKey)
	n.children = insertU32(n.children, ci+1, sp.right)
	if t.orderStat {
		// The split replaced the single child at ci with two: ci keeps the left
		// half, ci+1 holds the right. Their counts come straight off the split, so
		// this node's totals stay exact whether the underlying op was an insert or
		// a same-key replace that happened to overflow the page.
		n.counts[ci] = sp.leftCount
		n.counts = insertU32(n.counts, ci+1, sp.rightCount)
	}
	sp, err = t.writeOrSplitAr(pgno, n, ar)
	return
}

// bumpChildCount adds delta to the subtree row count this interior page stores
// for the child at slot ci, writing the four count bytes in place without
// decoding the page. ci == CellCount names the rightmost child, whose count lives
// in the per-type header at byte 20; a lower ci names a cell, whose count sits
// just past its four-byte child pointer.
func (t *Tree) bumpChildCount(pgno uint32, ci int, delta int32) error {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return err
	}
	interiorBumpCount(pg.Data, ci, delta)
	t.pgr.Unpin(pg, true)
	return nil
}

// interiorBumpCount adds delta to the stored count for child slot ci on the
// augmented interior page b.
func interiorBumpCount(b []byte, ci int, delta int32) {
	count := int(encoding.U16(b[2:]))
	var off int
	if ci >= count {
		off = 20 // rightmost-child count in the per-type header
	} else {
		off = int(encoding.U16(b[slotsStart+2*ci:])) + 4 // just past the child pointer
	}
	encoding.PutU32(b[off:], uint32(int32(encoding.U32(b[off:]))+delta))
}

// writeOrSplitAr writes n back to pgno if it fits, otherwise splits it. It uses ar for encoding and split
// separator allocation, eliminating per-cell and per-key heap allocations.
func (t *Tree) writeOrSplitAr(pgno uint32, n *node, ar *nodeArena) (*splitResult, error) {
	if n.size() <= t.usable() {
		return nil, t.writeNodeAr(pgno, n, ar)
	}
	if len(n.keys) < 2 {
		return nil, ErrCellTooLarge
	}
	left, right, sep := n.splitAr(ar)
	if left.size() > t.usable() || right.size() > t.usable() {
		return nil, ErrCellTooLarge
	}
	rightNo, err := t.writeNewNodeAr(right, ar)
	if err != nil {
		return nil, err
	}
	if n.leaf {
		left.rightSibling = rightNo
	}
	if err := t.writeNodeAr(pgno, left, ar); err != nil {
		return nil, err
	}
	res := &splitResult{sepKey: sep, right: rightNo}
	if t.orderStat {
		res.leftCount = left.rowCount()
		res.rightCount = right.rowCount()
	}
	return res, nil
}

// rowCount returns the number of leaf rows under this node: its own key count for
// a leaf, or the sum of its child counts for an augmented interior node. It is
// only called on an order-statistic tree, where every interior node carries
// counts, so the interior branch never sees a nil counts slice.
func (n *node) rowCount() uint32 {
	if n.leaf {
		return uint32(len(n.keys))
	}
	var total uint32
	for _, c := range n.counts {
		total += c
	}
	return total
}

// Delete removes key and reports whether it was present. Underfull pages are
// left in place; the tree shrinks in page count only on a full rewrite.
func (t *Tree) Delete(key []byte) (bool, error) {
	ar := nodeArenaPool.Get().(*nodeArena)
	defer func() { ar.reset(); nodeArenaPool.Put(ar) }()
	removed, err := t.delAr(t.root, key, ar)
	if err != nil {
		return removed, err
	}
	assertInvariants(t)
	return removed, nil
}

func (t *Tree) delAr(pgno uint32, key []byte, ar *nodeArena) (bool, error) {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return false, err
	}
	if pg.Data[0] != format.PageTypeBTreeLeaf {
		// Interior node: find the child to follow on the page, then descend.
		ci, child, derr := descendOnPage(pg.Data, key)
		t.pgr.Unpin(pg, false)
		if derr != nil {
			return false, derr
		}
		removed, derr := t.delAr(child, key, ar)
		if derr != nil {
			return removed, derr
		}
		// On an order-statistic tree a genuine removal shrinks the descended
		// child's subtree by one. Delete never merges or rebalances pages here, so
		// the only count change is a single in-place decrement per ancestor, the
		// mirror of the insert bump.
		if removed && t.orderStat {
			if berr := t.bumpChildCount(pgno, ci, -1); berr != nil {
				return removed, berr
			}
		}
		return removed, nil
	}
	n, derr := decodeNodeAr(pg.Data, ar)
	t.pgr.Unpin(pg, false)
	if derr != nil {
		return false, derr
	}
	i, ok := n.find(key)
	if !ok {
		return false, nil
	}
	n.keys = append(n.keys[:i], n.keys[i+1:]...)
	n.vals = append(n.vals[:i], n.vals[i+1:]...)
	return true, t.writeNodeAr(pgno, n, ar)
}

// find returns the index of key in a leaf and whether it is present. n.keys is
// sorted, so it lower-bounds key with a binary search rather than a linear scan.
func (n *node) find(key []byte) (int, bool) {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(n.keys[mid], key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(n.keys) && bytes.Equal(n.keys[lo], key) {
		return lo, true
	}
	return lo, false
}

// childIndex returns the child slot to descend into for key on an interior node.
// Separators are sorted, so this is the first one strictly greater than key.
func (n *node) childIndex(key []byte) int {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(n.keys[mid], key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// slotKeyAt returns the key bytes of the cell named by slot i on page b. keyPrefix
// is the number of bytes a cell of this page kind carries before its key-length
// varint: 0 for a leaf cell, 4 for an interior cell whose first four bytes are the
// left-child pointer. The returned slice aliases b, so the caller must hold the pin.
func slotKeyAt(b []byte, i, keyPrefix int) []byte {
	off := int(encoding.U16(b[slotsStart+2*i:])) + keyPrefix
	kl, m, _ := encoding.Uvarint(b[off:])
	off += m
	return b[off : off+int(kl)]
}

// searchSlots binary-searches the sorted slot array of page b for key. It returns
// the lower-bound slot position (the first slot whose key is >= key, or count when
// key is greater than every slot key) and whether that slot's key equals key.
// Every insert path keeps the slot array in ascending key order, so a binary search
// is correct and turns an append-heavy workload (where a linear scan walks every
// cell only to fall off the end) from O(n) into O(log n) probes per page.
func searchSlots(b []byte, count, keyPrefix int, key []byte) (pos int, found bool) {
	lo, hi := 0, count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bytes.Compare(slotKeyAt(b, mid, keyPrefix), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < count && bytes.Equal(slotKeyAt(b, lo, keyPrefix), key) {
		return lo, true
	}
	return lo, false
}

// descendOnPage finds the child to follow for key on the interior page b, reading
// each separator straight from the page buffer instead of decoding the whole node
// into an arena. ci is the same slot index childIndex would return, so a caller
// that later inserts a separator (after a child split) can reuse it; child is the
// page number to descend into. A descent through an interior level this way copies
// no key bytes and allocates nothing. The caller must hold a pin on the page for
// the duration of the call, since the returned values are read out of b.
func descendOnPage(b, key []byte) (ci int, child uint32, err error) {
	h, err := format.ParsePageHeader(b)
	if err != nil {
		return 0, 0, err
	}
	if h.Type != format.PageTypeBTreeInt {
		return 0, 0, fmt.Errorf("aki/btree: descend on non-interior page (type 0x%02x)", h.Type)
	}
	count := int(h.CellCount)
	// An augmented interior page carries a 4-byte subtree count after each child
	// pointer, so its key prefix is 8 instead of 4; the child pointer itself still
	// sits at the cell's first four bytes, so only the separator search shifts.
	keyPrefix := 4
	if h.Flags&format.FlagBTreeOrderStat != 0 {
		keyPrefix = 8
	}
	// Descend into the child left of the first separator strictly greater than key,
	// or the rightmost child when key is past every separator. The lower bound is the
	// first separator >= key; an exact hit must descend to the right of its separator,
	// so step one past an equal slot to match the original strict-less-than scan.
	pos, found := searchSlots(b, count, keyPrefix, key)
	if found {
		pos++
	}
	if pos >= count {
		return count, encoding.U32(b[16:]), nil
	}
	off := int(encoding.U16(b[slotsStart+2*pos:]))
	return pos, encoding.U32(b[off:]), nil
}

// leafLookup finds key on the leaf page b and returns a heap-owned copy of its
// value, reading cells straight from the page rather than decoding every cell
// into an arena. Leaf keys are stored sorted, so the scan stops as soon as it
// passes where key would be. The caller must hold a pin on the page for the
// duration of the call.
func leafLookup(b, key []byte) ([]byte, bool, error) {
	h, err := format.ParsePageHeader(b)
	if err != nil {
		return nil, false, err
	}
	if h.Type != format.PageTypeBTreeLeaf {
		return nil, false, fmt.Errorf("aki/btree: leaf lookup on non-leaf page (type 0x%02x)", h.Type)
	}
	count := int(h.CellCount)
	pos, found := searchSlots(b, count, 0, key)
	if !found {
		return nil, false, nil
	}
	off := int(encoding.U16(b[slotsStart+2*pos:]))
	kl, m, derr := encoding.Uvarint(b[off:])
	if derr != nil {
		return nil, false, derr
	}
	off += m + int(kl)
	vl, m2, derr := encoding.Uvarint(b[off:])
	if derr != nil {
		return nil, false, derr
	}
	off += m2
	return bytes.Clone(b[off : off+int(vl)]), true, nil
}

// upsertAr inserts key/val into a leaf in sorted order (or replaces the value
// if key is present), copying key and val into ar. It returns the previous
// value for key, or nil if the key was not present. The returned slice is a
// heap-owned copy safe to use after ar is reset.
func (n *node) upsertAr(key, val []byte, ar *nodeArena) (oldVal []byte, inserted bool) {
	i, ok := n.find(key)
	v := ar.copy(val)
	if ok {
		oldVal = bytes.Clone(n.vals[i])
		n.vals[i] = v
		return oldVal, false
	}
	n.keys = insertBytes(n.keys, i, ar.copy(key))
	n.vals = insertBytes(n.vals, i, v)
	return nil, true
}

// leafUpsertInPlace inserts or replaces key/val directly in the leaf page bytes b
// without decoding the whole page into a node and re-encoding it. It returns the
// previous value (a heap copy) when key was present, and done=true when the
// mutation was applied in place. done=false means the page lacks the contiguous
// free space this change needs and the caller must fall back to the
// decode/compact/split path; in that case b is left unmodified. The caller holds
// a write pin on b.
//
// Three cases, cheapest first:
//   - key present, value the same length: overwrite the value bytes where they
//     sit. No slot, header, or free space changes. This is the hot path for a SET
//     that rewrites an existing key with a same-size value.
//   - key present, value a different length: write a fresh cell into the free gap
//     and repoint the slot, abandoning the old cell as dead space that the next
//     full re-encode reclaims (decode reads only the cells the slots name).
//   - key absent: write a fresh cell and open a slot for it at the sorted
//     position by shifting the slots above it up by one.
//
// The page format packs cells downward from the end and grows the slot array up
// from slotsStart, so the free gap is the run of bytes from FreeStart to FreeEnd.
// Every case that returns done=true keeps that gap accounting correct: FreeEnd
// drops by the new cell's length and, for an insert, FreeStart rises by one slot.
func leafUpsertInPlace(b, key, val []byte) (oldVal []byte, inserted, done bool) {
	h, err := format.ParsePageHeader(b)
	if err != nil || h.Type != format.PageTypeBTreeLeaf {
		return nil, false, false
	}
	count := int(h.CellCount)
	pos, matched := searchSlots(b, count, 0, key)
	var matchVStart, matchVLen int
	if matched {
		off := int(encoding.U16(b[slotsStart+2*pos:]))
		kl, m, derr := encoding.Uvarint(b[off:])
		if derr != nil {
			return nil, false, false
		}
		vo := off + m + int(kl)
		vl, m2, derr := encoding.Uvarint(b[vo:])
		if derr != nil {
			return nil, false, false
		}
		matchVStart = vo + m2
		matchVLen = int(vl)
	}

	newCellLen := uvarintLen(uint64(len(key))) + len(key) + uvarintLen(uint64(len(val))) + len(val)
	freeStart := int(h.FreeStart)
	freeEnd := int(h.FreeEnd)

	if matched {
		oldVal = bytes.Clone(b[matchVStart : matchVStart+matchVLen])
		if matchVLen == len(val) {
			// Same length means the value-length varint is unchanged, so the value
			// bytes can be overwritten where they already sit.
			copy(b[matchVStart:], val)
			return oldVal, false, true
		}
		if newCellLen > freeEnd-freeStart {
			return oldVal, false, false
		}
		newOff := freeEnd - newCellLen
		putLeafCell(b, newOff, key, val)
		encoding.PutU16(b[slotsStart+2*pos:], uint16(newOff))
		encoding.PutU16(b[6:], uint16(newOff)) // FreeEnd
		return oldVal, false, true
	}

	// Insert a new key: the change needs the cell plus one 2-byte slot.
	if newCellLen+2 > freeEnd-freeStart {
		return nil, false, false
	}
	newOff := freeEnd - newCellLen
	putLeafCell(b, newOff, key, val)
	slotPos := slotsStart + 2*pos
	slotsEnd := slotsStart + 2*count
	copy(b[slotPos+2:slotsEnd+2], b[slotPos:slotsEnd])
	encoding.PutU16(b[slotPos:], uint16(newOff))
	encoding.PutU16(b[2:], uint16(count+1))    // CellCount
	encoding.PutU16(b[4:], uint16(slotsEnd+2)) // FreeStart
	encoding.PutU16(b[6:], uint16(newOff))     // FreeEnd
	encoding.PutU16(b[20:], uint16(count+1))   // duplicated slot count in the per-type header
	return nil, true, true
}

// putLeafCell writes a leaf cell (uvarint keylen, key, uvarint vallen, val) into b
// starting at off. The caller has already checked the cell fits, so appending into
// the zero-length slice b[off:off] writes straight into b without reallocating.
func putLeafCell(b []byte, off int, key, val []byte) {
	c := b[off:off]
	c = encoding.AppendUvarint(c, uint64(len(key)))
	c = append(c, key...)
	c = encoding.AppendUvarint(c, uint64(len(val)))
	c = append(c, val...)
	_ = c
}

// splitAr divides a full node into a lower and upper half. Keys are already in ar.buf so no copy is needed;
// slices are stable until the arena resets at the end of the enclosing Put.
func (n *node) splitAr(ar *nodeArena) (left, right *node, sep []byte) {
	_ = ar // keys already live in ar.buf; just slice, no copy needed
	mid := len(n.keys) / 2
	if n.leaf {
		left = &node{leaf: true, keys: n.keys[:mid], vals: n.vals[:mid]}
		right = &node{leaf: true, keys: n.keys[mid:], vals: n.vals[mid:], rightSibling: n.rightSibling}
		// Promote the shortest separator that still routes, not the whole first key
		// of the right page. Interior nodes only need a separator strictly greater
		// than the left page's last key and no greater than the right page's first
		// key; the full key bytes live in the leaf. For large keys (a set member is
		// the key) this shrinks the upper levels by the key length, which is what
		// keeps a larger-than-memory set's index resident instead of overflowing it.
		return left, right, shortestSep(n.keys[mid-1], n.keys[mid])
	}
	sep = n.keys[mid]
	left = &node{keys: n.keys[:mid], children: n.children[:mid+1]}
	right = &node{keys: n.keys[mid+1:], children: n.children[mid+1:]}
	if n.counts != nil {
		// Counts run parallel to children, so they split at the same boundary. The
		// separator key at mid is promoted, not kept, but its child pointers and
		// their counts stay: children[:mid+1] with the left half, children[mid+1:]
		// with the right.
		left.counts = n.counts[:mid+1]
		right.counts = n.counts[mid+1:]
	}
	return left, right, sep
}

// shortestSep returns the shortest byte string s with lo < s <= hi, as a prefix of
// hi. It is the suffix-truncated separator promoted on a leaf split: lo is the left
// page's last key, hi is the right page's first key, and the leaves are sorted and
// distinct so lo < hi holds. Routing only needs s to sit strictly above every left
// key and at or below every right key, so a prefix of hi long enough to exceed lo is
// enough, and it is far shorter than hi when the keys are large. The returned slice
// aliases hi (which lives in the split arena), so it carries no allocation and stays
// valid exactly as long as the full key did.
func shortestSep(lo, hi []byte) []byte {
	n := min(len(hi), len(lo))
	for i := range n {
		if hi[i] != lo[i] {
			// hi[i] > lo[i] because hi > lo and they agree on bytes [0, i), so the
			// prefix hi[:i+1] is the shortest prefix of hi strictly greater than lo.
			return hi[:i+1]
		}
	}
	// lo is a prefix of hi (hi extends lo), so the shortest prefix of hi greater
	// than lo is lo's length plus one more byte. hi is strictly longer than lo here
	// because hi > lo and they agree on all of lo, so hi[:n+1] is in range.
	return hi[:n+1]
}

// size returns the number of page bytes this node would occupy when serialized.
func (n *node) size() int {
	total := slotsStart + 2*len(n.keys)
	// An augmented interior cell carries a 4-byte subtree count after its 4-byte
	// child pointer, so its prefix is 8 bytes; the rightmost child's count reuses
	// the redundant slot-count bytes in the per-type header and costs nothing here.
	prefix := 4
	if !n.leaf && n.counts != nil {
		prefix = 8
	}
	for i, k := range n.keys {
		if n.leaf {
			total += cellLen(uvarintLen(uint64(len(k))) + len(k) + uvarintLen(uint64(len(n.vals[i]))) + len(n.vals[i]))
		} else {
			total += cellLen(prefix + uvarintLen(uint64(len(k))) + len(k))
		}
	}
	return total
}

// cellLen accounts for the cell payload only; the 2-byte slot is counted in
// size. Kept as a function so the accounting has one home.
func cellLen(payload int) int { return payload }

func (t *Tree) usable() int { return int(t.pgr.PageSize()) }

// readNode loads and decodes a page, copying its bytes so the page can be
// unpinned immediately.
func (t *Tree) readNode(pgno uint32) (*node, error) {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return nil, err
	}
	defer t.pgr.Unpin(pg, false)
	return decodeNode(pg.Data)
}

// readNodeAr is like readNode but decodes key and value bytes into ar instead
// of allocating individual heap slices per key/value.
func (t *Tree) readNodeAr(pgno uint32, ar *nodeArena) (*node, error) {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return nil, err
	}
	defer t.pgr.Unpin(pg, false)
	return decodeNodeAr(pg.Data, ar)
}

// writeNode encodes n into an existing page.
func (t *Tree) writeNode(pgno uint32, n *node) error {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return err
	}
	if err := encodeNode(pg.Data, n); err != nil {
		t.pgr.Unpin(pg, false)
		return err
	}
	t.pgr.Unpin(pg, true)
	return nil
}

// writeNodeAr is like writeNode but uses ar.tmp as a reusable cell buffer to
// avoid one heap allocation per cell during encoding.
func (t *Tree) writeNodeAr(pgno uint32, n *node, ar *nodeArena) error {
	pg, err := t.pgr.Get(pgno)
	if err != nil {
		return err
	}
	if err := encodeNodeAr(pg.Data, n, ar); err != nil {
		t.pgr.Unpin(pg, false)
		return err
	}
	t.pgr.Unpin(pg, true)
	return nil
}

// writeNewNode allocates a page, encodes n into it, and returns its number.
func (t *Tree) writeNewNode(n *node) (uint32, error) {
	pg, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	if err := encodeNode(pg.Data, n); err != nil {
		t.pgr.Unpin(pg, false)
		return 0, err
	}
	no := pg.No
	t.pgr.Unpin(pg, true)
	return no, nil
}

// writeNewNodeAr is like writeNewNode but uses ar.tmp during encoding.
func (t *Tree) writeNewNodeAr(n *node, ar *nodeArena) (uint32, error) {
	pg, err := t.pgr.Allocate()
	if err != nil {
		return 0, err
	}
	if err := encodeNodeAr(pg.Data, n, ar); err != nil {
		t.pgr.Unpin(pg, false)
		return 0, err
	}
	no := pg.No
	t.pgr.Unpin(pg, true)
	return no, nil
}

// decodeNodeAr is like decodeNode but copies key and value bytes into ar
// instead of allocating individual heap slices. The returned node's key/val
// slices point into ar.buf and are valid until ar.reset is called.
func decodeNodeAr(b []byte, ar *nodeArena) (*node, error) {
	h, err := format.ParsePageHeader(b)
	if err != nil {
		return nil, err
	}
	leaf := h.Type == format.PageTypeBTreeLeaf
	if !leaf && h.Type != format.PageTypeBTreeInt {
		return nil, fmt.Errorf("aki/btree: not a b-tree node (page type 0x%02x)", h.Type)
	}
	count := int(h.CellCount)
	n := ar.allocNode(leaf, count)

	if leaf {
		n.rightSibling = encoding.U32(b[16:])
		for i := range count {
			off := int(encoding.U16(b[slotsStart+2*i:]))
			kl, m, err := encoding.Uvarint(b[off:])
			if err != nil {
				return nil, err
			}
			off += m
			n.keys[i] = ar.copy(b[off : off+int(kl)])
			off += int(kl)
			vl, m, err := encoding.Uvarint(b[off:])
			if err != nil {
				return nil, err
			}
			off += m
			n.vals[i] = ar.copy(b[off : off+int(vl)])
		}
		return n, nil
	}

	rightmost := encoding.U32(b[16:])
	augmented := h.Flags&format.FlagBTreeOrderStat != 0
	keyPrefix := 4
	if augmented {
		keyPrefix = 8
		n.counts = make([]uint32, count+1)
	}
	for i := range count {
		off := int(encoding.U16(b[slotsStart+2*i:]))
		n.children[i] = encoding.U32(b[off:])
		if augmented {
			n.counts[i] = encoding.U32(b[off+4:])
		}
		off += keyPrefix
		kl, m, err := encoding.Uvarint(b[off:])
		if err != nil {
			return nil, err
		}
		off += m
		n.keys[i] = ar.copy(b[off : off+int(kl)])
	}
	n.children[count] = rightmost
	if augmented {
		n.counts[count] = encoding.U32(b[20:])
	}
	return n, nil
}

// decodeNode parses a page buffer into a node, copying key and value bytes.
func decodeNode(b []byte) (*node, error) {
	h, err := format.ParsePageHeader(b)
	if err != nil {
		return nil, err
	}
	leaf := h.Type == format.PageTypeBTreeLeaf
	if !leaf && h.Type != format.PageTypeBTreeInt {
		return nil, fmt.Errorf("aki/btree: not a b-tree node (page type 0x%02x)", h.Type)
	}
	n := &node{leaf: leaf}
	count := int(h.CellCount)

	if leaf {
		n.rightSibling = encoding.U32(b[16:])
		n.keys = make([][]byte, count)
		n.vals = make([][]byte, count)
		for i := range count {
			off := int(encoding.U16(b[slotsStart+2*i:]))
			kl, m, err := encoding.Uvarint(b[off:])
			if err != nil {
				return nil, err
			}
			off += m
			n.keys[i] = bytes.Clone(b[off : off+int(kl)])
			off += int(kl)
			vl, m, err := encoding.Uvarint(b[off:])
			if err != nil {
				return nil, err
			}
			off += m
			n.vals[i] = bytes.Clone(b[off : off+int(vl)])
		}
		return n, nil
	}

	rightmost := encoding.U32(b[16:])
	n.keys = make([][]byte, count)
	n.children = make([]uint32, count+1)
	augmented := h.Flags&format.FlagBTreeOrderStat != 0
	keyPrefix := 4
	if augmented {
		keyPrefix = 8
		n.counts = make([]uint32, count+1)
	}
	for i := range count {
		off := int(encoding.U16(b[slotsStart+2*i:]))
		n.children[i] = encoding.U32(b[off:])
		if augmented {
			n.counts[i] = encoding.U32(b[off+4:])
		}
		off += keyPrefix
		kl, m, err := encoding.Uvarint(b[off:])
		if err != nil {
			return nil, err
		}
		off += m
		n.keys[i] = bytes.Clone(b[off : off+int(kl)])
	}
	n.children[count] = rightmost
	if augmented {
		n.counts[count] = encoding.U32(b[20:])
	}
	return n, nil
}

// decodeInteriorChildren parses only the child pointers of an interior page,
// skipping the separator keys. The backward cursor walk (descendLast,
// descendRightmost, climbPrev) navigates purely by child index and never reads a
// separator, so cloning the keys, as decodeNode does, is pure waste; on a
// whole-set reverse scan that clone is the dominant allocation. This returns a
// node with children set and keys nil, one slice allocation per interior and
// nothing per key, so a reverse scan stays allocation-flat in the key count.
func decodeInteriorChildren(b []byte) (*node, error) {
	h, err := format.ParsePageHeader(b)
	if err != nil {
		return nil, err
	}
	if h.Type != format.PageTypeBTreeInt {
		return nil, fmt.Errorf("aki/btree: not an interior node (page type 0x%02x)", h.Type)
	}
	count := int(h.CellCount)
	n := &node{children: make([]uint32, count+1)}
	for i := range count {
		off := int(encoding.U16(b[slotsStart+2*i:]))
		n.children[i] = encoding.U32(b[off:])
	}
	n.children[count] = encoding.U32(b[16:])
	return n, nil
}

// encodeNodeAr is like encodeNode but uses ar.tmp as a reusable cell buffer,
// eliminating one heap allocation per cell (50 allocs per full page saved).
func encodeNodeAr(b []byte, n *node, ar *nodeArena) error {
	if n.size() > len(b) {
		return ErrCellTooLarge
	}
	for i := range b {
		b[i] = 0
	}
	count := len(n.keys)
	end := len(b)
	augmented := !n.leaf && n.counts != nil

	for i := range count {
		ar.tmp = ar.tmp[:0]
		if n.leaf {
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.keys[i])))
			ar.tmp = append(ar.tmp, n.keys[i]...)
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.vals[i])))
			ar.tmp = append(ar.tmp, n.vals[i]...)
		} else {
			ar.tmp = encoding.AppendU32(ar.tmp, n.children[i])
			if augmented {
				ar.tmp = encoding.AppendU32(ar.tmp, n.counts[i])
			}
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.keys[i])))
			ar.tmp = append(ar.tmp, n.keys[i]...)
		}
		end -= len(ar.tmp)
		copy(b[end:], ar.tmp)
		encoding.PutU16(b[slotsStart+2*i:], uint16(end))
	}

	if err := writeNodeHeader(b, n, count, end, augmented); err != nil {
		return err
	}
	return nil
}

// writeNodeHeader fills the common header and the per-type header after the cells
// have been packed. For an augmented interior page it sets the order-statistic
// flag and stores the rightmost child's subtree count in the per-type header
// (bytes 20..24) in place of the redundant slot-count and slot-array-start a
// plain page keeps there; the decoder recovers those two from the common header
// and the fixed slot start, so nothing is lost.
func writeNodeHeader(b []byte, n *node, count, end int, augmented bool) error {
	h := format.PageHeader{
		CellCount: uint16(count),
		FreeStart: uint16(slotsStart + 2*count),
		FreeEnd:   uint16(end),
	}
	if n.leaf {
		h.Type = format.PageTypeBTreeLeaf
	} else {
		h.Type = format.PageTypeBTreeInt
		if augmented {
			h.Flags = format.FlagBTreeOrderStat
		}
	}
	if err := h.MarshalTo(b); err != nil {
		return err
	}

	if n.leaf {
		encoding.PutU32(b[16:], n.rightSibling)
		encoding.PutU16(b[20:], uint16(count))
		encoding.PutU16(b[22:], slotsStart)
		return nil
	}
	encoding.PutU32(b[16:], n.children[count])
	if augmented {
		encoding.PutU32(b[20:], n.counts[count]) // rightmost child's subtree count
	} else {
		encoding.PutU16(b[20:], uint16(count))
		encoding.PutU16(b[22:], slotsStart)
	}
	return nil
}

// encodeNode writes n into a page buffer. It zeroes the buffer first, lays the
// slot array right after the per-type header, and packs cells from the end of
// the page downward.
func encodeNode(b []byte, n *node) error {
	if n.size() > len(b) {
		return ErrCellTooLarge
	}
	for i := range b {
		b[i] = 0
	}
	count := len(n.keys)
	end := len(b)
	augmented := !n.leaf && n.counts != nil

	for i := range count {
		var cell []byte
		if n.leaf {
			cell = encoding.AppendUvarint(cell, uint64(len(n.keys[i])))
			cell = append(cell, n.keys[i]...)
			cell = encoding.AppendUvarint(cell, uint64(len(n.vals[i])))
			cell = append(cell, n.vals[i]...)
		} else {
			cell = encoding.AppendU32(cell, n.children[i])
			if augmented {
				cell = encoding.AppendU32(cell, n.counts[i])
			}
			cell = encoding.AppendUvarint(cell, uint64(len(n.keys[i])))
			cell = append(cell, n.keys[i]...)
		}
		end -= len(cell)
		copy(b[end:], cell)
		encoding.PutU16(b[slotsStart+2*i:], uint16(end))
	}

	return writeNodeHeader(b, n, count, end, augmented)
}

// uvarintLen returns how many bytes encoding.AppendUvarint uses for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func insertBytes(s [][]byte, i int, v []byte) [][]byte {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

func insertU32(s []uint32, i int, v uint32) []uint32 {
	s = append(s, 0)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}
