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

// Open returns a tree rooted at the given page. The page is not read until the
// first operation.
func Open(pgr *pager.Pager, root uint32) *Tree {
	return &Tree{pgr: pgr, root: root}
}

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
	// rightSibling links leaves left to right for range scans.
	rightSibling uint32
}

// Get returns the value stored under key and whether it was found.
// Intermediate node data is decoded into a pooled arena so that only one
// allocation (the returned value copy) escapes per call.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	ar := nodeArenaPool.Get().(*nodeArena)
	defer func() { ar.reset(); nodeArenaPool.Put(ar) }()
	pgno := t.root
	for {
		n, err := t.readNodeAr(pgno, ar)
		if err != nil {
			return nil, false, err
		}
		if n.leaf {
			i, ok := n.find(key)
			if !ok {
				return nil, false, nil
			}
			// Clone the matched value: it lives in the arena which is reset on
			// return, so the caller needs an independently owned copy.
			return bytes.Clone(n.vals[i]), true, nil
		}
		pgno = n.children[n.childIndex(key)]
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
	oldVal, sp, err := t.insertAr(t.root, key, val, ar)
	if err != nil {
		return nil, err
	}
	if sp != nil {
		newRoot, err := t.writeNewNodeAr(&node{
			leaf:     false,
			keys:     [][]byte{sp.sepKey},
			children: []uint32{t.root, sp.right},
		}, ar)
		if err != nil {
			return nil, err
		}
		t.root = newRoot
	}
	assertInvariants(t)
	return oldVal, nil
}

func (t *Tree) put(key, val []byte) error {
	ar := nodeArenaPool.Get().(*nodeArena)
	defer func() { ar.reset(); nodeArenaPool.Put(ar) }()
	_, sp, err := t.insertAr(t.root, key, val, ar)
	if err != nil {
		return err
	}
	if sp == nil {
		return nil
	}
	// The root split: build a new interior root over the two halves.
	newRoot, err := t.writeNewNodeAr(&node{
		leaf:     false,
		keys:     [][]byte{sp.sepKey},
		children: []uint32{t.root, sp.right},
	}, ar)
	if err != nil {
		return err
	}
	t.root = newRoot
	return nil
}

// splitResult reports that inserting into a child page split it: sepKey is the
// separator to insert into the parent, and right is the new page holding the
// upper half.
type splitResult struct {
	sepKey []byte
	right  uint32
}

// insertAr traverses the tree from pgno and inserts key/val, using ar to avoid
// per-cell heap allocations. It returns the previous value stored under key
// (or nil if key was absent) so callers can inspect or free the old value in
// a single traversal without a separate Get.
func (t *Tree) insertAr(pgno uint32, key, val []byte, ar *nodeArena) (oldVal []byte, sp *splitResult, err error) {
	n, err := t.readNodeAr(pgno, ar)
	if err != nil {
		return nil, nil, err
	}

	if n.leaf {
		oldVal = n.upsertAr(key, val, ar)
		sp, err = t.writeOrSplitAr(pgno, n, ar)
		return
	}

	ci := n.childIndex(key)
	oldVal, sp, err = t.insertAr(n.children[ci], key, val, ar)
	if err != nil {
		return
	}
	if sp == nil {
		return
	}
	// Insert the new separator and child pointer at position ci.
	// sepKey is already in the arena so no extra copy needed here.
	n.keys = insertBytes(n.keys, ci, sp.sepKey)
	n.children = insertU32(n.children, ci+1, sp.right)
	sp, err = t.writeOrSplitAr(pgno, n, ar)
	return
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
	return &splitResult{sepKey: sep, right: rightNo}, nil
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
	n, err := t.readNodeAr(pgno, ar)
	if err != nil {
		return false, err
	}
	if n.leaf {
		i, ok := n.find(key)
		if !ok {
			return false, nil
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i], n.vals[i+1:]...)
		return true, t.writeNodeAr(pgno, n, ar)
	}
	return t.delAr(n.children[n.childIndex(key)], key, ar)
}

// find returns the index of key in a leaf and whether it is present.
func (n *node) find(key []byte) (int, bool) {
	for i, k := range n.keys {
		c := bytes.Compare(key, k)
		if c == 0 {
			return i, true
		}
		if c < 0 {
			return i, false
		}
	}
	return len(n.keys), false
}

// childIndex returns the child slot to descend into for key on an interior node.
func (n *node) childIndex(key []byte) int {
	for i, k := range n.keys {
		if bytes.Compare(key, k) < 0 {
			return i
		}
	}
	return len(n.keys)
}

// upsertAr inserts key/val into a leaf in sorted order (or replaces the value
// if key is present), copying key and val into ar. It returns the previous
// value for key, or nil if the key was not present. The returned slice is a
// heap-owned copy safe to use after ar is reset.
func (n *node) upsertAr(key, val []byte, ar *nodeArena) (oldVal []byte) {
	i, ok := n.find(key)
	v := ar.copy(val)
	if ok {
		oldVal = bytes.Clone(n.vals[i])
		n.vals[i] = v
		return
	}
	n.keys = insertBytes(n.keys, i, ar.copy(key))
	n.vals = insertBytes(n.vals, i, v)
	return nil
}

// splitAr divides a full node into a lower and upper half. Keys are already in ar.buf so no copy is needed;
// slices are stable until the arena resets at the end of the enclosing Put.
func (n *node) splitAr(ar *nodeArena) (left, right *node, sep []byte) {
	_ = ar // keys already live in ar.buf; just slice, no copy needed
	mid := len(n.keys) / 2
	if n.leaf {
		left = &node{leaf: true, keys: n.keys[:mid], vals: n.vals[:mid]}
		right = &node{leaf: true, keys: n.keys[mid:], vals: n.vals[mid:], rightSibling: n.rightSibling}
		return left, right, right.keys[0]
	}
	sep = n.keys[mid]
	left = &node{keys: n.keys[:mid], children: n.children[:mid+1]}
	right = &node{keys: n.keys[mid+1:], children: n.children[mid+1:]}
	return left, right, sep
}

// size returns the number of page bytes this node would occupy when serialized.
func (n *node) size() int {
	total := slotsStart + 2*len(n.keys)
	for i, k := range n.keys {
		if n.leaf {
			total += cellLen(uvarintLen(uint64(len(k))) + len(k) + uvarintLen(uint64(len(n.vals[i]))) + len(n.vals[i]))
		} else {
			total += cellLen(4 + uvarintLen(uint64(len(k))) + len(k))
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
	for i := range count {
		off := int(encoding.U16(b[slotsStart+2*i:]))
		n.children[i] = encoding.U32(b[off:])
		off += 4
		kl, m, err := encoding.Uvarint(b[off:])
		if err != nil {
			return nil, err
		}
		off += m
		n.keys[i] = ar.copy(b[off : off+int(kl)])
	}
	n.children[count] = rightmost
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
	for i := range count {
		off := int(encoding.U16(b[slotsStart+2*i:]))
		n.children[i] = encoding.U32(b[off:])
		off += 4
		kl, m, err := encoding.Uvarint(b[off:])
		if err != nil {
			return nil, err
		}
		off += m
		n.keys[i] = bytes.Clone(b[off : off+int(kl)])
	}
	n.children[count] = rightmost
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

	for i := range count {
		ar.tmp = ar.tmp[:0]
		if n.leaf {
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.keys[i])))
			ar.tmp = append(ar.tmp, n.keys[i]...)
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.vals[i])))
			ar.tmp = append(ar.tmp, n.vals[i]...)
		} else {
			ar.tmp = encoding.AppendU32(ar.tmp, n.children[i])
			ar.tmp = encoding.AppendUvarint(ar.tmp, uint64(len(n.keys[i])))
			ar.tmp = append(ar.tmp, n.keys[i]...)
		}
		end -= len(ar.tmp)
		copy(b[end:], ar.tmp)
		encoding.PutU16(b[slotsStart+2*i:], uint16(end))
	}

	h := format.PageHeader{
		CellCount: uint16(count),
		FreeStart: uint16(slotsStart + 2*count),
		FreeEnd:   uint16(end),
	}
	if n.leaf {
		h.Type = format.PageTypeBTreeLeaf
	} else {
		h.Type = format.PageTypeBTreeInt
	}
	if err := h.MarshalTo(b); err != nil {
		return err
	}

	if n.leaf {
		encoding.PutU32(b[16:], n.rightSibling)
	} else {
		encoding.PutU32(b[16:], n.children[count])
	}
	encoding.PutU16(b[20:], uint16(count))
	encoding.PutU16(b[22:], slotsStart)
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

	for i := range count {
		var cell []byte
		if n.leaf {
			cell = encoding.AppendUvarint(cell, uint64(len(n.keys[i])))
			cell = append(cell, n.keys[i]...)
			cell = encoding.AppendUvarint(cell, uint64(len(n.vals[i])))
			cell = append(cell, n.vals[i]...)
		} else {
			cell = encoding.AppendU32(cell, n.children[i])
			cell = encoding.AppendUvarint(cell, uint64(len(n.keys[i])))
			cell = append(cell, n.keys[i]...)
		}
		end -= len(cell)
		copy(b[end:], cell)
		encoding.PutU16(b[slotsStart+2*i:], uint16(end))
	}

	h := format.PageHeader{
		CellCount: uint16(count),
		FreeStart: uint16(slotsStart + 2*count),
		FreeEnd:   uint16(end),
	}
	if n.leaf {
		h.Type = format.PageTypeBTreeLeaf
	} else {
		h.Type = format.PageTypeBTreeInt
	}
	if err := h.MarshalTo(b); err != nil {
		return err
	}

	if n.leaf {
		encoding.PutU32(b[16:], n.rightSibling)
	} else {
		encoding.PutU32(b[16:], n.children[count])
	}
	// slot_count and slot_array_start, kept in sync with the common header.
	encoding.PutU16(b[20:], uint16(count))
	encoding.PutU16(b[22:], slotsStart)
	return nil
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
