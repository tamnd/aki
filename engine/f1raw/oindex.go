package f1raw

import (
	"bytes"
	"sync"
)

// The ordered element index is the in-memory form of the spec's per-collection
// sorted run (2064/f1_rewrite_ltm/03, the ordered-structure decision): a structure
// that lists a collection's element sub-keys in byte order so a bounded cursor can
// seek to a window and walk it, instead of materializing the whole collection. The
// lock-free hash index answers point ops (GetKind), but it is unordered, so it cannot
// enumerate one hash's fields in order without scanning the entire keyspace, which is
// exactly the whole-collection materialize the larger-than-memory design forbids.
//
// The structure is a skip list keyed by the composite element key bytes. A skip list
// is the right first-principles choice here: it gives O(log n) insert, delete, and
// predecessor/successor seek, and, unlike a hash index, it iterates in key order for
// free, which is all a bounded cursor needs. Because element keys are
// length-prefixed (uvarint(len(collKey)) | collKey | member), every element of one
// collection is a contiguous run under the prefix uvarint(len(collKey)) | collKey, so
// enumerating a collection is a seek to that prefix and a forward walk until the
// prefix stops matching.
//
// A node stores only the arena offset of its record, never a copy of the key: the key
// bytes are read from the immutable record header at that offset, so the index adds no
// key duplication. The offset is used only to read the key bytes for ordering and for
// the caller to re-resolve the value through the authoritative hash index; it is never
// used to read the value directly, because a value that outgrew its record was
// republished at a new offset while this node still points at the old one. The old
// record's key bytes are identical (grow-only arena, immutable key), so ordering stays
// correct, and the value always comes from a fresh GetKind by the caller. This is the
// spec's "the column is authoritative, the sibling index is a derived hint" contract.
//
// Concurrency: the index has its own RWMutex, distinct from the hash index's lock-free
// path, so it never touches the string hot path or the point HGET/HSET value path. A
// writer (insert or delete on a field create or delete) takes the write lock; a cursor
// scan takes the read lock for the span of one bounded batch and releases it between
// batches, so a large HGETALL does not hold the lock across the whole collection. The
// server already serializes writes to one collection key under its stripe lock, so the
// index writer lock only ever coordinates writes to different collections.

const (
	oindexMaxLevel = 20   // supports ~2^20 elements at p=1/4 before the top level saturates
	oindexP        = 0.25 // fraction of nodes that rise to the next level
)

// onode is one skip-list node. off is the arena offset of the indexed record; next
// holds the forward pointers, one per level the node participates in, so a node drawn
// to height h allocates exactly h pointers rather than a fixed maximum. width[i] is the
// order-statistic span of next[i]: the number of level-0 steps that pointer covers,
// i.e. the position distance from this node to next[i] (treating the end of the list as
// position count+1). Level-0 widths are always 1, and summing the widths traversed from
// the head to a node yields that node's position, which is what makes selection by rank
// and rank-of-key both O(log n) descents rather than an O(n) walk. This is the indexable
// skip list (Pugh's augmentation), the structure the random-selection commands
// (SPOP/SRANDMEMBER, section 10.1 of spec 2064/f1_rewrite_ltm/06) seek through.
type onode struct {
	off   uint64
	next  []*onode
	width []int
}

// oindex is the ordered element index. head is a sentinel whose forward pointers are
// the entry points at each level; it indexes no real record. rng is a small
// deterministic PRNG for level draws, seeded per store so the structure is balanced
// without depending on wall-clock or a global source. count is the number of live nodes,
// maintained so a newly-raised level's head pointer gets the correct spanning width.
type oindex struct {
	mu    sync.RWMutex
	store *Store
	head  *onode
	level int // highest level currently in use, 1..oindexMaxLevel
	count int // live node count, for order-statistic width maintenance
	rng   uint64
}

func newOIndex(s *Store) *oindex {
	head := &onode{next: make([]*onode, oindexMaxLevel), width: make([]int, oindexMaxLevel)}
	// The head's pointer at every level starts spanning the whole (empty) list to the
	// end, which under the "end is at position count+1" convention is distance 1 when
	// count is 0. The insert split arithmetic relies on this so the first insert at any
	// level lands the new node's widths exactly.
	for i := range head.width {
		head.width[i] = 1
	}
	return &oindex{
		store: s,
		head:  head,
		level: 1,
		// A non-zero seed derived from the arena base keeps distinct stores from
		// drawing identical level sequences; splitmix64 mixes it on each draw.
		rng: 0x9e3779b97f4a7c15,
	}
}

// randomLevel draws a node height with the classic geometric distribution: level 1
// always, and each further level with probability oindexP. It uses splitmix64 so the
// draw needs no locking beyond the write lock already held and no global rand source.
func (oi *oindex) randomLevel() int {
	lvl := 1
	for lvl < oindexMaxLevel {
		oi.rng += 0x9e3779b97f4a7c15
		z := oi.rng
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		// Take the low 2 bits: they are zero with probability 1/4, matching oindexP.
		if z&0x3 != 0 {
			break
		}
		lvl++
	}
	return lvl
}

// keyAt returns the immutable key bytes of the record at off. The bytes live in the
// grow-only arena and never change after publish, so the returned slice is valid for
// the store's life and safe to compare without copying.
func (s *Store) keyAt(off uint64) []byte {
	klen := s.klen(off)
	start := off + hdrSize
	return s.arena[start : start+klen]
}

// insert adds off's record to the ordered index. If a node with the same key already
// exists (a field overwrite that happened to republish, or a redundant call), the
// node's offset is refreshed rather than duplicated, so the index holds exactly one
// node per live key.
func (oi *oindex) insert(off uint64) {
	key := oi.store.keyAt(off)
	oi.mu.Lock()
	defer oi.mu.Unlock()

	var update [oindexMaxLevel]*onode
	var rank [oindexMaxLevel]int // rank[i] = position of update[i]: nodes passed at/above level i
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		if i == oi.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.next[i] != nil && bytes.Compare(oi.store.keyAt(x.next[i].off), key) < 0 {
			rank[i] += x.width[i]
			x = x.next[i]
		}
		update[i] = x
	}
	if nx := x.next[0]; nx != nil && bytes.Equal(oi.store.keyAt(nx.off), key) {
		nx.off = off // same key republished: point at the current record
		return
	}
	lvl := oi.randomLevel()
	if lvl > oi.level {
		for i := oi.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = oi.head
			// The head's pointer at a freshly-raised level spans every existing node to
			// the end: distance count+1 under the "end at position count+1" convention.
			oi.head.width[i] = oi.count + 1
		}
		oi.level = lvl
	}
	n := &onode{off: off, next: make([]*onode, lvl), width: make([]int, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
		// Split update[i]'s span at the insertion point. rank[0]-rank[i] is the number of
		// level-0 nodes between update[i] and the immediate predecessor update[0].
		n.width[i] = update[i].width[i] - (rank[0] - rank[i])
		update[i].width[i] = (rank[0] - rank[i]) + 1
	}
	// Pointers above the new node's height now span one extra node underneath.
	for i := lvl; i < oi.level; i++ {
		update[i].width[i]++
	}
	oi.count++
}

// remove unlinks the node whose key equals the given key bytes and reports whether it
// was present. The record's arena bytes are left intact (grow-only arena); only the
// index node is unlinked.
func (oi *oindex) remove(key []byte) bool {
	oi.mu.Lock()
	defer oi.mu.Unlock()

	var update [oindexMaxLevel]*onode
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(oi.store.keyAt(x.next[i].off), key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	target := x.next[0]
	if target == nil || !bytes.Equal(oi.store.keyAt(target.off), key) {
		return false
	}
	for i := 0; i < oi.level; i++ {
		if update[i].next[i] == target {
			// Bridge update[i] past the target, merging the two spans and dropping the
			// removed node from the count.
			update[i].width[i] += target.width[i] - 1
			update[i].next[i] = target.next[i]
		} else {
			// This pointer spanned over the target: one fewer node underneath it now.
			update[i].width[i]--
		}
	}
	for oi.level > 1 && oi.head.next[oi.level-1] == nil {
		oi.level--
	}
	oi.count--
	return true
}

// scanBatch collects up to limit record offsets whose key has the given prefix and is
// strictly greater than after (nil after means from the start of the prefix), in key
// order, appending them to dst. It returns the grown dst and the key bytes of the last
// offset appended (a subslice of the arena, valid for the store's life) so the caller
// can resume the next batch with it. Holding the read lock only for the batch keeps a
// large enumeration from blocking writers across the whole collection.
func (oi *oindex) scanBatch(prefix, after []byte, limit int, dst []uint64) ([]uint64, []byte) {
	oi.mu.RLock()
	defer oi.mu.RUnlock()

	// Seek to the first node at or after the greater of prefix and after. Seeking to
	// after (when it is set and sorts past the prefix start) skips the already-emitted
	// span in O(log n) instead of walking it.
	seek := prefix
	if after != nil && bytes.Compare(after, seek) > 0 {
		seek = after
	}
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(oi.store.keyAt(x.next[i].off), seek) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]

	var last []byte
	for x != nil && len(dst) < limit {
		k := oi.store.keyAt(x.off)
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		if after != nil && bytes.Compare(k, after) <= 0 {
			x = x.next[0]
			continue
		}
		dst = append(dst, x.off)
		last = k
		x = x.next[0]
	}
	return dst, last
}

// rankLocked returns the number of live nodes whose key sorts strictly before key,
// which is the 0-based position where key would fall. It descends the express lanes
// accumulating the widths it steps over, so it is O(log n), not an O(n) count. The
// caller must hold at least the read lock.
func (oi *oindex) rankLocked(key []byte) int {
	pos := 0
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(oi.store.keyAt(x.next[i].off), key) < 0 {
			pos += x.width[i]
			x = x.next[i]
		}
	}
	return pos
}

// rankInPrefix returns the 0-based position of key within the collection bounded by
// prefix, in key order, under a single read lock. The position is prefix-local: it
// subtracts the prefix's base rank (the count of nodes ordered before the collection's
// run) from key's global rank, so the result is what the ZRANK family returns directly.
// Both descents are O(log n). It does not verify key is a live element; the caller
// confirms membership through the element index first (ZRANK replies nil for an absent
// member before it ranks anything), so an absent key here reports where it would fall.
func (oi *oindex) rankInPrefix(prefix, key []byte) int {
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	return oi.rankLocked(key) - oi.rankLocked(prefix)
}

// selectAtLocked returns the node at 0-based position idx in key order, or nil when idx
// is out of range. It walks down from the top level following each pointer whose span
// does not overshoot the target position, so it reaches the idx-th node in O(log n)
// descents instead of an O(idx) forward walk. The caller must hold at least the read
// lock.
func (oi *oindex) selectAtLocked(idx int) *onode {
	if idx < 0 || idx >= oi.count {
		return nil
	}
	target := idx + 1 // widths are 1-based position distances; head sits at position 0
	pos := 0
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && pos+x.width[i] <= target {
			pos += x.width[i]
			x = x.next[i]
		}
	}
	return x
}

// selectInPrefix returns the composite key of the element at 0-based localIndex within
// the collection bounded by prefix, in key order, and whether it exists. It finds the
// prefix's base rank (the number of nodes ordered before the collection's run) and
// selects the node at base+localIndex, verifying the result still carries the prefix so
// a localIndex past the collection's cardinality reports absent rather than leaking a
// sibling collection's member. Both steps are O(log n), so a uniform random member is
// one descent, not a scan.
func (oi *oindex) selectInPrefix(prefix []byte, localIndex int) ([]byte, bool) {
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	base := oi.rankLocked(prefix)
	node := oi.selectAtLocked(base + localIndex)
	if node == nil {
		return nil, false
	}
	k := oi.store.keyAt(node.off)
	if !bytes.HasPrefix(k, prefix) {
		return nil, false
	}
	return k, true
}
