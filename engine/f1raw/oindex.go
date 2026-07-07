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
	// onodeInlineCap is how many leading composite-key bytes a node caches inline so the
	// skiplist descent can order without reading the arena. A hot set/hash/zset member key
	// is a length-prefixed collection key plus a short member, well under this, so the whole
	// key fits inline and the descent's byte compares never chase the record offset into the
	// arena, which the saturating SPOP profile named as 42.5% of CPU (all cache-miss-bound in
	// keyAt/klen). Keys longer than the cap keep the offset-and-arena compare path, so the
	// cache is a pure hint and ordering is unchanged. 32 bytes covers the common member key
	// whole while keeping the node under two cache lines.
	onodeInlineCap = 32
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
	off    uint64
	klen   uint32               // full composite-key length, so a fully-inlined key is known without an arena read
	kind   byte                 // record kind, captured at insert so the node re-resolves its address through the tier-aware index without reading the (possibly migrated) record
	inline [onodeInlineCap]byte // first min(klen, cap) key bytes, cached for arena-free ordering
	next   []*onode
	width  []int
}

// cmpKey orders the node's composite key against key the way bytes.Compare does, but reads
// the inline key cache instead of the arena whenever the whole key fits (klen <= cap). That
// keeps the hot skiplist descent from chasing off into the 1.9M-element arena for every byte
// compare, which is the cache-miss cost the SPOP profile is bound on. A key longer than the
// cap falls back to the arena bytes, so the cache never changes the ordering, only where the
// bytes are read from. The record's key is immutable (grow-only arena), so a republish that
// only moves off leaves klen and inline correct without a refresh.
func (n *onode) cmpKey(s *Store, key []byte) int {
	if int(n.klen) <= onodeInlineCap {
		return bytes.Compare(n.inline[:n.klen], key)
	}
	return bytes.Compare(s.keyAt(n.off), key)
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

// nodeKey returns the composite key bytes of ordered-index node n. When the whole key fits
// the node's inline cache (klen <= onodeInlineCap), it returns that cache directly: the
// inline copy is a verbatim prefix of the key taken at insert, so for a fully-inlined key it
// is byte-identical to keyAt(n.off), only read without chasing the record offset into the
// arena. That decoupling is what lets the ordered index yield an element's key after the
// element's record has migrated cold and its resident bytes were reclaimed, since for an
// inline-fitting key the retrieval path never touches the record, the same independence
// cmpKey already relies on for ordering. A key longer than the cache falls back to
// keyAt(n.off), the resident read; the record-tiering follow-up widens that tail to the cold
// frame. The common collection member key (a length-prefixed collection key plus a short
// member) fits inline, so the SPOP/HSCAN/SRANDMEMBER retrieval paths take the arena-free
// branch, matching the descent's own cache hit.
func (s *Store) nodeKey(n *onode) []byte {
	if int(n.klen) <= onodeInlineCap {
		return n.inline[:n.klen]
	}
	return s.keyAt(n.off)
}

// nodeAddr returns the current logical address of node n's record, tier-tagged so a value read
// resolves it through readValueByAddr regardless of which tier it sits in, along with whether the
// element is still live. This is D22 Option B (spec 2064/f1_rewrite_ltm/21): the node caches an
// offset that is correct only while the record is resident, so once the cold record region is
// engaged the node re-resolves its address through the tier-aware primary index on each access
// rather than trusting the cached off, which the migrator would leave dangling at a reclaimed
// segment after a cold flip. When no cold record region exists (the in-memory-fit regime), no
// record ever migrates, so the cached off is authoritative and this returns it with no probe,
// keeping enumeration on exactly the fast path it had before tiering. When the region is engaged,
// it probes the primary index by the node's own key and kind: a hit gives the live tier-tagged
// address, a miss means the element was deleted (the delete clears the primary slot synchronously,
// while only the ordered-index splice is deferred), so found is an authoritative liveness signal.
func (s *Store) nodeAddr(n *onode) (off uint64, live bool) {
	if s.recs == nil {
		return n.off, true
	}
	key := s.nodeKey(n)
	cur, _, _, _, found := s.find(key, hash(key), n.kind)
	return cur, found
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
		for x.next[i] != nil && x.next[i].cmpKey(oi.store, key) < 0 {
			rank[i] += x.width[i]
			x = x.next[i]
		}
		update[i] = x
	}
	if nx := x.next[0]; nx != nil && nx.cmpKey(oi.store, key) == 0 {
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
	n := &onode{off: off, klen: uint32(len(key)), kind: oi.store.arena[off+offKind], next: make([]*onode, lvl), width: make([]int, lvl)}
	copy(n.inline[:], key) // copies min(cap, len(key)); the tail beyond cap is unused (klen>cap falls back to arena)
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

// refresh points the node whose key equals keyAt(off) at off. It is the fix for a value
// that outgrew its record and was republished at a new offset: the ordered node still
// holds the old offset, and a value-carrying scan (CollScanKV) reads the value straight
// from the node's offset, so the node must track the live record or the scan would return
// a stale value. It is a no-op when no node holds the key (the record is not indexed, such
// as a header row), so the collection write path can call it after any republish without
// first checking membership. The traversal orders by keyAt of the nodes' offsets, and the
// republished record's key bytes are identical to the old record's (immutable key,
// grow-only arena), so the search lands on the right node. Serialize it with the
// collection's other writers, the same stripe lock CollInsert relies on.
func (oi *oindex) refresh(off uint64) {
	key := oi.store.keyAt(off)
	oi.mu.Lock()
	defer oi.mu.Unlock()

	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].cmpKey(oi.store, key) < 0 {
			x = x.next[i]
		}
	}
	if nx := x.next[0]; nx != nil && nx.cmpKey(oi.store, key) == 0 {
		nx.off = off
	}
}

// remove unlinks the node whose key equals the given key bytes and reports whether it
// was present. The record's arena bytes are left intact (grow-only arena); only the
// index node is unlinked.
func (oi *oindex) remove(key []byte) bool {
	oi.mu.Lock()
	defer oi.mu.Unlock()
	return oi.removeLocked(key)
}

// removeMany unlinks each key under a single write-lock acquisition, the batched form
// remove takes for a coalesced delete run (HDEL/SREM/ZREM folded across a pipeline). One
// lock cycle for the whole run instead of one per element is what keeps a burst of
// same-key deletes from ping-ponging the global index lock across every connection, the
// residual serialization that folding the stripe lock and count header alone leaves
// behind. Each key is still its own O(log n) descent; keys need not be sorted, and any
// key already absent is skipped. The keys must stay valid for the call, so the caller
// copies them out of the shared key scratch before handing them in.
func (oi *oindex) removeMany(keys [][]byte) {
	if len(keys) == 0 {
		return
	}
	oi.mu.Lock()
	defer oi.mu.Unlock()
	for _, key := range keys {
		oi.removeLocked(key)
	}
}

// removeManyLive is removeMany for the deferred folder: it splices each key out under a single
// write-lock acquisition, but first re-checks liveness against the authoritative hash index and
// skips any key whose record is live again. That re-check is what makes deferring the splice safe
// against a re-add. A delete queues the key here with the node still present and the hash record
// already gone; if the same key is added back before the folder runs, the re-add republishes the
// hash record and refreshes the existing node to it (oindex.insert's "same key republished"
// branch), so removing the node now would drop a live element. Reading ExistsKind under the same
// oi.mu that guards the splice orders the check against the re-add's own insert (which also takes
// oi.mu): the folder either sees the live record and keeps the node, or sees no record and splices
// a node that is and stays dead. buf holds the composite keys packed end to end with ends[k] the
// cumulative length through the k-th key, and kind is the namespace they share.
func (oi *oindex) removeManyLive(buf []byte, ends []int, kind byte) {
	if len(ends) == 0 {
		return
	}
	oi.mu.Lock()
	defer oi.mu.Unlock()
	prev := 0
	for _, e := range ends {
		key := buf[prev:e]
		prev = e
		if oi.store.ExistsKind(key, kind) {
			// Re-added since the tombstone was queued: the node points at the live record now,
			// so leave it. The stale tombstone is simply dropped.
			continue
		}
		oi.removeLocked(key)
	}
}

// removeLocked unlinks the node whose key equals the given bytes and reports whether it
// was present, assuming the write lock is already held. The record's arena bytes are left
// intact (grow-only arena); only the index node is unlinked.
func (oi *oindex) removeLocked(key []byte) bool {
	var update [oindexMaxLevel]*onode
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].cmpKey(oi.store, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	target := x.next[0]
	if target == nil || target.cmpKey(oi.store, key) != 0 {
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

// selectAndRemoveInPrefix selects the element at 0-based localIndex within the collection
// bounded by prefix, in key order, and unlinks it in the same descent, returning its
// composite key (a subslice of the immutable arena, valid for the store's life) and
// whether it existed. It is the fused form of selectInPrefix followed by remove, the pair
// SPOP-without-count runs on every op: one positional descent that collects the
// predecessor pointers by position lands on the victim and bridges over it, instead of one
// descent to select the key and a second, separate descent to find and unlink it. The
// prefix's base rank still costs one descent, so this is two descents and one write lock
// where the split path was three descents and a read-then-write lock pair. A localIndex
// past the collection's cardinality lands on a sibling collection's node (or nil) and is
// reported absent with nothing removed, exactly as selectInPrefix guards it.
func (oi *oindex) selectAndRemoveInPrefix(prefix []byte, localIndex int) ([]byte, bool) {
	// A positional descent reads order-statistic widths, which count a dead node the folder
	// has not yet spliced as if it were live. Reconcile the index before selecting by position
	// so base rank and target land on live members. Gated on tombPend, so a workload not
	// interleaved with deletes pays one atomic load here and never drains.
	oi.store.SyncPendingRemovals()
	oi.mu.Lock()
	defer oi.mu.Unlock()

	base := oi.rankLocked(prefix)
	target := base + localIndex + 1 // 1-based position of the victim; head sits at position 0
	if localIndex < 0 || base+localIndex >= oi.count {
		return nil, false
	}
	// Descend collecting the predecessor at each level: the last node whose position is
	// strictly before target, so update[i].next[i] is the victim at level 0.
	var update [oindexMaxLevel]*onode
	pos := 0
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && pos+x.width[i] < target {
			pos += x.width[i]
			x = x.next[i]
		}
		update[i] = x
	}
	victim := update[0].next[0]
	if victim == nil {
		return nil, false
	}
	k := oi.store.nodeKey(victim)
	if !bytes.HasPrefix(k, prefix) {
		// localIndex ran past this collection into a sibling: report absent, remove nothing.
		return nil, false
	}
	for i := 0; i < oi.level; i++ {
		if update[i].next[i] == victim {
			update[i].width[i] += victim.width[i] - 1
			update[i].next[i] = victim.next[i]
		} else {
			update[i].width[i]--
		}
	}
	for oi.level > 1 && oi.head.next[oi.level-1] == nil {
		oi.level--
	}
	oi.count--
	return k, true
}

// scanBatch collects up to limit record offsets whose key has the given prefix and is
// strictly greater than after (nil after means from the start of the prefix), in key
// order, appending them to dstOffs. When dstKeys is non-nil it also appends each node's
// composite key, in lockstep with the offsets, so dstKeys[i] is the key of dstOffs[i]; a
// caller that wants only the offsets passes nil and pays for no key collection. The keys
// come from nodeKey, the node's own inline cache for an inline-fitting key, so an
// enumeration returns an element's key without reading the record, which is what keeps the
// key correct after the element's record has migrated cold and its resident offset was
// reclaimed. It returns the grown offset and key slices and the key bytes of the last
// node appended, so the caller can resume the next batch with it. Holding the read lock
// only for the batch keeps a large enumeration from blocking writers across the whole
// collection.
func (oi *oindex) scanBatch(prefix, after []byte, limit int, dstOffs []uint64, dstKeys [][]byte) ([]uint64, [][]byte, []byte) {
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
		for x.next[i] != nil && x.next[i].cmpKey(oi.store, seek) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]

	// The seek positioned x at the first node whose key is >= max(prefix, after), so every
	// node before x is already excluded. Element keys are unique, so at most one node can
	// equal `after`, and if it exists it is exactly this first node. Skip it once here
	// rather than comparing every element against `after` inside the walk, which would run a
	// full bytes.Compare per element for a boundary that can match only the head of the batch.
	if after != nil && x != nil {
		if x.cmpKey(oi.store, after) <= 0 {
			x = x.next[0]
		}
	}

	// With deferred removal on, the list can hold nodes for elements the hash index has
	// already dropped but the folder has not yet spliced. Skip such a dead node so enumeration
	// never surfaces a since-deleted element. The check is gated on tombPend so a store with no
	// pending tombstones (the steady state, and always when deferred removal is off) pays one
	// atomic load for the whole batch and never the per-node liveAt probe.
	filter := oi.store.tombPend.Load() > 0

	// When the cold record region is engaged, a node's cached offset can dangle after a migration
	// reclaimed the resident segment it pointed at, so re-resolve each emitted address through the
	// tier-aware index (D22 Option B, spec 2064/f1_rewrite_ltm/21): the re-resolved address is
	// tier-tagged, so a later value read follows the record across the tier boundary. The
	// re-resolution's hit/miss is an authoritative liveness signal (the delete clears the primary
	// slot synchronously and only defers the ordered-index splice), so on this path it subsumes the
	// tombPend liveAt check. In the in-memory-fit regime no record ever migrates, so recs is nil,
	// the cached offset is authoritative, and the walk stays on the existing fast path.
	resolve := oi.store.recs != nil

	var last []byte
	for x != nil && len(dstOffs) < limit {
		k := oi.store.nodeKey(x)
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		off := x.off
		if resolve {
			var live bool
			if off, live = oi.store.nodeAddr(x); !live {
				x = x.next[0]
				continue
			}
		} else if filter && !oi.store.liveAt(x.off) {
			x = x.next[0]
			continue
		}
		dstOffs = append(dstOffs, off)
		if dstKeys != nil {
			dstKeys = append(dstKeys, k)
		}
		last = k
		x = x.next[0]
	}
	return dstOffs, dstKeys, last
}

// rankLocked returns the number of live nodes whose key sorts strictly before key,
// which is the 0-based position where key would fall. It descends the express lanes
// accumulating the widths it steps over, so it is O(log n), not an O(n) count. The
// caller must hold at least the read lock.
func (oi *oindex) rankLocked(key []byte) int {
	pos := 0
	x := oi.head
	for i := oi.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].cmpKey(oi.store, key) < 0 {
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
	// rankLocked sums order-statistic widths without filtering a dead node the folder has
	// not yet spliced, so reconcile the index before ranking by width. Gated on tombPend,
	// so a rank read not interleaved with deletes pays one atomic load and never drains.
	oi.store.SyncPendingRemovals()
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
	// Base rank and positional select both read order-statistic widths, which count a dead
	// node the folder has not yet spliced as if it were live. Reconcile the index first.
	// Gated on tombPend, so a select not interleaved with deletes pays one atomic load.
	oi.store.SyncPendingRemovals()
	oi.mu.RLock()
	defer oi.mu.RUnlock()
	base := oi.rankLocked(prefix)
	node := oi.selectAtLocked(base + localIndex)
	if node == nil {
		return nil, false
	}
	k := oi.store.nodeKey(node)
	if !bytes.HasPrefix(k, prefix) {
		return nil, false
	}
	return k, true
}
