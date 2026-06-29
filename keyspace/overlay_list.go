package keyspace

import (
	"sort"
)

// This file is the keyspace-layer groundwork for the in-memory list write fast
// path (spec 2064 note 244). liveColl in overlay.go handles the map-shaped
// collections (hash, set): its rows are keyed by opaque subkey and ordered
// lexically. A list is ordered by position, not by element bytes, so it needs a
// different resident shape, which liveList provides here.
//
// The premise is the same one liveColl attacks: a coll-form list push descends
// the shard tree and the element sub-tree per op, where Redis does an O(1) tail
// append, and under a deep pipeline that descent runs synchronously on the shard
// owner so every push pays a connection-to-owner round trip (note 241). A
// resident liveList absorbs pushes at map speed and folds a run of them into the
// sub-tree in one pass, so the per-push descent amortizes toward zero on a hot
// key, and the new length the push must reply is read from the window in memory
// rather than from behind the fold.
//
// The one structural difference from liveColl is that a list does not
// materialize every element. A coll list (list_tree.go) stores one row per
// element keyed by position under a head/tail window in the metadata row, and a
// push only ever extends an end, so the resident copy holds the window bounds
// authoritatively plus a pending map of just the positions written since the last
// fold. Positions not in pending are read from the sub-tree through an injected
// getter. That keeps materialization O(1) (read the window, walk nothing) so a
// huge push-target never gets pulled into memory just to append at its end.
//
// liveList is deliberately free of the list position byte encoding (listPosRow
// lives in the command layer, since the btree sees only opaque keys). The encoder
// and the per-element byte sizer are injected at the call site, the same way
// liveColl stores the command layer's already-encoded element bytes verbatim.
//
// Nothing here is wired into a command path yet. It is exercised by unit tests
// and a microbenchmark that validate the absorb-vs-descend premise and the
// positional read merge before the next slice gates it behind a config directive
// and routes coll-form LPUSH/RPUSH through it.

// liveList is the resident in-memory copy of one coll-form list. While a key is
// resident, head/tail/bytes are authoritative and the element sub-tree is its
// stale, asynchronously-folded backing. head is the lowest live position, tail is
// one past the highest, so count is tail-head; a right push writes at tail and
// advances it, a left push retreats head and writes there. pending holds the rows
// written since the last fold (position to element); dels holds positions removed
// since the last fold that were already in the sub-tree, so a fold deletes only
// what the tree actually holds. version advances on every mutation as the fold
// generation guard, the same shape as liveColl and the blob write-behind.
type liveList struct {
	head, tail int64
	bytes      uint64

	pending map[int64][]byte
	dels    map[int64]struct{}

	enc    uint8
	ttlMs  int64
	hasTTL bool

	// bodyRef is the element sub-tree root this resident copy folds back into,
	// captured from the key's metadata row when the copy is materialized and stable
	// while the key stays resident, so a fold can reopen the sub-tree without
	// re-reading the metadata row. It mirrors liveColl.bodyRef.
	bodyRef uint32

	version uint64
}

// newLiveList returns a resident list seeded from a key's window. Unlike
// materializeLiveColl this reads no element rows: the window (head, tail, bytes)
// is the whole authoritative state a push needs, and individual elements are read
// from the sub-tree on demand. A fresh key created directly in the overlay passes
// head==tail==0 and bytes==0.
func newLiveList(head, tail int64, bytes uint64, enc uint8, ttlMs int64, hasTTL bool, bodyRef uint32) *liveList {
	return &liveList{
		head:    head,
		tail:    tail,
		bytes:   bytes,
		pending: make(map[int64][]byte),
		dels:    make(map[int64]struct{}),
		enc:     enc,
		ttlMs:   ttlMs,
		hasTTL:  hasTTL,
		bodyRef: bodyRef,
	}
}

// count is the live element count, the value LLEN reports and the length a push
// replies. It is tail-head, always O(1) and always available without a fold,
// which is what lets a push answer synchronously off the resident copy.
func (ll *liveList) count() int64 { return ll.tail - ll.head }

// byteTotal is the listpack byte total, the input to the reported-encoding rule.
func (ll *liveList) byteTotal() uint64 { return ll.bytes }

// dirty reports whether the resident copy has unfolded mutations. A clean copy
// can be evicted without a fold.
func (ll *liveList) dirty() bool { return len(ll.pending) != 0 || len(ll.dels) != 0 }

// pushRight appends vals onto the tail end, each at a new highest position, and
// returns the new length. sizeOf gives each element's listpack byte cost so the
// byte total stays accurate for the reported-encoding rule. It mirrors the tail
// branch of listTreePush.
func (ll *liveList) pushRight(vals [][]byte, sizeOf func([]byte) uint64) int64 {
	for _, v := range vals {
		ll.pending[ll.tail] = append([]byte(nil), v...)
		delete(ll.dels, ll.tail)
		ll.bytes += sizeOf(v)
		ll.tail++
		ll.version++
	}
	return ll.count()
}

// pushLeft prepends vals onto the head end, each at a new lowest position, and
// returns the new length. As with LPUSH the pushed run ends up reversed because
// each value lands one position below the previous. It mirrors the head branch of
// listTreePush.
func (ll *liveList) pushLeft(vals [][]byte, sizeOf func([]byte) uint64) int64 {
	for _, v := range vals {
		ll.head--
		ll.pending[ll.head] = append([]byte(nil), v...)
		delete(ll.dels, ll.head)
		ll.bytes += sizeOf(v)
		ll.version++
	}
	return ll.count()
}

// treeGet reads the element at an absolute position out of the backing sub-tree.
// It is the injection point for CollReader.Get; positions found in pending never
// reach it. found is false when the row is absent (which should not happen for a
// position inside the live window unless it is still pending).
type treeGet func(pos int64) (val []byte, found bool, err error)

// at returns the element at absolute position pos, preferring the pending copy and
// falling back to the sub-tree. It is the positional read primitive LINDEX and
// LRANGE build on.
func (ll *liveList) at(pos int64, get treeGet) ([]byte, bool, error) {
	if v, ok := ll.pending[pos]; ok {
		return v, true, nil
	}
	return get(pos)
}

// index returns the element at logical index i, where a negative i counts from
// the tail, applying the same mapping as listTreeIndex. found is false when i is
// out of range.
func (ll *liveList) index(i int64, get treeGet) ([]byte, bool, error) {
	n := ll.count()
	pos := i
	if pos < 0 {
		pos += n
	}
	if pos < 0 || pos >= n {
		return nil, false, nil
	}
	return ll.at(ll.head+pos, get)
}

// rangeElems returns the elements in the inclusive logical range [start, stop],
// applying the Redis negative-index and clamp rules, by walking positions and
// merging pending over the sub-tree. It mirrors listTreeRange's bounds handling.
func (ll *liveList) rangeElems(start, stop int64, get treeGet) ([][]byte, error) {
	n := ll.count()
	lo, hi := listRangeClamp(start, stop, n)
	if lo > hi || n == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		v, ok, err := ll.at(ll.head+i, get)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, append([]byte(nil), v...))
	}
	return out, nil
}

// popLeft removes up to n elements from the head end and returns them head first.
// A popped position that is still pending is dropped from pending (a push then pop
// on a hot key never reaches the tree); a popped position already folded into the
// tree is recorded as a delete for the next fold. sizeOf keeps the byte total in
// step. head advances by the number popped.
func (ll *liveList) popLeft(n int, get treeGet, sizeOf func([]byte) uint64) ([][]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if ll.head >= ll.tail {
			break
		}
		pos := ll.head
		v, ok, err := ll.at(pos, get)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		out = append(out, append([]byte(nil), v...))
		ll.removeAt(pos, sizeOf(v))
		ll.head++
		ll.version++
	}
	return out, nil
}

// popRight removes up to n elements from the tail end and returns them tail first.
// It is the mirror of popLeft; tail retreats by the number popped.
func (ll *liveList) popRight(n int, get treeGet, sizeOf func([]byte) uint64) ([][]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if ll.tail <= ll.head {
			break
		}
		pos := ll.tail - 1
		v, ok, err := ll.at(pos, get)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		out = append(out, append([]byte(nil), v...))
		ll.removeAt(pos, sizeOf(v))
		ll.tail--
		ll.version++
	}
	return out, nil
}

// removeAt drops position pos from the resident copy, deducting its byte cost. A
// pending position is simply forgotten; a folded position is marked for deletion
// at the next fold. The caller moves head or tail.
func (ll *liveList) removeAt(pos int64, size uint64) {
	if _, ok := ll.pending[pos]; ok {
		delete(ll.pending, pos)
	} else {
		ll.dels[pos] = struct{}{}
	}
	if size > ll.bytes {
		ll.bytes = 0
	} else {
		ll.bytes -= size
	}
}

// fold writes the accumulated mutations into the element sub-tree through w and
// updates the window, then clears the dirty sets. Only the delta since the last
// fold is written: marked deletes are removed, pending positions are upserted.
// posRow encodes an absolute position as the sub-tree row key (the command layer
// passes listPosRow), keeping the position byte format out of the keyspace layer.
// A run of N absorbed pushes to one key collapses into N upserts here against one
// open writer instead of N separate descents from N connection goroutines. The
// caller holds the shard write lock and writes the metadata row back after fold
// returns. It mirrors liveColl.fold.
func (ll *liveList) fold(w *CollWriter, posRow func(int64) []byte) error {
	for pos := range ll.dels {
		if _, err := w.Delete(posRow(pos)); err != nil {
			return err
		}
	}
	// Upsert pending positions in ascending order so the sub-tree writes run in row
	// order, which is the friendliest insertion pattern for the B-tree.
	if len(ll.pending) != 0 {
		poss := make([]int64, 0, len(ll.pending))
		for pos := range ll.pending {
			poss = append(poss, pos)
		}
		sort.Slice(poss, func(a, b int) bool { return poss[a] < poss[b] })
		for _, pos := range poss {
			if _, err := w.Put(posRow(pos), ll.pending[pos]); err != nil {
				return err
			}
		}
	}
	w.SetHead(ll.head)
	w.SetTail(ll.tail)
	w.SetBytes(ll.bytes)
	w.SetCount(uint64(ll.count()))
	ll.pending = make(map[int64][]byte)
	ll.dels = make(map[int64]struct{})
	return nil
}

// listRangeClamp maps a Redis LRANGE start/stop pair onto an inclusive [lo, hi]
// index window over a list of length n, applying the negative-from-tail and clamp
// rules. It is the keyspace-local twin of the command layer's listRangeBounds, so
// the resident copy clamps a range exactly as the sub-tree path does. lo > hi
// signals an empty range.
func listRangeClamp(start, stop, n int64) (lo, hi int64) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	return start, stop
}
