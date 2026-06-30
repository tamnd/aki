package keyspace

import (
	"bytes"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/encoding"
)

// This file implements btree-backed collection storage (spec 2064 note 172). A
// large hash/set/zset/list is stored as a small metadata record at the user key,
// whose header carries FlagCollTree and a BodyRef pointing at a per-key element
// sub-tree. Each element is one row in that sub-tree, so a single-element op is
// O(log N) instead of decoding and rewriting a whole blob. The element rows are
// opaque to this layer: the command layer encodes a hash field, a set member, a
// zset score-or-member index, or a list position into the subkey bytes, and this
// layer just stores ordered subkey -> value rows plus the metadata counters.
//
// Small collections keep the inline-blob form (db.Set with a listpack/intset
// encoding). The command layer promotes a blob to this form when it grows past
// the listpack threshold, exactly where Redis flips listpack -> hashtable.

// collMeta is the metadata-row body of a btree-backed collection. count drives
// HLEN/SCARD/ZCARD/LLEN and the empty-key-deletes rule in O(1). head and tail are
// the list index window (the lowest and one-past-highest live positions); they
// stay zero for hash, set, and zset. bytes is the running sum of the raw element
// byte lengths, maintained for lists so the OBJECT ENCODING a coll list reports
// (listpack until the byte cap, then quicklist) can be decided from the metadata
// without walking the rows; it stays zero for the other types.
type collMeta struct {
	count uint64
	head  int64
	tail  int64
	bytes uint64
}

// collMetaSize is the current encoded size: count, head, tail, bytes as 8 bytes
// each. A legacy record written before the bytes field is 24 bytes; it decodes
// with bytes zero, which is harmless because every legacy coll list reported
// quicklist (it only reached coll form past the 128-entry threshold), and a
// quicklist floor never consults the byte total.
const collMetaSize = 32
const collMetaSizeLegacy = 24

func encodeCollMeta(m collMeta) []byte {
	b := make([]byte, 0, collMetaSize)
	b = encoding.AppendU64(b, m.count)
	b = encoding.AppendU64(b, uint64(m.head))
	b = encoding.AppendU64(b, uint64(m.tail))
	b = encoding.AppendU64(b, m.bytes)
	return b
}

func decodeCollMeta(b []byte) collMeta {
	if len(b) < collMetaSizeLegacy {
		return collMeta{}
	}
	m := collMeta{
		count: encoding.U64(b[0:]),
		head:  int64(encoding.U64(b[8:])),
		tail:  int64(encoding.U64(b[16:])),
	}
	if len(b) >= collMetaSize {
		m.bytes = encoding.U64(b[24:])
	}
	return m
}

// CollWriter is the write handle to a btree-backed collection, valid only inside
// the callback passed to DB.CollUpdate. The shard write lock is held for the whole
// callback, so a run of element ops plus the metadata update commit together. The
// command layer owns subkey encoding and the element count; this handle just moves
// opaque rows in and out of the sub-tree and carries the metadata counters.
type CollWriter struct {
	tree   *btree.Tree
	meta   collMeta
	enc    uint8
	encSet bool

	// hc, when set, is the in-memory row set this writer mutates on the hybrid-log
	// engine, which has no btree sub-tree. Get, Put, Delete, and Cursor route
	// through it; the whole set is serialized back into one store cell after the
	// callback. See keyspace/hybrid_coll.go.
	hc *hybridColl

	// live, when set, is the resident in-memory copy this writer absorbs element
	// ops into instead of descending the sub-tree (the hash write overlay). Get,
	// Put, and Delete route through it; the accumulated mutations fold back into
	// tree later. tree still points at the (stale) sub-tree so its root is known for
	// the metadata write. See keyspace/overlay.go.
	live *liveColl
}

// Get returns the value stored under sub and whether it is present.
func (w *CollWriter) Get(sub []byte) ([]byte, bool, error) {
	if w.hc != nil {
		v, ok := w.hc.get(sub)
		return v, ok, nil
	}
	if w.live != nil {
		v, ok := w.live.get(sub)
		return v, ok, nil
	}
	return w.tree.Get(sub)
}

// Put writes val under sub, replacing any existing value, and reports whether the
// subkey is new. It does not touch the count; the caller maintains the count via
// SetCount so types with more than one row per logical element (zset keeps a
// member row and a score row) stay accurate.
func (w *CollWriter) Put(sub, val []byte) (created bool, err error) {
	if w.hc != nil {
		return w.hc.put(sub, val), nil
	}
	if w.live != nil {
		return w.live.put(sub, val), nil
	}
	prev, err := w.tree.Upsert(sub, val)
	if err != nil {
		return false, err
	}
	return prev == nil, nil
}

// Delete removes sub and reports whether it was present.
func (w *CollWriter) Delete(sub []byte) (bool, error) {
	if w.hc != nil {
		return w.hc.del(sub), nil
	}
	if w.live != nil {
		return w.live.del(sub), nil
	}
	return w.tree.Delete(sub)
}

// Count, SetCount, Head, SetHead, Tail, SetTail read and write the metadata
// counters the caller maintains.
func (w *CollWriter) Count() uint64     { return w.meta.count }
func (w *CollWriter) SetCount(n uint64) { w.meta.count = n }
func (w *CollWriter) Head() int64       { return w.meta.head }
func (w *CollWriter) SetHead(h int64)   { w.meta.head = h }
func (w *CollWriter) Tail() int64       { return w.meta.tail }
func (w *CollWriter) SetTail(t int64)   { w.meta.tail = t }

// Bytes and SetBytes read and write the running element-byte-length total. A list
// maintains it so its reported encoding can be decided from the metadata; the
// other types leave it zero.
func (w *CollWriter) Bytes() uint64     { return w.meta.bytes }
func (w *CollWriter) SetBytes(n uint64) { w.meta.bytes = n }

// SetEnc overrides the OBJECT ENCODING written for this collection, letting the
// callback report an encoding it can only compute after mutating the rows (a list
// reports listpack while small even though it is already stored in coll form, then
// quicklist once it crosses the threshold). When the callback does not call it, the
// enc passed to CollUpdate is used.
func (w *CollWriter) SetEnc(e uint8) {
	w.enc = e
	w.encSet = true
}

// Cursor returns an ordered cursor over the element rows for range reads done
// inside a write (LPOP/RPOP/SPOP and similar). It reflects the sub-tree as of the
// call and must not be used after further Put/Delete on this writer. On a
// live-backed writer it iterates the resident copy in sorted subkey order, so the
// view matches the sub-tree's byte ordering.
func (w *CollWriter) Cursor() *CollCursor {
	if w.hc != nil {
		return &CollCursor{hc: &hybridCursor{hc: w.hc}}
	}
	if w.live != nil {
		return &CollCursor{live: newLiveCursor(w.live)}
	}
	return &CollCursor{c: w.tree.Cursor()}
}

// CollReader is the read handle, valid only inside the callback passed to
// DB.CollRead. The shard read lock is held for the whole callback.
type CollReader struct {
	tree *btree.Tree
	meta collMeta

	// hc, when set, is the in-memory row set this read serves from on the hybrid-log
	// engine. Get and Cursor consult it; Count/Head/Tail/Bytes read meta, which is
	// loaded from the same cell. See keyspace/hybrid_coll.go.
	hc *hybridColl

	// live, when set, is the resident in-memory copy this read serves from instead
	// of the sub-tree (the hash write overlay). The resident copy is authoritative
	// while present, so Get, Count, and Cursor consult it and the sub-tree is not
	// opened. See keyspace/overlay.go.
	live *liveColl
}

// Get returns the value stored under sub and whether it is present.
func (r *CollReader) Get(sub []byte) ([]byte, bool, error) {
	if r.hc != nil {
		v, ok := r.hc.get(sub)
		return v, ok, nil
	}
	if r.live != nil {
		v, ok := r.live.get(sub)
		return v, ok, nil
	}
	return r.tree.Get(sub)
}

// Count, Head, Tail, Bytes expose the metadata counters.
func (r *CollReader) Count() uint64 {
	if r.live != nil {
		return uint64(r.live.count())
	}
	return r.meta.count
}
func (r *CollReader) Head() int64   { return r.meta.head }
func (r *CollReader) Tail() int64   { return r.meta.tail }
func (r *CollReader) Bytes() uint64 { return r.meta.bytes }

// Cursor returns an ordered cursor over the element rows. On a resident read it
// iterates the in-memory copy in sorted subkey order so the view matches the
// sub-tree's byte ordering (HGETALL/HKEYS/HVALS order is stable across the
// overlay).
func (r *CollReader) Cursor() *CollCursor {
	if r.hc != nil {
		return &CollCursor{hc: &hybridCursor{hc: r.hc}}
	}
	if r.live != nil {
		return &CollCursor{live: newLiveCursor(r.live)}
	}
	return &CollCursor{c: r.tree.Cursor()}
}

// OrderStat reports whether this read is served from a sub-tree that maintains
// order-statistic counts, so the caller can take the O(log n) Rank/SelectAt path
// instead of a linear cursor walk. It is false for an overlay or hybrid read
// (those serve from memory) and for a legacy plain sub-tree opened before the
// augmentation existed, both of which keep the streaming fallback correct.
func (r *CollReader) OrderStat() bool {
	return r.tree != nil && r.hc == nil && r.live == nil && r.tree.OrderStat()
}

// Rank returns the number of element rows that sort strictly before sub and
// whether sub is itself present, in one O(log n) descent. It is valid only when
// OrderStat reports true.
func (r *CollReader) Rank(sub []byte) (uint64, bool, error) {
	return r.tree.Rank(sub)
}

// SelectAt returns the element row key at 0-based rank i and whether i is in
// range, in one O(log n) descent. It is valid only when OrderStat reports true.
func (r *CollReader) SelectAt(i uint64) ([]byte, bool, error) {
	return r.tree.SelectAt(i)
}

// CollCursor is an ordered iterator over a collection's element rows. It wraps the
// B-tree cursor, or a sorted snapshot of a resident copy for an overlay read, and
// is valid only while the enclosing CollUpdate/CollRead callback holds the shard
// lock.
type CollCursor struct {
	c    *btree.Cursor
	live *liveCursor
	hc   *hybridCursor
}

// UseArena asks the underlying btree cursor to decode visited nodes into a reused
// scratch buffer for a single-direction walk, so a bounded scan over a coll-form
// collection allocates a small constant instead of O(n). It serves both a forward
// walk (First/Seek then Next) and a backward one (Last/SeekForPrev then Prev). It
// is a no-op for the in-memory overlay and hybrid cursors, which hold their
// elements in memory and do not decode pages. Call it once, before positioning;
// the caller must copy any Key/Value it keeps before advancing, as those bytes
// alias the arena.
func (cc *CollCursor) UseArena() {
	if cc.c != nil {
		cc.c.UseArena()
	}
}

func (cc *CollCursor) First() error {
	if cc.hc != nil {
		cc.hc.first()
		return nil
	}
	if cc.live != nil {
		cc.live.first()
		return nil
	}
	return cc.c.First()
}
func (cc *CollCursor) Seek(sub []byte) error {
	if cc.hc != nil {
		cc.hc.seek(sub)
		return nil
	}
	if cc.live != nil {
		cc.live.seek(sub)
		return nil
	}
	return cc.c.Seek(sub)
}

// Last positions the cursor at the largest element row. Pair it with Prev for a
// bounded reverse walk (ZPOPMAX, ZREVRANGE tail) that never materializes the
// whole collection.
func (cc *CollCursor) Last() error {
	if cc.live != nil {
		cc.live.last()
		return nil
	}
	return cc.c.Last()
}

// SeekForPrev positions the cursor at the largest element row less than or equal
// to sub, the start of a reverse range scan at its upper bound.
func (cc *CollCursor) SeekForPrev(sub []byte) error {
	if cc.live != nil {
		cc.live.seekForPrev(sub)
		return nil
	}
	return cc.c.SeekForPrev(sub)
}

// Prev steps to the previous element row in order. The cursor must have been
// positioned by Last or SeekForPrev.
func (cc *CollCursor) Prev() error {
	if cc.live != nil {
		cc.live.prev()
		return nil
	}
	return cc.c.Prev()
}
func (cc *CollCursor) Valid() bool {
	if cc.hc != nil {
		return cc.hc.valid()
	}
	if cc.live != nil {
		return cc.live.valid()
	}
	return cc.c.Valid()
}
func (cc *CollCursor) Next() error {
	if cc.hc != nil {
		cc.hc.next()
		return nil
	}
	if cc.live != nil {
		cc.live.next()
		return nil
	}
	return cc.c.Next()
}
func (cc *CollCursor) Key() []byte {
	if cc.hc != nil {
		return cc.hc.key()
	}
	if cc.live != nil {
		return cc.live.key()
	}
	return cc.c.Key()
}
func (cc *CollCursor) Value() []byte {
	if cc.hc != nil {
		return cc.hc.value()
	}
	if cc.live != nil {
		return cc.live.value()
	}
	return cc.c.Value()
}

// collCreateTree creates a fresh element sub-tree for a collection of the given
// type. A zset sub-tree is created order-statistic so its dual member/score index
// answers ZRANK/ZREVRANK and ZRANGE-by-index in O(log n); every other type uses a
// plain tree, which carries no per-child counts and no augmentation cost. The flag
// is per-tree, so this single switch is the only place the choice is made on the
// create path.
func (db *DB) collCreateTree(typ uint8) (*btree.Tree, error) {
	if typ == TypeZSet {
		return btree.CreateOrderStat(db.ks.pgr)
	}
	return btree.Create(db.ks.pgr)
}

// collOpenTree opens an existing element sub-tree at root for a collection of the
// given type. A zset opens through OpenAutoOrderStat, which reads the root page to
// decide whether the tree is already augmented: a sub-tree built since the
// augmentation landed is order-stat, a legacy plain zset sub-tree stays plain and
// falls back to the streaming rank path, and neither is ever miswritten. Every
// other type opens plain.
func (db *DB) collOpenTree(root uint32, typ uint8) (*btree.Tree, error) {
	if typ == TypeZSet {
		return btree.OpenAutoOrderStat(db.ks.pgr, root)
	}
	return btree.Open(db.ks.pgr, root), nil
}

// CollUpdate runs fn against the btree-backed collection at key under the shard
// write lock, then writes the metadata row back. typ is the collection type byte
// and enc is the OBJECT ENCODING to report (hashtable/skiplist/quicklist). The
// caller must NOT hold any shard lock.
//
// Entry resolution:
//   - key is already a btree-backed collection: its sub-tree is opened and its
//     metadata loaded, so fn continues an existing collection.
//   - key is absent, or a blob (a small collection being promoted, or a different
//     value being overwritten): a fresh empty sub-tree is created and fn populates
//     it. A previous blob's overflow chain is freed; the row is replaced.
//
// After fn returns with no error, if the element count is zero the key and its
// sub-tree are removed; otherwise the metadata row is written with FlagCollTree
// and BodyRef pointing at the (possibly new) sub-tree root. The key's existing TTL
// is preserved.
func (db *DB) CollUpdate(key []byte, typ, enc uint8, fn func(w *CollWriter) error) error {
	if db.newHL != nil {
		return db.hlCollUpdate(key, typ, enc, fn)
	}
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	t, err := db.ensureShardTree(s)
	if err != nil {
		return err
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	prev, prevBody, prevExisted, err := db.read(t, ck)
	if err != nil {
		return err
	}
	prevIsTree := prevExisted && prev.IsColl()

	w := &CollWriter{}
	var lc *liveColl
	if db.overlayEngagesLocked(typ, prevIsTree) {
		lc, err = db.overlayResidentLocked(s, key, prev, prevBody)
		if err != nil {
			return err
		}
		w.live = lc
		w.meta.count = uint64(lc.count())
		// Open the sub-tree without descending it so its root names the metadata
		// write; element ops route through lc, not the tree.
		w.tree = btree.Open(db.ks.pgr, lc.bodyRef)
	} else if prevIsTree {
		w.tree, err = db.collOpenTree(uint32(prev.BodyRef), typ)
		if err != nil {
			return err
		}
		w.meta = decodeCollMeta(prevBody)
	} else {
		w.tree, err = db.collCreateTree(typ)
		if err != nil {
			return err
		}
	}

	if ferr := fn(w); ferr != nil {
		// A fresh sub-tree we created for this call is now orphaned; free it so the
		// failed op leaks nothing. An existing tree (or a resident copy's stale
		// sub-tree) is left as it was.
		if !prevIsTree {
			_ = btree.DropTree(db.ks.pgr, w.tree.Root())
		}
		return ferr
	}

	if lc != nil {
		return db.collFinishOverlayLocked(s, t, ck, key, w, lc, typ, enc, prev, prevExisted)
	}

	if w.meta.count == 0 {
		return db.collClearLocked(s, t, ck, key, w.tree.Root(), prev, prevExisted, prevIsTree)
	}
	// A callback may compute the reported encoding from the post-mutation counters
	// (a list flips listpack -> quicklist by element count or byte total); that
	// choice wins over the default passed by the caller.
	if w.encSet {
		enc = w.enc
	}
	return db.collWriteMetaLocked(s, t, ck, key, w, typ, enc, prev, prevExisted, prevIsTree)
}

// CollRoute is the decision a CollUpdateRouted router returns after seeing the
// existing value at key under the shard write lock, classifying which write path
// the command should take without a second read of the same metadata row.
type CollRoute uint8

const (
	// CollRouteColl proceeds with the collection element write: fn runs against the
	// existing sub-tree (or a fresh one when the key is absent), exactly as in
	// CollUpdate.
	CollRouteColl CollRoute = iota
	// CollRouteSkip does nothing and returns. The router uses it for the error and
	// no-op cases (a wrong-type key, or a must-exist command on an absent key) after
	// recording why through its own captured state.
	CollRouteSkip
	// CollRouteBlob reports that the value is not in coll form (an inline blob or a
	// fresh key the command keeps in blob form while small). No coll mutation runs;
	// the caller takes its own blob path, which may read and write the key again. Any
	// such follow-up is safe when the caller holds the key's RMW stripe across it, as
	// the write-behind fallback does, so no concurrent same-key writer can interleave.
	CollRouteBlob
)

// CollUpdateRouted is CollUpdate with the type and form routing folded in, so a
// collection write reads the metadata row once under the shard write lock instead
// of once for routing (a prior Peek/CollMetaHeader) and once more inside the
// update. It reads key, then asks route to classify it: route sees whether the key
// was found, its existing header, and, for a non-coll inline value, its blob body
// (valid only for the call) so the caller's blob path can reuse it.
//
// When route returns CollRouteColl, fn runs and the metadata is written back
// exactly as in CollUpdate (including the empty-collection teardown and the
// SetEnc override). Otherwise no coll mutation happens and the route is returned so
// the caller takes its skip or blob path. typ and enc carry the same meaning as in
// CollUpdate. The caller must NOT hold any shard lock.
func (db *DB) CollUpdateRouted(key []byte, typ, enc uint8, route func(found bool, h ValueHeader, blob []byte) CollRoute, fn func(w *CollWriter) error) (CollRoute, error) {
	if db.newHL != nil {
		return db.hlCollUpdateRouted(key, typ, enc, route, fn)
	}
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	t, err := db.ensureShardTree(s)
	if err != nil {
		return CollRouteSkip, err
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	prev, prevBody, prevExisted, err := db.read(t, ck)
	if err != nil {
		return CollRouteSkip, err
	}
	// An expired key routes as absent, matching the Peek-based routing this
	// replaces (Peek treats an expired key as not found).
	found := prevExisted && !db.expired(prev)
	var blob []byte
	if found && !prev.IsColl() {
		blob = prevBody
	}
	r := route(found, prev, blob)
	if r != CollRouteColl {
		return r, nil
	}

	prevIsTree := found && prev.IsColl()
	w := &CollWriter{}
	var lc *liveColl
	if db.overlayEngagesLocked(typ, prevIsTree) {
		lc, err = db.overlayResidentLocked(s, key, prev, prevBody)
		if err != nil {
			return CollRouteColl, err
		}
		w.live = lc
		w.meta.count = uint64(lc.count())
		w.tree = btree.Open(db.ks.pgr, lc.bodyRef)
	} else if prevIsTree {
		w.tree, err = db.collOpenTree(uint32(prev.BodyRef), typ)
		if err != nil {
			return CollRouteColl, err
		}
		w.meta = decodeCollMeta(prevBody)
	} else {
		w.tree, err = db.collCreateTree(typ)
		if err != nil {
			return CollRouteColl, err
		}
	}

	if ferr := fn(w); ferr != nil {
		if !prevIsTree {
			_ = btree.DropTree(db.ks.pgr, w.tree.Root())
		}
		return CollRouteColl, ferr
	}

	if lc != nil {
		return CollRouteColl, db.collFinishOverlayLocked(s, t, ck, key, w, lc, typ, enc, prev, prevExisted)
	}

	// CollUpdate frees a previous blob's overflow chain when it replaces a blob with
	// the coll form. The same applies here when route promoted a blob (found and not
	// a tree), so pass prevIsTree through to the same bookkeeping.
	if w.meta.count == 0 {
		return CollRouteColl, db.collClearLocked(s, t, ck, key, w.tree.Root(), prev, prevExisted, prevIsTree)
	}
	if w.encSet {
		enc = w.enc
	}
	return CollRouteColl, db.collWriteMetaLocked(s, t, ck, key, w, typ, enc, prev, prevExisted, prevIsTree)
}

// collWriteMetaLocked writes the metadata row for a non-empty collection and does
// the bookkeeping for an insert or update. Caller holds shard mu.Lock.
func (db *DB) collWriteMetaLocked(s int, t *btree.Tree, ck, key []byte, w *CollWriter, typ, enc uint8, prev ValueHeader, prevExisted, prevIsTree bool) error {
	body := encodeCollMeta(w.meta)
	h := ValueHeader{
		Type:     typ,
		Encoding: enc,
		Flags:    FlagInlineBody | FlagCollTree,
		TTLms:    -1,
		Version:  db.ks.version.next(key),
		BodyRef:  uint64(w.tree.Root()),
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	// Preserve the key's existing TTL across element ops.
	if prevExisted && prev.HasTTL() {
		h.Flags |= FlagHasTTL
		h.TTLms = prev.TTLms
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)

	if _, err := t.Upsert(ck, cell); err != nil {
		return err
	}
	db.shards[s].rootPage = t.Root()

	// A previous blob's overflow chain is now unreferenced. A previous tree is the
	// same tree we just mutated in place, so it is not freed here.
	if prevExisted && !prevIsTree && prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
		if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
			return err
		}
	}

	if !prevExisted {
		db.shards[s].keyCount.Add(1)
	} else {
		db.ks.dataBytes.Add(-(int64(len(key)) + int64(prev.BodyLen) + entryOverhead))
	}
	db.ks.dataBytes.Add(int64(len(key)) + int64(len(body)) + entryOverhead)
	if h.HasTTL() && (!prevExisted || !prev.HasTTL()) {
		db.shards[s].expireCount.Add(1)
	}
	db.recordAccess(key, !prevExisted)
	// The metadata body is not a usable value for the lock-free read path, so make
	// sure no stale blob (from before promotion) lingers in the hot cache.
	db.hc.Load().cinvalidate(key)
	return nil
}

// collClearLocked removes a collection that became empty: it drops the element
// sub-tree and, if the key existed, removes the metadata row and its bookkeeping.
// Caller holds shard mu.Lock.
func (db *DB) collClearLocked(s int, t *btree.Tree, ck, key []byte, subRoot uint32, prev ValueHeader, prevExisted, prevIsTree bool) error {
	if err := btree.DropTree(db.ks.pgr, subRoot); err != nil {
		return err
	}
	if !prevExisted {
		return nil
	}
	if _, err := t.Delete(ck); err != nil {
		return err
	}
	db.shards[s].rootPage = t.Root()
	db.shards[s].keyCount.Add(^uint64(0))
	db.ks.dataBytes.Add(-(int64(len(key)) + int64(prev.BodyLen) + entryOverhead))
	db.dropAccess(key)
	db.hc.Load().cinvalidate(key)
	if prev.HasTTL() {
		db.shards[s].expireCount.Add(^uint64(0))
	}
	// A previous blob's overflow chain must still be freed (its body did not move
	// into the sub-tree). A previous tree was already dropped above.
	if !prevIsTree && prev.Flags&FlagInlineBody == 0 && prev.BodyRef != 0 {
		if err := db.ks.freeOverflow(uint32(prev.BodyRef)); err != nil {
			return err
		}
	}
	return nil
}

// CollRead runs fn against the btree-backed collection at key under the shard read
// lock. ok is false when the key is absent or not in the btree-backed form, in
// which case the caller falls back to its blob path; fn is not called. An expired
// key reads as absent. The caller must NOT hold any shard lock.
func (db *DB) CollRead(key []byte, fn func(r *CollReader) error) (ok bool, err error) {
	if db.newHL != nil {
		return db.hlCollRead(key, fn)
	}
	s := ShardOf(key)
	db.shards[s].mu.RLock()
	defer db.shards[s].mu.RUnlock()

	t := db.loadShardTree(s)
	if t == nil {
		return false, nil
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	h, body, found, rerr := db.read(t, ck)
	ckPool.Put(ckp)
	if rerr != nil || !found || !h.IsColl() || db.expired(h) {
		return false, rerr
	}
	r := &CollReader{meta: decodeCollMeta(body)}
	// A resident copy is authoritative; serve from it without opening the (stale)
	// sub-tree. The shard read lock excludes the writer that mutates the residency
	// map, so this lookup is safe. Only an already-resident key is served here; a
	// not-yet-resident key reads the sub-tree as usual, since materializing needs
	// the write lock.
	if lc := db.shards[s].live[string(key)]; lc != nil {
		r.live = lc
	} else {
		r.tree, err = db.collOpenTree(uint32(h.BodyRef), h.Type)
		if err != nil {
			return false, err
		}
	}
	return true, fn(r)
}

// CollMetaHeader returns the value header for key without recording an access,
// for the command layer to decide whether key is btree-backed (h.IsColl), a blob,
// or absent before choosing a read path. The body is not returned because for a
// btree-backed key it is only the metadata counters.
func (db *DB) CollMetaHeader(key []byte) (ValueHeader, bool, error) {
	_, h, found, err := db.Peek(key)
	return h, found, err
}

// CollSetTTL sets (ttlMs >= 0) or clears (ttlMs < 0) the key-level TTL on a
// btree-backed collection's metadata record in place, leaving its element
// sub-tree untouched. A coll key's body is only the metadata counters, so it
// cannot be rewritten through Set (that tears the sub-tree down); EXPIRE family,
// PERSIST, and the TTL repair path route here instead. ok is false when the key
// is absent or not in coll form, so the caller can fall back to its blob path.
// Caller must NOT hold any shard lock.
func (db *DB) CollSetTTL(key []byte, ttlMs int64) (ok bool, err error) {
	if db.newHL != nil {
		return db.hlCollSetTTL(key, ttlMs)
	}
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()
	t := db.loadShardTree(s)
	if t == nil {
		return false, nil
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	h, body, found, err := db.read(t, ck)
	if err != nil || !found || !h.IsColl() {
		return false, err
	}
	prevHasTTL := h.HasTTL()
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	} else {
		h.Flags &^= FlagHasTTL
		h.TTLms = -1
	}
	h.Version = db.ks.version.next(key)
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	if _, err := t.Upsert(ck, cell); err != nil {
		return true, err
	}
	db.shards[s].rootPage = t.Root()
	switch newHasTTL := ttlMs >= 0; {
	case newHasTTL && !prevHasTTL:
		db.shards[s].expireCount.Add(1)
	case !newHasTTL && prevHasTTL:
		db.shards[s].expireCount.Add(^uint64(0))
	}
	// Keep a resident copy's TTL in step so a later fold rewrites the metadata with
	// the current TTL rather than the one captured at materialization.
	if lc := db.shards[s].live[string(key)]; lc != nil {
		lc.hasTTL = ttlMs >= 0
		lc.ttlMs = ttlMs
	}
	db.hc.Load().cinvalidate(key)
	return true, nil
}

// clearCollTTL removes the key-level TTL from a btree-backed collection's
// metadata record in place. The active-expiry repair path uses it; new callers
// should use CollSetTTL directly.
func (db *DB) clearCollTTL(key []byte) error {
	_, err := db.CollSetTTL(key, -1)
	return err
}

// CollCopyTo deep-copies the btree-backed collection at srcKey in db into dstKey
// in dst, preserving the source's type, encoding, TTL, and element rows. dst may
// be the same DB (RENAME) or another database (MOVE/COPY across the SELECT
// index); both share one pager, so the fresh sub-tree lives in the same file. Any
// value already at dstKey is replaced (its sub-tree dropped or overflow chain
// freed). ok is false when srcKey is absent or not in coll form, so the caller
// falls back to its blob copy path. Caller must NOT hold any shard lock.
//
// A coll key cannot be carried by reading its 32-byte metadata body and writing
// it through Set: that body is only counters, and Set would leave dst pointing at
// the source's sub-tree (a shared BodyRef that a later write to either key would
// corrupt). Copying the rows into a fresh sub-tree keeps the two keys
// independent.
func (db *DB) CollCopyTo(srcKey []byte, dst *DB, dstKey []byte) (ok bool, err error) {
	if db.newHL != nil {
		return db.hlCollCopyTo(srcKey, dst, dstKey)
	}
	// Snapshot the source rows and metadata under the source shard read lock, then
	// release it before taking the destination write lock. The command write
	// closures run serialized through a single writer, so no concurrent op can
	// mutate srcKey between the snapshot and the destination write, and holding the
	// two locks in sequence (never nested) sidesteps any lock-ordering hazard when
	// src and dst land on the same shard.
	ss := ShardOf(srcKey)
	db.shards[ss].mu.RLock()
	st := db.loadShardTree(ss)
	if st == nil {
		db.shards[ss].mu.RUnlock()
		return false, nil
	}
	sckp := ckPool.Get().(*[]byte)
	*sckp = appendCompositeKey(*sckp, srcKey)
	sck := *sckp
	sh, sbody, sfound, rerr := db.read(st, sck)
	ckPool.Put(sckp)
	if rerr != nil || !sfound || !sh.IsColl() {
		db.shards[ss].mu.RUnlock()
		return false, rerr
	}
	type collRow struct{ k, v []byte }
	var rows []collRow
	if lc := db.shards[ss].live[string(srcKey)]; lc != nil {
		// The source is resident: its newest rows live in the overlay, and the
		// sub-tree is stale. The resident copy is the complete authoritative set, so
		// copy from it directly (no fold needed under this read lock). Order does not
		// matter; the destination sub-tree is rebuilt by sorted Upsert.
		for k, v := range lc.rows {
			rows = append(rows, collRow{
				k: []byte(k),
				v: append([]byte(nil), v...),
			})
		}
	} else {
		sub := btree.Open(db.ks.pgr, uint32(sh.BodyRef))
		cur := sub.Cursor()
		for cerr := cur.First(); cur.Valid(); cerr = cur.Next() {
			if cerr != nil {
				db.shards[ss].mu.RUnlock()
				return false, cerr
			}
			rows = append(rows, collRow{
				k: append([]byte(nil), cur.Key()...),
				v: append([]byte(nil), cur.Value()...),
			})
		}
	}
	metaBody := append([]byte(nil), sbody...)
	srcType, srcEnc, srcFlags, srcTTL := sh.Type, sh.Encoding, sh.Flags, sh.TTLms
	db.shards[ss].mu.RUnlock()

	// Build the fresh sub-tree and install the metadata row under the destination
	// shard write lock.
	ds := ShardOf(dstKey)
	dst.shards[ds].mu.Lock()
	defer dst.shards[ds].mu.Unlock()
	dt, err := dst.ensureShardTree(ds)
	if err != nil {
		return false, err
	}
	dckp := ckPool.Get().(*[]byte)
	*dckp = appendCompositeKey(*dckp, dstKey)
	dck := *dckp
	defer ckPool.Put(dckp)

	prevH, _, prevExisted, err := dst.read(dt, dck)
	if err != nil {
		return false, err
	}

	nt, err := dst.collCreateTree(srcType)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if _, uerr := nt.Upsert(row.k, row.v); uerr != nil {
			_ = btree.DropTree(dst.ks.pgr, nt.Root())
			return false, uerr
		}
	}

	// Tear down whatever dst held: a coll sub-tree is dropped, a non-inline blob's
	// overflow chain is freed. An inline blob needs neither.
	if prevExisted {
		if prevH.IsColl() {
			if derr := btree.DropTree(dst.ks.pgr, uint32(prevH.BodyRef)); derr != nil {
				_ = btree.DropTree(dst.ks.pgr, nt.Root())
				return false, derr
			}
		} else if prevH.Flags&FlagInlineBody == 0 && prevH.BodyRef != 0 {
			if ferr := dst.ks.freeOverflow(uint32(prevH.BodyRef)); ferr != nil {
				_ = btree.DropTree(dst.ks.pgr, nt.Root())
				return false, ferr
			}
		}
	}

	h := ValueHeader{
		Type:     srcType,
		Encoding: srcEnc,
		Flags:    FlagInlineBody | FlagCollTree,
		TTLms:    -1,
		Version:  dst.ks.version.next(dstKey),
		BodyRef:  uint64(nt.Root()),
		BodyLen:  uint32(len(metaBody)),
		RefCount: 1,
	}
	if srcFlags&FlagHasTTL != 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = srcTTL
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(metaBody)))
	cell = append(cell, metaBody...)
	if _, err := dt.Upsert(dck, cell); err != nil {
		_ = btree.DropTree(dst.ks.pgr, nt.Root())
		return false, err
	}
	dst.shards[ds].rootPage = dt.Root()

	if !prevExisted {
		dst.shards[ds].keyCount.Add(1)
	} else {
		dst.ks.dataBytes.Add(-(int64(len(dstKey)) + int64(prevH.BodyLen) + entryOverhead))
		if prevH.HasTTL() {
			dst.shards[ds].expireCount.Add(^uint64(0))
		}
	}
	dst.ks.dataBytes.Add(int64(len(dstKey)) + int64(len(metaBody)) + entryOverhead)
	if h.HasTTL() {
		dst.shards[ds].expireCount.Add(1)
	}
	dst.recordAccess(dstKey, !prevExisted)
	dst.hc.Load().cinvalidate(dstKey)
	return true, nil
}

// CollMove transfers the btree-backed collection at srcKey onto dstKey in the same
// database, handing dst ownership of src's element sub-tree without copying a single
// element: src's metadata row is removed (leaving the sub-tree intact), then dst's
// metadata row is rewritten to point at that same sub-tree root, tearing down
// whatever dst held before. The cost is O(1) regardless of how many elements the
// collection holds, so a result far larger than RAM moves without ever materializing.
// dst takes src's type, encoding, and metadata counters; its TTL is cleared (the
// streamed STORE results that use this carry none). ok is false (and nothing changes)
// when src is absent, not in coll form, or resident in the write overlay, so the
// caller can fall back. src and dst must be distinct keys; the caller holds no shard
// lock and relies on the serialized command writer so no op interleaves between the
// two locked phases.
//
// This is the move twin of CollCopyTo: CollCopyTo keeps both keys by reproducing the
// sub-tree (O(n) rows in RAM), while CollMove consumes src and re-points its sub-tree
// (O(1), no rows in RAM), which is what a STORE into a scratch key followed by an
// install onto the destination needs.
func (db *DB) CollMove(srcKey, dstKey []byte) (ok bool, err error) {
	if bytes.Equal(srcKey, dstKey) {
		return false, nil
	}
	if db.newHL != nil {
		// Hybrid never hosts coll keys (string-only engine); keep correctness with a
		// deep copy plus delete in the unreachable case it ever does.
		copied, e := db.hlCollCopyTo(srcKey, db, dstKey)
		if e != nil || !copied {
			return false, e
		}
		_, e = db.hlDelete(srcKey)
		return copied, e
	}

	// Phase 1: under the source shard write lock, snapshot src's coll metadata and
	// remove its row, deliberately NOT dropping the sub-tree (dst will adopt it).
	ss := ShardOf(srcKey)
	db.shards[ss].mu.Lock()
	st := db.loadShardTree(ss)
	if st == nil {
		db.shards[ss].mu.Unlock()
		return false, nil
	}
	sckp := ckPool.Get().(*[]byte)
	*sckp = appendCompositeKey(*sckp, srcKey)
	sck := *sckp
	sh, sbody, sfound, rerr := db.read(st, sck)
	if rerr != nil || !sfound || !sh.IsColl() || db.shards[ss].live[string(srcKey)] != nil {
		ckPool.Put(sckp)
		db.shards[ss].mu.Unlock()
		return false, rerr
	}
	srcRoot := uint32(sh.BodyRef)
	metaBody := append([]byte(nil), sbody...)
	srcType, srcEnc := sh.Type, sh.Encoding
	if _, derr := st.Delete(sck); derr != nil {
		ckPool.Put(sckp)
		db.shards[ss].mu.Unlock()
		return false, derr
	}
	ckPool.Put(sckp)
	db.shards[ss].rootPage = st.Root()
	db.shards[ss].keyCount.Add(^uint64(0))
	db.ks.dataBytes.Add(-(int64(len(srcKey)) + int64(sh.BodyLen) + entryOverhead))
	db.dropAccess(srcKey)
	db.hc.Load().cinvalidate(srcKey)
	if sh.HasTTL() {
		db.shards[ss].expireCount.Add(^uint64(0))
	}
	db.shards[ss].mu.Unlock()

	// Phase 2: under the destination shard write lock, tear down dst's old value and
	// install a metadata row pointing at src's sub-tree. The sub-tree root is now
	// referenced by nobody until this Upsert lands, so a failure here only leaks it
	// (no double reference), the safer side of the trade.
	ds := ShardOf(dstKey)
	db.shards[ds].mu.Lock()
	defer db.shards[ds].mu.Unlock()
	dt, err := db.ensureShardTree(ds)
	if err != nil {
		return false, err
	}
	dckp := ckPool.Get().(*[]byte)
	*dckp = appendCompositeKey(*dckp, dstKey)
	dck := *dckp
	defer ckPool.Put(dckp)

	prevH, _, prevExisted, err := db.read(dt, dck)
	if err != nil {
		return false, err
	}
	if prevExisted {
		if prevH.IsColl() {
			db.overlayEvictLocked(ds, dstKey)
			if derr := btree.DropTree(db.ks.pgr, uint32(prevH.BodyRef)); derr != nil {
				return false, derr
			}
		} else if prevH.Flags&FlagInlineBody == 0 && prevH.BodyRef != 0 {
			if ferr := db.ks.freeOverflow(uint32(prevH.BodyRef)); ferr != nil {
				return false, ferr
			}
		}
	}

	h := ValueHeader{
		Type:     srcType,
		Encoding: srcEnc,
		Flags:    FlagInlineBody | FlagCollTree,
		TTLms:    -1,
		Version:  db.ks.version.next(dstKey),
		BodyRef:  uint64(srcRoot),
		BodyLen:  uint32(len(metaBody)),
		RefCount: 1,
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(metaBody)))
	cell = append(cell, metaBody...)
	if _, err := dt.Upsert(dck, cell); err != nil {
		return false, err
	}
	db.shards[ds].rootPage = dt.Root()

	if !prevExisted {
		db.shards[ds].keyCount.Add(1)
	} else {
		db.ks.dataBytes.Add(-(int64(len(dstKey)) + int64(prevH.BodyLen) + entryOverhead))
		if prevH.HasTTL() {
			db.shards[ds].expireCount.Add(^uint64(0))
		}
	}
	db.ks.dataBytes.Add(int64(len(dstKey)) + int64(len(metaBody)) + entryOverhead)
	db.recordAccess(dstKey, !prevExisted)
	db.hc.Load().cinvalidate(dstKey)
	return true, nil
}
