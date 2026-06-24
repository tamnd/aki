package keyspace

import (
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
// stay zero for hash, set, and zset.
type collMeta struct {
	count uint64
	head  int64
	tail  int64
}

// collMetaSize is the fixed encoded size: count, head, tail as 8 bytes each.
const collMetaSize = 24

func encodeCollMeta(m collMeta) []byte {
	b := make([]byte, 0, collMetaSize)
	b = encoding.AppendU64(b, m.count)
	b = encoding.AppendU64(b, uint64(m.head))
	b = encoding.AppendU64(b, uint64(m.tail))
	return b
}

func decodeCollMeta(b []byte) collMeta {
	if len(b) < collMetaSize {
		return collMeta{}
	}
	return collMeta{
		count: encoding.U64(b[0:]),
		head:  int64(encoding.U64(b[8:])),
		tail:  int64(encoding.U64(b[16:])),
	}
}

// CollWriter is the write handle to a btree-backed collection, valid only inside
// the callback passed to DB.CollUpdate. The shard write lock is held for the whole
// callback, so a run of element ops plus the metadata update commit together. The
// command layer owns subkey encoding and the element count; this handle just moves
// opaque rows in and out of the sub-tree and carries the metadata counters.
type CollWriter struct {
	tree *btree.Tree
	meta collMeta
}

// Get returns the value stored under sub and whether it is present.
func (w *CollWriter) Get(sub []byte) ([]byte, bool, error) { return w.tree.Get(sub) }

// Put writes val under sub, replacing any existing value, and reports whether the
// subkey is new. It does not touch the count; the caller maintains the count via
// SetCount so types with more than one row per logical element (zset keeps a
// member row and a score row) stay accurate.
func (w *CollWriter) Put(sub, val []byte) (created bool, err error) {
	prev, err := w.tree.Upsert(sub, val)
	if err != nil {
		return false, err
	}
	return prev == nil, nil
}

// Delete removes sub and reports whether it was present.
func (w *CollWriter) Delete(sub []byte) (bool, error) { return w.tree.Delete(sub) }

// Count, SetCount, Head, SetHead, Tail, SetTail read and write the metadata
// counters the caller maintains.
func (w *CollWriter) Count() uint64    { return w.meta.count }
func (w *CollWriter) SetCount(n uint64) { w.meta.count = n }
func (w *CollWriter) Head() int64      { return w.meta.head }
func (w *CollWriter) SetHead(h int64)  { w.meta.head = h }
func (w *CollWriter) Tail() int64      { return w.meta.tail }
func (w *CollWriter) SetTail(t int64)  { w.meta.tail = t }

// Cursor returns an ordered cursor over the element rows for range reads done
// inside a write (LPOP/RPOP/SPOP and similar). It reflects the sub-tree as of the
// call and must not be used after further Put/Delete on this writer.
func (w *CollWriter) Cursor() *CollCursor { return &CollCursor{c: w.tree.Cursor()} }

// CollReader is the read handle, valid only inside the callback passed to
// DB.CollRead. The shard read lock is held for the whole callback.
type CollReader struct {
	tree *btree.Tree
	meta collMeta
}

// Get returns the value stored under sub and whether it is present.
func (r *CollReader) Get(sub []byte) ([]byte, bool, error) { return r.tree.Get(sub) }

// Count, Head, Tail expose the metadata counters.
func (r *CollReader) Count() uint64 { return r.meta.count }
func (r *CollReader) Head() int64   { return r.meta.head }
func (r *CollReader) Tail() int64   { return r.meta.tail }

// Cursor returns an ordered cursor over the element rows.
func (r *CollReader) Cursor() *CollCursor { return &CollCursor{c: r.tree.Cursor()} }

// CollCursor is an ordered iterator over a collection's element rows. It wraps the
// B-tree cursor and is valid only while the enclosing CollUpdate/CollRead callback
// holds the shard lock.
type CollCursor struct{ c *btree.Cursor }

func (cc *CollCursor) First() error        { return cc.c.First() }
func (cc *CollCursor) Seek(sub []byte) error { return cc.c.Seek(sub) }
func (cc *CollCursor) Valid() bool         { return cc.c.Valid() }
func (cc *CollCursor) Next() error         { return cc.c.Next() }
func (cc *CollCursor) Key() []byte         { return cc.c.Key() }
func (cc *CollCursor) Value() []byte       { return cc.c.Value() }

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
	if prevIsTree {
		w.tree = btree.Open(db.ks.pgr, uint32(prev.BodyRef))
		w.meta = decodeCollMeta(prevBody)
	} else {
		w.tree, err = btree.Create(db.ks.pgr)
		if err != nil {
			return err
		}
	}

	if ferr := fn(w); ferr != nil {
		// A fresh sub-tree we created for this call is now orphaned; free it so the
		// failed op leaks nothing. An existing tree is left as it was.
		if !prevIsTree {
			_ = btree.DropTree(db.ks.pgr, w.tree.Root())
		}
		return ferr
	}

	if w.meta.count == 0 {
		return db.collClearLocked(s, t, ck, key, w.tree.Root(), prev, prevExisted, prevIsTree)
	}
	return db.collWriteMetaLocked(s, t, ck, key, w, typ, enc, prev, prevExisted, prevIsTree)
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
		Version:  db.ks.version.Add(1),
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
	if h.HasTTL() && !(prevExisted && prev.HasTTL()) {
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
	r := &CollReader{
		tree: btree.Open(db.ks.pgr, uint32(h.BodyRef)),
		meta: decodeCollMeta(body),
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

// clearCollTTL removes the key-level TTL from a btree-backed collection's
// metadata record in place, leaving its element sub-tree untouched. The TTL
// repair path uses it because a coll key cannot be rewritten through Set (that
// would tear the sub-tree down). Caller must NOT hold any shard lock.
func (db *DB) clearCollTTL(key []byte) error {
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()
	t := db.loadShardTree(s)
	if t == nil {
		return nil
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)

	h, body, found, err := db.read(t, ck)
	if err != nil || !found || !h.IsColl() {
		return err
	}
	if !h.HasTTL() {
		return nil
	}
	h.Flags &^= FlagHasTTL
	h.TTLms = -1
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	if _, err := t.Upsert(ck, cell); err != nil {
		return err
	}
	db.shards[s].rootPage = t.Root()
	db.shards[s].expireCount.Add(^uint64(0))
	db.hc.Load().cinvalidate(key)
	return nil
}
