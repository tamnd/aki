package keyspace

import (
	"bytes"
	"sort"

	"github.com/tamnd/aki/encoding"
)

// hybrid_coll.go carries the btree-backed collection surface onto the hybrid-log
// engine (spec 2064 rewrite, slice S3). The btree engine stores a large
// hash/set/zset/list as a metadata row plus a per-key element sub-tree. The
// hybrid store has no sub-trees, so a tree-form collection lives as one
// self-contained cell in the store: the same FlagCollTree header, then the
// metadata counters, then every ordered subkey -> value row inline. The
// CollWriter/CollReader/CollCursor handles serve this in-memory row set with the
// exact ordered API the sub-tree gave, so the whole command layer (and the
// blob-to-tree promotion in *_tree.go) is unchanged.
//
// The trade is that an element op rewrites the whole cell, O(n) per write, where
// the sub-tree was O(log n). That keeps the surface correct on the fast engine
// now; an element-granular store is a later perf slice. Small collections still
// take the inline-blob form through db.Set/db.Get, which is already hybrid-aware,
// and never reach this file.

// hybridColl is the in-memory element set of a tree-form collection on the hybrid
// engine: subkeys and their values held parallel and sorted by subkey bytes, so a
// cursor walks them in the same order the btree sub-tree produced. The command
// layer encodes order into the subkey bytes (a zset score index, a list position),
// so byte order is the collection order, exactly as on the sub-tree.
type hybridColl struct {
	keys [][]byte
	vals [][]byte
}

// search returns the position of sub and whether it is present. A miss returns the
// insertion point that keeps keys sorted.
func (hc *hybridColl) search(sub []byte) (int, bool) {
	i := sort.Search(len(hc.keys), func(i int) bool { return bytes.Compare(hc.keys[i], sub) >= 0 })
	if i < len(hc.keys) && bytes.Equal(hc.keys[i], sub) {
		return i, true
	}
	return i, false
}

func (hc *hybridColl) get(sub []byte) ([]byte, bool) {
	if i, ok := hc.search(sub); ok {
		return hc.vals[i], true
	}
	return nil, false
}

// put writes val under sub, replacing any existing value, and reports whether the
// subkey is new. Both sub and val are copied because they may alias caller buffers
// or the store page the cell was read from.
func (hc *hybridColl) put(sub, val []byte) bool {
	i, ok := hc.search(sub)
	v := append([]byte(nil), val...)
	if ok {
		hc.vals[i] = v
		return false
	}
	k := append([]byte(nil), sub...)
	hc.keys = append(hc.keys, nil)
	copy(hc.keys[i+1:], hc.keys[i:])
	hc.keys[i] = k
	hc.vals = append(hc.vals, nil)
	copy(hc.vals[i+1:], hc.vals[i:])
	hc.vals[i] = v
	return true
}

func (hc *hybridColl) del(sub []byte) bool {
	i, ok := hc.search(sub)
	if !ok {
		return false
	}
	hc.keys = append(hc.keys[:i], hc.keys[i+1:]...)
	hc.vals = append(hc.vals[:i], hc.vals[i+1:]...)
	return true
}

// hybridCursor walks a hybridColl's rows in sorted subkey order, the same contract
// the btree cursor gives the CollCursor.
type hybridCursor struct {
	hc  *hybridColl
	idx int
}

func (c *hybridCursor) first() { c.idx = 0 }
func (c *hybridCursor) seek(sub []byte) {
	c.idx = sort.Search(len(c.hc.keys), func(i int) bool { return bytes.Compare(c.hc.keys[i], sub) >= 0 })
}
func (c *hybridCursor) valid() bool   { return c.idx >= 0 && c.idx < len(c.hc.keys) }
func (c *hybridCursor) next()         { c.idx++ }
func (c *hybridCursor) key() []byte   { return c.hc.keys[c.idx] }
func (c *hybridCursor) value() []byte { return c.hc.vals[c.idx] }

// encodeHybridColl serializes the metadata counters and every row into the cell
// body: the 32-byte collMeta, a uvarint row count, then each subkey and value as a
// uvarint length and bytes. The row order is the stored sorted order.
func encodeHybridColl(m collMeta, hc *hybridColl) []byte {
	size := collMetaSize + 5
	for i := range hc.keys {
		size += 10 + len(hc.keys[i]) + len(hc.vals[i])
	}
	b := make([]byte, 0, size)
	b = encoding.AppendU64(b, m.count)
	b = encoding.AppendU64(b, uint64(m.head))
	b = encoding.AppendU64(b, uint64(m.tail))
	b = encoding.AppendU64(b, m.bytes)
	b = encoding.AppendUvarint(b, uint64(len(hc.keys)))
	for i := range hc.keys {
		b = encoding.AppendUvarint(b, uint64(len(hc.keys[i])))
		b = append(b, hc.keys[i]...)
		b = encoding.AppendUvarint(b, uint64(len(hc.vals[i])))
		b = append(b, hc.vals[i]...)
	}
	return b
}

// decodeHybridColl is the inverse of encodeHybridColl. The returned row slices
// alias body, which aliases the store page; the caller reads them under the shard
// lock and put copies anything it keeps, so no row escapes the page lifetime.
func decodeHybridColl(body []byte) (collMeta, *hybridColl) {
	m := decodeCollMeta(body)
	hc := &hybridColl{}
	if len(body) < collMetaSize {
		return m, hc
	}
	off := collMetaSize
	n, k, err := encoding.Uvarint(body[off:])
	if err != nil {
		return m, hc
	}
	off += k
	hc.keys = make([][]byte, 0, n)
	hc.vals = make([][]byte, 0, n)
	for i := uint64(0); i < n; i++ {
		kl, k1, err := encoding.Uvarint(body[off:])
		if err != nil {
			break
		}
		off += k1
		if off+int(kl) > len(body) {
			break
		}
		key := body[off : off+int(kl)]
		off += int(kl)
		vl, k2, err := encoding.Uvarint(body[off:])
		if err != nil {
			break
		}
		off += k2
		if off+int(vl) > len(body) {
			break
		}
		val := body[off : off+int(vl)]
		off += int(vl)
		hc.keys = append(hc.keys, key)
		hc.vals = append(hc.vals, val)
	}
	return m, hc
}

// hlCollUpdate is the hybrid-engine CollUpdate: load the key's cell, run fn against
// the in-memory rows, then write the cell back (or delete it when the collection
// emptied). The shard write lock serializes the read-modify-write against other
// collection writers on the same shard, the same atomicity the btree path gets
// from holding the shard lock across fn.
func (db *DB) hlCollUpdate(key []byte, typ, enc uint8, fn func(w *CollWriter) error) error {
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	body, prev, found, err := db.hlGet(key)
	if err != nil {
		return err
	}
	w := &CollWriter{hc: &hybridColl{}}
	if found && prev.IsColl() {
		w.meta, w.hc = decodeHybridColl(body)
	}
	if ferr := fn(w); ferr != nil {
		return ferr
	}
	if w.meta.count == 0 {
		if found {
			_, err = db.hlDelete(key)
		}
		return err
	}
	if w.encSet {
		enc = w.enc
	}
	return db.hlCollStoreLocked(key, w, typ, enc, prev, found)
}

// hlCollUpdateRouted is the hybrid-engine CollUpdateRouted: it reads the cell once,
// asks route to classify it (so a wrong-type key or a blob value is seen and the
// caller takes its own path), and only runs fn for the CollRouteColl decision.
func (db *DB) hlCollUpdateRouted(key []byte, typ, enc uint8, route func(found bool, h ValueHeader, blob []byte) CollRoute, fn func(w *CollWriter) error) (CollRoute, error) {
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	body, prev, found, err := db.hlGet(key)
	if err != nil {
		return CollRouteSkip, err
	}
	var blob []byte
	if found && !prev.IsColl() {
		blob = body
	}
	r := route(found, prev, blob)
	if r != CollRouteColl {
		return r, nil
	}
	w := &CollWriter{hc: &hybridColl{}}
	if found && prev.IsColl() {
		w.meta, w.hc = decodeHybridColl(body)
	}
	if ferr := fn(w); ferr != nil {
		return CollRouteColl, ferr
	}
	if w.meta.count == 0 {
		if found {
			_, err = db.hlDelete(key)
		}
		return CollRouteColl, err
	}
	if w.encSet {
		enc = w.enc
	}
	return CollRouteColl, db.hlCollStoreLocked(key, w, typ, enc, prev, found)
}

// hlCollStoreLocked writes the collection cell back to the store with a fresh
// FlagCollTree header, carrying the key's existing TTL. Caller holds the shard
// write lock.
func (db *DB) hlCollStoreLocked(key []byte, w *CollWriter, typ, enc uint8, prev ValueHeader, prevExisted bool) error {
	st, err := db.ensureHL()
	if err != nil {
		return err
	}
	body := encodeHybridColl(w.meta, w.hc)
	h := ValueHeader{
		Type:     typ,
		Encoding: enc,
		Flags:    FlagInlineBody | FlagCollTree,
		TTLms:    -1,
		Version:  db.ks.version.Add(1),
		BodyLen:  uint32(len(body)),
		RefCount: 1,
	}
	if prevExisted && prev.HasTTL() {
		h.Flags |= FlagHasTTL
		h.TTLms = prev.TTLms
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	prevValLen, err := st.SetWithPrev(key, cell)
	if err != nil {
		return err
	}
	db.hlAccountStore(key, len(body), prevValLen)
	return nil
}

// hlCollRead is the hybrid-engine CollRead: serve fn a reader over the cell's rows
// when the key is in tree form, else report not-found so the caller falls back to
// its blob path. The shard read lock excludes a concurrent collection writer, so fn
// sees a stable cell.
func (db *DB) hlCollRead(key []byte, fn func(r *CollReader) error) (bool, error) {
	s := ShardOf(key)
	db.shards[s].mu.RLock()
	defer db.shards[s].mu.RUnlock()

	body, h, found, err := db.hlGet(key)
	if err != nil || !found || !h.IsColl() {
		return false, err
	}
	meta, hc := decodeHybridColl(body)
	r := &CollReader{meta: meta, hc: hc}
	return true, fn(r)
}

// hlCollSetTTL is the hybrid-engine CollSetTTL: rewrite the cell's header TTL in
// place, leaving the rows untouched, so EXPIRE/PERSIST on a tree-form collection
// does not tear it down. ok is false when the key is absent or not in tree form.
func (db *DB) hlCollSetTTL(key []byte, ttlMs int64) (bool, error) {
	s := ShardOf(key)
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()

	body, h, found, err := db.hlGet(key)
	if err != nil || !found || !h.IsColl() {
		return false, err
	}
	if ttlMs >= 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = ttlMs
	} else {
		h.Flags &^= FlagHasTTL
		h.TTLms = -1
	}
	h.Version = db.ks.version.Add(1)
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	st := db.hl.Load()
	return true, st.Set(key, cell)
}

// hlCollCopyTo is the hybrid-engine CollCopyTo: the cell is self-contained, so the
// copy is just the body re-stamped with a fresh header under dstKey, preserving
// type, encoding, and TTL. The source read lock is released before the destination
// write lock, so RENAME onto the same shard does not self-deadlock.
func (db *DB) hlCollCopyTo(srcKey []byte, dst *DB, dstKey []byte) (bool, error) {
	ss := ShardOf(srcKey)
	db.shards[ss].mu.RLock()
	body, sh, found, err := db.hlGet(srcKey)
	if err != nil || !found || !sh.IsColl() {
		db.shards[ss].mu.RUnlock()
		return false, err
	}
	bodyCopy := append([]byte(nil), body...)
	srcType, srcEnc, srcFlags, srcTTL := sh.Type, sh.Encoding, sh.Flags, sh.TTLms
	db.shards[ss].mu.RUnlock()

	ds := ShardOf(dstKey)
	dst.shards[ds].mu.Lock()
	defer dst.shards[ds].mu.Unlock()
	st, err := dst.ensureHL()
	if err != nil {
		return false, err
	}
	h := ValueHeader{
		Type:     srcType,
		Encoding: srcEnc,
		Flags:    FlagInlineBody | FlagCollTree,
		TTLms:    -1,
		Version:  dst.ks.version.Add(1),
		BodyLen:  uint32(len(bodyCopy)),
		RefCount: 1,
	}
	if srcFlags&FlagHasTTL != 0 {
		h.Flags |= FlagHasTTL
		h.TTLms = srcTTL
	}
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(bodyCopy)))
	cell = append(cell, bodyCopy...)
	prevValLen, err := st.SetWithPrev(dstKey, cell)
	if err != nil {
		return false, err
	}
	dst.hlAccountStore(dstKey, len(bodyCopy), prevValLen)
	return true, nil
}
