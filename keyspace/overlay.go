package keyspace

import (
	"sort"

	"github.com/tamnd/aki/btree"
)

// This file is the keyspace-layer groundwork for the in-memory collection write
// fast path (spec 2064 note 223). It defines liveColl, the in-memory
// authoritative copy of a btree-backed collection while it is resident in the
// write overlay, plus the two operations that bracket residency: materialize a
// liveColl from an element sub-tree, and fold the accumulated mutations back
// into one.
//
// The premise the overlay attacks (measured in note 222) is that every
// collection element write descends the shard tree and the element sub-tree per
// op, where Redis does an O(1) in-memory map insert or tail append. A resident
// liveColl absorbs element writes at map speed and folds a run of them into the
// sub-tree in one pass, so the per-op btree descent amortizes toward zero on a
// hot key.
//
// liveColl is deliberately type-agnostic: it stores rows verbatim as the command
// layer's encoded element bytes (a hash row, a set member's empty value, and so
// on), keyed by the same opaque subkey the CollWriter uses. That is enough for
// the map-shaped collections (hash, set). The ordered types (list, zset) need an
// in-memory index on top and ride later slices; this file does not model them.
//
// Nothing here is wired into a command path yet. It is exercised by unit tests
// and a microbenchmark that validates the absorb-vs-descend premise before the
// next slice gates it behind a config directive and routes HSET through it.

// liveColl is the resident in-memory copy of one collection. While a key is
// resident, this copy is authoritative and the element sub-tree is its stale,
// asynchronously-folded backing. rows holds every live element; dirtyPut and
// dirtyDel record the subkeys mutated since the last fold so a fold writes only
// the delta. version is the fold-generation guard: it advances on every mutation
// so a fold can tell whether a newer write landed while it ran, the same shape
// as the blob write-behind's version guard.
type liveColl struct {
	rows     map[string][]byte
	dirtyPut map[string]struct{}
	dirtyDel map[string]struct{}

	typ    uint8
	enc    uint8
	ttlMs  int64
	hasTTL bool

	// bodyRef is the element sub-tree root this resident copy folds back into. It
	// is the BodyRef of the key's metadata row, captured when the copy is
	// materialized and stable while the key stays resident (every mutator either
	// routes through the overlay under the shard lock or evicts the copy first), so
	// a fold can reopen the sub-tree without re-reading the metadata row.
	bodyRef uint32

	version uint64
}

// overlayFoldThreshold is the count of unfolded mutations a resident copy
// accumulates before a write inline-folds it back into the sub-tree. A larger
// value batches more element writes per fold (better locality, fewer metadata
// touches) at the cost of more rows replayed from the append log after a crash;
// 256 matches the write worker's drain-batch bound so a fold spans about one
// batch of absorbed writes.
const overlayFoldThreshold = 256

// newLiveColl returns an empty resident collection of the given type, encoding,
// and TTL. It is used when a collection is created directly in the overlay (a
// fresh key whose first element write is absorbed) rather than materialized from
// an existing sub-tree.
func newLiveColl(typ, enc uint8, ttlMs int64, hasTTL bool) *liveColl {
	return &liveColl{
		rows:     make(map[string][]byte),
		dirtyPut: make(map[string]struct{}),
		dirtyDel: make(map[string]struct{}),
		typ:      typ,
		enc:      enc,
		ttlMs:    ttlMs,
		hasTTL:   hasTTL,
	}
}

// materializeLiveColl reads every element row out of a CollReader into a fresh
// resident copy. This is the one O(n) cost the overlay pays per hot key, on
// first touch, amortized over every subsequent absorbed write. The reader's
// shard read lock is held by the caller for the duration. The new copy starts
// clean (no dirty subkeys): it matches the sub-tree exactly, so an immediate
// fold is a no-op.
func materializeLiveColl(r *CollReader, typ, enc uint8, ttlMs int64, hasTTL bool) (*liveColl, error) {
	lc := newLiveColl(typ, enc, ttlMs, hasTTL)
	cur := r.Cursor()
	for err := cur.First(); cur.Valid(); err = cur.Next() {
		if err != nil {
			return nil, err
		}
		// Key and value alias the cursor's page buffers, valid only until the next
		// move, so both are copied into the resident map.
		k := string(cur.Key())
		lc.rows[k] = append([]byte(nil), cur.Value()...)
	}
	return lc, nil
}

// put writes row under sub, reporting whether the subkey is new. It records the
// subkey as a pending put so the next fold carries it to the sub-tree.
func (lc *liveColl) put(sub, row []byte) (created bool) {
	sk := string(sub)
	_, existed := lc.rows[sk]
	lc.rows[sk] = append([]byte(nil), row...)
	delete(lc.dirtyDel, sk)
	lc.dirtyPut[sk] = struct{}{}
	lc.version++
	return !existed
}

// del removes sub, reporting whether it was present. It records the subkey as a
// pending delete so the next fold removes it from the sub-tree, and clears any
// pending put for the same subkey (a put then delete since the last fold nets to
// a delete).
func (lc *liveColl) del(sub []byte) (existed bool) {
	sk := string(sub)
	if _, ok := lc.rows[sk]; !ok {
		return false
	}
	delete(lc.rows, sk)
	delete(lc.dirtyPut, sk)
	lc.dirtyDel[sk] = struct{}{}
	lc.version++
	return true
}

// get returns the row stored under sub and whether it is present. The returned
// slice aliases the resident copy and must not be mutated by the caller.
func (lc *liveColl) get(sub []byte) ([]byte, bool) {
	v, ok := lc.rows[string(sub)]
	return v, ok
}

// liveCursor iterates a resident copy's rows in sorted subkey order, matching the
// byte ordering of the element sub-tree it shadows so HGETALL/HKEYS/HVALS return
// the same order whether or not a key is resident. It snapshots the subkeys at
// construction, so it is stable against further mutation of the copy (the enclosing
// callback holds the shard lock for its lifetime regardless).
type liveCursor struct {
	lc   *liveColl
	keys []string
	i    int
}

// newLiveCursor snapshots the resident copy's subkeys in sorted order.
func newLiveCursor(lc *liveColl) *liveCursor {
	keys := make([]string, 0, len(lc.rows))
	for k := range lc.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &liveCursor{lc: lc, keys: keys}
}

func (lcur *liveCursor) first()        { lcur.i = 0 }
func (lcur *liveCursor) valid() bool   { return lcur.i >= 0 && lcur.i < len(lcur.keys) }
func (lcur *liveCursor) next()         { lcur.i++ }
func (lcur *liveCursor) prev()         { lcur.i-- }
func (lcur *liveCursor) last()         { lcur.i = len(lcur.keys) - 1 }
func (lcur *liveCursor) key() []byte   { return []byte(lcur.keys[lcur.i]) }
func (lcur *liveCursor) value() []byte { return lcur.lc.rows[lcur.keys[lcur.i]] }

// seek positions the cursor at the first subkey greater than or equal to sub, the
// same semantics as the B-tree cursor's Seek.
func (lcur *liveCursor) seek(sub []byte) {
	target := string(sub)
	lcur.i = sort.SearchStrings(lcur.keys, target)
}

// seekForPrev positions the cursor at the largest subkey less than or equal to
// sub, the same semantics as the B-tree cursor's SeekForPrev. It leaves the cursor
// invalid (index -1) when every subkey is greater.
func (lcur *liveCursor) seekForPrev(sub []byte) {
	target := string(sub)
	i := sort.SearchStrings(lcur.keys, target)
	if i < len(lcur.keys) && lcur.keys[i] == target {
		lcur.i = i
		return
	}
	lcur.i = i - 1
}

// count is the number of live elements, the value HLEN/SCARD report.
func (lc *liveColl) count() int { return len(lc.rows) }

// dirty reports whether the resident copy has unflushed mutations. A clean copy
// can be evicted without a fold.
func (lc *liveColl) dirty() bool { return len(lc.dirtyPut) != 0 || len(lc.dirtyDel) != 0 }

// dirtyTotal is the count of unfolded mutations since the last fold, compared
// against overlayFoldThreshold to decide when an absorbed write inline-folds.
func (lc *liveColl) dirtyTotal() int { return len(lc.dirtyPut) + len(lc.dirtyDel) }

// hashOverlayOn reports whether the in-memory hash write overlay is enabled. The
// command layer toggles it through Keyspace.SetHashOverlay after weighing the
// commit policy: it stays off under commitAlways, where an absorbed write would
// have no durable record before its reply, and off entirely until a config
// directive turns it on.
func (db *DB) hashOverlayOn() bool { return db.ks.hashOverlay.Load() }

// HashOverlayEnabled reports whether the in-memory hash write overlay is currently
// engaged. The command layer owns the policy that drives it; this is a read-only
// view for introspection and tests.
func (ks *Keyspace) HashOverlayEnabled() bool { return ks.hashOverlay.Load() }

// overlayEngagesLocked reports whether a coll write to a key of type typ should
// route through the resident overlay. The overlay only continues an existing
// btree-backed hash (prevIsTree): a fresh key or a blob being promoted takes the
// normal path and becomes resident on its next write, which keeps the engage
// decision a single bool test and avoids materializing a copy that a one-shot write
// would never reuse. The caller holds the shard write lock.
func (db *DB) overlayEngagesLocked(typ uint8, prevIsTree bool) bool {
	return prevIsTree && typ == TypeHash && db.hashOverlayOn()
}

// collFinishOverlayLocked completes a coll write that absorbed its element ops into
// the resident copy lc. When the hash emptied, the key and its sub-tree are torn
// down. Otherwise the metadata row is rewritten every call (so the version advances
// for WATCH and the count stays accurate) while the element writes stay in memory;
// once the unfolded mutations cross the fold threshold they are folded back into the
// sub-tree in one pass first. The caller holds the shard write lock. w carries the
// closure-maintained counters; w.tree was opened at lc.bodyRef so its root names the
// (unchanged) sub-tree for a deferred write.
func (db *DB) collFinishOverlayLocked(s int, t *btree.Tree, ck, key []byte, w *CollWriter, lc *liveColl, typ, enc uint8, prev ValueHeader, prevExisted bool) error {
	if w.meta.count == 0 {
		// The last element was removed: drop the resident copy and tear the key down,
		// freeing the sub-tree at lc.bodyRef. The deferred element writes never reached
		// the sub-tree, but it is going away regardless.
		db.overlayEvictLocked(s, key)
		return db.collClearLocked(s, t, ck, key, lc.bodyRef, prev, prevExisted, true)
	}
	if w.encSet {
		enc = w.enc
	}
	if lc.dirtyTotal() >= overlayFoldThreshold {
		fw := &CollWriter{tree: btree.Open(db.ks.pgr, lc.bodyRef), meta: w.meta}
		if err := lc.fold(fw); err != nil {
			return err
		}
		lc.bodyRef = fw.tree.Root()
		return db.collWriteMetaLocked(s, t, ck, key, fw, typ, enc, prev, prevExisted, true)
	}
	return db.collWriteMetaLocked(s, t, ck, key, w, typ, enc, prev, prevExisted, true)
}

// overlayResidentLocked returns the resident copy for key on shard s, materializing
// it from the element sub-tree on first touch. The caller holds the shard write
// lock and has confirmed key is a btree-backed collection with header prev and
// metadata body prevBody. The copy is stored in the shard residency map so later
// reads and writes find it.
func (db *DB) overlayResidentLocked(s int, key []byte, prev ValueHeader, prevBody []byte) (*liveColl, error) {
	sk := string(key)
	if lc := db.shards[s].live[sk]; lc != nil {
		return lc, nil
	}
	sub := btree.Open(db.ks.pgr, uint32(prev.BodyRef))
	r := &CollReader{tree: sub, meta: decodeCollMeta(prevBody)}
	lc, err := materializeLiveColl(r, prev.Type, prev.Encoding, prev.TTLms, prev.HasTTL())
	if err != nil {
		return nil, err
	}
	lc.bodyRef = uint32(prev.BodyRef)
	if db.shards[s].live == nil {
		db.shards[s].live = make(map[string]*liveColl)
	}
	db.shards[s].live[sk] = lc
	return lc, nil
}

// overlayEvictLocked drops the resident copy for key without folding it, for when
// the key's value is being removed or overwritten outside the overlay so the stale
// copy must not survive. The caller holds the shard write lock. It is a no-op when
// the key is not resident.
func (db *DB) overlayEvictLocked(s int, key []byte) {
	if db.shards[s].live == nil {
		return
	}
	delete(db.shards[s].live, string(key))
}

// overlayFoldKeyLocked folds one resident copy's accumulated mutations back into
// the element sub-tree and rewrites the metadata row, leaving the copy resident
// and clean. It reports whether it wrote anything, so a persist boundary knows the
// fold dirtied pages that a checkpoint must flush. The caller holds the shard write
// lock. It is the standalone fold used at persist boundaries and on overlay
// teardown; the absorb path folds inline against its own open writer instead. A
// clean copy is left untouched.
func (db *DB) overlayFoldKeyLocked(s int, key []byte, lc *liveColl) (bool, error) {
	if !lc.dirty() {
		return false, nil
	}
	t, err := db.ensureShardTree(s)
	if err != nil {
		return false, err
	}
	ckp := ckPool.Get().(*[]byte)
	*ckp = appendCompositeKey(*ckp, key)
	ck := *ckp
	defer ckPool.Put(ckp)
	prev, _, prevExisted, err := db.read(t, ck)
	if err != nil {
		return false, err
	}
	w := &CollWriter{tree: btree.Open(db.ks.pgr, lc.bodyRef)}
	w.meta.count = uint64(lc.count())
	if err := lc.fold(w); err != nil {
		return false, err
	}
	lc.bodyRef = w.tree.Root()
	if err := db.collWriteMetaLocked(s, t, ck, key, w, lc.typ, lc.enc, prev, prevExisted, prevExisted && prev.IsColl()); err != nil {
		return false, err
	}
	return true, nil
}

// FoldAllOverlay folds every resident collection copy in every database back into
// its sub-tree so a snapshot or a durable checkpoint sees the absorbed writes. The
// copies stay resident and clean. It reports whether any fold wrote, so the caller
// can decide whether a checkpoint is owed. The command layer calls it before SAVE,
// BGSAVE, an RDB snapshot, an AOF rewrite, and a clean shutdown, so the persisted
// file is never missing an unfolded write. It is safe to call when the overlay was
// never used.
func (ks *Keyspace) FoldAllOverlay() (bool, error) {
	folded := false
	for _, db := range ks.dbs {
		for s := range db.shards {
			db.shards[s].mu.Lock()
			did, err := db.foldShardLocked(s, false)
			db.shards[s].mu.Unlock()
			folded = folded || did
			if err != nil {
				return folded, err
			}
		}
	}
	return folded, nil
}

// foldShardLocked folds every resident copy on shard s, reporting whether any fold
// wrote. When evict is set the residency map is dropped afterward, returning the
// shard to a no-overlay state. The caller holds the shard write lock.
func (db *DB) foldShardLocked(s int, evict bool) (bool, error) {
	folded := false
	for sk, lc := range db.shards[s].live {
		did, err := db.overlayFoldKeyLocked(s, []byte(sk), lc)
		folded = folded || did
		if err != nil {
			return folded, err
		}
	}
	if evict {
		db.shards[s].live = nil
	}
	return folded, nil
}

// SetHashOverlay enables or disables the in-memory hash write overlay. Enabling
// just sets the flag; new coll-form hash writes then route through the residency
// map. Disabling folds every resident copy back into its sub-tree and drops them,
// so no copy outlives the overlay while later writes bypass it. The command layer
// serializes this against all writers (it holds the engine write lock), so no
// absorb or fold races the teardown.
func (ks *Keyspace) SetHashOverlay(on bool) (folded bool, err error) {
	if on {
		ks.hashOverlay.Store(true)
		return false, nil
	}
	ks.hashOverlay.Store(false)
	for _, db := range ks.dbs {
		for s := range db.shards {
			db.shards[s].mu.Lock()
			did, ferr := db.foldShardLocked(s, true)
			db.shards[s].mu.Unlock()
			folded = folded || did
			if ferr != nil {
				return folded, ferr
			}
		}
	}
	return folded, nil
}

// fold writes the accumulated mutations into the element sub-tree through w and
// updates the metadata count, then clears the dirty sets. Only the delta since
// the last fold is written: pending deletes are removed, pending puts are
// upserted. A run of N absorbed writes to one key collapses into one descent per
// distinct subkey here instead of one descent per write. The caller holds the
// shard write lock (it is inside a CollUpdate callback) and writes the metadata
// row back after fold returns.
func (lc *liveColl) fold(w *CollWriter) error {
	for sk := range lc.dirtyDel {
		if _, err := w.Delete([]byte(sk)); err != nil {
			return err
		}
	}
	for sk := range lc.dirtyPut {
		if _, err := w.Put([]byte(sk), lc.rows[sk]); err != nil {
			return err
		}
	}
	w.SetCount(uint64(len(lc.rows)))
	lc.dirtyPut = make(map[string]struct{})
	lc.dirtyDel = make(map[string]struct{})
	return nil
}
