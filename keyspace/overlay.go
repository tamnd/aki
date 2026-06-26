package keyspace

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

	version uint64
}

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

// count is the number of live elements, the value HLEN/SCARD report.
func (lc *liveColl) count() int { return len(lc.rows) }

// dirty reports whether the resident copy has unflushed mutations. A clean copy
// can be evicted without a fold.
func (lc *liveColl) dirty() bool { return len(lc.dirtyPut) != 0 || len(lc.dirtyDel) != 0 }

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
