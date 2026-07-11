package zset

import (
	"bytes"
	"math"

	"github.com/tamnd/aki/engine/f3/store"
	structs "github.com/tamnd/aki/engine/f3/struct"
)

// nativeStore is the native zset band (spec 2064/f3/12 section 2): the dual
// structure of a member hash and a counted B+ tree, coordinated so that for
// every present member the raw score bits in its hash record equal the score
// encoded in its tree key, with exactly one tree entry per member. The member
// hash answers ZSCORE in one probe and hands every write its old score so the
// tree delete addresses the exact old key; the tree answers everything ordered.
// It is owner-local (F1): one shard goroutine reads and writes it, so the
// invariant needs no lock, only serial execution.
//
// Storage discipline follows the M1 member table (set/member.go): the Swiss
// table holds record ordinals, the records hold slab offsets and the raw
// IEEE-754 score bits, and the member bytes live once in a byte slab. The tree
// stores no member bytes either; its 4-byte refs are these same record
// ordinals, resolved through the Members callback only on a score tie. The
// tree key is the sortable form of the score (codec.go), the hash record keeps
// the raw bits, and the two are bijections of each other except for the sign
// of zero, which only the raw bits carry, so ZSCORE formats "-0" for a stored
// -0.0 while its tree key sorts as +0.0.
type nativeStore struct {
	tbl  structs.Table // member hash: control bytes plus record ordinals
	recs []natRecord   // indexed by record ordinal, the tree's refs
	slab []byte        // member bytes, appended; holes left by rem until rebuild
	tree *structs.Tree // counted B+ tree keyed (sortable score, member by ref)

	// Removed records are never reused and their bytes never move in place: a
	// tree separator is a copy of a boundary key and legitimately outlives the
	// entry it was copied from, so its ref must keep resolving to the original
	// member bytes for routing compares. Reclamation is therefore a wholesale
	// rebuild (maybeRebuild) once dead records or dead bytes outweigh live
	// ones, which rebuilds the separators too, so churn stays amortized O(1)
	// per op and nothing dangles.
	deadBytes int // slab bytes behind removed records
	deadRecs  int // removed record cells awaiting the rebuild

	// pending buffers the sorted entries of a band promotion between
	// appendSorted calls and seal's bulk load, nil at any other time.
	pending []structs.Entry
}

// natRecord is the fixed per-member cell, 16 bytes: where the member bytes
// live, and the raw score bits ZSCORE formats from without a decode.
type natRecord struct {
	loc  uint32 // slab offset of this member's bytes
	mlen uint32 // member byte length
	bits uint64 // raw IEEE-754 score bits, the sign-of-zero source of truth
}

func newNativeStore(hint int) *nativeStore {
	n := &nativeStore{
		tbl:  structs.MakeTable(hint),
		tree: structs.NewTree(),
	}
	if hint > 0 {
		n.recs = make([]natRecord, 0, hint)
	}
	return n
}

// Match confirms a tag hit: the member stored at ord must equal key. It is the
// structs.Set half the table probes through, and it allocates nothing.
func (n *nativeStore) Match(ord uint32, key []byte) bool {
	r := &n.recs[ord]
	return bytes.Equal(n.slab[r.loc:r.loc+r.mlen], key)
}

// Rehash recomputes a member's hash from its bytes for a table resize, since
// the record caches none.
func (n *nativeStore) Rehash(ord uint32) uint64 {
	r := &n.recs[ord]
	return store.Hash(n.slab[r.loc : r.loc+r.mlen])
}

// Member resolves a tree ref back to its member bytes, the tie-break callback
// the tree invokes only when two keys share a score.
func (n *nativeStore) Member(ref uint32) []byte {
	r := &n.recs[ref]
	return n.slab[r.loc : r.loc+r.mlen]
}

func (n *nativeStore) card() int { return n.tbl.Len() }

// score is the ZSCORE read: one hash probe, the raw bits decoded to a float,
// zero allocation. The tree is never touched.
func (n *nativeStore) score(m []byte) (float64, bool) {
	ord, ok := n.tbl.Find(store.Hash(m), m, n)
	if !ok {
		return 0, false
	}
	return math.Float64frombits(n.recs[ord].bits), true
}

// insert adds a member the caller has checked is absent: seat the record and
// the member bytes, insert the hash slot, insert the tree entry.
func (n *nativeStore) insert(m []byte, score float64) {
	hash := store.Hash(m)
	ord := n.newRecord(m, math.Float64bits(score))
	n.tbl.Insert(hash, ord, n)
	n.tree.Insert(scoreKey(score), m, ord, n)
}

// rescore moves an existing member to a new score: tree delete at the old key,
// record bits overwritten, tree reinsert at the new key. The three steps run
// back to back on the owner, so no read can observe the member half-moved; the
// serial execution is the atomicity.
func (n *nativeStore) rescore(m []byte, score float64) {
	ord, ok := n.tbl.Find(store.Hash(m), m, n)
	if !ok {
		return
	}
	old := math.Float64frombits(n.recs[ord].bits)
	n.tree.Delete(scoreKey(old), m, n)
	n.recs[ord].bits = math.Float64bits(score)
	n.tree.Insert(scoreKey(score), m, ord, n)
}

// rem deletes m and reports whether it was present. The record cell and its
// slab bytes stay behind untouched, because a tree separator copied from this
// entry may still resolve the ref; the dead counters drive the amortized
// rebuild that reclaims them.
func (n *nativeStore) rem(m []byte) bool {
	hash := store.Hash(m)
	ord, ok := n.tbl.Delete(hash, m, n)
	if !ok {
		return false
	}
	r := &n.recs[ord]
	n.tree.Delete(scoreKey(math.Float64frombits(r.bits)), m, n)
	n.deadBytes += int(r.mlen)
	n.deadRecs++
	n.maybeRebuild()
	return true
}

// newRecord seats m's bytes in the slab and takes a fresh record ordinal.
// Ordinals are never reused between rebuilds (see the deadRecs comment).
func (n *nativeStore) newRecord(m []byte, bits uint64) uint32 {
	loc := uint32(len(n.slab))
	n.slab = append(n.slab, m...)
	ord := uint32(len(n.recs))
	n.recs = append(n.recs, natRecord{loc: loc, mlen: uint32(len(m)), bits: bits})
	return ord
}

// appendSorted takes one entry of a band promotion, already in zset order.
// It seats the record and hash slot now and buffers the tree entry; seal bulk
// loads the tree once the blob is drained. Only listpackToNative calls it.
func (n *nativeStore) appendSorted(m []byte, score float64) {
	hash := store.Hash(m)
	ord := n.newRecord(m, math.Float64bits(score))
	n.tbl.Insert(hash, ord, n)
	n.pending = append(n.pending, structs.Entry{Score: scoreKey(score), Ref: ord})
}

// seal finishes a band promotion: the buffered sorted entries bulk-load the
// tree at the right-edge 0.9 fill (section 2.4), so a freshly promoted zset
// starts at the bulk memory bar instead of the split-thrashed 0.5 fill.
func (n *nativeStore) seal() {
	n.tree = structs.BulkLoad(n.pending)
	n.pending = nil
}

// each visits every member in ascending zset order. The member bytes alias the
// slab and are valid only until the next write.
func (n *nativeStore) each(fn func(m []byte, score float64)) {
	n.tree.Each(func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.slab[r.loc:r.loc+r.mlen], math.Float64frombits(r.bits))
		return true
	})
}

// rank returns the count of members sorting before m, its score, and whether m
// is present, in a single member-hash probe plus one counted descent (section
// 6.3). The probe yields the raw score bits, which decode to the float ZRANK
// WITHSCORE formats and encode to the sortable key the tree descends on, so the
// hot path touches the hash once and the tree once, no second lookup. The
// descent is O(log n) on the subtree counts, no walk.
func (n *nativeStore) rank(m []byte) (int, float64, bool) {
	ord, ok := n.tbl.Find(store.Hash(m), m, n)
	if !ok {
		return 0, 0, false
	}
	sc := math.Float64frombits(n.recs[ord].bits)
	r, _ := n.tree.Rank(scoreKey(sc), m, n)
	return int(r), sc, true
}

// walkRange streams entries at forward ranks lo..hi inclusive in ascending
// order, handing each member (aliasing the slab, valid until the next write)
// and its raw score bits to fn. It seeks to lo with a counted select then
// follows the leaf chain over just the window (section 6.4), so a far ZRANGE is
// a seek plus a bounded walk, not a full scan, and it allocates nothing.
func (n *nativeStore) walkRange(lo, hi int, fn func(m []byte, bits uint64)) {
	remaining := hi - lo + 1
	n.tree.WalkFromRank(uint64(lo), func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.slab[r.loc:r.loc+r.mlen], r.bits)
		remaining--
		return remaining > 0
	})
}

// walkRangeRev streams the same forward-rank window hi..lo in descending order,
// the ZRANGE REV and ZREVRANGE walk. It descends to the high end and walks back
// with the tree's reverse leaf walk, re-seeking at most once per leaf boundary.
func (n *nativeStore) walkRangeRev(lo, hi int, fn func(m []byte, bits uint64)) {
	remaining := hi - lo + 1
	n.tree.WalkFromRankRev(uint64(hi), func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.slab[r.loc:r.loc+r.mlen], r.bits)
		remaining--
		return remaining > 0
	})
}

// scoreWindow returns the half-open forward-rank window [lo, hiExcl) of the
// members whose score falls in [min, max] (spec 2064/f3/12 section 6.4). It is
// two counted descents and no walk: lo is the count of entries strictly below
// the low bound, hiExcl the count at or below the high bound, so ZCOUNT is
// hiExcl-lo and a ZRANGEBYSCORE stream is the index range over that window. The
// low bound seeks with the member -inf sentinel (nil), so an inclusive bound
// lands the first entry at the bound's score and an exclusive bound skips the
// whole tied score band via the +1 on the sortable key, one comparison tweak in
// the descent rather than a post-filter (section 6.5).
func (n *nativeStore) scoreWindow(min, max scoreBound) (lo, hiExcl int) {
	lk := scoreKey(min.value)
	if min.exclusive {
		lk++ // entries at the bound score sort before (lk, nil), so they count as below
	}
	lr, _ := n.tree.Rank(lk, nil, n)

	hk := scoreKey(max.value)
	if !max.exclusive {
		hk++ // include the bound score: count entries strictly below the next key
	}
	hr, _ := n.tree.Rank(hk, nil, n)

	lo = int(lr)
	hiExcl = int(hr)
	if hiExcl < lo {
		hiExcl = lo
	}
	return lo, hiExcl
}

// lexWindow returns the forward-rank window [lo, hiExcl) of the members whose
// bytes fall in the lex band [min, max], defined at equal scores (section 3.2).
// The band's score is the score of the leftmost entry, so the seek is to (band
// score, low member) and the walk runs to the high member, the exact shape
// section 3.2 names; over mixed scores the result is unspecified, matching
// Redis. Two counted descents, so ZLEXCOUNT is hiExcl-lo with no walk.
func (n *nativeStore) lexWindow(min, max lexBound) (lo, hiExcl int) {
	card := n.card()
	if card == 0 {
		return 0, 0
	}
	band, _, _ := n.tree.SelectAt(0) // the tied band's sortable score key

	switch min.inf {
	case lexNegInf:
		lo = 0
	case lexPosInf:
		return card, card
	default:
		r, present := n.tree.Rank(band, min.value, n)
		lo = int(r)
		if min.exclusive && present {
			lo++
		}
	}

	switch max.inf {
	case lexPosInf:
		hiExcl = card
	case lexNegInf:
		hiExcl = 0
	default:
		r, present := n.tree.Rank(band, max.value, n)
		hiExcl = int(r)
		if !max.exclusive && present {
			hiExcl++
		}
	}
	if hiExcl < lo {
		hiExcl = lo
	}
	return lo, hiExcl
}

// maybeRebuild rebuilds the whole dual structure from its live entries once
// removals leave more dead bytes or dead records than live ones, so churn
// cannot grow the store without bound. The rebuild walks the tree in order and
// bulk-loads a fresh store, which also refreshes every separator, so no stale
// ref survives it. Amortized maintenance, not a steady-path cost.
func (n *nativeStore) maybeRebuild() {
	live := n.tbl.Len()
	bytesHeavy := n.deadBytes >= 4096 && n.deadBytes > len(n.slab)/2
	recsHeavy := n.deadRecs >= 1024 && n.deadRecs > live
	if !bytesHeavy && !recsHeavy {
		return
	}
	oldTree, oldRecs, oldSlab := n.tree, n.recs, n.slab
	fresh := newNativeStore(live)
	oldTree.Each(func(_ uint64, ref uint32) bool {
		r := &oldRecs[ref]
		fresh.appendSorted(oldSlab[r.loc:r.loc+r.mlen], math.Float64frombits(r.bits))
		return true
	})
	fresh.seal()
	*n = *fresh
}

// bytes is the structure's allocated footprint for the memory tests: the tree
// arenas, the table's control and ordinal arrays, the record cells, and the
// member slab. The tests subtract the live member bytes and the 8B score per
// entry, which are data in every engine, to read the overhead.
func (n *nativeStore) bytes() int {
	return n.tree.Bytes() + n.tbl.CapSlots()*5 + cap(n.recs)*16 + cap(n.slab)
}
