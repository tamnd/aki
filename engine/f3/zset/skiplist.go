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

	// cold is the demoted-member chunk state (cold.go), nil until the demote pass
	// retiers the first member. A demoted record keeps its cell, its hash slot, and
	// its tree ref (the score stays resident in bits), but its loc turns from a slab
	// offset into a chunk locator with the tierCold high bit set, and its member
	// bytes leave the slab for a cold chunk. bytesOf routes a cold record's byte
	// reads through a pread; every other record reads the slab exactly as before, so
	// a zset with no cold tier (cold == nil, no loc carries tierCold) is
	// byte-identical to the M0-M6 native band.
	cold *coldChunks

	// stable buffers a cold member's bytes when a delete path must pass them into a
	// hash or tree probe: the probe's own Match preads other candidates into the
	// shared cold scratch, which would clobber a member that aliased it, so a cold
	// delete copies the member here first. Owner-local, reused across deletes.
	stable []byte

	// Removed records are never reused and their bytes never move in place: a
	// tree separator is a copy of a boundary key and legitimately outlives the
	// entry it was copied from, so its ref must keep resolving to the original
	// member bytes for routing compares. Reclamation is therefore a wholesale
	// rebuild (maybeRebuild) once dead records or dead bytes outweigh live
	// ones, which rebuilds the separators too, so churn stays amortized O(1)
	// per op and nothing dangles.
	deadBytes int // slab bytes behind removed records
	deadRecs  int // removed record cells awaiting the rebuild

	// scatteredInserts estimates how far the slab's insertion order has diverged
	// from the tree's rank order: it counts the inserts and rescores since the
	// last slab co-location, so a store built by incremental ZADD carries a full
	// cardinality of divergence while one promoted in sorted order (seal) or just
	// co-located starts at zero. maybeColocate reads it to decide when an ordered
	// read should reorder the slab (see colocateSlab).
	scatteredInserts int

	// readElems accumulates the elements ordered reads have streamed since the
	// last co-location. A reorder is O(card), so it fires only once reads have
	// touched a cardinality's worth of elements, which caps the reorder at one
	// sequential copy per read element amortized and keeps a write-heavy, rarely
	// read store (few read elements) from ever paying for a reorder it will not
	// recover. Reset with scatteredInserts on each co-location.
	readElems int

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

// bytesOf resolves a record to its member bytes, the one tier-aware accessor every
// read path funnels through: a resident record aliases the slab as before, a cold
// record preads its owning chunk into the shared cold scratch. The returned bytes
// are valid only until the next write or the next cold read, the same single-call
// lifetime the slab alias always carried. A torn cold frame yields an empty slice,
// which a membership compare reads as a miss. The tierCold check is one bit test, so
// a store with no cold tier keeps the exact slab read it had before.
func (n *nativeStore) bytesOf(r *natRecord) []byte {
	if r.loc&tierCold != 0 {
		m, _ := n.cold.member(r.loc)
		return m
	}
	return n.slab[r.loc : r.loc+r.mlen]
}

// stableBytes returns a member's bytes safe to pass into a hash or tree probe: a
// resident member aliases the slab (probes never touch the slab), a cold member is
// copied into n.stable so the probe's own cold preads cannot clobber it mid-compare.
// The delete paths use it because they resolve a member from the store and then hand
// it back to tbl.Delete and tree.Delete, whose Match tie-breaks pread other cold
// members through the same scratch.
func (n *nativeStore) stableBytes(r *natRecord) []byte {
	if r.loc&tierCold != 0 {
		m, _ := n.cold.member(r.loc)
		n.stable = append(n.stable[:0], m...)
		return n.stable
	}
	return n.slab[r.loc : r.loc+r.mlen]
}

// dropBytes accounts a just-removed member's storage: a resident member leaves dead
// slab bytes for the churn rebuild to reclaim, a cold member leaves a dead entry in
// its chunk that the promotion repack reclaims, marked on the directory so a later
// pass finds it. The record cell itself is counted dead by the caller either way.
func (n *nativeStore) dropBytes(r *natRecord, m []byte) {
	if r.loc&tierCold != 0 {
		n.cold.markDirty(discOf(scoreKey(math.Float64frombits(r.bits)), m))
		return
	}
	n.deadBytes += int(r.mlen)
}

// Match confirms a tag hit: the member stored at ord must equal key. It is the
// structs.Set half the table probes through, and it allocates nothing for a
// resident member.
func (n *nativeStore) Match(ord uint32, key []byte) bool {
	return bytes.Equal(n.bytesOf(&n.recs[ord]), key)
}

// Rehash recomputes a member's hash from its bytes for a table resize, since
// the record caches none.
func (n *nativeStore) Rehash(ord uint32) uint64 {
	return store.Hash(n.bytesOf(&n.recs[ord]))
}

// Member resolves a tree ref back to its member bytes, the tie-break callback
// the tree invokes only when two keys share a score.
func (n *nativeStore) Member(ref uint32) []byte {
	return n.bytesOf(&n.recs[ref])
}

func (n *nativeStore) card() int { return n.tbl.Len() }

// hasResident reports whether the band still holds a member whose bytes live in the
// slab, the demote trigger's cheap "anything left to shed" probe. A cold member
// contributes no live slab bytes (demote marks them dead, the rebuild frees them),
// so the live slab, the slab length past its dead bytes, is exactly the resident
// members' bytes; a positive live slab means one demote quantum can still move
// bytes to the cold tier. O(1), so demoteVictim can weigh every zset at an
// over-budget boundary without a walk.
func (n *nativeStore) hasResident() bool { return len(n.slab) > n.deadBytes }

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
	n.scatteredInserts++ // slab appended out of rank order, one member diverged
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
	n.scatteredInserts++ // the member's rank moved; its slab position is now stale
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
	n.dropBytes(r, m)
	n.deadRecs++
	n.maybeRebuild()
	return true
}

// pop removes up to count entries from an end of the native band, handing each
// popped member (aliasing the slab) and its score to emit in pop order: ascending
// from the low end when min, descending from the high end otherwise. It is the
// ZPOPMIN/ZPOPMAX-count and ZMPOP drain (spec 2064/f3/12 section 6.7). Each step
// is one fused tree pop (single descent, count fixup and rebalance in the same
// pass) plus the member-hash delete for the same member, so the dual structure
// stays coordinated. The member is handed to emit before the hash delete and
// before any rebuild, since a rebuild moves the slab out from under the alias.
// The reclamation runs once at the end, not per element, so a large drain pays
// one amortized rebuild at most.
func (n *nativeStore) pop(min bool, count int, emit func(m []byte, score float64)) int {
	popped := 0
	for popped < count {
		var (
			ref uint32
			ok  bool
		)
		if min {
			_, ref, ok = n.tree.PopMin()
		} else {
			_, ref, ok = n.tree.PopMax()
		}
		if !ok {
			break
		}
		r := &n.recs[ref]
		m := n.stableBytes(r)
		emit(m, math.Float64frombits(r.bits))
		n.tbl.Delete(store.Hash(m), m, n)
		n.dropBytes(r, m)
		n.deadRecs++
		popped++
	}
	if popped > 0 && n.card() > 0 {
		n.maybeRebuild()
	}
	return popped
}

// at resolves the member at forward rank idx to its bytes (aliasing the slab)
// and raw score bits, the ZRANDMEMBER draw's rank-to-member step: one counted
// select on the tree, then the record read. The caller guarantees idx is in
// range.
func (n *nativeStore) at(idx int) ([]byte, uint64) {
	_, ref, _ := n.tree.SelectAt(uint64(idx))
	r := &n.recs[ref]
	return n.bytesOf(r), r.bits
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
	n.scatteredInserts = 0 // appendSorted laid the slab in sorted (rank) order
}

// each visits every member in ascending zset order. The member bytes alias the
// slab and are valid only until the next write.
func (n *nativeStore) each(fn func(m []byte, score float64)) {
	n.tree.Each(func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.bytesOf(r), math.Float64frombits(r.bits))
		return true
	})
}

// eachUntil visits every member in ascending zset order, stopping early when fn
// returns false. It is the early-exit form each lacks, backing the algebra
// probe and merge loops (section 6.12) and ZINTERCARD's LIMIT stop. Member bytes
// alias the slab and are valid only until the next write.
func (n *nativeStore) eachUntil(fn func(m []byte, s float64) bool) {
	n.tree.Each(func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		return fn(n.bytesOf(r), math.Float64frombits(r.bits))
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

// colocateFloor is the cardinality below which the record and slab arrays fit in
// cache, so the insertion-order scatter is free and reordering would only add its
// transient without buying a sequential-read win: lab 09 (labs/f3/m2) measured
// the scatter as a 1M-scale, memory-bound effect, with 10k and 100k within cache
// noise. The GamingPC box A/B of the zrange cells tunes it.
const colocateFloor = 1 << 16

// colocateSlab rewrites the member slab in tree (rank) order and repoints each
// record's loc at its new position, so an ordered walk reads member bytes
// sequentially instead of chasing insertion-order offsets across cold memory.
// This is architecture A of the ZRANGE co-location plan (spec 2064/f3
// milestones/M2-zrange-colocation-plan.md, lab 09): it moves only slab bytes and
// rec.loc fields, never a record ordinal, so ZSCAN's downward record cursor and
// the tree's separator refs (which resolve a member through recs[ref].loc) stay
// valid, and it adds no steady memory since the fresh slab is the same size. It
// runs only with no dead records, so every live record appears in the tree
// exactly once and there is nothing to compact (the churn rebuild owns that);
// the transient second slab it allocates is the same shape as rebuild's. It is
// owner-serial, so it needs no lock.
func (n *nativeStore) colocateSlab() {
	if n.deadRecs != 0 {
		return
	}
	fresh := make([]byte, 0, len(n.slab))
	n.tree.Each(func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		if r.loc&tierCold != 0 {
			return true // cold member has no slab bytes; its locator stays put
		}
		loc := uint32(len(fresh))
		fresh = append(fresh, n.slab[r.loc:r.loc+r.mlen]...)
		r.loc = loc
		return true
	})
	n.slab = fresh
	n.scatteredInserts = 0
	n.readElems = 0
}

// maybeColocate reorders the slab into rank order on an ordered read once the
// insertion order has diverged enough for the scatter to bite and reads have paid
// for the reorder. readElems is the number of elements the current read streams.
// It fires when every one of these holds:
//
//   - the store has no dead records, since the churn rebuild owns the layout
//     while removals are pending;
//   - the zset is large enough for the scatter to leave cache (colocateFloor);
//   - at least card/8 members have been inserted or rescored since the last
//     reorder, so a single insert into a settled zset does not reorder a slab
//     that is still almost entirely in place;
//   - ordered reads have streamed at least a cardinality of elements since the
//     last reorder, so the O(card) reorder is amortized to one sequential copy
//     per read element and a write-heavy, rarely read store never pays for it.
//
// Called at the head of the ordered walks so the walk itself reads the
// co-located slab.
func (n *nativeStore) maybeColocate(readElems int) {
	if n.deadRecs != 0 {
		return
	}
	card := n.card()
	if card < colocateFloor {
		return
	}
	n.readElems += readElems
	if 8*n.scatteredInserts < card || n.readElems < card {
		return
	}
	n.colocateSlab()
}

// walkRange streams entries at forward ranks lo..hi inclusive in ascending
// order, handing each member (aliasing the slab, valid until the next write)
// and its raw score bits to fn. It seeks to lo with a counted select then
// follows the leaf chain over just the window (section 6.4), so a far ZRANGE is
// a seek plus a bounded walk, not a full scan, and it allocates nothing.
func (n *nativeStore) walkRange(lo, hi int, fn func(m []byte, bits uint64)) {
	n.maybeColocate(hi - lo + 1)
	remaining := hi - lo + 1
	n.tree.WalkFromRank(uint64(lo), func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.bytesOf(r), r.bits)
		remaining--
		return remaining > 0
	})
}

// walkRangeRev streams the same forward-rank window hi..lo in descending order,
// the ZRANGE REV and ZREVRANGE walk. It descends to the high end and walks back
// with the tree's reverse leaf walk, re-seeking at most once per leaf boundary.
func (n *nativeStore) walkRangeRev(lo, hi int, fn func(m []byte, bits uint64)) {
	n.maybeColocate(hi - lo + 1)
	remaining := hi - lo + 1
	n.tree.WalkFromRankRev(uint64(hi), func(_ uint64, ref uint32) bool {
		r := &n.recs[ref]
		fn(n.bytesOf(r), r.bits)
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
// removals leave more dead bytes or dead records than live ones, so churn cannot
// grow the store without bound. It reclaims into a fresh store that keeps the
// record order of the live members rather than re-sorting them, and refreshes
// every tree separator, so no stale ref survives it. Amortized maintenance, not a
// steady-path cost.
func (n *nativeStore) maybeRebuild() {
	live := n.tbl.Len()
	bytesHeavy := n.deadBytes >= 4096 && n.deadBytes > len(n.slab)/2
	recsHeavy := n.deadRecs >= 1024 && n.deadRecs > live
	if !bytesHeavy && !recsHeavy {
		return
	}
	n.rebuild(live)
}

// rebuild compacts the dual structure to its live members, preserving their
// record order instead of re-sorting them. The stable order is what keeps an
// interleaved ZSCAN's downward record cursor honest (spec 2064/f3/12 section
// 6.11): a live member's record index can only fall across a rebuild, because the
// only cells that leave are the dead ones below it, so a record still below the
// cursor stays below it and the at-least-once guarantee survives the reclaim. The
// tree is rebuilt sorted by a bulk load with its refs remapped to the new record
// ordinals, so the ordered views stay correct while the scan order stays stable.
func (n *nativeStore) rebuild(live int) {
	liveMark := make([]bool, len(n.recs))
	n.tree.Each(func(_ uint64, ref uint32) bool {
		liveMark[ref] = true
		return true
	})
	remap := make([]uint32, len(n.recs))
	// The fresh store shares the cold chunk state: a demoted member survives a
	// rebuild still cold, its record cell relocated but its chunk locator and its
	// packed bytes untouched, so the reclaim never drags the cold tier resident.
	fresh := &nativeStore{tbl: structs.MakeTable(live), cold: n.cold}
	if live > 0 {
		fresh.recs = make([]natRecord, 0, live)
		fresh.slab = make([]byte, 0, len(n.slab)-n.deadBytes)
	}
	for old := 0; old < len(n.recs); old++ {
		if !liveMark[old] {
			continue
		}
		r := n.recs[old]
		newOrd := uint32(len(fresh.recs))
		remap[old] = newOrd
		if r.loc&tierCold != 0 {
			// A cold member keeps its chunk locator; only its record cell moves. Its
			// bytes are preadd once for the fresh hash slot and never copied to the
			// slab, so a cold-heavy zset rebuilds without materializing its cold bytes.
			m := n.bytesOf(&r)
			fresh.recs = append(fresh.recs, natRecord{loc: r.loc, mlen: r.mlen, bits: r.bits})
			fresh.tbl.Insert(store.Hash(m), newOrd, fresh)
			continue
		}
		m := n.slab[r.loc : r.loc+r.mlen]
		loc := uint32(len(fresh.slab))
		fresh.slab = append(fresh.slab, m...)
		fresh.recs = append(fresh.recs, natRecord{loc: loc, mlen: r.mlen, bits: r.bits})
		fresh.tbl.Insert(store.Hash(m), newOrd, fresh)
	}
	entries := make([]structs.Entry, 0, live)
	n.tree.Each(func(score uint64, ref uint32) bool {
		entries = append(entries, structs.Entry{Score: score, Ref: remap[ref]})
		return true
	})
	fresh.tree = structs.BulkLoad(entries)
	// The fresh slab is in record (insertion) order, not rank order, so a later
	// ordered read can re-co-locate it; mark it fully diverged. deadRecs is zero
	// on the fresh store, so the next qualifying walk is free to reorder.
	fresh.scatteredInserts = live
	*n = *fresh
}

// scanPage returns one ZSCAN page over the member records and the cursor to
// resume from, 0 when the scan completes (spec 2064/f3/12 section 6.11). The
// cursor rides the record array downward, the same downward-cursor convention
// SSCAN established (set/scan.go): records [b, len) have been returned by earlier
// pages and [0, b) remain, and a page examines up to count records from the
// boundary down. New members append above the boundary, on the already-scanned
// side, so growth mid-scan is never revisited; a removed member leaves a dead
// record cell in place that this walk skips, so a member present for the whole
// scan keeps its index and is returned at least once. A reclaim rebuild only
// lowers a live record's index (rebuild, above), so the guarantee survives it.
// COUNT bounds records examined, not members emitted; MATCH filters the survivors.
func (n *nativeStore) scanPage(cursor uint64, count int, match []byte, emit func(m []byte, bits uint64)) uint64 {
	total := uint64(len(n.recs))
	if total == 0 {
		return 0
	}
	b := total
	if cursor != 0 && cursor < b {
		b = cursor
	}
	lo := uint64(0)
	if b > uint64(count) {
		lo = b - uint64(count)
	}
	for i := b; i > lo; i-- {
		ord := uint32(i - 1)
		r := &n.recs[ord]
		m := n.stableBytes(r)
		if !n.liveAt(ord, m) {
			continue
		}
		if match != nil && !globMatch(match, m) {
			continue
		}
		emit(m, r.bits)
	}
	return lo
}

// liveAt reports whether record ord is still a live member: its bytes must probe
// back to this same ordinal. A removed record keeps valid bytes until the next
// rebuild, since records never move in place, so the slab read is always safe; a
// member re-added after removal takes a fresh ordinal, so an equal-bytes probe
// that resolves to a different ordinal correctly reads the old cell as dead.
func (n *nativeStore) liveAt(ord uint32, m []byte) bool {
	got, ok := n.tbl.Find(store.Hash(m), m, n)
	return ok && got == ord
}

// removeRange deletes the entries at forward ranks [lo, hiExcl) as a bounded tree
// operation and reports how many left (spec 2064/f3/12 section 6.9). It resolves
// to a loop of fused single-descent rank deletes (tree.DeleteAt), the same
// loop-of-fused-pops shape lab 04 froze for the pops (labs/f3/m2): the DeleteAt
// descent routes on the subtree counts with no member compare and fixes the one
// count path and any underflow in the same pass, and the spine it re-walks stays
// hot across the window. The loop runs high rank to low, so each delete takes the
// current right edge of the shrinking window and the ranks still below it do not
// shift. Each removed entry still pays its member-hash delete and dead accounting,
// the honest O(w) floor, and one amortized rebuild runs at the end rather than per
// element, so a native ZREMRANGEBY* has no deferred teardown to shoulder a p99.
func (n *nativeStore) removeRange(lo, hiExcl int) int {
	removed := 0
	for r := hiExcl - 1; r >= lo; r-- {
		_, ref, ok := n.tree.DeleteAt(uint64(r))
		if !ok {
			break
		}
		rec := &n.recs[ref]
		m := n.stableBytes(rec)
		n.tbl.Delete(store.Hash(m), m, n)
		n.dropBytes(rec, m)
		n.deadRecs++
		removed++
	}
	if removed > 0 && n.card() > 0 {
		n.maybeRebuild()
	}
	return removed
}

// bytes is the structure's allocated footprint for the memory tests: the tree
// arenas, the table's control and ordinal arrays, the record cells, and the
// member slab. The tests subtract the live member bytes and the 8B score per
// entry, which are data in every engine, to read the overhead.
func (n *nativeStore) bytes() int {
	b := n.tree.Bytes() + n.tbl.CapSlots()*5 + cap(n.recs)*16 + cap(n.slab)
	if n.cold != nil {
		b += int(n.cold.residentBytes())
	}
	return b
}
