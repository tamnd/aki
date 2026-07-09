package f1raw

import (
	"slices"
	"sync/atomic"
)

// The sorted-hash side array is the set type's fourth resident structure (spec
// 2064/f1_rewrite_ltm/24), and it exists for exactly one command family: the set algebra
// reads SINTER/SDIFF/SINTERCARD. Doc 18 gave the set an O(1) draw off an unordered dense
// vector, doc 20 dropped the last sorted structure the set carried to win the point ops,
// and in doing so it deleted the sorted-merge arm SINTER used, leaving the algebra path to
// probe the smallest source at random, one DRAM miss per member, which is exactly what
// Redis and Valkey do, so no probe can beat them by 2x. The lever the labs/setintersect and
// labs/seteager microbenchmarks proved is a two-pointer merge over two hash-sorted arrays:
// it reads both sets sequentially (prefetcher-served) where the probe misses DRAM per
// member, it is 3-5x the probe single-threaded, and because two same-P partitioned sets
// intersect partition-by-partition it fans out across cores where the rivals run one thread.
// The one condition, and the whole reason this structure exists, is that the merge only wins
// if the set is ALREADY in hash order: sorting per call is ~10x slower than the probe.
//
// This structure is that hash order, held beside the dense vector, never in front of it. The
// vector stays authoritative for SPOP/SMEMBERS/SSCAN/SRANDMEMBER (doc 20); this array is read
// only by the algebra merge. It is a derived acceleration structure like the vector: never
// persisted, rebuilt from the member rows on demand, and maintained off the reply path so a
// SADD returns at the vector's append speed. The seteager numbers settled the shape: a single
// flat sorted array per set has the ideal merge (5.80 ms) but a ~90 us O(n) SADD; a skiplist
// has the cheapest write (83 ns) but a 183 ms merge because an in-order walk chases a pointer
// per node; per-partition sorted arrays win both, an O(n/P) fold and a merge identical to the
// single array (5.78 vs 5.80 ms), because the merge's whole advantage is sequential memory and
// only a packed array has sequential memory. So the container is a per-partition sorted
// []uint64 of member hashes, and this file is that container plus its batch fold and the merge.
//
// Two parallel arrays, not an array of structs. The merge's hot loop compares only h[i] against
// h[j], so keeping the hashes densely packed at eight bytes each with no padding is what lets the
// prefetcher stream them, the layout labs/seteager's BenchmarkMergeFlat measured. The off array
// is touched only on a hash match (to byte-confirm the members, section below) and is otherwise
// cold, so splitting it out of the comparison stream costs nothing and keeps the stream tight.
//
// Ordering and the byte-confirm. The arrays are sorted by the total order (hash, off): ties on
// the 64-bit member hash are broken by the arena offset so the sort is deterministic and a run of
// equal hashes is contiguous. Two DISTINCT members can share a 64-bit hash (astronomically rare
// per pair but a correctness obligation, not a performance one), so a hash match in the merge is a
// candidate, not a result: the merge scans the short run of equal hashes on both sides and calls a
// caller-supplied confirm that resolves the two arena offsets to their member bytes and compares
// them, exactly as a fingerprint probe rejects a hash hit that fails the byte-check. Runs of length
// one are the overwhelming case, so the cross-confirm is O(1) amortized; the run scan is the general
// correct form. The offsets are also how a match resolves the member bytes for a SINTERSTORE
// destination, and how a stale offset (a member removed since the last fold) is filtered: the live
// hash index remains the membership authority, this array only orders candidates.
//
// Maintenance is a batch fold, never a per-op sorted insert. On the reply path SADD/SREM append a
// foldDelta (hash, off, add-or-remove) to a per-partition journal under the stripe lock they already
// hold, one slice append, no sort. The shard worker later drains the journal into foldBatch, which
// sorts the batch's adds among themselves once and merges them into the existing sorted array in a
// single O(n/P + k) pass while dropping the batch's removes, then publishes a fresh snapshot. That
// is what keeps "eager" meaning always-materialized rather than built-on-read: the array is kept
// continuously current off the reply path, so a read-then-write-then-read workload never pays a
// rebuild, and a SADD never pays the ~205 ns sorted insert the seteager lab measured.
//
// Concurrency. foldBatch always allocates fresh arrays and never mutates a published snapshot in
// place, so a merge holding an older snapshot is undisturbed and the read path needs no atomic slot
// access the way the vector's in-place swap-remove does. The working arrays are touched only by the
// folder (one goroutine per partition's drain, serialized by the shard worker), and the published
// snapshot is swapped through an atomic pointer the merge loads lock-free. gen records the vector
// generation the array was last folded up to, so a merge can tell whether the array is current and,
// if it is behind, fold the short pending journal inline or fall back to the doc-20 probe (spec 24
// section 4.3); this file stores and exposes gen, and the wiring slice consumes it.

// sortedSnap is an immutable published snapshot of a partition's members in ascending
// (hash, off) order, the thing the merge reads. h[i] pairs with off[i]; len(h) == len(off).
// A reader atomic-loads the pointer and gets a consistent (h, off) pair, then walks both with
// no lock. foldBatch never mutates a published snapshot, so a reader holding one is never torn
// by a concurrent fold. gen is the vector generation this snapshot was folded up to, for the
// staleness check the merge does before trusting it.
type sortedSnap struct {
	h   []uint64
	off []uint64
	gen uint64
}

// sortedHashes is one partition's sorted member-hash array. view is the atomically-published
// snapshot the merge reads lock-free; h and off are the folder's working copies, touched only
// under the partition's fold serialization, kept in ascending (hash, off) order. gen is the last
// vector generation folded in. A freshly-created sortedHashes publishes an empty snapshot so a
// merge against a set that has never been folded reads an empty array rather than a nil pointer.
type sortedHashes struct {
	view atomic.Pointer[sortedSnap]
	h    []uint64
	off  []uint64
	gen  uint64
}

// foldDelta is one pending change the folder must apply to the sorted array, appended to the
// per-partition journal on the reply path by SADD (add=true) and SREM/SPOP/SMOVE-source
// (add=false) under the stripe lock they already hold. hash is hash64(member); off is the
// member row's arena offset, which uniquely identifies the record, so a remove is keyed by off
// and an add-then-remove of the same record within one batch nets out (the add is skipped because
// its off is in the batch's remove set). Appending one of these is the entire cost the sorted
// array adds to the point-op write path.
type foldDelta struct {
	hash uint64
	off  uint64
	add  bool
}

// newSortedHashes builds an empty sorted array with an empty published snapshot. capHint sizes
// the initial working arrays to avoid early regrowth on a partition known to be non-trivial.
func newSortedHashes(capHint int) *sortedHashes {
	sh := &sortedHashes{
		h:   make([]uint64, 0, capHint),
		off: make([]uint64, 0, capHint),
	}
	sh.publish()
	return sh
}

// publish snapshots the current working arrays as the read view. It is called at the end of every
// fold, after the working arrays are the freshly-merged result, so a merge always loads a view that
// is a valid ascending (hash, off) array. The snapshot shares the working backing arrays, but foldBatch
// replaces the working arrays with a fresh allocation on the next fold rather than mutating them in
// place, so a reader holding this snapshot keeps its backing alive and is never overwritten.
func (sh *sortedHashes) publish() {
	sh.view.Store(&sortedSnap{h: sh.h, off: sh.off, gen: sh.gen})
}

// load returns the current published snapshot for a lock-free merge read.
func (sh *sortedHashes) load() *sortedSnap {
	return sh.view.Load()
}

// foldBatch applies a batch of pending deltas to the sorted array in one O(n + k) pass and
// publishes a fresh snapshot, advancing gen to toGen. It is the folder's whole job, run off the
// reply path by the shard worker. The algorithm is the seteager lab's batched bulk insert: collect
// the batch's removes into a set keyed by offset, sort the batch's live adds (those whose offset is
// not also removed in the same batch) by (hash, off) once, then walk the existing array and the
// sorted adds together into a fresh array, skipping any existing entry whose offset was removed. The
// result is sorted by construction because both inputs to the walk are sorted and equal-hash ties
// break on offset consistently. Passing an empty batch still republishes at toGen, which lets the
// folder record that it has caught up to a generation with no structural change (e.g. a batch that
// was entirely add-then-remove).
func (sh *sortedHashes) foldBatch(deltas []foldDelta, toGen uint64) {
	// Partition the batch into removes (a set of offsets to drop) and live adds. An add whose
	// offset also appears as a remove in the same batch was added and removed before the folder
	// ran, so it is dropped from both: it is skipped from the adds here and, since it is not in
	// the existing array, the remove set entry for it simply matches nothing.
	var removed map[uint64]struct{}
	for i := range deltas {
		if !deltas[i].add {
			if removed == nil {
				removed = make(map[uint64]struct{}, len(deltas))
			}
			removed[deltas[i].off] = struct{}{}
		}
	}
	adds := make([]hashOff, 0, len(deltas))
	for i := range deltas {
		d := deltas[i]
		if !d.add {
			continue
		}
		if _, gone := removed[d.off]; gone {
			continue
		}
		adds = append(adds, hashOff{h: d.hash, off: d.off})
	}
	slices.SortFunc(adds, cmpHashOff)

	// Merge the surviving existing entries with the sorted adds into fresh arrays. Fresh arrays,
	// not an in-place edit, are what keep a concurrent merge holding the old snapshot correct.
	oldH, oldOff := sh.h, sh.off
	n := len(oldH) + len(adds)
	nh := make([]uint64, 0, n)
	noff := make([]uint64, 0, n)
	i, j := 0, 0
	for i < len(oldH) && j < len(adds) {
		if removed != nil {
			if _, gone := removed[oldOff[i]]; gone {
				i++
				continue
			}
		}
		if oldH[i] < adds[j].h || (oldH[i] == adds[j].h && oldOff[i] <= adds[j].off) {
			nh = append(nh, oldH[i])
			noff = append(noff, oldOff[i])
			i++
		} else {
			nh = append(nh, adds[j].h)
			noff = append(noff, adds[j].off)
			j++
		}
	}
	for ; i < len(oldH); i++ {
		if removed != nil {
			if _, gone := removed[oldOff[i]]; gone {
				continue
			}
		}
		nh = append(nh, oldH[i])
		noff = append(noff, oldOff[i])
	}
	for ; j < len(adds); j++ {
		nh = append(nh, adds[j].h)
		noff = append(noff, adds[j].off)
	}

	sh.h = nh
	sh.off = noff
	sh.gen = toGen
	sh.publish()
}

// hashOff is one member's (hash, offset) pair, used to sort a fold batch's adds before merging.
type hashOff struct {
	h   uint64
	off uint64
}

// cmpHashOff is the total order the sorted array is kept in, (hash, off) ascending: ties on the
// 64-bit member hash break on the arena offset so a run of equal hashes is contiguous and the sort
// is deterministic. foldBatch sorts a batch's adds by it, and build sorts a whole set by it, so an
// array built in bulk is byte-identical to one grown one fold at a time.
func cmpHashOff(a, b hashOff) int {
	if a.h != b.h {
		if a.h < b.h {
			return -1
		}
		return 1
	}
	if a.off != b.off {
		if a.off < b.off {
			return -1
		}
		return 1
	}
	return 0
}

// build replaces the sorted array with one built from entries in a single sort, discarding whatever
// was there, and publishes a fresh snapshot at toGen. It is the bulk counterpart to foldBatch: a
// caller that already holds a set's whole member list (a SINTERSTORE destination it just wrote, say)
// folds it once here instead of appending a per-member journal and paying an incremental fold per
// member, which regrows the flat array O(n) times for an O(n^2) build. Fresh arrays, like foldBatch,
// so a merge holding an older snapshot is undisturbed. entries is sorted in place (the caller's slice
// is scratch it does not keep). Passing nil clears the array to empty, which is how a destination that
// a STORE emptied drops its stale order.
func (sh *sortedHashes) build(entries []hashOff, toGen uint64) {
	slices.SortFunc(entries, cmpHashOff)
	nh := make([]uint64, len(entries))
	noff := make([]uint64, len(entries))
	for i := range entries {
		nh[i] = entries[i].h
		noff[i] = entries[i].off
	}
	sh.h = nh
	sh.off = noff
	sh.gen = toGen
	sh.publish()
}

// confirmFunc resolves two arena offsets to their member bytes and reports whether they name the
// same member. The merge calls it only on a 64-bit hash match, to reject the astronomically rare
// case of two distinct members sharing a hash and to filter a stale offset that no longer resolves
// to a live member. offA comes from the A operand's array, offB from the B operand's.
type confirmFunc func(offA, offB uint64) bool

// intersectEmit walks two ascending (hash, off) snapshots and, for each member present in both,
// calls onPair with the candidate (A, B) offsets in A's ascending order. onPair byte-confirms the
// pair and, when it names the same member, emits it and returns true so the walk stops scanning that
// A member's B run. Folding confirm and emit into one callback lets the caller resolve the A member's
// bytes once for both the confirm and the emit instead of resolving the offset a second time to
// materialize it (the SINTER hot path charged that third keyAtTiered per matched member). It is the
// SINTER merge inner loop over one partition pair: two forward cursors, sequential reads the
// prefetcher serves, no random probe. On a hash match it scans the run of equal hashes on both sides;
// runs of length one are the common case, so the cross-scan is O(1) amortized, and both cursors
// advance past the whole matched run so a run is processed once. The P partition merges share
// nothing, so they run in parallel.
func intersectEmit(a, b *sortedSnap, onPair func(offA, offB uint64) bool) {
	i, j := 0, 0
	na, nb := len(a.h), len(b.h)
	for i < na && j < nb {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			hv := a.h[i]
			iend := i
			for iend < na && a.h[iend] == hv {
				iend++
			}
			jend := j
			for jend < nb && b.h[jend] == hv {
				jend++
			}
			for x := i; x < iend; x++ {
				for y := j; y < jend; y++ {
					if onPair(a.off[x], b.off[y]) {
						break
					}
				}
			}
			i, j = iend, jend
		}
	}
}

// intersectCount is intersectEmit without materializing the result, for SINTERCARD. It stops early
// once the running count reaches limit (pass limit <= 0 for no bound), which is exactly SINTERCARD's
// LIMIT. It returns the number of confirmed members in both, capped at limit when a positive limit
// is given.
func intersectCount(a, b *sortedSnap, confirm confirmFunc, limit int) int {
	count := 0
	i, j := 0, 0
	na, nb := len(a.h), len(b.h)
	for i < na && j < nb {
		switch {
		case a.h[i] < b.h[j]:
			i++
		case a.h[i] > b.h[j]:
			j++
		default:
			hv := a.h[i]
			iend := i
			for iend < na && a.h[iend] == hv {
				iend++
			}
			jend := j
			for jend < nb && b.h[jend] == hv {
				jend++
			}
			for x := i; x < iend; x++ {
				for y := j; y < jend; y++ {
					if confirm(a.off[x], b.off[y]) {
						count++
						if limit > 0 && count >= limit {
							return count
						}
						break
					}
				}
			}
			i, j = iend, jend
		}
	}
	return count
}

// diffEmit walks two ascending snapshots and calls emit with the A-side offset of every member of A
// that is NOT present in B (byte-confirmed), in A's ascending order. It is the SDIFF(A, B) merge over
// one partition pair. A member of A survives the difference when its hash is below B's cursor with no
// match, or when its hash matches a B run but no B member in the run byte-confirms equal (a bare hash
// collision, so the members differ and A's member is kept). Multi-source SDIFF applies this once per
// excluding operand over A's surviving candidates.
func diffEmit(a, b *sortedSnap, confirm confirmFunc, emit func(offA uint64)) {
	i, j := 0, 0
	na, nb := len(a.h), len(b.h)
	for i < na {
		if j >= nb || a.h[i] < b.h[j] {
			emit(a.off[i])
			i++
			continue
		}
		if a.h[i] > b.h[j] {
			j++
			continue
		}
		// Equal hash: A's member survives unless some B member in the equal-hash run confirms equal.
		hv := a.h[i]
		jend := j
		for jend < nb && b.h[jend] == hv {
			jend++
		}
		for i < na && a.h[i] == hv {
			matched := false
			for y := j; y < jend; y++ {
				if confirm(a.off[i], b.off[y]) {
					matched = true
					break
				}
			}
			if !matched {
				emit(a.off[i])
			}
			i++
		}
		j = jend
	}
}

// unionEmit walks two ascending snapshots and calls emitA/emitB with the A-side or B-side offset of
// every distinct member across both, in the merged ascending hash order. It is the SUNION(A, B) merge
// over one partition pair. Every A member is in the union, so each is emitted through emitA. A B member
// is emitted through emitB only when no A member in its equal-hash run byte-confirms equal to it, so a
// member both sets hold is emitted once (from A) and a bare hash collision keeps both distinct members.
// Two side callbacks rather than one let the caller resolve an A offset against A's prefix length and a
// B offset against B's, which differ whenever the two sets have different key lengths. The merge streams
// both arrays sequentially with no per-member hashmap, which is the win over the seen-set probe form that
// pays an O(union) dictionary insert per member.
func unionEmit(a, b *sortedSnap, confirm confirmFunc, emitA, emitB func(off uint64)) {
	i, j := 0, 0
	na, nb := len(a.h), len(b.h)
	for i < na && j < nb {
		switch {
		case a.h[i] < b.h[j]:
			emitA(a.off[i])
			i++
		case a.h[i] > b.h[j]:
			emitB(b.off[j])
			j++
		default:
			hv := a.h[i]
			iend := i
			for iend < na && a.h[iend] == hv {
				iend++
			}
			jend := j
			for jend < nb && b.h[jend] == hv {
				jend++
			}
			for x := i; x < iend; x++ {
				emitA(a.off[x])
			}
			for y := j; y < jend; y++ {
				matched := false
				for x := i; x < iend; x++ {
					if confirm(a.off[x], b.off[y]) {
						matched = true
						break
					}
				}
				if !matched {
					emitB(b.off[y])
				}
			}
			i, j = iend, jend
		}
	}
	for ; i < na; i++ {
		emitA(a.off[i])
	}
	for ; j < nb; j++ {
		emitB(b.off[j])
	}
}
