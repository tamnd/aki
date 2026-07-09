// Package setvecbuild is the lab behind the STORE destination dense-vector bulk-build fix. A set
// STORE (SINTERSTORE/SUNIONSTORE/SDIFFSTORE) into a reused destination key clears the old set first
// (clearSetRows drops the destination's dense member vector wholesale, CollRandDrop) and then
// rebuilds a fresh vector from the result. This lab measures the three build shapes the destination
// vector went through, because on the aki-bench algebra store sweep the rebuild is the shared cost the
// three STORE forms all pay and the one Redis never pays: Redis swaps the destination object in O(1),
// while aki tears the vector down and grows it back on every single STORE.
//
// The original path rebuilt one member at a time: every stored member called CollRandInsertOff, which
// appended its arena offset to the vector through memberVec.add. That is expensive per member for three
// compounding reasons, all inside memberVec.add:
//
//   - The back-index is a Go map[uint64]int. Rebuilding it from empty is k map inserts that rehash and
//     grow the bucket array several times as the map fills, the classic cost of building a map without a
//     size hint even though the final size is known the moment the result cardinality is known.
//   - Every add republishes the read snapshot (publish allocates a fresh vecSlots header so a lock-free
//     draw loads a consistent slice), so a k-member build allocates k snapshot headers when a lock-free
//     reader only ever needs to see the final one. During a STORE the destination's stripe lock is held,
//     so no reader can even observe an intermediate snapshot.
//   - The slots slice starts at a 64 capacity and doubles as it fills, so a large result copies the
//     backing array log2(k/64) times.
//
// The first fix built the whole vector in one pass from the offsets the STORE already has in hand (the
// bulk sorted-hash build collects every stored member's (hash, offset) into a per-partition bucket, so
// the offsets to seed the vector are already gathered): size slots and the back map to the exact final
// cardinality, fill both in a tight loop, publish once. That dropped the per-member rehash, the
// per-member snapshot allocation, and the doubling copies, but it still built the whole back map at STORE
// time, and at large cardinalities that construction dominated. A big map[uint64]int roots a wide GC scan
// and fills with poor cache locality, so building it eagerly on every STORE was measurably slower than
// the k tiny short-lived snapshot allocations the per-member path made (those die young, which Go's
// allocator handles cheaply). The map was the cost, not the vector.
//
// The second fix is the one that ships: the back index is only ever read by the mutation paths
// (memberVec.add/remove/retierSlot), never by any draw (SRANDMEMBER/SPOP read only the slots snapshot)
// or membership read (SISMEMBER uses the hash index, not the vector). So the bulk build copies the
// offsets into slots in one append, publishes once, and leaves back nil. A STORE destination that is only
// ever drawn or re-cleared (the aki-bench sweep re-STOREs the same dst every iteration) never builds the
// map at all; the first mutation on it materializes the map once through ensureBack from the slots
// snapshot. This drops both the per-member work of the incremental build and the whole-map construction
// the eager bulk still paid.
//
// This lab models memberVec's exact shape (an atomically-published vecSlots snapshot over a slots slice
// plus a back map, with the same lazy ensureBack discipline) so the build shapes are measured on the real
// data structure, not a proxy. It compares:
//
//   - perMemberBuild: newVec(64) then add each offset, the original path (drop then grow from empty).
//   - eagerBulkBuild: one pass sized to the exact cardinality, building slots and back, publish once.
//   - lazyBulkBuild:  copy offsets into slots, publish once, leave back nil (the shipped path).
//
// # Result (Apple M4, go test -bench . ./labs/setvecbuild/)
//
//	BenchmarkVecBuild/k=500    perMember 23431 ns 516 allocs -> eager 6333 ns 7 allocs -> lazy 322 ns 3 allocs
//	BenchmarkVecBuild/k=5000   perMember 240514 ns 5053 allocs -> eager 61950 ns 21 allocs -> lazy 2258 ns 3 allocs
//	BenchmarkVecBuild/k=50000  perMember 2463551 ns 50289 allocs -> eager 810843 ns 133 allocs -> lazy 20966 ns 3 allocs
//
// The lazy build's isolated speed overstates the end-to-end win because the real STORE still does the
// merge/probe, the sorted-hash build, and clearSetRows around the vector build; on the in-process store
// compute bench dropping the eager map for the lazy one moved SINTERSTORE from 152us to 100us at 1000
// members and from 7.0ms to 6.2ms at 100000, faster at every size with no regression. The stored
// cardinalities k=500/5000/50000 are the result sizes of the aki-bench algebra store sweep at
// 1000/10000/100000 members (SINTER and SDIFF each yield members/2). The fix lives in
// engine/f1raw/randvec.go (CollRandBulkBuild + ensureBack) wired from storeAlgebra in
// f1srv/set_algebra_store.go, which drops the per-member CollRandInsertOff in favour of one bulk build
// from the collected offsets.
package setvecbuild
