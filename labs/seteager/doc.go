// Package seteager is the follow-on lab to setintersect. That lab proved the 2x
// lever for a large symmetric SINTER is a two-pointer merge over two hash-sorted
// arrays (~12 ms) instead of a random point-probe (~40 ms), on the one condition
// that the set is ALREADY in hash order: sorting per call is ~10x slower than the
// probe. So the whole question becomes a write-path question. What container keeps
// a SET's members in hash order cheaply enough on SADD/SREM to leave the 12 ms
// merge as the operative read cost, without giving back doc 19's per-key
// partitioning and doc 20's O(1) point ops?
//
// This lab measures the write side and the read side of three candidate containers
// so the engine choice is a number, not a guess.
//
// # The three shapes
//
//   - flat sorted []uint64: one array per set, kept sorted by member hash. A merge
//     reads it straight down, the friendliest possible layout for the prefetcher,
//     so it is the read ceiling. But an insert binary-searches then memmoves the
//     tail, O(n) per SADD, so building an n-member set is O(n^2). This is the naive
//     "just keep it sorted" strawman.
//   - partitioned sorted [][]uint64: P arrays, member m in partition hash(m)&(P-1),
//     each partition kept sorted. This is exactly f1raw's doc 19 partition routing
//     with each partition's members additionally held in hash order. An insert sorts
//     into one partition of ~n/P members, O(n/P), so with P grown to hold the
//     per-partition count near a constant (which doc 19 already does as a set grows)
//     the insert is effectively O(1) and the build is O(n). The key algebra fact:
//     two sets with the SAME P intersect partition-by-partition, because a member in
//     both sets hashes the same and so lands in the same partition index in both, so
//     SINTER is P independent two-pointer merges over sorted arrays, each sequential,
//     and they parallelize across P with no cross-partition coordination.
//   - skiplist of hashes: O(log n) insert, the textbook ordered-map answer and the
//     shape of the oindex doc 20 dropped. Cheap writes, but an in-order read chases a
//     pointer per node, so the merge loses the sequential-streaming advantage that
//     made the array merge win in the first place.
//
// # What the numbers say (Apple M4, GOMAXPROCS=10)
//
// Build an n-member set from empty (ns for the whole build; buildN = 1<<16):
//
//	BenchmarkBuildFlatSorted         122.6 ms   O(n^2) sorted-insert, one array
//	BenchmarkBuildPartitioned/P=64     6.55 ms  O(n^2/P) sorted-insert, 64 partitions
//	BenchmarkBuildPartitioned/P=256    5.01 ms  O(n^2/P) sorted-insert, 256 partitions
//	BenchmarkBuildSkiplist            16.62 ms  O(n log n) skiplist insert
//
// Steady-state single SADD into a set that already holds n members (ns/op, an
// insert-then-remove pair except the skiplist, which is insert only; n = 1<<20):
//
//	BenchmarkInsertFlatSorted        178950 ns  binary search + O(n) tail memmove (~90 us each)
//	BenchmarkInsertPartitioned/P=64    1770 ns  O(n/P) memmove in one partition (~885 ns each)
//	BenchmarkInsertPartitioned/P=256    411 ns  O(n/P) memmove in one partition (~205 ns each)
//	BenchmarkInsertSkiplist              83 ns  O(log n) pointer splice
//
// Merge-intersect two half-overlapping sets, |A|=|B|=1<<20 (ns/op):
//
//	BenchmarkMergeFlat                 5.80 ms  two-pointer over two sorted arrays
//	BenchmarkMergePartitionedSameP     5.78 ms  P independent partition-pair merges
//	BenchmarkMergeSkiplist           183.4 ms   in-order walk of two skiplists
//	BenchmarkProbeBaseline             9.27 ms  random probe, the thing to beat
//
// # The lesson
//
// Four things the numbers settle, and they point at one container.
//
// One: the skiplist is out, and it is the read that kills it, not the write. Its
// O(log n) insert is the cheapest of the lot (83 ns, 2.5x under the P=256 array), which
// is the whole reason the oindex was a skiplist. But its merge is 183 ms, 32x slower than
// the array merge and 20x slower than the probe it was supposed to beat. Every step of an
// in-order skiplist walk chases a pointer to a node the allocator scattered across the heap,
// so it pays a cache miss per element exactly where the array streams. The eager structure
// has to be an ARRAY, because the merge's entire advantage is sequential memory, and a tree
// or skiplist does not have sequential memory no matter how cheap its writes are.
//
// Two: one flat array per set is out, and here it is the write that kills it. Its merge is
// the read ceiling (5.80 ms, the fastest read measured) but a steady insert into a 1M-member
// set is ~90 us, an O(n) tail memmove, and building the set is 122 ms of O(n^2). A SADD that
// pays 90 us cannot clear its own 2x gate. So a single sorted array per set gives the perfect
// read and an impossible write.
//
// Three: partitioning resolves the write-read conflict, and it is already in the engine.
// Splitting the set into P sorted arrays by hash(m)&(P-1) (f1raw's doc 19 layout) cuts the
// insert to an O(n/P) memmove in one partition: 885 ns at P=64, 205 ns at P=256, ~100-400x
// under the single flat array and within a small multiple of the skiplist, on an absolute
// scale sub-microsecond. And it costs the read NOTHING: BenchmarkMergePartitionedSameP is
// 5.78 ms against the single array's 5.80 ms. That equality is the load-bearing measurement.
// It holds because two sets with the SAME P intersect partition-by-partition (a member in both
// hashes the same, so it lands in the same partition index in both), so SINTER becomes P
// independent two-pointer merges, each still forward-only and sequential, and the split adds no
// per-element cost. It also makes the merge embarrassingly parallel: the P partition-pair merges
// share nothing, so a hot SINTER on a many-core box runs them across P cores while Redis and
// Valkey run one thread. That parallelism, not the single-thread merge, is where the 2x lives:
// the single-thread merge is only 1.6x over this lab's idealized compact probe (5.80 vs 9.27 ms),
// but the compact probe is the friendliest case; against the realistic diluted composite probe
// f1srv actually runs (setintersect's 40-73 ms) the single-thread merge is already 3-5x, and the
// partition parallelism multiplies that against single-threaded rivals on a hot key.
//
// Four, the SADD tax is real and the fix is to fold order asynchronously. Even at 205 ns (P=256)
// the sorted-array maintenance is not free next to a bare O(1) vector append, and on the SADD
// hot path it would eat into SADD's own margin. The resolution keeps "eager" meaning always
// materialized, not built-on-read, while moving the sorted insert OFF the reply path: the shard
// worker's async folder (the same mechanism the list and hash overlays use) maintains the
// per-partition sorted hash arrays behind the O(1) unsorted vector SADD already appends to, so a
// SADD returns at vector-append speed and the algebra path always finds an up-to-date sorted array.
// Lazy-on-first-algebra-call was the rejected alternative (the setintersect lab's option two); this
// keeps the array current continuously instead, so a read-then-write-then-read workload never pays
// a rebuild.
//
// The container the numbers choose: per-partition sorted []uint64 of member hashes (paired with
// the arena offset for the byte-confirm on a hash tie), maintained by the async folder, merged
// partition-by-partition and in parallel for a large same-P SINTER/SDIFF, with the existing
// probe-off-the-smallest-source kept for the asymmetric case where one source is far smaller.
//
// The real code this informs is aki/engine/f1raw (the per-partition set structure
// in partset.go and the dense member vector in randvec.go) and aki/f1srv/set_algebra.go
// (sinterEach, sdiffEach, and the adaptive merge-vs-probe driver selection).
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.
package seteager
