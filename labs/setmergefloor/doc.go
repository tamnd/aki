// Package setmergefloor is the lab behind lowering setMergeFloor from 1024 to 128, the smallest source
// cardinality at which SINTER/SINTERCARD/SDIFF engage the sorted-hash merge instead of the
// smallest-source point-probe (f1srv/set_algebra.go, spec 2064/f1_rewrite_ltm/24). The old 1024 floor
// came from labs/seteager's single-key crossover sweep, and the GamingPC three-way gate against Redis
// 8.8 and Valkey 9.1 showed 1024 was far too high: with the merge enabled and the floor at 128,
// SINTER 256 went from 0.64x to 1.28x and SINTERCARD 256 from 0.64x to 1.78x, while 16-member sets
// (which already run ~3.59x on the probe) stay below the floor and are untouched. This lab isolates
// why the seteager number was wrong, so the floor change rests on a model and not just the gate run.
//
// # What seteager measured, and what it missed
//
// labs/seteager settled the merge-vs-probe crossover by racing a two-pointer merge against a probe
// that walked the smaller source into a compact, per-set hash table (newProbeSet(big)). That probe was
// cheap per member for two reasons the live engine does not enjoy:
//
//   - It probed a small dedicated table that stays hot in cache, not the one global open-addressed
//     index every set in the keyspace shares. The live setMemberExists path hashes a composite key and
//     walks engine/f1raw's single find() table, so each probe lands at an unrelated, cache-cold
//     position; under a saturating client load those probes evict each other's lines from the shared
//     last-level cache and each pays a real miss. seteager's per-set table hid that entirely.
//   - It probed a bare uint64, doing no key construction. The live probe first builds the composite
//     memberKey (uvarint(len(setKey)) | setKey | member) and hashes ~24 bytes word-at-a-time, on every
//     single member. The merge does none of this at read time: the async folder computed each member's
//     hash off the reply path, so the two-pointer walk only compares pre-folded uint64s.
//
// Because seteager understated the probe's per-member cost, it put the crossover near 1024. Correct the
// probe model and the crossover collapses.
//
// # What this lab models
//
// It builds two equal-cardinality sets A and B overlapping in half and computes SINTER(A,B) two ways,
// each on a data structure shaped like the engine's:
//
//   - probeSINTER: drive off A and, for each member, build the composite memberKey, hash it
//     (word-at-a-time, like engine/f1raw's hash), and look it up in a shared 256 MB open-addressed
//     index seeded with ~16M unrelated keys so the probe misses even a desktop's large last-level cache
//     and pays real latency, the way the live global index does.
//   - mergeSINTER: SyncSortedHashes (a steady-state atomic check), PinMerge (RLock the partition set,
//     snapshot the sorted-array header), then a two-pointer walk over the two pre-folded sorted
//     member-hash arrays, which stay compact and per-set so they live in each core's own cache.
//
// TestModelsAgree pins both to n/2 at every cardinality so a fast-but-wrong benchmark is caught. A
// concurrent pair (BenchmarkFloorProbePar/MergePar) runs the same two reads under GOMAXPROCS workers,
// each on a distinct scattered region of the shared index, to reproduce the many-connection contention
// the GamingPC gate runs under rather than a single uncontended thread.
//
// # Result (Apple M4, GOMAXPROCS=10, go test -bench Floor ./labs/setmergefloor/)
//
// Single thread, ns/op for the whole intersection at per-source cardinality n:
//
//	n      merge    probe    probe/member   merge/member
//	   8   21.9 ns   52.2 ns   6.5 ns         ~2 ns
//	  16   38.4 ns  101.4 ns   6.3 ns         ~2 ns
//	  64  135.3 ns  418.0 ns   6.5 ns         ~2 ns
//	 128  310.8 ns  879.6 ns   6.9 ns         ~2 ns
//	 256  518.9 ns  1786  ns   7.0 ns         ~2 ns
//	1024  2012  ns  8066  ns   7.9 ns         ~2 ns
//
// The point of the table is the two per-member slopes, not the absolute crossover. The probe costs
// ~6.5 ns per member and creeps up toward ~8 ns as the working set spills more cache; the merge costs
// ~2 ns per pre-folded element and stays flat. So the probe's cost grows ~3.5x faster per member than
// the merge's. That single asymmetry is the mechanism by which the probe, competitive with Redis at
// tiny cardinalities, falls behind it as the sources grow into the hundreds and thousands, exactly the
// shape the GamingPC three-way saw: SINTER on the probe ran 0.64x at 256 and 0.52x at 65536, and the
// merge lifted both above 1x. seteager's hot-table, no-key-build probe measured a per-member cost of
// roughly a third of this and so never saw the probe fall off, which is why it chose 1024.
//
// # Why the floor is 128 and not the lab's own crossover
//
// This lab deliberately models only the read-path per-member costs faithfully; it under-models the
// merge's fixed per-call setup (the real path also pins P partitions, epoch-pins the arena, builds a
// fan plan across shard workers, and dispatches through the affinity layer), so the lab's own break-even
// sits well below 128 and cannot by itself set the floor. The floor is set by the GamingPC three-way
// gate, which pays that full setup end to end: 16-member SINTER already runs ~3.59x on the probe, so
// there is nothing to win there and the floor keeps those cases on the cheaper path; 256-member SINTER
// flips from 0.64x on the probe to 1.28x on the merge. 128 sits one bracket below the first cardinality
// the merge is needed at, so every source large enough for the probe to have started losing to Redis
// engages the merge, and every source still comfortably beating Redis on the probe is left alone. The
// lab's contribution is the per-member asymmetry that explains why the crossover moved down from
// seteager's 1024 at all; the gate places the exact number.
package setmergefloor
