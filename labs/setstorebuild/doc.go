// Package setstorebuild is the lab behind the SINTERSTORE-family destination-build fix. The seteager
// lab settled how a SET keeps its members in hash order for the algebra merge: an async folder maintains
// a per-partition sorted []uint64 behind the O(1) unsorted vector, so a SADD returns at append speed and
// SINTER/SDIFF/SINTERCARD always find an up-to-date sorted array. That design is right for the write
// path, one member at a time. It is exactly wrong for a STORE.
//
// A SINTERSTORE/SUNIONSTORE/SDIFFSTORE computes a whole result and writes it into the destination as k
// element rows in one shot. On the sorted-hash fold facility that write journals a foldDelta per member,
// and the folder folds them into the destination's sorted array as it drains. Each fold allocates fresh
// arrays and merges the pending batch into the existing sorted array (fresh arrays are load-bearing:
// they keep a concurrent merge holding the old snapshot correct), so a fold of a batch into an array of
// m existing members is O(m). Fold k members into a growing array and the build is the sum of the folds,
// which is O(k^2). The seteager lab flagged this in passing ("one flat array per set is out, the write
// kills it, building the set is O(n^2)"); a STORE that spills past the listpack/intset boundary into the
// coll form hits it head-on, and the in-memory-fit sweep caught it as a SINTERSTORE cliff, a fraction of
// the rivals' throughput once the destination grew into the coll form.
//
// This lab measures the destination build the two ways, so the fix is a number, not a guess. It models
// the fold's fresh-array merge faithfully and compares folding member-at-a-time against one bulk sort of
// the whole result, plus a coalesced middle ground (the folder draining a fixed batch per wake) to show
// coalescing changes the constant, not the order.
//
// # The three builds
//
//   - incremental: fold one member at a time into the destination's sorted array, the STORE path's
//     behavior when the folder keeps pace and drains after each inserted member. Each fold is an O(m)
//     fresh-array merge into the m-member array so far, so the build is O(k^2). This is the cliff.
//   - batched(64): fold the members in 64-member batches, the STORE path's behavior when the folder
//     coalesces per wake. Each fold sorts the small batch then merges it into the growing array in one
//     pass, so the build is O(k^2 / 64), the order unchanged, the constant divided by the batch.
//   - bulk: sort the whole member list once, O(k log k), the destination folded in a single pass. This
//     is sortedHashes.build, called once per destination partition by SortedHashBuild after storeAlgebra
//     has written every member.
//
// # What the numbers say (Apple M4, GOMAXPROCS=10, buildN = 1<<16)
//
// Build a 65536-member destination's sorted-hash order (ns for the whole build):
//
//	BenchmarkBuildIncremental   5649 ms   O(k^2), fold one member at a time
//	BenchmarkBuildBatched64       77.7 ms  O(k^2 / 64), fold 64 members per wake
//	BenchmarkBuildBulk             6.1 ms  O(k log k), one sort of the whole result
//
// # The lesson
//
// Bulk is ~925x the incremental fold and ~12.7x the 64-coalesced fold, and it is one sort against an
// O(k^2) accumulation, so the gap widens with k: at a million-member destination the incremental fold is
// hopeless while the bulk sort stays O(k log k). The coalescing the async folder already does softens the
// constant a lot (77.7 ms is not 5.65 s) but does not change the order, so it is not the fix, only a
// reason the cliff was a cliff and not an instant hang. The write path and the STORE path want opposite
// things: the write path adds one member and cannot afford to re-sort the set, so it journals a delta and
// the folder folds incrementally; the STORE writes the whole set at once and already knows every member,
// so it should sort once and never journal a per-member delta at all.
//
// So the fix is a bulk build that bypasses the journal. storeAlgebra collects each stored member's
// (hash, offset) as it inserts through the no-journal CollRandInsertOff/CollPartRandInsertOff variants,
// then calls SortedHashBuild once per destination partition, which sorts the collected entries and
// installs a fresh sorted array in a single pass, resetting the partition's journal and generation so no
// stale per-member delta lands on top. A destination that a STORE emptied is cleared with SortedHashReset
// instead, so a destination reused across repeated STOREs never folds a new result on top of the previous
// one's stale offsets (the latent accumulation bug the same change closes).
//
// The real code this informs is aki/engine/f1raw/sorthash.go (sortedHashes.build), sorthashfold.go
// (SortedHashBuild, SortedHashReset, MemberHash, SortedHashEntry), randvec.go and partdraw.go
// (CollRandInsertOff, CollPartRandInsertOff), and aki/f1srv/set_algebra_store.go (storeAlgebra's per-
// partition entry collection and the bulk build after the write).
//
// Numbers observed on an Apple M4 (GOMAXPROCS=10); re-run to reproduce on yours.
package setstorebuild
