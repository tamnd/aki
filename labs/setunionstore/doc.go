// Package setunionstore is the lab behind the SUNIONSTORE dedup-once fix. SUNION and SUNIONSTORE
// shared one walker. The read form, sunionEach, has to hand the client each distinct member exactly
// once, so it deduplicates through a Go seen-set keyed by the member bytes: enumerate every source's
// members, skip any the map has already seen, emit the rest. That map is correct and necessary for the
// read form, where there is no other place the duplicates could be filtered.
//
// The store form reused it, and that was the wrong assumption. SUNIONSTORE does not emit to a client;
// it inserts the result into a destination set, and the destination is itself a membership structure:
// its index rejects a member it already holds, and the result's cardinality, encoding fold and
// sorted-hash order all advance only on a member the insert reports as new. The destination already
// deduplicates. Running the members through a seen-set first, then through the destination insert,
// deduplicates the same stream twice. The seen-set is pure overhead on the store path, and it is the
// expensive kind of overhead: an allocation sized to the whole union that rehashes as it fills, an
// O(union) hash pass whose cost climbs with cardinality. On the in-memory-fit sweep SUNIONSTORE was
// the worst-scaling algebra store for exactly this reason, 0.43x the rivals at a thousand members and
// falling to 0.26x at a hundred thousand as the map grew.
//
// The fix is to walk the sources raw for the store form (sunionEachRaw) and let the one dedup that has
// to happen anyway, the destination insert, be the only one. This lab measures the two shapes so the
// removal is a number, not a guess. It models the destination as a Go map used insert-if-new, the same
// role the real element index plays, and compares:
//
//   - mapThenInsert: build a seen-set over the sources, emit the distinct members, insert each into the
//     destination. Two dedups over the same stream, the pre-fix store path.
//   - rawThenInsert: walk the sources with no seen-set, insert every member into the destination and
//     let the destination reject the duplicates. One dedup, the post-fix store path.
//
// Both build a byte-identical destination; the only difference is whether the union is deduplicated
// once or twice. The sources are the aki-bench algebra overlap model: two sets of n members that share
// half their members (set a over m0..m{n-1}, set b over the shifted band m{n/2}..m{n+n/2-1}), so the
// union is 1.5n distinct members and n/2 of the 2n enumerated members are duplicates the destination
// would have caught on its own.
//
// # Result (Apple M4, go test -bench . ./labs/setunionstore/)
//
//	BenchmarkUnionStore/n=1000/mapThenInsert   ~1.85x the time and 2x the allocs of rawThenInsert
//	BenchmarkUnionStore/n=10000/mapThenInsert  ~1.87x, the gap holding as n grows
//
// The second map is a constant-factor tax the store path never needed. In the real server the win is
// larger than the model shows, because the real seen-set is sized to the union upper bound with
// algebraBufCap and rehashes from empty each call, while this model reuses nothing either way; the
// model isolates the redundant-dedup mechanism, not the allocator behavior on top of it. The fix lives
// in f1srv/set_algebra.go (sunionEachRaw) wired from cmdSUnionStore in f1srv/set_algebra_store.go.
package setunionstore
