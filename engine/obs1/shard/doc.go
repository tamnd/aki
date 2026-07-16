// Package shard is the f3 execution model, ported by copy from
// engine/f3/shard per the 2064/obs1 doc 11 section 2 inventory:
// worker-per-core shard ownership, the batched MPSC hop transport, and
// the per-batch epoch bracket (spec 2064/f3/03). One shard, one pinned
// worker, and nothing else ever touches a shard's structures; every
// structure below this package assumes that contract and carries no
// locks because of it.
//
// The one adaptation is the route (2064/obs1 doc 02 sections 1.2 and
// 1.3, doc 07 section 2): f3's wyhash ShardOf is replaced by slot
// routing in slot.go, key to CRC16 hash slot (with the hash-tag rule)
// to contiguous slot group to shard by group id mod shard count. The
// group is obs1's unit of lease, manifest, and handoff; a group never
// spans shards, so every per-shard invariant this package carries
// applies per group unchanged, exactly the replacement the f3 code
// comment promises. Nothing below the route decision sees the
// difference.
package shard
