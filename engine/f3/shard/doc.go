// Package shard is the f3 execution model (spec 2064/f3/03): worker-per-core
// shard ownership, wyhash key routing, the batched MPSC hop transport, and the
// per-batch epoch bracket. One shard, one pinned worker, and nothing else ever
// touches a shard's structures; every structure below this package assumes that
// contract and carries no locks because of it.
package shard
