package f1srv

// Key-affinity execution routing (spec 2064/f1_rewrite_ltm/17-key-affinity-execution.md).
//
// The element-delete family is the last command family short of the 2x bar on the
// single-hot-key benchmark, and doc 16 section 15 traced the cause to a stripe-lock
// convoy plus a per-core network path heavier than single-threaded Redis: many reactor
// loops collide on one key's stripe mutex and bounce its cardinality header cache line
// between cores on every delete. The fix designed in doc 17 is to route every command
// for a key to the single worker that owns the key's shard, so the serial data op runs
// lock-free on one core while parse and network run off-core.
//
// This file holds the two pieces that are correct in isolation and that every later
// routing slice builds on: the exec-model selector (the rollout flag, defaulting to the
// current shared-store behavior) and the shard-assignment function that maps a top-level
// key to its owning shard. The routing, the per-shard worker loops, the hop queues, and
// the reply reordering land in later slices on top of these; see doc 17 section 13 for
// the slice plan. Until those land, execModelAffinity is inert: the selector parses and
// records the choice, and nothing consults it on the command path yet.

// execModel selects how a command reaches the store.
//
// execModelShared is today's behavior: any reactor loop runs any key's command against
// the one shared store, serialized on that key's stripe mutex. It is the default so this
// work cannot regress anything already passing while the affinity path is built up.
//
// execModelAffinity is the shared-nothing target: the keyspace is partitioned into one
// shard per worker, and every command for a key is routed to the single worker that owns
// its shard, which runs it lock-free because it is the sole owner. It is selected only by
// an explicit flag and is not wired onto the command path until the routing slices land.
type execModel uint8

const (
	execModelShared execModel = iota
	execModelAffinity
)

// parseExecModel resolves the --exec-model flag string to an execModel. An unrecognized
// or empty value resolves to execModelShared, so a typo falls back to the safe default
// rather than failing to start; ok reports whether the value was recognized, letting the
// caller log a warning on a typo without refusing to serve.
func parseExecModel(s string) (m execModel, ok bool) {
	switch s {
	case "", "shared":
		return execModelShared, true
	case "affinity":
		return execModelAffinity, true
	default:
		return execModelShared, false
	}
}

// String renders an execModel back to its flag spelling, for the startup banner and INFO.
func (m execModel) String() string {
	switch m {
	case execModelAffinity:
		return "affinity"
	default:
		return "shared"
	}
}

// shardFor maps a top-level key to its owning shard in [0, nShards). It is the single
// point that decides which worker serves a key, so it has to be a pure function of the
// key bytes alone: the routing home loop calls it before it has touched the store, and
// every worker and every command must agree on the same answer without a lookup.
//
// A command's shard is chosen from its top-level key only, and every record the command
// derives from that key (a hash field row, a set member row, a zset member and score row,
// the cardinality header, TTL sidecars) is served by the same shard, so a single-key
// command is always handled entirely by one worker with no cross-shard hop. That
// co-location is the whole point of routing by the top-level key rather than by each
// composite record's own hash; see doc 17 section 4.
//
// nShards <= 1 collapses to a single shard, which is the degenerate single-owner store
// (correct, just not parallel), so callers need not special-case a one-worker server.
func shardFor(key []byte, nShards int) int {
	if nShards <= 1 {
		return 0
	}
	h := shardHash(key)
	// Lemire's multiply-shift reduction maps the 32-bit hash uniformly onto nShards with
	// one multiply and a shift, avoiding both a modulo and its low-bit bias, and it works
	// for any shard count, not just powers of two, so the worker count can track the core
	// count exactly rather than being rounded to a power of two.
	return int((uint64(h) * uint64(nShards)) >> 32)
}

// shardHash is a self-contained 64-bit key hash reduced to 32 bits for shardFor. It is
// independent of the engine's internal index hash and of the stripe hash on purpose: the
// shard assignment is a routing decision that must stay stable and self-consistent on its
// own terms, not tied to an engine internal that is free to change. It is FNV-1a with a
// final avalanche mix so distinct keys that differ only in low bytes still spread across
// shards rather than clustering, which a raw FNV low word does not guarantee.
func shardHash(key []byte) uint32 {
	var h uint64 = 1469598103934665603
	for _, b := range key {
		h ^= uint64(b)
		h *= 1099511628211
	}
	// Final mix (splitmix64 finalizer) so the high bits Lemire's reduction reads are well
	// diffused; FNV-1a alone leaves adjacent keys correlated in the top bits.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return uint32(h >> 32)
}
