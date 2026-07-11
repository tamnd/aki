package set

import "github.com/tamnd/aki/engine/f3/shard"

// The set type keeps its per-key structures in an owner-local registry hung off
// the shard's Ctx.Coll (spec 2064/f3/11): one map from key to the inline set,
// touched only by the shard goroutine, so it holds no lock. The string store
// and this registry are separate keyspaces for now; the WRONGTYPE guard below
// keeps a set command off a key the string store owns, and the keyspace
// commands set.Type, set.Exists, and set.Del span both. Full cross-type
// unification (a SET overwriting a set, multi-key DEL over sets) lands with the
// keyspace slice; this slice keeps the set surface self-consistent and refuses
// the cross-type collisions it cannot yet resolve.

// reg is the shard's set registry plus its draw randomness. The PRNG is
// owner-local, so SPOP and SRANDMEMBER never touch shared rand state (doc 11
// section 5.6).
type reg struct {
	m   map[string]*set
	rng uint64
}

// registry returns the shard's set registry, building it on first use. The
// Ctx and thus the registry live for the worker's whole life, so a set added
// on one command is there for the next.
func registry(cx *shard.Ctx) *reg {
	if cx.Coll == nil {
		cx.Coll = &reg{m: make(map[string]*set), rng: 0x9e3779b97f4a7c15}
	}
	return cx.Coll.(*reg)
}

// next advances the xorshift PRNG and returns a value in [0, n). n is always
// positive at the call sites (a draw only happens on a non-empty set).
func (g *reg) next(n int) int {
	x := g.rng
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	g.rng = x
	return int(x % uint64(n))
}

// lookup finds the set for key. present is false when no set exists; wrong is
// true when the key instead holds a value in the string store, which every set
// command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (s *set, wrong bool) {
	if s = g.m[string(key)]; s != nil {
		return s, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// drop removes an emptied set from the registry: Redis deletes a set the
// moment its last member leaves.
func (g *reg) drop(key []byte) { delete(g.m, string(key)) }
