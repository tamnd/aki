package zset

import "github.com/tamnd/aki/engine/f3/shard"

// The zset type keeps its per-key structures in an owner-local registry hung off
// the shard's Ctx.ZColl (spec 2064/f3/12): one map from key to the inline zset,
// touched only by the shard goroutine, so it holds no lock. The string store and
// this registry are separate keyspaces for now; the WRONGTYPE guard keeps a zset
// command off a key the string store owns. Cross-type unification with the set
// registry (a zset and a set colliding on one key, TYPE reporting zset) lands
// with the keyspace slice; this slice keeps the zset surface self-consistent and
// refuses the string-store collision it can resolve.

const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

type reg struct {
	m map[string]*zset
}

// registry returns the shard's zset registry, building it on first use. The Ctx
// and thus the registry live for the worker's whole life, so a zset added on one
// command is there for the next.
func registry(cx *shard.Ctx) *reg {
	if cx.ZColl == nil {
		cx.ZColl = &reg{m: make(map[string]*zset)}
	}
	return cx.ZColl.(*reg)
}

// lookup finds the zset for key. present is false when no zset exists; wrong is
// true when the key instead holds a value in the string store, which every zset
// command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (z *zset, wrong bool) {
	if z = g.m[string(key)]; z != nil {
		return z, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// drop removes an emptied zset: Redis deletes a zset the moment its last member
// leaves.
func (g *reg) drop(key []byte) { delete(g.m, string(key)) }
