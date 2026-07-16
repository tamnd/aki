package list

import (
	"sync"

	"github.com/tamnd/aki/engine/obs1/shard"
)

// The list type keeps its per-key structures in an owner-local registry: one map
// from key to the inline list, touched only by the shard goroutine, so it holds
// no lock. The set type hangs its registry off Ctx.Coll and the zset off
// Ctx.ZColl; the list has no dedicated Ctx slot yet and shard is owned by
// another slice, so the registry hangs off a map keyed by the shard's store
// pointer, which is stable for the worker's life and unique per owner. Each
// entry is reached and mutated only by its owning shard goroutine; the sync.Map
// guards nothing but the first-touch creation race between shards. The
// keyspace-unification slice folds this into the shared collection holder Ctx
// grows, at which point this map goes away.
type reg struct {
	m       map[string]*list
	waiters map[string]*waitList
	wpool   waitPool
	// ready is the serve-chain worklist: keys a served BLMOVE pushed onto whose
	// own blocked waiters may now be servable. It stays nil until the first move
	// serves (a plain push serving BLPOP/BLMPOP waiters never allocates it) and is
	// truncated back to empty at the end of every serveWaiters call, so its grown
	// capacity is reused across chains without holding keys between drains.
	ready []string

	// resident is the running sum of every live list's resident-byte footprint
	// (list.residentBytes), the figure the shard reads to weigh the list heap
	// against the store's resident cap at a demote boundary (spec 2064/f3/06
	// section 6). It is maintained by note and drop so the shard never walks the
	// registry to size it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to demote a list into, there
	// is nothing to weigh, so note and drop skip the bookkeeping entirely and the
	// push path stays byte-identical to M0, holding the L9 zero-delta contract for
	// a store with no resident cap.
	acctOn bool
}

var regs sync.Map // *store.Store -> *reg

// registry returns the shard's list registry, building it on first use. The
// store pointer is set once when the worker starts and never changes, so it is a
// stable per-shard key.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	v, _ := regs.LoadOrStore(cx.St, &reg{
		m:       make(map[string]*list),
		waiters: make(map[string]*waitList),
		acctOn:  cx.St != nil && cx.St.ColdConfigured(),
	})
	return v.(*reg)
}

// lookup finds the list for key. wrong is true when the key instead holds a
// value in the string store, which every list command answers with WRONGTYPE.
// Cross-type collisions with the set and zset registries are not resolved in
// this slice, the same deferral the set and zset slices carry until keyspace
// unification threads every type through one holder.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (l *list, wrong bool) {
	if l = g.m[string(key)]; l != nil {
		return l, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// note reconciles l's footprint into the running resident total: it posts the
// delta since the last note, so the total stays the exact sum of every live
// list's footprint. A mutating command calls it before returning on any list that
// survives the command (an emptied list goes through drop instead), which keeps
// the total exact at every command boundary, the only point the shard reads it. It
// is a single bool load when accounting is off. Owner goroutine only.
func (g *reg) note(l *list) {
	if !g.acctOn {
		return
	}
	nb := l.residentBytes()
	g.resident += nb - l.acct
	l.acct = nb
}

// drop removes an emptied list from the registry: Redis deletes a list the
// moment its last element leaves. It takes the list's last-posted footprint back
// out of the running total, so the total never carries a gone list's bytes.
func (g *reg) drop(key []byte) {
	if g.acctOn {
		if l := g.m[string(key)]; l != nil {
			g.resident -= l.acct
		}
	}
	delete(g.m, string(key))
}

// demote sheds a bounded run of key's interior chunks into the cold region and
// returns how many it shed, then reconciles the freed bytes into the running total.
// It is a no-op (returns 0) when the key is absent, still inline (a listpack is
// below one chunk's worth, nothing to demote), or its interior is already cold. The
// trigger that decides which key to demote when the shard overshoots its resident
// cap lands with the dispatch slice; this is the per-key pass it drives. Owner
// goroutine only.
func (g *reg) demote(cx *shard.Ctx, key []byte) int {
	l := g.m[string(key)]
	if l == nil || l.nat == nil {
		return 0
	}
	n := l.nat.demote(cx.St, key)
	if n > 0 {
		g.note(l)
	}
	return n
}

// ResidentBytes is the running sum of every live list's resident-byte footprint on
// this shard, the collection contribution to the store's memory-pressure figure
// (spec 2064/f3/06 section 6). It is zero when the store runs no cold tier. The
// shard reads it at a demote boundary; the trigger that consumes it lands with the
// list demotion slice. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's list-registry resident-byte total to the
// worker's demote loop. The list registry hangs off the shared regs map keyed by
// the shard's store, not a Ctx slot, so this reads that map without building a
// registry on a shard that never ran a list command: it is zero before the first
// list command, or when the store runs no cold tier. Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg).ResidentBytes()
	}
	return 0
}

// waitListFor returns the waiter FIFO for key, creating an empty one on first
// block. It lazily initializes the map so a registry built directly in a unit
// test (with a nil waiters map) can still park; the real registry() path
// pre-builds it.
func (g *reg) waitListFor(key []byte) *waitList {
	if g.waiters == nil {
		g.waiters = make(map[string]*waitList)
	}
	wl := g.waiters[string(key)]
	if wl == nil {
		wl = &waitList{pool: &g.wpool, key: string(key), head: nilIdx, tail: nilIdx}
		g.waiters[string(key)] = wl
	}
	return wl
}

// dropWaitersIfEmpty removes a waiter list from the registry once its last
// waiter leaves, mirroring drop for the value map so a key that was blocked on
// and then drained leaves nothing behind.
func (g *reg) dropWaitersIfEmpty(wl *waitList) {
	if wl.n == 0 {
		delete(g.waiters, wl.key)
	}
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
