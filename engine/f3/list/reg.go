package list

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
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

// Has reports whether key holds a list on this shard, without building the
// registry when none exists yet: the presence probe the unified TYPE consults
// across the collection types. A string value or another collection at key reads
// false, leaving the type to the caller's other probes.
func Has(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	l, _ := v.(*reg).lookup(cx, key)
	return l != nil
}

// Delete removes key when it holds a list on this shard and reports whether it
// did: the list arm of the unified single-key DEL. It builds no registry when
// none exists, so a DEL over a key of another type touches nothing here. Cold
// chunks a demoted list left behind are not reclaimed yet, the same deferral
// every collection carries until the cold-reclamation slice threads DEL.
func Delete(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	g := v.(*reg)
	if g.live(cx, key) == nil {
		return false
	}
	logDeleteKey(cx, key)
	g.drop(key)
	return true
}

// Flush drops every list on this shard, the list arm of FLUSHALL and FLUSHDB. It
// clears the map and zeroes the resident-byte total, so a flush leaves the
// registry empty and weighing nothing, matching the store the flush just reset.
// The blocking-pop waiters are kept: FLUSHALL does not unblock a parked BLPOP or
// BLMOVE client (Redis leaves blocked clients blocked), so a later RPUSH to the
// key serves them just as it would have. It builds no registry when none exists.
func Flush(cx *shard.Ctx) {
	v, ok := regs.Load(cx.St)
	if !ok {
		return
	}
	g := v.(*reg)
	g.m = make(map[string]*list)
	g.resident = 0
}

// Len is the number of lists this shard holds, the list contribution to DBSIZE. A
// dropped list leaves the map, so the map size is the live count; it reads zero
// before any list command has built a registry on this shard.
func Len(cx *shard.Ctx) int {
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	return len(v.(*reg).m)
}

// VolatileLen counts the lists on this shard carrying a key-level TTL, the list
// contribution to INFO's Keyspace expires field. It walks the registry map
// counting a non-zero deadline whether or not it has passed, matching the
// map-size basis of Len (a lazily-expired-but-unreaped list still shows in both
// totals until a read drops it). INFO is a cold path, so the O(keys) walk is off
// every command's critical path. It builds no registry when none exists.
func VolatileLen(cx *shard.Ctx) uint64 {
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	var n uint64
	for _, l := range v.(*reg).m {
		if l.expireAt != 0 {
			n++
		}
	}
	return n
}

// RangeKeys calls fn with every list key on this shard, the list contribution to
// the unified KEYS and SCAN walk. It reaches the registry through regs.Load so a
// shard that ran no list command builds nothing and yields nothing. It returns
// false when fn asked to stop, halting the outer walk for a bounded scan. The
// slice fn receives is the map key's bytes, valid only for that call; fn copies
// what it keeps.
func RangeKeys(cx *shard.Ctx, fn func(key []byte) bool) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return true
	}
	now := cx.NowMs
	for k, l := range v.(*reg).m {
		// Skip a list whose key-level deadline has passed so KEYS and SCAN never
		// surface a key EXISTS would report absent. The skip is read-only (no drop) to
		// match the string store's expiry-aware walk, which reaps nothing during a scan.
		if l.expireAt != 0 && l.expireAt <= now {
			continue
		}
		if !fn([]byte(k)) {
			return false
		}
	}
	return true
}

// lookup finds the list for key. wrong is true when the key instead holds a
// value in the string store, which every list command answers with WRONGTYPE.
// Cross-type collisions with the set and zset registries are not resolved in
// this slice, the same deferral the set and zset slices carry until keyspace
// unification threads every type through one holder.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (l *list, wrong bool) {
	if l = g.live(cx, key); l != nil {
		return l, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// live returns the list at key, or nil when none exists or the list's key-level
// deadline has passed (spec 2064/f3/16 section 2). An expired list is dropped here
// and treated as absent, so it is dead to this command and every later one in the
// epoch, the lazy-expiry half of the TTL contract. Unlike the hash there is no
// per-field TTL to reap, so this is the plain deadline check the set and zset carry.
// This is the one funnel every read, mutate, create, and probe path routes through.
func (g *reg) live(cx *shard.Ctx, key []byte) *list {
	l := g.m[string(key)]
	if l == nil {
		return nil
	}
	if l.expireAt != 0 && l.expireAt <= cx.NowMs {
		g.drop(key)
		return nil
	}
	return l
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
