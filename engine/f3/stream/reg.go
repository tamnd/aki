package stream

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream type keeps its per-key objects in an owner-local registry, the same
// seam the list and hash types use: a map from key to the stream, touched only by
// the shard goroutine and hung off a sync.Map keyed by the shard's store pointer,
// which is stable for the worker's life and unique per owner. The sync.Map guards
// nothing but the first-touch creation race between shards; every entry is reached
// and mutated only by its owning shard. Streams have no dedicated Ctx slot, so
// they take this seam until the keyspace-unification slice folds every type into
// one holder.
type reg struct {
	m map[string]*stream
	// waiters holds the blocking-XREAD FIFO per key, and wpool the shared node
	// slab behind them (waiter.go). Both stay empty until the first XREAD BLOCK
	// parks, so a stream workload that never blocks carries only the map header.
	waiters map[string]*waitList
	wpool   waitPool
	// serveOrder is the reusable FIFO-snapshot scratch serveWaiters walks, so a
	// wake that unlinks nodes mid-walk keeps its place without a per-XADD alloc.
	serveOrder []uint32
	// dirty is the gc worklist: the native streams a tombstone has landed in since
	// the last maintenance pass (gc.go). XDEL and exact XTRIM append a stream here
	// once (guarded by stream.gcDirty), and maintain, run at the owner's idle
	// boundary through the shard maintainer seam, drains it. Owner-goroutine-only,
	// so it needs no lock; it stays empty for a stream workload that never deletes.
	dirty []*stream

	// resident is the running sum of every live stream's resident-byte footprint
	// (stream.residentBytes), the figure the shard reads to weigh the stream heap
	// against the store's resident cap at a demote boundary (spec 2064/f3/06
	// section 6). note maintains it so the shard never walks the registry to size
	// it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to spill a block into, there is
	// nothing to weigh, so note skips the bookkeeping entirely and the write path
	// stays byte-identical to M0, holding the L9 zero-delta contract for a store with
	// no resident cap.
	acctOn bool
}

var regs sync.Map // *store.Store -> *reg

// registry returns the shard's stream registry, building it on first use.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	g := &reg{
		m:       make(map[string]*stream),
		waiters: make(map[string]*waitList),
		acctOn:  cx.St != nil && cx.St.ColdConfigured(),
	}
	v, loaded := regs.LoadOrStore(cx.St, g)
	if !loaded {
		// First touch of this shard's stream registry: register its gc maintainer
		// with the shard so the worker drains g.dirty at every idle boundary. Done
		// once, under the LoadOrStore winner, so a losing racer never double-registers.
		shard.RegisterMaintainer(cx.St, g.maintain)
	}
	return v.(*reg)
}

// markDirty enqueues a native stream for the next gc pass, at most once between
// passes. XDEL and exact XTRIM call it after tombstoning a sealed-band entry; the
// gcDirty flag on the stream keeps the worklist free of duplicates while the stream
// waits, and the maintainer clears it. Owner-goroutine-only, so no lock.
func (g *reg) markDirty(s *stream) {
	if s.gcDirty {
		return
	}
	s.gcDirty = true
	g.dirty = append(g.dirty, s)
}

// maintain is the shard's registered between-batches step (maintain.go): it runs one
// gc pass over every stream a tombstone dirtied since the last pass, then clears the
// worklist. It runs on the owner goroutine at the worker's idle boundary, with the
// queue drained and no streamed reply in flight, so a rewrite can move a block's bytes
// with no arena snapshot naming them. Cheap when idle: the common no-delete workload
// leaves dirty empty, so this is one length check.
func (g *reg) maintain() {
	if len(g.dirty) == 0 {
		return
	}
	for _, s := range g.dirty {
		s.gc()
		s.gcDirty = false
		// gc rewrote or dropped sealed blocks, shrinking the resident block bytes;
		// reconcile the freed bytes into the running total at the idle boundary.
		g.note(s)
	}
	g.dirty = g.dirty[:0]
}

// note reconciles s's footprint into the running resident total: it posts the
// delta since the last note, so the total stays the exact sum of every live
// stream's footprint. A mutating command calls it before returning on the stream
// it touched, which keeps the total exact at every command boundary, the only
// point the shard reads it. It is a single bool load when accounting is off. A
// stream is never dropped from the registry once created (an emptied stream is
// kept, section 4.5, and DEL routing is owed to keyspace unification), so unlike
// the other collection registries this one carries no drop counterpart yet; the
// unification slice that removes a stream key will take its acct back out here.
// Owner goroutine only.
func (g *reg) note(s *stream) {
	if !g.acctOn {
		return
	}
	nb := s.residentBytes()
	g.resident += nb - s.acct
	s.acct = nb
}

// ResidentBytes is the running sum of every live stream's resident-byte footprint
// on this shard, the collection contribution to the store's memory-pressure figure
// (spec 2064/f3/06 section 6). It is zero when the store runs no cold tier. The
// shard reads it at a demote boundary. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's stream-registry resident-byte total to the
// worker's demote loop. The stream registry hangs off the shared regs map keyed by
// the shard's store, not a Ctx slot, so this reads that map without building a
// registry on a shard that never ran a stream command: it is zero before the first
// stream command, or when the store runs no cold tier. Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg).ResidentBytes()
	}
	return 0
}

// Has reports whether key holds a stream on this shard, without building the
// registry when none exists yet: the presence probe the unified TYPE consults
// across the collection types. Reaching the registry through regs.Load rather
// than registry() also keeps a bare TYPE probe from registering the stream
// maintainer on a shard that never ran a stream command. A string value or
// another collection at key reads false, leaving the type to the caller's other
// probes.
func Has(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	s, _ := v.(*reg).lookup(cx, key)
	return s != nil
}

// Delete removes key when it holds a stream on this shard and reports whether it
// did: the stream arm of the unified single-key DEL. Unlike an emptied stream,
// which the registry keeps in place (XLEN reads 0), a deleted key leaves nothing
// behind, so this is the one path that drops a stream from the map. A stream
// keeps no drop counterpart, so this reconciles the running total and, if a
// tombstone left the stream on the gc worklist, takes it off, so the maintainer
// never gcs a detached stream and adds its bytes back into the total. Parked
// XREAD waiters live in a separate per-key list untouched here, so a later XADD
// recreates the stream and serves them, as in Redis. Cold blocks a demoted
// stream left behind are not reclaimed yet, the same deferral every collection
// carries until the cold-reclamation slice threads DEL. Owner goroutine only.
func Delete(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	g := v.(*reg)
	s := g.m[string(key)]
	if s == nil {
		return false
	}
	if s.gcDirty {
		g.undirty(s)
	}
	if g.acctOn {
		g.resident -= s.acct
	}
	delete(g.m, string(key))
	return true
}

// Flush drops every stream on this shard, the stream arm of FLUSHALL and
// FLUSHDB. It clears the map, empties the gc worklist, and zeroes the
// resident-byte total, so a flush leaves the registry empty and weighing
// nothing, matching the store the flush just reset. The blocking-XREAD waiters
// are kept: FLUSHALL does not unblock a parked client (Redis leaves blocked
// clients blocked), so a later XADD to the key recreates the stream and serves
// them. The registry object itself stays, so its registered gc maintainer needs
// no re-registration. It builds no registry when none exists on this shard.
func Flush(cx *shard.Ctx) {
	v, ok := regs.Load(cx.St)
	if !ok {
		return
	}
	g := v.(*reg)
	g.m = make(map[string]*stream)
	g.dirty = g.dirty[:0]
	g.resident = 0
}

// Len is the number of streams this shard holds, the stream contribution to
// DBSIZE. An emptied stream is kept in the map (Redis leaves an empty stream as a
// live key), so the map size is the key count; it reads zero before any stream
// command has built a registry on this shard.
func Len(cx *shard.Ctx) int {
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	return len(v.(*reg).m)
}

// RangeKeys calls fn with every stream key on this shard, the stream
// contribution to the unified KEYS and SCAN walk. An emptied-but-kept stream is
// a live key (XLEN reads 0), so it shows like any other. It reaches the registry
// through regs.Load so a shard that ran no stream command builds nothing and
// yields nothing. It returns false when fn asked to stop, halting the outer walk
// for a bounded scan. The slice fn receives is the map key's bytes, valid only
// for that call; fn copies what it keeps.
func RangeKeys(cx *shard.Ctx, fn func(key []byte) bool) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return true
	}
	for k := range v.(*reg).m {
		if !fn([]byte(k)) {
			return false
		}
	}
	return true
}

// undirty takes a stream off the gc worklist, swapping the tail into its slot
// since the worklist order carries no meaning. It is used only by Delete, so a
// stream removed from the registry never reaches the maintainer.
func (g *reg) undirty(s *stream) {
	for i, d := range g.dirty {
		if d == s {
			g.dirty[i] = g.dirty[len(g.dirty)-1]
			g.dirty = g.dirty[:len(g.dirty)-1]
			break
		}
	}
	s.gcDirty = false
}

// lookup finds the stream for key. present is nil when no stream exists; wrong is
// true when the key instead holds a string value, which every stream command
// answers with WRONGTYPE. Cross-type collisions with the other collection
// registries are not resolved in this slice, the same deferral those slices carry
// until keyspace unification. An emptied stream (all entries XDEL'd) is kept, not
// dropped: Redis leaves an empty stream in place (invariant that XLEN can read 0).
func (g *reg) lookup(cx *shard.Ctx, key []byte) (s *stream, wrong bool) {
	if s = g.m[string(key)]; s != nil {
		return s, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// waitListFor returns the blocking-XREAD FIFO for key, creating an empty one on
// first block. It lazily initializes the map so a registry built directly in a
// unit test can still park; the real registry() path pre-builds it.
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

// dropWaitersIfEmpty removes a waiter list from the registry once its last waiter
// leaves, so a key blocked on and then served leaves nothing behind.
func (g *reg) dropWaitersIfEmpty(wl *waitList) {
	if wl.n == 0 {
		delete(g.waiters, wl.key)
	}
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
