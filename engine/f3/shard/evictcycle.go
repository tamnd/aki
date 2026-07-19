package shard

// The maxmemory eviction seam on the shard side (spec 2064/f3/16 sections 6 and
// 7). The policy machinery and the cross-keyspace victim walk live in the server
// layer, which is the only package that imports every keyspace; the shard holds
// just the hook the worker calls at a boundary and the counter INFO sums. This is
// the same one-way seam the active-expiry reaper uses (expirecycle.go) and the
// collection demoter uses (worker.demoteColl): the store cannot import the type
// packages, so the policy is injected as a func the worker owns.
//
// The hook self-gates: it reads the live maxmemory setting first and returns at
// once when eviction is not configured, so a store that never sets maxmemory pays
// one nil check plus the hook's own leading load per boundary and nothing else,
// the L9 zero-delta contract the gate configs (which never set maxmemory) rely on.

// UseEvictor registers the maxmemory eviction hook every worker's boundary calls
// (runEvict). The server layer passes the cross-keyspace evictor (dispatch.Evictor),
// which weighs the store's live RAM plus every collection's resident footprint
// against the shard's budget share and sheds victims by policy; a runtime that
// wants no eviction leaves the hook nil and the boundary skips it. Fixed before
// Start like Use and UseDemoter, so the worker reads it with no synchronization on
// the hot path. The hook returns the number of keys it evicted this call.
func (r *Runtime) UseEvictor(fn func(*Ctx) int) {
	if r.started {
		panic("shard: UseEvictor after Start")
	}
	for _, w := range r.workers {
		w.evictor = fn
	}
}

// runEvict runs one bounded eviction pass at a worker boundary when a hook is
// registered. A nil hook (a runtime with no evictor wired) is one branch; a wired
// hook that finds maxmemory unset returns after its own leading load. A pass that
// sheds keys credits them to the shard counter INFO reports as evicted_keys.
func (w *worker) runEvict() {
	if w.evictor == nil {
		return
	}
	if n := w.evictor(&w.cx); n > 0 {
		w.evictedKeys += uint64(n)
	}
}

// Shards reports the runtime's shard count to a hook running on the owner, the
// divisor the evictor uses to turn the global maxmemory into this shard's 1/N
// budget share (spec 2064/f3/16 section 6.1). It reads one for a bare Ctx built
// outside a runtime (tests), so a share computed off it equals the whole budget.
func (cx *Ctx) Shards() int {
	if cx.w == nil || cx.w.rt == nil {
		return 1
	}
	return len(cx.w.rt.workers)
}

// EvictedKeys reports this shard's cumulative eviction count for INFO's
// evicted_keys stat. Owner goroutine only, like every other counter read; a bare
// Ctx reports zero.
func (cx *Ctx) EvictedKeys() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.evictedKeys
}
