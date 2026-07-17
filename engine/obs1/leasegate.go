package obs1

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// LeaseGate adapts a LeaseGuard to the shard runtime's serving gate: it is
// the LeaseView the drivers hand to Runtime.UseLeaseView, satisfied
// structurally because the engine root cannot import the shard package (the
// same import-boundary shape as the WriteLog seam). The guard keeps the
// doc 02 section 3.5 belief per group; the gate adds what serving needs on
// top of it: a cached horizon so the per-write fast path is one atomic
// compare against the batch clock, a renewal counter as the stall window's
// progress signal, and the demotion table that turns a parked write into the
// doc 07 MOVED redirect.
//
// Renewal comes from the holder's own successful chain appends: wire
// OnAppended as the committer's append hook (WriteLogConfig.Gate) and every
// committed batch extends the deadlines of the groups it carried, which is
// literally the doc 02 progress rule. The heartbeat loop that renews idle
// groups is the O3a lease manager's job and plugs into the same Renewed;
// until it exists, an idle gated group runs down its TTL and suspends, which
// is the honest single-PR behavior.
//
// Clock convention: the shard side runs on the worker batch clock, Unix
// milliseconds, so the view methods take nowMs and convert with
// time.UnixMilli; the guard side keeps time.Time because the O3a scheduler
// wants Horizon as an instant.
type LeaseGate struct {
	guard *LeaseGuard

	// renewals is the LeaseView progress signal; horizonMs caches the
	// guard's horizon as batch-clock milliseconds so Gated is one load and
	// one compare. Tracking nothing caches math.MinInt64: always gated,
	// every group suspended by definition.
	renewals  atomic.Uint64
	horizonMs atomic.Int64

	// demotedN keeps the Gated fast path free of the mutex: it mirrors
	// len(demoted) so an all-renewed gate with no demotions never locks.
	demotedN atomic.Int32
	mu       sync.Mutex
	demoted  map[uint16]string
}

// NewLeaseGate builds a gate over a fresh guard; zero durations take the
// doc 02 defaults. The gate starts tracking nothing, so every group is
// suspended until the boot path grants and renews them (SetGroup's lease
// analogue): grant records folded at boot tell the node what it holds, and
// one Renewed per held group arms the gate.
func NewLeaseGate(ttl, skew time.Duration) *LeaseGate {
	g := &LeaseGate{
		guard:   NewLeaseGuard(ttl, skew),
		demoted: make(map[uint16]string),
	}
	g.horizonMs.Store(math.MinInt64)
	return g
}

// recalcHorizon refreshes the cached fast-path horizon after any guard
// mutation.
func (g *LeaseGate) recalcHorizon() {
	h, ok := g.guard.Horizon()
	if !ok {
		g.horizonMs.Store(math.MinInt64)
		return
	}
	g.horizonMs.Store(h.UnixMilli())
}

// Renewed records a successful append that extended the group's lease at
// local time at. It deliberately does not clear a demotion: the gate checks
// Demoted before Suspended, so a late renewal that raced a foreign grant
// cannot resurrect a demoted group; only the explicit re-grant path (O3a)
// clears it through Regrant.
// The count moves before the deadline becomes visible: a worker that saw
// the group un-suspend and delivered a held reply must find the count
// already moved, or an observer sequenced behind the reply reads a stale
// count. The stall window only needs movement, so counting a hair early
// is harmless in the other direction.
func (g *LeaseGate) Renewed(group uint16, at time.Time) {
	g.renewals.Add(1)
	g.guard.Renewed(group, at)
	g.recalcHorizon()
}

// OnAppended is the committer hook (CommitterConfig.OnAppended): a batch of
// commit records landed on the chain, so every group in it renewed at now.
// One renewal count per batch, matching one append per batch. The count
// moves first for the Renewed reason above.
func (g *LeaseGate) OnAppended(groups []uint16) {
	g.renewals.Add(1)
	now := time.Now()
	g.guard.mu.Lock()
	for _, gr := range groups {
		g.guard.deadline[gr] = now.Add(g.guard.ttl)
	}
	g.guard.mu.Unlock()
	g.recalcHorizon()
}

// Demote hands the group to a taker a foreign grant named: the belief is
// dropped and writes redirect to endpoint ("host:port", possibly ":port"
// with no host, doc 07 section 2) until Regrant.
func (g *LeaseGate) Demote(group uint16, endpoint string) {
	g.guard.Drop(group)
	g.mu.Lock()
	g.demoted[group] = endpoint
	g.demotedN.Store(int32(len(g.demoted)))
	g.mu.Unlock()
	g.recalcHorizon()
}

// Release forgets the group voluntarily, no redirect: its writes suspend
// until a new grant renews it.
func (g *LeaseGate) Release(group uint16) {
	g.guard.Drop(group)
	g.recalcHorizon()
}

// Regrant clears a demotion after the fold showed the group granted back to
// this node, and renews it at local time at; the O3a grant path calls it.
func (g *LeaseGate) Regrant(group uint16, at time.Time) {
	g.mu.Lock()
	delete(g.demoted, group)
	g.demotedN.Store(int32(len(g.demoted)))
	g.mu.Unlock()
	g.Renewed(group, at)
}

// Gated implements the LeaseView fast path: false while every tracked
// group's horizon sits past now and nothing is demoted.
func (g *LeaseGate) Gated(nowMs int64) bool {
	return nowMs >= g.horizonMs.Load() || g.demotedN.Load() > 0
}

// Suspended implements the LeaseView per-group check on the guard's belief.
func (g *LeaseGate) Suspended(group uint16, nowMs int64) bool {
	return g.guard.Suspended(group, time.UnixMilli(nowMs))
}

// AnySuspended reports whether any group is suspended at now, the keyless
// write gate. It reads the cached horizon, which covers tracked groups; a
// group the gate never tracked is invisible here, so the boot path must
// renew every group the node serves before writes flow (the O1b harness
// grants all groups, and the O3a manager owns the general case).
func (g *LeaseGate) AnySuspended(nowMs int64) bool {
	return nowMs >= g.horizonMs.Load()
}

// Demoted reports the taker's endpoint for a demoted group.
func (g *LeaseGate) Demoted(group uint16) (string, bool) {
	if g.demotedN.Load() == 0 {
		return "", false
	}
	g.mu.Lock()
	ep, ok := g.demoted[group]
	g.mu.Unlock()
	return ep, ok
}

// Renewals implements the LeaseView progress signal.
func (g *LeaseGate) Renewals() uint64 {
	return g.renewals.Load()
}
