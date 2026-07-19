package shard

import (
	"sync/atomic"
	"time"
)

// The active-expiry cycle's shard-side seam (spec 2064/f3/16 section 3). f3's
// correctness guarantee is lazy expiry: every read funnels through a live/peek that
// reaps an expired key on touch, so no command ever observes a key past its
// deadline. The active cycle only bounds how long an untouched expired key keeps its
// memory: it runs on the owner between batches and drops a bounded sample of expired
// keys across every keyspace, so a write-once key with a short TTL that no one reads
// again still frees on its own instead of lingering until a scan or a flush.
//
// The reaper itself lives in the dispatch package, the only one that imports all six
// keyspaces; it registers here through a plain func value so the one-way dependency
// (the type packages import shard, never the reverse) is never crossed. The worker
// calls it at the idle boundary, right after the maintainer, on the owner goroutine
// with the queue drained and no streamed reply in flight.

// activeExpireIntervalMs is the minimum wall-clock gap between two sweeps on one
// shard. It keeps a shard that drains and refills rapidly from sweeping on every
// idle boundary; a genuinely idle shard sweeps once and then parks, which is enough
// because nothing ages into expiry while no command runs. 100ms matches the cadence
// redis's active cycle targets.
const activeExpireIntervalMs int64 = 100

// activeExpireBudget caps how many keys one sweep examines per keyspace, so a cycle
// stays O(budget) and never monopolizes the owner's idle slice on a large keyspace.
// Whatever a sweep does not reach this pass a later pass or a first access reaps,
// and map iteration randomizes which slice each pass samples.
const activeExpireBudget = 400

// expiryReaper is the dispatch-registered cross-keyspace reap. It is set once at
// process start (dispatch's init, before any worker runs) and only read afterward,
// so a plain package var needs no synchronization of its own.
var expiryReaper func(cx *Ctx, nowMs int64, budget int) int

// RegisterExpiryReaper records the cross-keyspace reap the worker runs at idle. The
// dispatch package calls it once from init, before any shard starts.
func RegisterExpiryReaper(fn func(cx *Ctx, nowMs int64, budget int) int) { expiryReaper = fn }

// activeExpireOn toggles the cycle for the whole process, the state DEBUG
// SET-ACTIVE-EXPIRE flips. It is global rather than per-shard because the debug
// command lands on one arbitrary shard yet must gate every shard's cycle, and a
// single atomic-bool load at each idle boundary is free next to the sweep it guards.
// It starts enabled, matching redis's default.
var activeExpireOn atomic.Bool

func init() { activeExpireOn.Store(true) }

// SetActiveExpire enables or disables the active-expiry cycle across every shard,
// the effect DEBUG SET-ACTIVE-EXPIRE carries. Disabling leaves lazy expiry intact,
// so a key is still reaped on its next access; only the background reclamation of
// untouched keys pauses, which is exactly what the redis debug knob does for tests
// that want a key to survive untouched until they read it.
func SetActiveExpire(on bool) { activeExpireOn.Store(on) }

// ActiveExpireEnabled reports whether the cycle is currently running.
func ActiveExpireEnabled() bool { return activeExpireOn.Load() }

// runActiveExpire runs one sweep if the cycle is enabled, a reaper is registered,
// and the cadence interval has elapsed since this shard's last sweep. It reads its
// own wall clock the way fireTimers does, since the idle boundary carries no batch
// clock, and passes it to the reaper so every keyspace reaps against one instant. It
// counts the reaped keys into the shard's cumulative total, the figure INFO sums
// across shards as expired_keys. Owner goroutine only.
func (w *worker) runActiveExpire() {
	if expiryReaper == nil || !activeExpireOn.Load() {
		return
	}
	now := time.Now().UnixMilli()
	if now-w.lastExpireMs < activeExpireIntervalMs {
		return
	}
	w.lastExpireMs = now
	if n := expiryReaper(&w.cx, now, activeExpireBudget); n > 0 {
		w.expiredKeys += uint64(n)
	}
}

// ExpiredKeys is the cumulative number of keys this shard's active-expiry cycle has
// reaped, the count INFO surfaces as expired_keys. Zero on a bare Ctx with no
// worker. Owner goroutine only.
func (cx *Ctx) ExpiredKeys() uint64 {
	if cx.w == nil {
		return 0
	}
	return cx.w.expiredKeys
}
