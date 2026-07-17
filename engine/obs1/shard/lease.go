package shard

// LeaseView is the worker's read side of the holder's lease belief (doc 02
// section 3.5, doc 04 section 6): whether a group's writes may be acked right
// now, and if not, whether they should park or redirect. The engine's
// LeaseGate satisfies it structurally, the same import-boundary shape as
// WriteLog: the shard package never imports the engine root, and the driver
// that owns both declares the typed field.
//
// The contract mirrors the guard it fronts. A group whose believed deadline
// minus the skew bound has passed is suspended: its writes park under
// ParkLease, reads keep flowing, and the retry loop keeps re-running the gate.
// A group a foreign grant demoted redirects instead: parked and fresh writes
// take the doc 07 MOVED reply naming the taker, never an error. The progress
// signal is Renewals, which moves on every successful chain append of the
// holder's own (a commit or a heartbeat), because that is exactly what
// extends the believed deadline.
//
// All methods take the worker's batch clock (Ctx.NowMs, Unix milliseconds)
// so the view stays deterministic under test and the hot path never reads a
// wall clock of its own. Implementations must be safe for concurrent readers:
// every shard owner consults one shared view.
type LeaseView interface {
	// Gated reports whether any group might need parking or redirecting at
	// now: the cheap whole-view check the executeCmd gate loads before it
	// spends a hash on the key. False means every tracked group's deadline
	// sits safely in the future and nothing is demoted, so writes flow with
	// no per-group lookup. True only widens the check; the per-group methods
	// decide.
	Gated(nowMs int64) bool
	// Suspended reports whether the group must park its writes at now: its
	// believed deadline minus the skew bound has passed, or the view never
	// saw it renewed (suspended by definition, doc 02 section 3.5).
	Suspended(group uint16, nowMs int64) bool
	// AnySuspended reports whether any group is suspended at now, the gate
	// for keyless writes (FLUSHALL fan subs) that touch every group at once.
	AnySuspended(nowMs int64) bool
	// Demoted reports the endpoint a foreign grant handed the group to,
	// "host:port" as the view learned it (possibly ":port" with no host,
	// doc 07 section 2), and whether the group is demoted at all. Checked
	// before Suspended so a demoted group redirects rather than parks.
	Demoted(group uint16) (endpoint string, ok bool)
	// Renewals counts successful deadline extensions, the lease stall
	// window's progress signal: while it moves, appends are landing and a
	// parked write is one renewal away from running.
	Renewals() uint64
}
