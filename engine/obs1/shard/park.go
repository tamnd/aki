package shard

// The obs1 park-reason taxonomy (spec 2064/obs1 doc 04 section 6). f3 parks a
// write for exactly one reason, a full arena; obs1 keeps that machinery
// (backpressure.go: ParkFull, the full-waiter FIFO, retryFull, the stall
// window) and names three reasons a write can park, each with its own progress
// signal and its own stall reply:
//
//   - resident: the store's resident budget is exhausted and eviction needs
//     fold to catch up, since only folded records are evictable (doc 05). This
//     is the f3 arena-full park under its obs1 name; doc 04 keeps its stall
//     reply as the f3 string unchanged (store.ErrFull with the store taxonomy
//     cause appended). Until fold exists the progress signal is the f3 cold
//     cursor the port carried; the fold milestone swaps it for the group's
//     fold cursor.
//   - flushlag: the WAL buffer exceeds its cap, which in practice means the
//     bucket is refusing PUTs or the chain is refusing appends. Progress
//     signal: a successful flush. Stall reply: "ERR store: flush stalled".
//     Registered here, raised by nothing until the WAL lands (O1b).
//   - lease: the group self-suspended (doc 02 section 3.5). Progress signal:
//     a successful chain append under the same epoch, or demotion, in which
//     case parked writers fail over with the doc 07 MOVED redirect rather
//     than an error. Registered here, raised by nothing until the lease guard
//     is wired into serving.
//
// Registration means the names exist, every park and stall-out is counted
// under its reason, and the per-reason counters render in INFO through the
// stats schema (stats.go), so the flushlag and lease rows sit at zero in every
// INFO until their slices raise them and the park-storm lab can read the split
// without a schema change.

// ParkReason names why a write parked (doc 04 section 6).
type ParkReason uint8

const (
	// ParkResident is the resident-budget park, the f3 arena-full park under
	// its obs1 name; the only reason raised until the WAL and lease slices.
	ParkResident ParkReason = iota
	// ParkFlushlag is the WAL-buffer-over-cap park; registered, not yet raised.
	ParkFlushlag
	// ParkLease is the self-suspended-group park; registered, not yet raised.
	ParkLease
	numParkReasons
)

// parkReasonNames are the INFO suffixes, fixed by doc 04 section 6.
var parkReasonNames = [numParkReasons]string{
	ParkResident: "resident",
	ParkFlushlag: "flushlag",
	ParkLease:    "lease",
}

// String reports the reason's doc 04 name, "unknown" for a value off the
// taxonomy.
func (r ParkReason) String() string {
	if r >= numParkReasons {
		return "unknown"
	}
	return parkReasonNames[r]
}

// ParkWaits is the cumulative number of writes this shard has parked for
// reason r, the per-reason split of BackpressureWaits the INFO taxonomy rows
// surface. Zero on a bare Ctx with no worker or for a value off the taxonomy.
// Owner goroutine only.
func (cx *Ctx) ParkWaits(r ParkReason) uint64 {
	if cx.w == nil || r >= numParkReasons {
		return 0
	}
	return cx.w.bpReasonWaits[r]
}

// ParkStalls is the cumulative number of parked writes this shard has failed
// after a genuine stall, split by the reason they were parked under, the
// per-reason split of BackpressureStalls. Zero on a bare Ctx with no worker or
// for a value off the taxonomy. Owner goroutine only.
func (cx *Ctx) ParkStalls(r ParkReason) uint64 {
	if cx.w == nil || r >= numParkReasons {
		return 0
	}
	return cx.w.bpReasonStalls[r]
}
