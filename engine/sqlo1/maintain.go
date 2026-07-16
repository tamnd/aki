package sqlo1

// The maintenance half of the Store seam, doc 04 section 13. The
// Store interface carries data traffic; a backend that also feels
// disk-side pressure implements Maintainer, and the backpressure
// ladder type-asserts for it. MemStore does not implement it, so the
// WAL and free-extent rungs read zero over the placeholder exactly as
// they did when they were stubs.

import (
	"context"
	"errors"
)

// Pressure is a Maintainer's signal snapshot. Both gauges are
// continuous ratios, not booleans, because the ladder's whole shape
// is graded responses instead of cliff edges.
type Pressure struct {
	// Wal is checkpoint lag: WAL bytes appended past the trim barrier
	// over the checkpoint byte cadence. At 1 a checkpoint is due and
	// the timer tick takes it; at 4 it is overdue enough to ride the
	// command path.
	Wal float64
	// Extent is free-space scarcity: 0 at or above the free-extent
	// reserve, 1 at the hard minimum, above 1 past it. Any positive
	// value promotes compaction to foreground priority.
	Extent float64
	// Shed is the floor: headroom is down to one full drain plus a
	// checkpoint, and accepting more writes would risk wedging the
	// file. Writes bounce with ErrShed while it holds.
	Shed bool
}

// Maintainer is the optional maintenance surface behind the Store
// seam. Pressure must be cheap enough to poll per command; Checkpoint
// and CompactOnce are the two verbs the ladder spends pressure on,
// and CompactOnce reports whether it found debt worth paying.
type Maintainer interface {
	Pressure() Pressure
	Checkpoint() error
	CompactOnce(ctx context.Context) (bool, error)
}

// Minter is the optional Store capability behind rooth minting. The
// type layer never mints straight off a RAM counter: it takes a lease
// of n counters, durable before MintLease returns, and mints from the
// leased range, so a restart can never re-issue a counter whose rooth
// may already own live records on disk. Counters a crash strands in a
// lease are abandoned; MintRooth is a bijection, so holes cost address
// space and nothing else. MemStore implements it with a volatile mark,
// which is exactly as durable as everything else it holds.
type Minter interface {
	// MintLease reserves the next n rooth counters and returns the
	// first, so the caller owns [start, start+n).
	MintLease(ctx context.Context, n uint64) (start uint64, err error)
}

// ErrShed rejects a write at the disk hard minimum. This is the
// honest failure mode: Redis errors at maxmemory, sqlo1 errors at
// disk-full, and the bench protocol records both identically. Reads
// and deletes keep working, and the store recovers on its own once
// compaction frees space or the cap is raised.
var ErrShed = errors.New("sqlo1: disk at hard minimum, write shed")
