package sqlo1b

import "errors"

// The ring backend (doc 04 section 12, milestone B5): io_uring behind
// the same Backend shape as IOPool, raw syscalls, no cgo, linux only.
// This file is the portable surface; ioring_linux.go holds the ring,
// ioring_other.go stubs every other platform. iopool stays the
// correctness baseline everywhere (R-I8), and the startup path is
// probe-then-fallback: a ring that cannot set up, or a kernel or
// seccomp profile that denies it, means iopool, never an error the
// caller has to route.

// ErrRingUnsupported reports that io_uring is not usable here: not
// Linux, a kernel too old for the ops the backend needs, or a seccomp
// profile (Docker's default) denying the syscalls. Callers fall back
// to IOPool and record the fact in INFO, because a gate run must know
// which backend ran (doc 13).
var ErrRingUnsupported = errors.New("sqlo1b: io_uring unsupported")

// Batch-submit thresholds (doc 04 section 12): 16 is the measured
// syscall amortization point, 8 the tail guard the submitter drops to
// under pressure. Both are provisional until the ringpool sweep on
// the gate box; the sweep prices exactly this pair.
const (
	ringBatchTarget = 16
	ringBatchLow    = 8
)

// ringFlushNow is the submitter's batching decision, pure so it tests
// without a ring. pending counts pushed-but-unsubmitted SQEs; the
// queue-empty case is the drain-window tick (nothing else is coming,
// holding the batch only adds latency); a full SQ must flush before
// the next push can land; otherwise accumulate to the target, which
// drops to the low threshold once in-flight work covers half the CQ,
// because every queued request then waits behind a deep line and
// smaller batches are what keeps p99 flat.
func ringFlushNow(pending, sqEntries, inflight, cqEntries int, queueEmpty bool) bool {
	if pending == 0 {
		return false
	}
	if queueEmpty || pending >= sqEntries {
		return true
	}
	target := ringBatchTarget
	if inflight >= cqEntries/2 {
		target = ringBatchLow
	}
	return pending >= target
}
