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
