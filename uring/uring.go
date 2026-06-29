// Package uring is a minimal, zero-dependency binding to the Linux io_uring
// interface, built for aki's networking layer.
//
// The reactor in networking/reactor_linux.go services many ready connections off
// one epoll_wait, but it still issues one read and one write syscall per
// connection. A CPU profile of the integrated server at GET saturation puts about
// 69% of all CPU in those two syscalls, write alone at 51% (Spec/2064 note 299),
// so the only lever with the mass to move throughput is cutting the syscall count.
// io_uring is the primitive for that: one io_uring_enter submits and reaps the
// reads and writes for every connection a loop woke that turn, instead of a
// syscall per connection.
//
// This package is the foundation slice. It owns the ring setup, submission, and
// completion mechanics and nothing else; it is wired into no server path yet, so
// it cannot regress the running server or the compatibility surface. The reactor
// integration is a later slice that builds on this.
//
// Only the opcodes and flags aki needs are defined. The Linux implementation is in
// uring_linux.go; other platforms get a stub that reports ErrUnsupported so the
// package, and anything that imports it, still builds everywhere.
package uring

import "errors"

// ErrUnsupported is returned by New on platforms without io_uring (everything that
// is not Linux) and by a ring whose kernel refused setup.
var ErrUnsupported = errors.New("uring: io_uring not supported on this platform")

// Completion is one reaped io_uring completion: the user_data the submission
// carried and the result the kernel returned (a byte count when non-negative, or
// the negation of an errno when negative, matching the raw read/write contract).
type Completion struct {
	UserData uint64
	Res      int32
	Flags    uint32
}
