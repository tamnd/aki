//go:build !linux

package uring

// Ring is the non-Linux stub. The constructor always fails, so callers fall back
// to the goroutine networking path exactly as they do when the Linux kernel
// refuses a ring.
type Ring struct{}

// New reports that io_uring is unavailable on this platform.
func New(entries uint32) (*Ring, error) { return nil, ErrUnsupported }

// The remaining methods exist so the type satisfies the same shape on every
// platform; none is reachable because New never returns a ring here.

func (r *Ring) Close() error { return nil }

func (r *Ring) PrepRecv(userData uint64, fd int, buf []byte) bool { return false }

func (r *Ring) PrepSend(userData uint64, fd int, buf []byte) bool { return false }

func (r *Ring) PrepNop(userData uint64) bool { return false }

func (r *Ring) Submit() (int, error) { return 0, ErrUnsupported }

func (r *Ring) SubmitAndWait(minComplete uint32) (int, error) { return 0, ErrUnsupported }

func (r *Ring) Reap(into []Completion) int { return 0 }
