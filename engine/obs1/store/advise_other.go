//go:build !linux

package store

// releasePages is a no-op off Linux, where MADV_DONTNEED is not the mechanism
// to hand pages back. The larger-than-memory regime is gated on Linux under a
// cgroup cap; elsewhere a freed segment's pages stay resident, which is fine
// without a hard cap.
func (a *arena) releasePages(off, n uint64) {}
