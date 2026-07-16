//go:build !linux

package store

// adviseDontNeed is a no-op off Linux, where FADV_DONTNEED is not available.
// The larger-than-memory regime is gated on Linux; elsewhere log reads leave
// their pages in the OS cache, which is fine without a hard cap.
func (l *vlog) adviseDontNeed(off uint64, n int) {}

// adviseRandom is a no-op off Linux; see adviseDontNeed.
func (l *vlog) adviseRandom() {}
