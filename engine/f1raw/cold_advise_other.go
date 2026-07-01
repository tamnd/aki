//go:build !linux

package f1raw

// adviseDontNeed is a no-op off Linux, where FADV_DONTNEED is not available. The
// larger-than-memory regime is benchmarked on Linux; elsewhere the cold reads leave
// their pages in the OS cache, which is fine without a hard cgroup memory cap.
func (c *coldLog) adviseDontNeed(off uint64, n int) {}

// adviseRandom is a no-op off Linux. Disabling readahead to bound the cold-read cache
// only matters under the Linux cgroup memory cap the LTM regime is benchmarked with.
func (c *coldLog) adviseRandom() {}
