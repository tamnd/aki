package main

// Cap-aware buffer-pool sizing. The .aki file is read with buffered I/O, so every
// page the buffer pool holds also sits in the OS page cache. Under a tight memory
// cap (a cgroup limit in a container) the pool and the page cache are charged to
// the same budget and fight for it: a pool sized at a large fraction of the cap
// crowds the page cache out, so far fewer unique file pages stay resident and
// larger-than-memory reads fault to disk. Measured on a 300 MB cgroup over a 3.1 GB
// zset, a 128 MB pool (the old default, 43% of the cap) ran ZRANK at ~1.4k rps while
// a 64-96 MB pool ran it at ~6k rps, a memory-pressure cliff documented in
// Spec/2064 ltm note 353.
//
// The fix is to default the pool to a quarter of the detected memory cap, leaving
// three quarters for the OS page cache and the Go heap, which keeps the cliff away
// and lets the page cache cover the rest of the working set. On an uncapped host
// there is no contention (the page cache uses free RAM the pool never touches), so
// the historical 128 MB default stands.

const (
	// defaultBufferPoolBytes is the pool size on an uncapped host, the value the
	// --buffer-pool-size flag carried before cap-aware sizing.
	defaultBufferPoolBytes = 128 * 1024 * 1024

	// minAutoBufferPoolBytes floors the auto size so a very small cap still leaves
	// the pool enough frames to hold the hot interior pages of the B-tree.
	minAutoBufferPoolBytes = 16 * 1024 * 1024
)

// autoPoolBytesFor is the pure sizing rule, split out so it can be tested without
// reading real cgroup or meminfo files. host is total system RAM in bytes (0 if
// unknown); limit is the cgroup memory cap in bytes (0 if there is none or it is
// unlimited). When a finite cap sits below host RAM we are in a constrained
// container and size to a quarter of the cap (floored); otherwise we keep the
// historical default.
func autoPoolBytesFor(host, limit int64) int64 {
	capped := limit > 0 && (host <= 0 || limit < host)
	if !capped {
		return defaultBufferPoolBytes
	}
	b := limit / 4
	if b < minAutoBufferPoolBytes {
		b = minAutoBufferPoolBytes
	}
	return b
}

// autoBufferPoolBytes resolves the buffer-pool size for the "auto" setting by
// reading the host RAM and the cgroup cap, then applying autoPoolBytesFor. The two
// readers are platform specific (real files on Linux, zero elsewhere), so on a
// non-Linux dev box this returns the plain default.
func autoBufferPoolBytes() int64 {
	return autoPoolBytesFor(hostMemoryBytes(), cgroupMemoryLimitBytes())
}
