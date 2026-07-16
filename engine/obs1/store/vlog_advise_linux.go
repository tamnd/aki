//go:build linux

package store

import "syscall"

// fadvDontNeed is POSIX_FADV_DONTNEED: the kernel may drop the clean
// page-cache pages of the advised range.
const fadvDontNeed = 4

// fadvRandom is POSIX_FADV_RANDOM: sets the file's readahead window to zero.
// Log reads are random point lookups; without this each read prefetches a
// full readahead window (typically 128KiB) whose pages the per-read DONTNEED
// does not cover, and under a memory cap that prefetch cache alone can fill
// the cgroup.
const fadvRandom = 1

// adviseDontNeed drops the page cache for the range just read from the log,
// rounded out to page boundaries so the value's pages are fully covered. It
// is a hint and failure is ignored: the worst case is the cache lingers.
// Bounding the read cache to the in-flight reads is what holds the resident
// footprint at index-plus-keys under a hard memory cap.
func (l *vlog) adviseDontNeed(off uint64, n int) {
	start := off &^ (pageSize - 1)
	end := (off + uint64(n) + pageSize - 1) &^ (pageSize - 1)
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, l.f.Fd(), uintptr(start), uintptr(end-start), fadvDontNeed, 0, 0)
}

// adviseRandom marks the whole log for random access, once at open, before
// any read. Offset 0 and length 0 mean the entire file for posix_fadvise.
func (l *vlog) adviseRandom() {
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, l.f.Fd(), 0, 0, fadvRandom, 0, 0)
}
