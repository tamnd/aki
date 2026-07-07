//go:build linux

package f1raw

import (
	"os"
	"syscall"
	"unsafe"
)

// fadvDontNeed is POSIX_FADV_DONTNEED: advise the kernel that a file range is not
// needed soon, so it may drop those clean page-cache pages.
const fadvDontNeed = 4

// fadvRandom is POSIX_FADV_RANDOM: advise the kernel that a file is read at random,
// which sets its readahead window to zero. Cold reads are random point lookups, so
// without this the kernel prefetches a full readahead window (typically 128 KiB) per
// 1 KiB value read; the per-read DONTNEED only drops the range actually read, leaving
// the surrounding prefetched pages cached. Those readahead pages accumulate across a
// read storm and refill the cgroup even though each explicit range is dropped, which
// is what still tripped the OOM killer after per-read DONTNEED alone. Disabling
// readahead makes each cold read pull exactly the value's pages, which DONTNEED then
// drops, so the resident cache stays bounded to the in-flight reads.
const fadvRandom = 1

// pageSize is the host page size, cached once. FADV_DONTNEED acts on whole pages, so a
// range is rounded out to page boundaries before it is advised away.
var pageSize = uint64(os.Getpagesize())

// adviseDontNeed drops the page cache for the range just read from the cold log. The
// range is rounded down at the start and up at the end to page boundaries so the pages
// holding the value are fully covered. It is a hint, not a guarantee, and failure is
// ignored: the worst case is the cache lingers a little longer. Keeping this cache
// bounded to the in-flight reads is what holds f1raw's resident footprint at
// index-plus-keys under a hard memory cap.
func (c *coldLog) adviseDontNeed(off uint64, n int) {
	start := off &^ (pageSize - 1)
	end := (off + uint64(n) + pageSize - 1) &^ (pageSize - 1)
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, c.f.Fd(), uintptr(start), uintptr(end-start), fadvDontNeed, 0, 0)
}

// releaseArenaPages returns the physical pages backing arena offsets [off, off+n) to the OS
// with MADV_DONTNEED, so a segment freed under memory pressure drops its resident footprint
// instead of pinning its pages for the store's life. Without it, once a larger-than-memory
// firehose has touched every segment once, the whole arena stays resident even as segments are
// drained cold and reused, and that resident set plus the Go heap can cross a tight cgroup cap
// and trip the OOM killer (the 3.1GB-under-400M firehose death). The arena is a pointer-free
// byte slice, so dropping its pages is safe: a later write into a reused segment faults fresh
// zero pages, which initRecord fully rewrites before any index entry exposes the record. Only
// whole pages strictly inside the range are advised, so the partial page at either end that may
// hold a neighbor segment's bytes is never touched. It is a hint and failure is ignored: the
// worst case is the pages linger until the next reclaim. It runs only on the segmented reclaim
// path, so the non-segmented hot path never reaches it.
func (s *Store) releaseArenaPages(off, n uint64) {
	if len(s.arena) == 0 {
		return
	}
	ps := uintptr(pageSize)
	abs := uintptr(unsafe.Pointer(&s.arena[0]))
	astart := (abs + uintptr(off) + ps - 1) &^ (ps - 1)
	aend := (abs + uintptr(off+n)) &^ (ps - 1)
	if aend <= astart {
		return
	}
	_ = syscall.Madvise(s.arena[astart-abs:aend-abs], syscall.MADV_DONTNEED)
}

// adviseRandom marks the whole cold log for random access so the kernel disables
// readahead on it. It is applied once at open, before any read. Offset 0 and length 0
// mean "the entire file" for posix_fadvise. Failure is ignored: without it the cache
// still drains through DONTNEED, just with more prefetch churn.
func (c *coldLog) adviseRandom() {
	_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64, c.f.Fd(), 0, 0, fadvRandom, 0, 0)
}
