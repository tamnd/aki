//go:build linux

package f1raw

import (
	"os"
	"syscall"
)

// fadvDontNeed is POSIX_FADV_DONTNEED: advise the kernel that a file range is not
// needed soon, so it may drop those clean page-cache pages.
const fadvDontNeed = 4

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
