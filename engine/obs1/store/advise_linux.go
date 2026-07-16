//go:build linux

package store

import (
	"os"
	"syscall"
	"unsafe"
)

var pageSize = uint64(os.Getpagesize())

// releasePages returns the physical pages backing arena offsets [off, off+n)
// to the OS with MADV_DONTNEED, so a freed segment drops its resident
// footprint instead of pinning its pages for the store's life. The arena is a
// pointer-free byte slice, so dropping pages is safe: a later write into a
// reused segment faults fresh zero pages, which initRecord fully rewrites
// before any index entry exposes the record. Only whole pages strictly inside
// the range are advised, so a partial page shared with a neighbor segment is
// never touched. It is a hint and failure is ignored; the worst case is the
// pages linger.
func (a *arena) releasePages(off, n uint64) {
	if len(a.buf) == 0 {
		return
	}
	ps := uintptr(pageSize)
	abs := uintptr(unsafe.Pointer(&a.buf[0]))
	astart := (abs + uintptr(off) + ps - 1) &^ (ps - 1)
	aend := (abs + uintptr(off+n)) &^ (ps - 1)
	if aend <= astart {
		return
	}
	_ = syscall.Madvise(a.buf[astart-abs:aend-abs], syscall.MADV_DONTNEED)
}
