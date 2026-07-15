//go:build linux

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// openIO opens path with O_DIRECT so reads and writes hit the device
// instead of the page cache; that is the whole measurement. Callers
// must use 4096-aligned buffers, offsets, and lengths.
func openIO(path string, write, direct bool) (*os.File, bool, error) {
	flags := os.O_RDONLY
	if write {
		flags = os.O_RDWR
	}
	if direct {
		flags |= syscall.O_DIRECT
	}
	f, err := os.OpenFile(path, flags, 0)
	return f, direct, err
}

// alignedBuf returns an n-byte slice whose base address is a multiple
// of align, as O_DIRECT requires.
func alignedBuf(n, align int) []byte {
	b := make([]byte, n+align)
	off := align - int(uintptr(unsafe.Pointer(&b[0]))&uintptr(align-1))
	if off == align {
		off = 0
	}
	return b[off : off+n : off+n]
}
