//go:build darwin

package main

import (
	"os"
	"syscall"
	"unsafe"
)

// openIO opens path with F_NOCACHE, the darwin stand-in for O_DIRECT.
// Local smoke only; the verdict runs on the linux gate box.
func openIO(path string, write, direct bool) (*os.File, bool, error) {
	flags := os.O_RDONLY
	if write {
		flags = os.O_RDWR
	}
	f, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, false, err
	}
	if direct {
		if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_NOCACHE, 1); errno != 0 {
			f.Close()
			return nil, false, errno
		}
	}
	return f, direct, nil
}

// alignedBuf returns an n-byte slice whose base address is a multiple
// of align; F_NOCACHE does not require it, but keeping the IO path
// identical to the linux build keeps the smoke honest.
func alignedBuf(n, align int) []byte {
	b := make([]byte, n+align)
	off := align - int(uintptr(unsafe.Pointer(&b[0]))&uintptr(align-1))
	if off == align {
		off = 0
	}
	return b[off : off+n : off+n]
}
