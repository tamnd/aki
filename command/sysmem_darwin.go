//go:build darwin

package command

import "syscall"

// systemMemoryBytes reads hw.memsize, the total physical memory in bytes. The
// sysctl returns the value as raw little-endian bytes, so we decode them by hand.
// syscall.Sysctl strips one trailing NUL, which only ever drops a high zero byte
// here, so padding the missing high bytes with zero reconstructs the value. It
// returns 0 when the sysctl is unavailable.
func systemMemoryBytes() int64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0
	}
	b := []byte(s)
	var v uint64
	for i := 0; i < len(b) && i < 8; i++ {
		v |= uint64(b[i]) << (8 * uint(i))
	}
	return int64(v)
}
