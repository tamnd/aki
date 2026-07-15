//go:build !linux && !darwin

package main

import "os"

// openIO has no direct-IO flag on this platform; the direct column
// records 0 so a cached run can never be mistaken for a verdict.
func openIO(path string, write, _ bool) (*os.File, bool, error) {
	flags := os.O_RDONLY
	if write {
		flags = os.O_RDWR
	}
	f, err := os.OpenFile(path, flags, 0)
	return f, false, err
}

func alignedBuf(n, _ int) []byte {
	return make([]byte, n)
}
