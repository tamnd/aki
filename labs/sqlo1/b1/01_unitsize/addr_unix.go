//go:build linux || darwin

package main

import "unsafe"

// bufAddr exposes a slice's base address for the alignment test.
func bufAddr(b []byte) uintptr {
	return uintptr(unsafe.Pointer(&b[0]))
}
