//go:build !linux && !darwin

package main

// bufAddr is only asserted on the direct-IO platforms.
func bufAddr(_ []byte) uintptr {
	return 0
}
