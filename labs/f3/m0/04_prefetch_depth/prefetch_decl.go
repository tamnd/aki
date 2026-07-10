//go:build arm64 || amd64

package main

import "unsafe"

// prefetch issues a non-faulting L1 prefetch for the line holding p
// (PRFM PLDL1KEEP on arm64, PREFETCHT0 on amd64).
//
//go:noescape
func prefetch(p unsafe.Pointer)
