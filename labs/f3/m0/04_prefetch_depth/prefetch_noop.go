//go:build !arm64 && !amd64

package main

import "unsafe"

// prefetch is a no-op on architectures without a shim; the sweep degrades to
// measuring the pipeline restructuring alone.
func prefetch(p unsafe.Pointer) { _ = p }
