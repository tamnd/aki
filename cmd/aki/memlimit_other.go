//go:build !linux

package main

// On non-Linux hosts there is no /proc/meminfo or cgroup hierarchy to read, so the
// auto sizer has no cap to detect and falls back to the plain default. These stubs
// keep the cross-platform build and the dev box (macOS) on the historical 128 MB
// pool.

func hostMemoryBytes() int64 { return 0 }

func cgroupMemoryLimitBytes() int64 { return 0 }
