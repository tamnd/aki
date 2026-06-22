//go:build !linux && !darwin

package command

// systemMemoryBytes has no portable form on other platforms, so it reports 0 and
// the disk-vs-ram ratio is omitted. aki targets Linux and macOS.
func systemMemoryBytes() int64 { return 0 }
