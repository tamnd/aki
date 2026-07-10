//go:build linux

package shard

import (
	"os"
	"strconv"
	"strings"
)

// readRSS reads the process resident set from /proc/self/statm: the second
// field is resident pages. It runs on the INFO render path only, so one file
// read per call is fine, and any failure reads as zero, which the render
// treats as "omit the field".
func readRSS() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
