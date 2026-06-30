//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// hostMemoryBytes reads MemTotal from /proc/meminfo and returns it in bytes, or 0
// if it cannot be read. This is the physical RAM ceiling used to decide whether a
// cgroup limit is actually a cap (limit below host RAM) or just the host size.
func hostMemoryBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		// "MemTotal:  16384256 kB"
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

// cgroupMemoryLimitBytes returns the effective cgroup memory cap in bytes, or 0 if
// there is none or it is unlimited. It tries cgroup v2 first (the unified
// hierarchy systemd uses), walking from the process's own cgroup up to the root
// and taking the tightest finite memory.max, then falls back to cgroup v1's
// memory.limit_in_bytes. An unlimited level reads "max" (v2) or a near-INT64 max
// sentinel (v1), both treated as no cap.
func cgroupMemoryLimitBytes() int64 {
	if v := cgroupV2Limit(); v > 0 {
		return v
	}
	return cgroupV1Limit()
}

// cgroupV2Limit resolves the process's unified cgroup path from /proc/self/cgroup
// (a single "0::/path" line under v2) and reads memory.max at that level and every
// ancestor up to the mount root, returning the smallest finite value found.
func cgroupV2Limit() int64 {
	rel := cgroupV2Path()
	if rel == "" {
		return 0
	}
	const root = "/sys/fs/cgroup"
	dir := filepath.Join(root, filepath.Clean("/"+rel))
	var best int64
	for {
		if v := readCgroupMax(filepath.Join(dir, "memory.max")); v > 0 {
			if best == 0 || v < best {
				best = v
			}
		}
		if dir == root || !strings.HasPrefix(dir, root) {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return best
}

// cgroupV2Path returns the path portion of the unified cgroup v2 entry in
// /proc/self/cgroup, or "" if the process is not on a v2 hierarchy.
func cgroupV2Path() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		// v2 unified line is "0::/the/path"
		if strings.HasPrefix(line, "0::") {
			return strings.TrimPrefix(line, "0::")
		}
	}
	return ""
}

// readCgroupMax reads a single memory.max-style file: "max" means unlimited
// (returns 0), a numeric value returns the byte count, anything else returns 0.
func readCgroupMax(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

// cgroupV1Limit reads the cgroup v1 memory controller limit. The unlimited
// sentinel is a value at or near INT64 max (often 0x7ffffffffffff000), which the
// caller's host-RAM comparison discards, but we also reject the obvious sentinel
// here so a v1 host with no limit reports no cap.
func cgroupV1Limit() int64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	// Anything within a page of INT64 max is the "unlimited" sentinel.
	if v >= (1<<63)-4096 {
		return 0
	}
	return v
}
