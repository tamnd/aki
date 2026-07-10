//go:build !linux

package shard

// readRSS reports zero where no cheap resident-set read exists; the INFO
// render omits used_memory_rss rather than invent a number.
func readRSS() uint64 { return 0 }
