// Package akifile is the single-file durability format (spec 2064/f3/07):
// 16KiB pages, dual meta slots, cold-chunk and value-log regions, checkpoints,
// forkless log-watermark snapshots, and per-shard parallel recovery.
package akifile
