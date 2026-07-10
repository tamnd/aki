// Package zset is the sorted-set type (spec 2064/f3/12): the counted B+ tree
// over (score, member) for O(log n) rank and range, the member table for O(1)
// ZSCORE, and the streamed zset algebra.
package zset
