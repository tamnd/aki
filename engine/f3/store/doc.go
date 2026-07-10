// Package store is the per-shard memory engine (spec 2064/f3/04): the
// open-addressed bucket index with dashtable segment-split growth, the
// segmented bump arena, and the 16-byte record frame. Everything here is
// single-owner by contract: exactly one goroutine reads and writes a shard's
// store, so every load is a plain load and every store is a plain store, and
// the package holds no mutex, no CAS, and no atomic on any path.
package store
