//go:build !race

package shard

// raceEnabled is false in a normal build, where the shared-pool identity
// assertions are deterministic under pinPool.
const raceEnabled = false
