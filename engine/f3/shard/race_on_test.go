//go:build race

package shard

// raceEnabled is true when the binary is built with the race detector. Tests
// that assert sync.Pool Put-then-Get identity skip in this mode, because
// sync.Pool.Put randomly drops values under -race and identity cannot hold.
const raceEnabled = true
