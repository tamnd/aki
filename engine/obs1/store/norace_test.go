//go:build !race

package store

// raceEnabled mirrors race_test.go for regular builds.
const raceEnabled = false
