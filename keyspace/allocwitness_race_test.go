//go:build race

package keyspace

// raceEnabled is true when the binary is built with -race. The race detector
// allocates shadow memory on essentially every access, so testing.AllocsPerRun
// reports counts that have nothing to do with the code under test. The
// allocation-witness tests use it to skip their thresholds under -race.
const raceEnabled = true
