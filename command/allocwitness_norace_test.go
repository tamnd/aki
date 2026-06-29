//go:build !race

package command

// raceEnabled is false on a normal build, where testing.AllocsPerRun measures
// only the code under test and the allocation-witness thresholds are meaningful.
const raceEnabled = false
