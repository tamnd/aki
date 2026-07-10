//go:build race

package store

// raceEnabled reports the race detector is on; the RSS ground-truth bar in
// the churn test is skipped there because the detector's shadow memory
// inflates the process footprint by a workload-dependent multiple.
const raceEnabled = true
