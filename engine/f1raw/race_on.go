//go:build race

package f1raw

// raceEnabled reports whether the binary was built with -race. The hot-key
// contention test reads it to skip the one case whose seqlock value memcpy is a
// benign-by-construction data race the detector cannot model.
const raceEnabled = true
