//go:build !race

package f2raw

// raceEnabled reports whether the binary was built with -race. See race_on.go.
const raceEnabled = false
