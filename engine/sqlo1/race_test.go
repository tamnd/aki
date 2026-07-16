//go:build race

package sqlo1

// raceEnabled gates allocation-count assertions: the race detector
// instruments allocations and the counts stop meaning anything.
const raceEnabled = true
