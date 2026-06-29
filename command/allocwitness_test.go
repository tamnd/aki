package command

import "testing"

// skipAllocWitnessUnderRace bails out of an allocation-witness test when the
// binary is built with -race. These tests assert that a command touches only a
// bounded, count-driven number of allocations instead of materializing a whole
// collection, measured with testing.AllocsPerRun. The race detector adds its own
// shadow-memory allocations on every access, which swamps the signal and pushes
// the count orders of magnitude past the threshold (a 600-object budget reads as
// nearly a million under -race). The behavior these tests guard is verified for
// correctness by the matching equivalence tests, which still run under -race; the
// allocation bound is a property of an ordinary build, so that is where we check
// it. The CI debug job runs -race, so without this guard every witness fails
// there even though the code is fine.
func skipAllocWitnessUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("testing.AllocsPerRun is unreliable under -race; the allocation bound is checked on a normal build")
	}
}
