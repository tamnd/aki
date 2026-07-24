package main

import "testing"

// TestCleanScheduleSmoke keeps the lab honest in CI: the baseline
// schedule must run, recover, and land inside its band.
func TestCleanScheduleSmoke(t *testing.T) {
	sc := schedules()[0]
	r, err := runSchedule(sc)
	if err != nil {
		t.Fatal(err)
	}
	if !r.pass {
		t.Fatalf("clean recovery %v outside [%v, %v]", r.recovery, sc.lo, sc.hi)
	}
}
