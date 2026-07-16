package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// TestSchedulesAgree runs a handful of full schedules on the zero-latency
// simulator with ambiguous-PUT faults on and requires zero violations
// plus real teeth: some grants rejected, some sections dead.
func TestSchedulesAgree(t *testing.T) {
	ctx := context.Background()
	var m metrics
	for s := int64(1); s <= 3; s++ {
		st, err := openStore("sim", "", s, 15)
		if err != nil {
			t.Fatal(err)
		}
		cfg := config{
			store: "sim", prefix: fmt.Sprintf("ft-%d", s),
			steps: 120, nodes: 4, groups: 8, faultPct: 15, seed: s,
		}
		if err := schedule(ctx, st, cfg, rand.New(rand.NewSource(s)), &m); err != nil {
			t.Fatalf("seed %d: %v", s, err)
		}
	}
	if m.violations != 0 {
		t.Fatalf("%d violations", m.violations)
	}
	if m.grantsRej == 0 || m.sectionsDead == 0 {
		t.Fatalf("toothless: %d rejected grants, %d dead sections", m.grantsRej, m.sectionsDead)
	}
}

// TestCrashRestartTortured makes sure the crash arm actually fires under
// the small test sizes, since a restart is where the incarnation
// recognition and the whole-chain replay earn their keep.
func TestCrashRestartTortured(t *testing.T) {
	ctx := context.Background()
	var m metrics
	for s := int64(10); m.crashes == 0 && s < 20; s++ {
		st, err := openStore("sim", "", s, 15)
		if err != nil {
			t.Fatal(err)
		}
		cfg := config{
			store: "sim", prefix: fmt.Sprintf("ft-%d", s),
			steps: 120, nodes: 3, groups: 4, faultPct: 15, seed: s,
		}
		if err := schedule(ctx, st, cfg, rand.New(rand.NewSource(s)), &m); err != nil {
			t.Fatalf("seed %d: %v", s, err)
		}
	}
	if m.crashes == 0 {
		t.Fatal("no schedule crashed a node in ten seeds")
	}
	if m.violations != 0 {
		t.Fatalf("%d violations", m.violations)
	}
}

func TestCSVShape(t *testing.T) {
	row := fmt.Sprintf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
		"sim", 1, 2, 3, 4, 5, int64(6), uint64(7), uint64(8), uint64(9),
		uint64(10), uint64(11), uint64(12), uint64(13), uint64(14))
	if strings.Count(row, ",") != strings.Count(csvHeader, ",") {
		t.Fatalf("row has %d commas, header %d", strings.Count(row, ","), strings.Count(csvHeader, ","))
	}
}
