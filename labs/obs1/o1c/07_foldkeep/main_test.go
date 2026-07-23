package main

import (
	"testing"
	"time"
)

// The whole pipeline at shrunken constants, under race in CI: samples
// carry lag readings while ingest runs and the quiesced floor is exact.
func TestFoldKeepSmoke(t *testing.T) {
	samples, final, err := run(cfg{
		payloadBytes: 32 << 20, groups: 2, valBytes: 500,
		flushSize: 1 << 20, segTarget: 2 << 20,
		foldAge: 50 * time.Millisecond, sampleEvery: 8 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) < 2 {
		t.Fatalf("only %d samples", len(samples))
	}
	if final.published == 0 || final.segPuts == 0 {
		t.Fatalf("fold pipeline idle: %+v", final)
	}
	if final.maxLag != 0 {
		t.Fatalf("quiesced floor not exact: %+v", final)
	}
}

func TestGrew(t *testing.T) {
	s := []sample{{maxLag: 10}, {maxLag: 20}, {maxLag: 30}, {maxLag: 15}}
	fm, sm, ratio := grew(s)
	if fm != 20 || sm != 30 || ratio != 1.5 {
		t.Fatalf("fm %d sm %d ratio %f", fm, sm, ratio)
	}
}
