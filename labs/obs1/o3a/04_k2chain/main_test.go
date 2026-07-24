package main

import (
	"testing"
	"time"
)

// TestQuantile pins the rank arithmetic the scored table leans on.
func TestQuantile(t *testing.T) {
	d := []time.Duration{4, 1, 3, 2}
	if got := quantile(d, 0.5); got != 2 {
		t.Fatalf("p50 = %v, want 2", got)
	}
	if got := quantile(d, 1.0); got != 4 {
		t.Fatalf("max = %v, want 4", got)
	}
	if got := quantile(nil, 0.5); got != 0 {
		t.Fatalf("empty = %v, want 0", got)
	}
}
