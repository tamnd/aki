package store

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

// TestStallReason covers the block-not-drop stall taxonomy classifier (stall.go,
// doc 06 section 8.3) across the cases reachable today: no tier, a full cold
// device, a non-ENOSPC cold I/O error, a stream pinning migration, and an
// exhausted arena with the migrator idle. Owner-goroutine white-box: the cold
// write error and the open-stream count are set directly to stand in for the
// device and stream states a live run would produce.
func TestStallReason(t *testing.T) {
	// No cold region: a plain out-of-memory refusal, no larger-than-memory tier.
	if got := testStore(t, 4).StallReason(); got != "no larger-than-memory tier" {
		t.Fatalf("no-cold reason = %q", got)
	}

	s := migratorStore(t, 64<<10)

	// A cold write that failed with ENOSPC: the device is full.
	s.cold.werr = fmt.Errorf("cold pwrite: %w", syscall.ENOSPC)
	if got := s.StallReason(); got != "cold device full" {
		t.Fatalf("ENOSPC reason = %q, want cold device full", got)
	}

	// Any other cold I/O error names itself.
	s.cold.werr = errors.New("input/output error")
	if got := s.StallReason(); got != "cold write failed: input/output error" {
		t.Fatalf("io-error reason = %q", got)
	}
	s.cold.werr = nil

	// An open stream pins migration ahead of the arena catch-all.
	s.openStreams++
	if got := s.StallReason(); got != "migration pinned by an open stream" {
		t.Fatalf("open-stream reason = %q", got)
	}
	s.openStreams--

	// A healthy cold tier with no pin and the migrator idle: the arena is
	// exhausted, the catch-all the not-yet-reachable no-residue and leaked-epoch
	// cases fold into until later milestones split them out.
	if got := s.StallReason(); got != "arena exhausted, migrator idle" {
		t.Fatalf("idle reason = %q", got)
	}
}
