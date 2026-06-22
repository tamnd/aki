package command

import (
	"testing"
	"time"
)

// TestRecordIOLatency checks the slow I/O accounting: a commit under the
// threshold is ignored, one over it bumps io_slowops_total, and a zero threshold
// turns the check off.
func TestRecordIOLatency(t *testing.T) {
	d := New(Config{})

	// The default threshold is 5ms, so a 1ms commit does not count.
	d.recordIOLatency("checkpoint", 1*time.Millisecond)
	if got := d.ioSlowOps.Load(); got != 0 {
		t.Fatalf("fast commit counted: %d", got)
	}

	// A 50ms commit is over the threshold and counts.
	d.recordIOLatency("checkpoint", 50*time.Millisecond)
	if got := d.ioSlowOps.Load(); got != 1 {
		t.Fatalf("slow commit not counted: %d", got)
	}

	// A zero threshold disables the check, so the counter holds.
	d.conf.set("max-io-latency-warn", "0")
	d.recordIOLatency("checkpoint", 50*time.Millisecond)
	if got := d.ioSlowOps.Load(); got != 1 {
		t.Fatalf("disabled check still counted: %d", got)
	}
}

// TestIOSlowOpsInfoField checks io_slowops_total shows up in INFO stats and the
// config default matches the spec.
func TestIOSlowOpsInfoField(t *testing.T) {
	r, c := startData(t)

	if got := configGet(t, r, c, "max-io-latency-warn"); got != "5" {
		t.Fatalf("max-io-latency-warn default = %q want 5", got)
	}

	// Disable the check so a slow commit on a loaded runner cannot make this
	// nonzero, then confirm the field is present and reads 0.
	sendArgs(t, r, c, "CONFIG", "SET", "max-io-latency-warn", "0")
	if got := infoField(t, r, c, "stats", "io_slowops_total"); got != "0" {
		t.Fatalf("io_slowops_total = %q want 0", got)
	}
}
