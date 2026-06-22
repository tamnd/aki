package command

import (
	"strings"
	"testing"
)

// TestWaitAOFBadNumLocal checks numlocal outside 0..1 is rejected.
func TestWaitAOFBadNumLocal(t *testing.T) {
	d := newMetricsDispatcher(t)
	out := runReply(d, "WAITAOF", "2", "0", "0")
	if !strings.Contains(out, "numlocal must be 0 or 1") {
		t.Fatalf("WAITAOF 2 0 0: want numlocal error, got %q", out)
	}
}

// TestWaitAOFNoAppendonly checks numlocal of 1 errors when appendonly is off.
func TestWaitAOFNoAppendonly(t *testing.T) {
	d := newMetricsDispatcher(t)
	out := runReply(d, "WAITAOF", "1", "0", "0")
	if !strings.Contains(out, "appendonly is disabled") {
		t.Fatalf("WAITAOF 1 0 0 with AOF off: want disabled error, got %q", out)
	}
}

// TestWaitAOFZeroLocal checks numlocal 0 with no replicas replies [0, 0] without
// blocking, even when appendonly is off.
func TestWaitAOFZeroLocal(t *testing.T) {
	d := newMetricsDispatcher(t)
	out := runReply(d, "WAITAOF", "0", "0", "0")
	if out != "*2\r\n:0\r\n:0\r\n" {
		t.Fatalf("WAITAOF 0 0 0: want [0,0], got %q", out)
	}
}

// TestWaitAOFLocalAcked checks that with appendonly on, numlocal 1 fsyncs the AOF
// and reports one local copy durable.
func TestWaitAOFLocalAcked(t *testing.T) {
	d := newMetricsDispatcher(t)
	if err := d.SetConfig("dir", t.TempDir()); err != nil {
		t.Fatalf("set dir: %v", err)
	}
	if err := d.SetConfig("appendonly", "yes"); err != nil {
		t.Fatalf("set appendonly: %v", err)
	}
	d.initAOF()

	out := runReply(d, "WAITAOF", "1", "0", "0")
	if out != "*2\r\n:1\r\n:0\r\n" {
		t.Fatalf("WAITAOF 1 0 0 with AOF on: want [1,0], got %q", out)
	}
}
