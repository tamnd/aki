package main

import (
	"testing"
	"time"
)

// TestFoldLoadSmoke runs both arms at smoke constants and asserts the
// invariant the lab exists to check: identical ingest produces an
// identical WAL stream whether or not the folder is consuming it.
func TestFoldLoadSmoke(t *testing.T) {
	c := cfg{
		payloadBytes: 16 << 20, groups: 2, valBytes: 200,
		flushSize: 1 << 20, segTarget: 4 << 20, foldAge: 50 * time.Millisecond,
	}
	c.withFold = false
	off, err := run(c)
	if err != nil {
		t.Fatal(err)
	}
	c.withFold = true
	on, err := run(c)
	if err != nil {
		t.Fatal(err)
	}
	if off.ops != on.ops || off.payload != on.payload {
		t.Fatalf("arms ingested differently: off %+v on %+v", off, on)
	}
	// WAL object packing is timing-dependent: a flush racing the barrier can
	// split one object into two, shifting the count by one and the bytes by
	// one object's framing. The invariant is the stream content, so tolerate
	// exactly that.
	df := int64(off.walFlushes) - int64(on.walFlushes)
	db := int64(off.walBytes) - int64(on.walBytes)
	if df < -1 || df > 1 || db < -2048 || db > 2048 {
		t.Fatalf("the fold moved the WAL stream: off %d flushes %d bytes, on %d flushes %d bytes",
			off.walFlushes, on.walFlushes, off.walBytes, on.walBytes)
	}
	if on.segPuts == 0 || on.published == 0 {
		t.Fatalf("the fold arm never folded: %+v", on)
	}
	if off.segPuts != 0 {
		t.Fatalf("the fold-off arm folded: %+v", off)
	}
	if oh := off.overheadPerOp(); oh <= 0 || oh > 200 {
		t.Fatalf("overhead per op %.1f B out of any sane band", oh)
	}
}
