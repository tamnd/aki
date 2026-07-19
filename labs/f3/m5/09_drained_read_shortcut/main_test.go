package main

import "testing"

// TestArmsAgreeOnDrained is the correctness anchor: on a drained cursor (after ==
// lastID) the walk arm finds zero newer entries and the shortcut arm returns true
// (nil-read), so both agree the read is empty. The optimization must never turn an
// empty read non-empty or vice versa.
func TestArmsAgreeOnDrained(t *testing.T) {
	for _, n := range []int{1, 16, 256, 4096} {
		tb := makeTail(n)
		if walkArm(tb, tb.lastID) != 0 {
			t.Fatalf("n=%d: walk arm found entries newer than the last ID", n)
		}
		if !shortcutArm(tb, tb.lastID) {
			t.Fatalf("n=%d: shortcut arm did not treat the drained cursor as empty", n)
		}
	}
}

// TestArmsAgreeWithNewEntry confirms the guard does not swallow a real new entry:
// when the after-ID is below the last ID (a new XADD landed), the walk arm finds
// the newer entries and the shortcut arm returns false (do the walk), so the fix
// falls through to the real read exactly when there is something to read.
func TestArmsAgreeWithNewEntry(t *testing.T) {
	tb := makeTail(100)
	after := tb.ids[50] // 49 entries strictly above
	if got := walkArm(tb, after); got != 49 {
		t.Fatalf("walk arm counted %d newer entries, want 49", got)
	}
	if shortcutArm(tb, after) {
		t.Fatal("shortcut arm skipped a walk that had newer entries to deliver")
	}
}
