package command

import (
	"sort"
	"testing"
)

func TestSAddAndCard(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "SADD s a b c"); got != ":3" {
		t.Fatalf("SADD = %q want :3", got)
	}
	// Re-adding an existing member counts only the new one.
	if got := sendLine(t, r, c, "SADD s a d"); got != ":1" {
		t.Fatalf("SADD dup = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":4" {
		t.Fatalf("SCARD = %q want :4", got)
	}
	if got := sendLine(t, r, c, "SCARD nokey"); got != ":0" {
		t.Fatalf("SCARD missing = %q want :0", got)
	}
}

func TestSMembers(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c")
	got := array(t, r, c, "SMEMBERS s")
	sort.Strings(got)
	if !equalSlice(got, []string{"a", "b", "c"}) {
		t.Fatalf("SMEMBERS = %v", got)
	}
	if got := sendLine(t, r, c, "SMEMBERS nokey"); got != "*0" {
		t.Fatalf("SMEMBERS missing = %q want *0", got)
	}
}

func TestSRem(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c")
	if got := sendLine(t, r, c, "SREM s a missing c"); got != ":2" {
		t.Fatalf("SREM = %q want :2", got)
	}
	if got := array(t, r, c, "SMEMBERS s"); !equalSlice(got, []string{"b"}) {
		t.Fatalf("after SREM = %v", got)
	}
	// Removing the last member deletes the key.
	if got := sendLine(t, r, c, "SREM s b"); got != ":1" {
		t.Fatalf("SREM last = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":0" {
		t.Fatalf("emptied set should be deleted, EXISTS = %q", got)
	}
}

func TestSIsMemberAndMIsMember(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c")
	if got := sendLine(t, r, c, "SISMEMBER s a"); got != ":1" {
		t.Fatalf("SISMEMBER present = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER s z"); got != ":0" {
		t.Fatalf("SISMEMBER absent = %q want :0", got)
	}
	got := intArray(t, r, c, "SMISMEMBER s a z c")
	if !equalSlice(got, []string{":1", ":0", ":1"}) {
		t.Fatalf("SMISMEMBER = %v", got)
	}
}

func TestSPop(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c d e")
	// No count returns one member.
	got := bulk(t, r, c, "SPOP s")
	if got == "<nil>" {
		t.Fatalf("SPOP returned nil on non-empty set")
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":4" {
		t.Fatalf("SCARD after SPOP = %q want :4", got)
	}
	// Count pops that many distinct members.
	popped := array(t, r, c, "SPOP s 2")
	if len(popped) != 2 || popped[0] == popped[1] {
		t.Fatalf("SPOP 2 = %v want 2 distinct", popped)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":2" {
		t.Fatalf("SCARD after SPOP 2 = %q want :2", got)
	}
	// Count past the size drains and deletes the key.
	all := array(t, r, c, "SPOP s 10")
	if len(all) != 2 {
		t.Fatalf("SPOP 10 = %v want the last 2", all)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":0" {
		t.Fatalf("drained set should be deleted, EXISTS = %q", got)
	}
	if got := bulk(t, r, c, "SPOP nokey"); got != "<nil>" {
		t.Fatalf("SPOP missing = %q want nil", got)
	}
	if got := sendLine(t, r, c, "SPOP nokey -1"); got != "-ERR value is out of range, must be positive" {
		t.Fatalf("SPOP negative = %q", got)
	}
}

func TestSRandMember(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD s a b c")
	// Positive count is distinct and does not remove.
	got := array(t, r, c, "SRANDMEMBER s 2")
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("SRANDMEMBER 2 = %v want 2 distinct", got)
	}
	if got := sendLine(t, r, c, "SCARD s"); got != ":3" {
		t.Fatalf("SRANDMEMBER must not remove, SCARD = %q", got)
	}
	// Negative count allows duplicates and returns its magnitude.
	got = array(t, r, c, "SRANDMEMBER s -5")
	if len(got) != 5 {
		t.Fatalf("SRANDMEMBER -5 len = %d want 5", len(got))
	}
	if got := bulk(t, r, c, "SRANDMEMBER nokey"); got != "<nil>" {
		t.Fatalf("SRANDMEMBER missing = %q want nil", got)
	}
}

func TestSMove(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD src a b c")
	_ = sendLine(t, r, c, "SADD dst x")
	if got := sendLine(t, r, c, "SMOVE src dst a"); got != ":1" {
		t.Fatalf("SMOVE = %q want :1", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER src a"); got != ":0" {
		t.Fatalf("a should leave src, SISMEMBER = %q", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER dst a"); got != ":1" {
		t.Fatalf("a should land in dst, SISMEMBER = %q", got)
	}
	// Moving an absent member is a no-op returning 0.
	if got := sendLine(t, r, c, "SMOVE src dst nope"); got != ":0" {
		t.Fatalf("SMOVE absent = %q want :0", got)
	}
}

func TestSMoveDeletesEmptiedSource(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD src only")
	if got := sendLine(t, r, c, "SMOVE src dst only"); got != ":1" {
		t.Fatalf("SMOVE = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS src"); got != ":0" {
		t.Fatalf("emptied source should be deleted, EXISTS = %q", got)
	}
}

// TestSAddSRemLargeBatch exercises the map-based dedup path that kicks in at
// setDedupMapThreshold members, making sure it counts and stores the same as the
// linear path. The batch is deliberately above the threshold and includes
// duplicate members, which must be folded to one.
func TestSAddSRemLargeBatch(t *testing.T) {
	r, c := startData(t)
	// 12 distinct members plus a repeat of "m0" => 12 added, not 13.
	if got := sendLine(t, r, c, "SADD big m0 m1 m2 m3 m4 m5 m6 m7 m8 m9 m10 m11 m0"); got != ":12" {
		t.Fatalf("SADD big = %q want :12", got)
	}
	if got := sendLine(t, r, c, "SCARD big"); got != ":12" {
		t.Fatalf("SCARD = %q want :12", got)
	}
	// Re-adding existing members adds nothing.
	if got := sendLine(t, r, c, "SADD big m0 m1 m2 m3 m4 m5 m6 m7 m8"); got != ":0" {
		t.Fatalf("SADD existing = %q want :0", got)
	}
	// Remove a large batch that names a missing member and a duplicate.
	if got := sendLine(t, r, c, "SREM big m0 m1 m2 m3 m4 m5 m6 m7 nope m0"); got != ":8" {
		t.Fatalf("SREM big = %q want :8", got)
	}
	if got := sendLine(t, r, c, "SCARD big"); got != ":4" {
		t.Fatalf("SCARD after SREM = %q want :4", got)
	}
}

func TestSetWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	_ = sendLine(t, r, c, "SADD realset a")
	for _, cmd := range []string{
		"SADD s a", "SREM s a", "SMEMBERS s", "SISMEMBER s a",
		"SMISMEMBER s a", "SCARD s", "SPOP s", "SRANDMEMBER s",
		"SMOVE s realset a", "SMOVE realset s a",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
