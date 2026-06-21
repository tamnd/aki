package command

import (
	"sort"
	"testing"
)

func TestSInter(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b c d")
	_ = sendLine(t, r, c, "SADD B b c d e")
	_ = sendLine(t, r, c, "SADD C c d e f")
	got := array(t, r, c, "SINTER A B C")
	sort.Strings(got)
	if !equalSlice(got, []string{"c", "d"}) {
		t.Fatalf("SINTER = %v want [c d]", got)
	}
	// A missing key makes the intersection empty.
	if got := sendLine(t, r, c, "SINTER A nokey"); got != "*0" {
		t.Fatalf("SINTER with missing = %q want *0", got)
	}
}

func TestSUnion(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b")
	_ = sendLine(t, r, c, "SADD B b c")
	got := array(t, r, c, "SUNION A B")
	sort.Strings(got)
	if !equalSlice(got, []string{"a", "b", "c"}) {
		t.Fatalf("SUNION = %v", got)
	}
}

func TestSDiff(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b c d")
	_ = sendLine(t, r, c, "SADD B b")
	_ = sendLine(t, r, c, "SADD C c")
	got := array(t, r, c, "SDIFF A B C")
	sort.Strings(got)
	if !equalSlice(got, []string{"a", "d"}) {
		t.Fatalf("SDIFF = %v want [a d]", got)
	}
}

func TestSInterStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b c")
	_ = sendLine(t, r, c, "SADD B b c d")
	if got := sendLine(t, r, c, "SINTERSTORE dest A B"); got != ":2" {
		t.Fatalf("SINTERSTORE = %q want :2", got)
	}
	got := array(t, r, c, "SMEMBERS dest")
	sort.Strings(got)
	if !equalSlice(got, []string{"b", "c"}) {
		t.Fatalf("dest after SINTERSTORE = %v", got)
	}
	// An empty result deletes the destination.
	_ = sendLine(t, r, c, "SADD X x")
	_ = sendLine(t, r, c, "SADD Y y")
	if got := sendLine(t, r, c, "SINTERSTORE dest X Y"); got != ":0" {
		t.Fatalf("SINTERSTORE empty = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS dest"); got != ":0" {
		t.Fatalf("empty result should delete dest, EXISTS = %q", got)
	}
}

func TestSUnionStoreAndDiffStore(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b")
	_ = sendLine(t, r, c, "SADD B b c")
	if got := sendLine(t, r, c, "SUNIONSTORE u A B"); got != ":3" {
		t.Fatalf("SUNIONSTORE = %q want :3", got)
	}
	if got := sendLine(t, r, c, "SDIFFSTORE d A B"); got != ":1" {
		t.Fatalf("SDIFFSTORE = %q want :1", got)
	}
	if got := array(t, r, c, "SMEMBERS d"); !equalSlice(got, []string{"a"}) {
		t.Fatalf("d after SDIFFSTORE = %v want [a]", got)
	}
}

func TestSInterCard(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SADD A a b c d")
	_ = sendLine(t, r, c, "SADD B b c d e")
	if got := sendLine(t, r, c, "SINTERCARD 2 A B"); got != ":3" {
		t.Fatalf("SINTERCARD = %q want :3", got)
	}
	if got := sendLine(t, r, c, "SINTERCARD 2 A B LIMIT 1"); got != ":1" {
		t.Fatalf("SINTERCARD LIMIT 1 = %q want :1", got)
	}
	// LIMIT 0 means no limit.
	if got := sendLine(t, r, c, "SINTERCARD 2 A B LIMIT 0"); got != ":3" {
		t.Fatalf("SINTERCARD LIMIT 0 = %q want :3", got)
	}
	if got := sendLine(t, r, c, "SINTERCARD 0 A"); got != "-ERR numkeys can't be non-positive" {
		t.Fatalf("SINTERCARD numkeys 0 = %q", got)
	}
	if got := sendLine(t, r, c, "SINTERCARD 2 A B LIMIT"); got != "-ERR syntax error" {
		t.Fatalf("SINTERCARD LIMIT no value = %q", got)
	}
	if got := sendLine(t, r, c, "SINTERCARD 2 A B LIMIT -1"); got != "-ERR LIMIT can't be negative" {
		t.Fatalf("SINTERCARD LIMIT -1 = %q", got)
	}
}

func TestSetAlgebraWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	_ = sendLine(t, r, c, "SADD a x")
	for _, cmd := range []string{
		"SINTER a s", "SUNION a s", "SDIFF a s",
		"SINTERSTORE dest a s", "SUNIONSTORE dest a s", "SDIFFSTORE dest a s",
		"SINTERCARD 2 a s",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
	// A non-set destination is also WRONGTYPE.
	if got := sendLine(t, r, c, "SINTERSTORE s a"); got != "-"+wrongTypeError {
		t.Fatalf("SINTERSTORE non-set dest = %q", got)
	}
}
