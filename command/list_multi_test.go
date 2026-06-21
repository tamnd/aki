package command

import (
	"bufio"
	"net"
	"strconv"
	"testing"
)

func TestRPopLPush(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH src a b c")
	if got := bulk(t, r, c, "RPOPLPUSH src dst"); got != "c" {
		t.Fatalf("RPOPLPUSH = %q want c", got)
	}
	if got := array(t, r, c, "LRANGE src 0 -1"); !equalSlice(got, []string{"a", "b"}) {
		t.Fatalf("src after RPOPLPUSH = %v", got)
	}
	if got := array(t, r, c, "LRANGE dst 0 -1"); !equalSlice(got, []string{"c"}) {
		t.Fatalf("dst after RPOPLPUSH = %v", got)
	}
	if got := bulk(t, r, c, "RPOPLPUSH missing dst"); got != "<nil>" {
		t.Fatalf("RPOPLPUSH missing src = %q want nil", got)
	}
}

func TestRPopLPushRotation(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c")
	// Rotating once moves the tail to the head.
	if got := bulk(t, r, c, "RPOPLPUSH k k"); got != "c" {
		t.Fatalf("rotate = %q want c", got)
	}
	if got := array(t, r, c, "LRANGE k 0 -1"); !equalSlice(got, []string{"c", "a", "b"}) {
		t.Fatalf("after rotate = %v", got)
	}
}

func TestLMove(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH src a b c")
	if got := bulk(t, r, c, "LMOVE src dst LEFT RIGHT"); got != "a" {
		t.Fatalf("LMOVE LEFT RIGHT = %q want a", got)
	}
	if got := array(t, r, c, "LRANGE src 0 -1"); !equalSlice(got, []string{"b", "c"}) {
		t.Fatalf("src after LMOVE = %v", got)
	}
	if got := bulk(t, r, c, "LMOVE src dst RIGHT LEFT"); got != "c" {
		t.Fatalf("LMOVE RIGHT LEFT = %q want c", got)
	}
	if got := array(t, r, c, "LRANGE dst 0 -1"); !equalSlice(got, []string{"c", "a"}) {
		t.Fatalf("dst after second LMOVE = %v", got)
	}
	if got := sendLine(t, r, c, "LMOVE src dst UP DOWN"); got != "-ERR syntax error" {
		t.Fatalf("LMOVE bad dir = %q", got)
	}
}

func TestLMoveDeletesEmptiedSource(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH src only")
	if got := bulk(t, r, c, "LMOVE src dst LEFT LEFT"); got != "only" {
		t.Fatalf("LMOVE = %q want only", got)
	}
	if got := sendLine(t, r, c, "EXISTS src"); got != ":0" {
		t.Fatalf("emptied source should be deleted, EXISTS = %q", got)
	}
}

func TestLPosNoCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c a b c a")
	if got := sendLine(t, r, c, "LPOS k a"); got != ":0" {
		t.Fatalf("LPOS a = %q want :0", got)
	}
	if got := sendLine(t, r, c, "LPOS k a RANK 2"); got != ":3" {
		t.Fatalf("LPOS a RANK 2 = %q want :3", got)
	}
	if got := sendLine(t, r, c, "LPOS k a RANK -1"); got != ":6" {
		t.Fatalf("LPOS a RANK -1 = %q want :6", got)
	}
	if got := bulk(t, r, c, "LPOS k nope"); got != "<nil>" {
		t.Fatalf("LPOS missing element = %q want nil", got)
	}
}

func TestLPosWithCount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a b c a b c a")
	if got := intArray(t, r, c, "LPOS k a COUNT 0"); !equalSlice(got, []string{":0", ":3", ":6"}) {
		t.Fatalf("LPOS a COUNT 0 = %v", got)
	}
	if got := intArray(t, r, c, "LPOS k a COUNT 2"); !equalSlice(got, []string{":0", ":3"}) {
		t.Fatalf("LPOS a COUNT 2 = %v", got)
	}
	if got := intArray(t, r, c, "LPOS k a RANK -1 COUNT 0"); !equalSlice(got, []string{":6", ":3", ":0"}) {
		t.Fatalf("LPOS a RANK -1 COUNT 0 = %v", got)
	}
	if got := intArray(t, r, c, "LPOS k a COUNT 0 MAXLEN 1"); !equalSlice(got, []string{":0"}) {
		t.Fatalf("LPOS MAXLEN 1 = %v want [:0]", got)
	}
	if got := sendLine(t, r, c, "LPOS k nope COUNT 0"); got != "*0" {
		t.Fatalf("LPOS missing COUNT 0 = %q want *0", got)
	}
}

func TestLPosErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k a")
	if got := sendLine(t, r, c, "LPOS k a RANK 0"); got != "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list" {
		t.Fatalf("LPOS RANK 0 = %q", got)
	}
	if got := sendLine(t, r, c, "LPOS k a COUNT -1"); got != "-ERR COUNT can't be negative" {
		t.Fatalf("LPOS COUNT -1 = %q", got)
	}
	if got := sendLine(t, r, c, "LPOS k a MAXLEN -1"); got != "-ERR MAXLEN can't be negative" {
		t.Fatalf("LPOS MAXLEN -1 = %q", got)
	}
}

// lmpopReply reads an LMPOP reply, a two-element array of the key and its popped
// elements, flattened to [key, elem, elem...]. A nil array returns nil.
func lmpopReply(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	line := sendLine(t, r, c, cmd)
	if line == "*-1" {
		return nil
	}
	if line != "*2" {
		t.Fatalf("expected *2 header after %q, got %q", cmd, line)
	}
	out := []string{bulk(t, r, c, "")}
	inner := sendLineRead(t, r)
	if inner == "" || inner[0] != '*' {
		t.Fatalf("expected inner array header after %q, got %q", cmd, inner)
	}
	n, err := strconv.Atoi(inner[1:])
	if err != nil {
		t.Fatalf("parse inner array len %q: %v", inner, err)
	}
	for range n {
		out = append(out, bulk(t, r, c, ""))
	}
	return out
}

func TestLMPop(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH k2 a b c")
	// k1 is absent, so the pop falls through to k2.
	got := lmpopReply(t, r, c, "LMPOP 2 k1 k2 LEFT")
	if !equalSlice(got, []string{"k2", "a"}) {
		t.Fatalf("LMPOP = %v want [k2 a]", got)
	}
	got = lmpopReply(t, r, c, "LMPOP 2 k1 k2 RIGHT COUNT 2")
	if !equalSlice(got, []string{"k2", "c", "b"}) {
		t.Fatalf("LMPOP COUNT 2 = %v want [k2 c b]", got)
	}
	if got := sendLine(t, r, c, "EXISTS k2"); got != ":0" {
		t.Fatalf("drained list should be deleted, EXISTS = %q", got)
	}
	if got := sendLine(t, r, c, "LMPOP 2 k1 k2 LEFT"); got != "*-1" {
		t.Fatalf("LMPOP all empty = %q want *-1", got)
	}
}

func TestLMPopErrors(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "LMPOP 0 k LEFT"); got != "-ERR numkeys can't be zero" {
		t.Fatalf("LMPOP 0 = %q", got)
	}
	if got := sendLine(t, r, c, "LMPOP -1 k LEFT"); got != "-ERR numkeys can't be negative" {
		t.Fatalf("LMPOP -1 = %q", got)
	}
	if got := sendLine(t, r, c, "LMPOP 1 k UP"); got != "-ERR syntax error" {
		t.Fatalf("LMPOP bad dir = %q", got)
	}
	if got := sendLine(t, r, c, "LMPOP 1 k LEFT COUNT 0"); got != "-ERR COUNT can't be zero" {
		t.Fatalf("LMPOP COUNT 0 = %q", got)
	}
	if got := sendLine(t, r, c, "LMPOP 1 k LEFT COUNT -1"); got != "-ERR COUNT can't be negative" {
		t.Fatalf("LMPOP COUNT -1 = %q", got)
	}
}

func TestListMultiWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET s hello")
	_ = sendLine(t, r, c, "RPUSH l a")
	for _, cmd := range []string{
		"RPOPLPUSH s l", "RPOPLPUSH l s", "LMOVE s l LEFT RIGHT",
		"LMOVE l s LEFT RIGHT", "LPOS s a", "LMPOP 1 s LEFT",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
