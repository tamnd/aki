package stream

import (
	"testing"
)

// The XACK suite (spec 2064/f3/14 section 7.5): an ack retires a pending entry and
// reports the count it actually removed, resolving the owning consumer from the
// slab. IDs that were never pending count zero, a missing key or group is not an
// error, and a bad ID leaves the PEL untouched.

func TestXackRetiresPending(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	wantInt(t, do(t, c, opXack, "s", "g", "1-0", "3-0"), 2)
	// The two acked entries leave the PEL; only 2-0 remains pending.
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "pending", "1")
	es := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", "0"))["s"]
	if len(es) != 1 || es[0].id != "2-0" {
		t.Fatalf("remaining pending = %v, want just 2-0", es)
	}
}

func TestXackNonPendingCountsZero(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	do(t, c, opXack, "s", "g", "1-0")
	// A second ack of the same ID, and an ID that was never delivered, both miss.
	wantInt(t, do(t, c, opXack, "s", "g", "1-0", "9-0"), 0)
}

func TestXackMissingKeyOrGroupIsZero(t *testing.T) {
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opXack, "nokey", "g", "1-0"), 0)
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantInt(t, do(t, c, opXack, "s", "nope", "1-0"), 0)
}

func TestXackBadIDErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	// A malformed ID anywhere rejects the whole command, leaving the PEL intact.
	wantErr(t, do(t, c, opXack, "s", "g", "1-0", "notanid"), errInvalidID)
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "pending", "1")
}

func TestXackWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "str", "v")
	wantErr(t, do(t, c, opXack, "str", "g", "1-0"), wrongType)
}

func TestXackDropsConsumerCount(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	do(t, c, opXack, "s", "g", "1-0")
	// The consumer's own pending count follows the group's down.
	got := decodeReply(t, do(t, c, opXpending, "s", "g"))
	arr := got.([]any)
	consumers := arr[3].([]any)
	if len(consumers) != 1 {
		t.Fatalf("consumers = %v, want one owner", consumers)
	}
	row := consumers[0].([]any)
	if row[0].(string) != "c" || row[1].(string) != "1" {
		t.Fatalf("owner row = %v, want [c 1]", row)
	}
}
