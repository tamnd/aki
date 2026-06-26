package command

import (
	"bufio"
	"net"
	"slices"
	"testing"
)

// flatCmd sends a command and returns its reply flattened to leaf tokens.
func flatCmd(t *testing.T, r *bufio.Reader, c net.Conn, cmd string) []string {
	t.Helper()
	return xinfoReply(t, r, c, cmd)
}

// contains reports whether toks holds tok.
func contains(toks []string, tok string) bool {
	return slices.Contains(toks, tok)
}

func TestXGroupCreate(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	if got := sendLine(t, r, c, "XGROUP CREATE s g1 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	// Same name again is BUSYGROUP.
	if got := sendLine(t, r, c, "XGROUP CREATE s g1 0"); got != "-BUSYGROUP Consumer Group name already exists" {
		t.Fatalf("XGROUP CREATE dup = %q", got)
	}
	// Missing key without MKSTREAM is no such key.
	if got := sendLine(t, r, c, "XGROUP CREATE missing g 0"); got != "-"+errStreamNoSuchKey {
		t.Fatalf("XGROUP CREATE missing = %q", got)
	}
	// MKSTREAM creates the stream.
	if got := sendLine(t, r, c, "XGROUP CREATE fresh g 0 MKSTREAM"); got != "+OK" {
		t.Fatalf("XGROUP CREATE MKSTREAM = %q", got)
	}
	if got := sendLine(t, r, c, "XLEN fresh"); got != ":0" {
		t.Fatalf("XLEN fresh = %q want :0", got)
	}
}

func TestXGroupSetIDAndDestroy(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	if got := sendLine(t, r, c, "XGROUP SETID s g1 $"); got != "+OK" {
		t.Fatalf("XGROUP SETID = %q", got)
	}
	// After SETID $ there is nothing new to deliver.
	if got := sendLine(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >"); got != "*-1" {
		t.Fatalf("XREADGROUP after SETID $ = %q want *-1", got)
	}
	if got := sendLine(t, r, c, "XGROUP SETID s missing $"); got != "-"+nogroupError("missing", "s") {
		t.Fatalf("XGROUP SETID missing group = %q", got)
	}
	if got := sendLine(t, r, c, "XGROUP DESTROY s g1"); got != ":1" {
		t.Fatalf("XGROUP DESTROY = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XGROUP DESTROY s g1"); got != ":0" {
		t.Fatalf("XGROUP DESTROY again = %q want :0", got)
	}
}

func TestXReadGroupDelivery(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")

	toks := flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	for _, want := range []string{"s", "1-1", "2-1", "a", "b"} {
		if !contains(toks, want) {
			t.Fatalf("XREADGROUP delivery missing %q in %v", want, toks)
		}
	}
	// Nothing new on a second > read.
	if got := sendLine(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >"); got != "*-1" {
		t.Fatalf("XREADGROUP second > = %q want *-1", got)
	}
	// PEL re-read returns the still-pending entries.
	toks = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s 0")
	if !contains(toks, "1-1") || !contains(toks, "2-1") {
		t.Fatalf("XREADGROUP PEL re-read = %v", toks)
	}
}

func TestXAck(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")

	if got := sendLine(t, r, c, "XACK s g1 1-1"); got != ":1" {
		t.Fatalf("XACK = %q want :1", got)
	}
	// Acking the same ID again counts zero.
	if got := sendLine(t, r, c, "XACK s g1 1-1"); got != ":0" {
		t.Fatalf("XACK repeat = %q want :0", got)
	}
	// Only 2-1 remains pending.
	toks := flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s 0")
	if contains(toks, "1-1") || !contains(toks, "2-1") {
		t.Fatalf("PEL after XACK = %v", toks)
	}
	if got := sendLine(t, r, c, "XACK missing g1 1-1"); got != "-"+nogroupError("g1", "missing") {
		t.Fatalf("XACK missing key = %q", got)
	}
}

func TestXReadGroupNoAck(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 NOACK STREAMS s >")
	// With NOACK the entry never enters the PEL, so a re-read finds nothing.
	toks := flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s 0")
	if contains(toks, "1-1") {
		t.Fatalf("NOACK left a PEL entry: %v", toks)
	}
	// NOACK with an explicit ID is rejected.
	if got := sendLine(t, r, c, "XREADGROUP GROUP g1 c1 NOACK STREAMS s 0"); got != "-ERR The NOACK option is not valid for XREADGROUP with an explicit ID" {
		t.Fatalf("NOACK explicit = %q", got)
	}
}

func TestXReadGroupTombstone(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	// Delete the entry while it is still pending.
	if got := sendLine(t, r, c, "XDEL s 1-1"); got != ":1" {
		t.Fatalf("XDEL = %q", got)
	}
	// The PEL re-read still lists the ID but with null fields.
	toks := flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s 0")
	if !contains(toks, "1-1") || !contains(toks, "<nil>") {
		t.Fatalf("tombstone re-read = %v", toks)
	}
}

func TestXGroupConsumers(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	if got := sendLine(t, r, c, "XGROUP CREATECONSUMER s g1 c1"); got != ":1" {
		t.Fatalf("CREATECONSUMER = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XGROUP CREATECONSUMER s g1 c1"); got != ":0" {
		t.Fatalf("CREATECONSUMER dup = %q want :0", got)
	}
	// Give c1 a pending entry, then deleting it returns the PEL count.
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	if got := sendLine(t, r, c, "XGROUP DELCONSUMER s g1 c1"); got != ":1" {
		t.Fatalf("DELCONSUMER = %q want :1", got)
	}
}

func TestXInfoGroupsAndConsumers(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = bulk(t, r, c, "XADD s 2-1 b 2")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")

	toks := flatCmd(t, r, c, "XINFO GROUPS s")
	if !contains(toks, "g1") {
		t.Fatalf("XINFO GROUPS missing g1: %v", toks)
	}
	if v := valueAfter(t, toks, "last-delivered-id"); v != "2-1" {
		t.Fatalf("last-delivered-id = %q want 2-1", v)
	}
	if v := valueAfter(t, toks, "pending"); v != "2" {
		t.Fatalf("group pending = %q want 2", v)
	}

	toks = flatCmd(t, r, c, "XINFO CONSUMERS s g1")
	if !contains(toks, "c1") {
		t.Fatalf("XINFO CONSUMERS missing c1: %v", toks)
	}
	if got := sendLine(t, r, c, "XINFO CONSUMERS s missing"); got != "-"+nogroupError("missing", "s") {
		t.Fatalf("XINFO CONSUMERS missing group = %q", got)
	}
}

func TestXInfoStreamFullGroups(t *testing.T) {
	r, c := startData(t)
	_ = bulk(t, r, c, "XADD s 1-1 a 1")
	_ = sendLine(t, r, c, "XGROUP CREATE s g1 0")
	_ = flatCmd(t, r, c, "XREADGROUP GROUP g1 c1 STREAMS s >")
	toks := flatCmd(t, r, c, "XINFO STREAM s FULL")
	if !contains(toks, "g1") || !contains(toks, "c1") {
		t.Fatalf("XINFO STREAM FULL groups = %v", toks)
	}
	// Redis 7.x emits lag per group and pel-count per consumer in STREAM FULL.
	// Both were missing before; guard against the regression.
	if !contains(toks, "lag") {
		t.Fatalf("XINFO STREAM FULL missing group lag: %v", toks)
	}
	if !contains(toks, "pel-count") {
		t.Fatalf("XINFO STREAM FULL missing pel-count: %v", toks)
	}
}

func TestXGroupWrongType(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	for _, cmd := range []string{
		"XGROUP CREATE k g 0", "XACK k g 1-1", "XINFO GROUPS k",
	} {
		if got := sendLine(t, r, c, cmd); got != "-"+wrongTypeError {
			t.Fatalf("%s = %q want WRONGTYPE", cmd, got)
		}
	}
}
