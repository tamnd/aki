package command

import "testing"

// TestRoleStandaloneMaster checks ROLE on a fresh standalone instance: it is a
// master with offset 0 and no connected replicas, so the reply is the three
// element array ["master", 0, []].
func TestRoleStandaloneMaster(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "ROLE"); got != "*3" {
		t.Fatalf("ROLE header = %q want *3", got)
	}
	if got := sendLineRead(t, r); got != "$6" {
		t.Fatalf("ROLE[0] header = %q want $6", got)
	}
	if got := sendLineRead(t, r); got != "master" {
		t.Fatalf("ROLE[0] = %q want master", got)
	}
	if got := sendLineRead(t, r); got != ":0" {
		t.Fatalf("ROLE[1] offset = %q want :0", got)
	}
	if got := sendLineRead(t, r); got != "*0" {
		t.Fatalf("ROLE[2] replicas = %q want *0 (no replicas)", got)
	}
}

// TestModuleList checks MODULE LIST reports the empty set on a binary that loads
// no modules, and that MODULE LOAD and MODULE UNLOAD report the action is
// unavailable rather than answering unknown command.
func TestModuleList(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "MODULE LIST"); got != "*0" {
		t.Fatalf("MODULE LIST = %q want *0", got)
	}
	if got := sendLine(t, r, c, "MODULE LOAD /tmp/x.so"); got != "-ERR Error loading the extension. Please check the server logs." {
		t.Fatalf("MODULE LOAD = %q", got)
	}
	if got := sendLine(t, r, c, "MODULE UNLOAD nope"); got != "-ERR Error unloading module: no such module with that name" {
		t.Fatalf("MODULE UNLOAD = %q", got)
	}
}

// TestRestoreAsking checks RESTORE-ASKING behaves as RESTORE in standalone mode:
// a payload produced by DUMP round-trips back through RESTORE-ASKING into the
// same value.
func TestRestoreAsking(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET src hello")
	// DUMP returns a bulk string; capture its header then body line.
	if got := sendLine(t, r, c, "DUMP src"); got[0] != '$' {
		t.Fatalf("DUMP header = %q want bulk", got)
	}
	payload := sendLineRead(t, r)
	// Restore it under a new key via RESTORE-ASKING, then read it back.
	rawCmd(t, c, "RESTORE-ASKING", "dst", "0", payload)
	if got := sendLineRead(t, r); got != "+OK" {
		t.Fatalf("RESTORE-ASKING = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "GET dst"); got != "$5" {
		t.Fatalf("GET dst header = %q want $5", got)
	}
	if got := sendLineRead(t, r); got != "hello" {
		t.Fatalf("GET dst = %q want hello", got)
	}
}
