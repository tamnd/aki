package command

import "testing"

// TestClusterConnCommands checks ASKING, READONLY and READWRITE are recognized
// and reply OK, that they take no arguments, and that ordinary commands still run
// after them. aki serves every slot from one node, so the flags change no
// routing; the point is wire compatibility with cluster-aware clients.
func TestClusterConnCommands(t *testing.T) {
	r, c := startData(t)

	for _, cmd := range []string{"ASKING", "READONLY", "READWRITE"} {
		if got := sendLine(t, r, c, cmd); got != "+OK" {
			t.Fatalf("%s = %q want +OK", cmd, got)
		}
	}

	// Each takes no extra argument.
	for _, cmd := range []string{"ASKING x", "READONLY x", "READWRITE x"} {
		got := sendLine(t, r, c, cmd)
		if got == "" || got[0] != '-' {
			t.Fatalf("%s = %q want arity error", cmd, got)
		}
	}

	// A command runs normally after ASKING.
	if got := sendLine(t, r, c, "ASKING"); got != "+OK" {
		t.Fatalf("ASKING = %q", got)
	}
	if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
		t.Fatalf("SET after ASKING = %q", got)
	}

	// READONLY then a read still works, and READWRITE restores the default.
	if got := sendLine(t, r, c, "READONLY"); got != "+OK" {
		t.Fatalf("READONLY = %q", got)
	}
	if got := sendLine(t, r, c, "GET k"); got != "$1" {
		t.Fatalf("GET after READONLY = %q", got)
	}
	if got := sendLine(t, r, c, ""); got != "v" {
		t.Fatalf("GET body after READONLY = %q", got)
	}
	if got := sendLine(t, r, c, "READWRITE"); got != "+OK" {
		t.Fatalf("READWRITE = %q", got)
	}
}
