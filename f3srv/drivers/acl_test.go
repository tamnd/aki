package drivers

import (
	"testing"
)

// TestAclWhoami checks ACL WHOAMI names the built-in default superuser.
func TestAclWhoami(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "ACL", "WHOAMI"); got != "default" {
		t.Fatalf("ACL WHOAMI = %v, want default", got)
	}
}

// TestAclUsersList checks USERS lists the one default user and LIST renders its
// unrestricted passwordless rule line.
func TestAclUsersList(t *testing.T) {
	_, nc, br := startServer(t)

	users, ok := sendCmd(t, br, nc, "ACL", "USERS").([]any)
	if !ok || len(users) != 1 || users[0] != "default" {
		t.Fatalf("ACL USERS = %v, want [default]", users)
	}
	list, ok := sendCmd(t, br, nc, "ACL", "LIST").([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("ACL LIST = %v, want one rule line", list)
	}
	line, _ := list[0].(string)
	if line != "user default on nopass ~* &* +@all" {
		t.Fatalf("ACL LIST line = %q, want the default superuser rule", line)
	}
}

// TestAclCat checks bare CAT lists the category vocabulary and CAT with an
// argument declines, since f3 does not track per-command category membership.
func TestAclCat(t *testing.T) {
	_, nc, br := startServer(t)

	cats, ok := sendCmd(t, br, nc, "ACL", "CAT").([]any)
	if !ok || len(cats) == 0 {
		t.Fatalf("ACL CAT = %v, want a non-empty category list", cats)
	}
	seen := map[string]bool{}
	for _, c := range cats {
		seen[c.(string)] = true
	}
	for _, want := range []string{"read", "write", "keyspace", "dangerous"} {
		if !seen[want] {
			t.Fatalf("ACL CAT missing category %q", want)
		}
	}
	if _, ok := sendCmd(t, br, nc, "ACL", "CAT", "read").(errorReply); !ok {
		t.Fatalf("ACL CAT read did not decline")
	}
}

// TestAclGetUser checks GETUSER default returns the superuser field map and any
// other name is a null reply, since f3 holds no other user.
func TestAclGetUser(t *testing.T) {
	_, nc, br := startServer(t)

	m, ok := sendCmd(t, br, nc, "ACL", "GETUSER", "default").([]any)
	if !ok || len(m) != 12 {
		t.Fatalf("ACL GETUSER default = %v, want a 12-element field map", m)
	}
	fields := map[string]any{}
	for i := 0; i < len(m); i += 2 {
		fields[m[i].(string)] = m[i+1]
	}
	if fields["commands"] != "+@all" {
		t.Fatalf("commands = %v, want +@all", fields["commands"])
	}
	if fields["keys"] != "~*" {
		t.Fatalf("keys = %v, want ~*", fields["keys"])
	}
	flags, ok := fields["flags"].([]any)
	if !ok || len(flags) == 0 || flags[0] != "on" {
		t.Fatalf("flags = %v, want a list starting with on", fields["flags"])
	}

	if got := sendCmd(t, br, nc, "ACL", "GETUSER", "nobody"); got != nil {
		t.Fatalf("ACL GETUSER nobody = %v, want null", got)
	}
}

// TestAclUnsupported checks a mutating or unknown subcommand errors rather than
// fabricating a success, since f3 has no user store.
func TestAclUnsupported(t *testing.T) {
	_, nc, br := startServer(t)
	for _, sub := range [][]string{
		{"ACL", "SETUSER", "alice", "on"},
		{"ACL", "DELUSER", "alice"},
		{"ACL", "NOPE"},
	} {
		if _, ok := sendCmd(t, br, nc, sub...).(errorReply); !ok {
			t.Fatalf("%v did not error", sub)
		}
	}
}
