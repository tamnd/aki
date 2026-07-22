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

// isHex reports whether s is non-empty and made only of lowercase hex digits, the
// alphabet ACL GENPASS draws from.
func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// TestAclGenPass checks GENPASS emits a lowercase-hex secret of the right length:
// 64 chars by default (256 bits), ceil(bits/4) for an explicit count, a fresh
// value each call, and a clear error for an out-of-range bit count.
func TestAclGenPass(t *testing.T) {
	_, nc, br := startServer(t)

	def, ok := sendCmd(t, br, nc, "ACL", "GENPASS").(string)
	if !ok || len(def) != 64 || !isHex(def) {
		t.Fatalf("ACL GENPASS = %q, want 64 hex chars", def)
	}

	// A second call yields a different secret; a fixed generator would repeat.
	again, _ := sendCmd(t, br, nc, "ACL", "GENPASS").(string)
	if again == def {
		t.Fatalf("ACL GENPASS repeated %q, want a fresh value", def)
	}

	// ceil(bits/4) characters: 32 bits -> 8, 1 bit -> 1, 4095 bits -> 1024.
	for _, tc := range []struct {
		bits string
		want int
	}{{"32", 8}, {"1", 1}, {"5", 2}, {"4096", 1024}, {"4095", 1024}} {
		got, ok := sendCmd(t, br, nc, "ACL", "GENPASS", tc.bits).(string)
		if !ok || len(got) != tc.want || !isHex(got) {
			t.Fatalf("ACL GENPASS %s = %q (len %d), want %d hex chars", tc.bits, got, len(got), tc.want)
		}
	}

	for _, bad := range []string{"0", "-1", "4097", "abc"} {
		if _, ok := sendCmd(t, br, nc, "ACL", "GENPASS", bad).(errorReply); !ok {
			t.Fatalf("ACL GENPASS %s did not error", bad)
		}
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
