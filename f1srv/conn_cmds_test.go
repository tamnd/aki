package f1srv

import (
	"strconv"
	"testing"
)

// TIME replies with a two-element array of unix seconds and leftover microseconds, both bulk
// strings that parse as non-negative integers, with microseconds under one million.
func TestTime(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "TIME")
	expect(t, rw, "*2")
	secs := readBulk(t, rw)
	micros := readBulk(t, rw)
	if s, err := strconv.ParseInt(secs, 10, 64); err != nil || s <= 0 {
		t.Fatalf("seconds = %q, want a positive integer", secs)
	}
	m, err := strconv.ParseInt(micros, 10, 64)
	if err != nil || m < 0 || m >= 1_000_000 {
		t.Fatalf("microseconds = %q, want 0..999999", micros)
	}
}

// TIME rejects any argument.
func TestTimeArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "TIME", "extra")
	expect(t, rw, "-ERR wrong number of arguments for 'time' command")
}

// ROLE on this standalone master replies ["master", 0, []].
func TestRole(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ROLE")
	expect(t, rw, "*3")
	if s := readBulk(t, rw); s != "master" {
		t.Fatalf("role = %q, want master", s)
	}
	expect(t, rw, ":0")
	expect(t, rw, "*0")
}

// ROLE rejects any argument.
func TestRoleArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ROLE", "extra")
	expect(t, rw, "-ERR wrong number of arguments for 'role' command")
}

// AUTH on a server with no password configured fails every way, and the reply depends only on
// how many arguments the client passed.
func TestAuth(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "AUTH")
	expect(t, rw, "-ERR wrong number of arguments for 'auth' command")
	cmd(t, rw, "AUTH", "somepass")
	expect(t, rw, "-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	cmd(t, rw, "AUTH", "user", "pass")
	expect(t, rw, "-WRONGPASS invalid username-password pair or user is disabled.")
	cmd(t, rw, "AUTH", "a", "b", "c")
	expect(t, rw, "-ERR syntax error")
}
