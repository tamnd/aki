package f1srv

import "testing"

// CLIENT ID returns a positive integer, and each fresh connection gets a distinct one. The two
// connections here share one server, so their ids come from the same counter and must differ.
func TestClientID(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rwA, "CLIENT", "ID")
	a := readReply(t, rwA)
	cmd(t, rwB, "CLIENT", "ID")
	b := readReply(t, rwB)
	if a[0] != ':' || b[0] != ':' {
		t.Fatalf("CLIENT ID replies not integers: a=%q b=%q", a, b)
	}
	if a == b {
		t.Fatalf("two connections got the same CLIENT ID: %q", a)
	}

	cmd(t, rwA, "CLIENT", "ID", "x")
	expect(t, rwA, "-ERR wrong number of arguments for 'client|id' command")
}

// CLIENT GETNAME is the nil bulk until a name is set, then reads back the SETNAME value on the
// same connection. A second connection keeps its own (still unnamed) state.
func TestClientGetSetName(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rwA, "CLIENT", "GETNAME")
	expect(t, rwA, "$-1")

	cmd(t, rwA, "CLIENT", "SETNAME", "conn-a")
	expect(t, rwA, "+OK")
	cmd(t, rwA, "CLIENT", "GETNAME")
	expect(t, rwA, "$conn-a")

	// The name is per connection: B never set one, so it still reads nil.
	cmd(t, rwB, "CLIENT", "GETNAME")
	expect(t, rwB, "$-1")

	// An empty name clears the label back to unnamed.
	cmd(t, rwA, "CLIENT", "SETNAME", "")
	expect(t, rwA, "+OK")
	cmd(t, rwA, "CLIENT", "GETNAME")
	expect(t, rwA, "$-1")
}

// CLIENT SETNAME rejects names with spaces, newlines, or non-printable bytes, and takes exactly
// one name argument.
func TestClientSetNameErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CLIENT", "SETNAME", "bad name")
	expect(t, rw, "-ERR Client names cannot contain spaces, newlines or special characters.")
	cmd(t, rw, "CLIENT", "SETNAME", "bad\nname")
	expect(t, rw, "-ERR Client names cannot contain spaces, newlines or special characters.")
	cmd(t, rw, "CLIENT", "SETNAME")
	expect(t, rw, "-ERR wrong number of arguments for 'client|setname' command")
	cmd(t, rw, "CLIENT", "SETNAME", "a", "b")
	expect(t, rw, "-ERR wrong number of arguments for 'client|setname' command")
}

// CLIENT SETINFO accepts LIB-NAME and LIB-VER, rejects any other option by name, and needs exactly
// an option and a value.
func TestClientSetInfo(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CLIENT", "SETINFO", "lib-name", "mylib")
	expect(t, rw, "+OK")
	cmd(t, rw, "CLIENT", "SETINFO", "LIB-VER", "1.0")
	expect(t, rw, "+OK")
	cmd(t, rw, "CLIENT", "SETINFO", "bogus", "x")
	expect(t, rw, "-ERR Unrecognized option 'bogus'")
	cmd(t, rw, "CLIENT", "SETINFO", "lib-name")
	expect(t, rw, "-ERR wrong number of arguments for 'client|setinfo' command")
	cmd(t, rw, "CLIENT", "SETINFO")
	expect(t, rw, "-ERR wrong number of arguments for 'client|setinfo' command")
}

// CLIENT NO-EVICT and NO-TOUCH accept ON or OFF and reply OK, reject any other value as a syntax
// error, and need exactly the one toggle argument.
func TestClientNoEvictNoTouch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CLIENT", "NO-EVICT", "ON")
	expect(t, rw, "+OK")
	cmd(t, rw, "CLIENT", "NO-EVICT", "off")
	expect(t, rw, "+OK")
	cmd(t, rw, "CLIENT", "NO-EVICT", "bad")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "CLIENT", "NO-EVICT")
	expect(t, rw, "-ERR wrong number of arguments for 'client|no-evict' command")

	cmd(t, rw, "CLIENT", "NO-TOUCH", "ON")
	expect(t, rw, "+OK")
	cmd(t, rw, "CLIENT", "NO-TOUCH", "bad")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "CLIENT", "NO-TOUCH")
	expect(t, rw, "-ERR wrong number of arguments for 'client|no-touch' command")
}

// CLIENT GETREDIR reports no redirection, CLIENT HELP returns the help array, and an unknown
// subcommand or a missing subcommand is rejected with the Redis wording.
func TestClientGetRedirHelpUnknown(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "CLIENT", "GETREDIR")
	expect(t, rw, ":-1")
	cmd(t, rw, "CLIENT", "GETREDIR", "x")
	expect(t, rw, "-ERR wrong number of arguments for 'client|getredir' command")

	cmd(t, rw, "CLIENT", "HELP")
	expect(t, rw, "*"+itoa(len(clientHelp)))
	for range clientHelp {
		readReply(t, rw)
	}

	cmd(t, rw, "CLIENT")
	expect(t, rw, "-ERR wrong number of arguments for 'client' command")
	cmd(t, rw, "CLIENT", "BADSUB")
	expect(t, rw, "-ERR unknown subcommand 'BADSUB'. Try CLIENT HELP.")
	cmd(t, rw, "CLIENT", "KILL", "1.2.3.4:5")
	expect(t, rw, "-ERR unknown subcommand 'KILL'. Try CLIENT HELP.")
}
