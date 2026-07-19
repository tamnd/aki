package drivers

import (
	"bufio"
	"net"
	"testing"
)

// dial opens a second connection to an already-running test server, so a test
// can check that two connections get distinct ids. The connection closes with
// the test.
func dial(t *testing.T, s *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatalf("dial second connection: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc, bufio.NewReader(nc)
}

// TestClientID checks CLIENT ID answers a positive integer and that a second
// connection gets a distinct, higher id from the same monotonic sequence.
func TestClientID(t *testing.T) {
	s, nc, br := startServer(t)
	first, ok := sendCmd(t, br, nc, "CLIENT", "ID").(int64)
	if !ok || first <= 0 {
		t.Fatalf("CLIENT ID = %v, want positive integer", first)
	}

	nc2, br2 := dial(t, s)
	second, ok := sendCmd(t, br2, nc2, "CLIENT", "ID").(int64)
	if !ok || second <= first {
		t.Fatalf("second CLIENT ID = %v, want greater than %d", second, first)
	}
}

// TestClientSetGetName checks the name round-trips: GETNAME is the empty bulk
// until SETNAME, then returns the label, and an empty SETNAME clears it.
func TestClientSetGetName(t *testing.T) {
	_, nc, br := startServer(t)

	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "" {
		t.Fatalf("fresh CLIENT GETNAME = %v, want empty bulk", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "SETNAME", "worker-1"); got != "OK" {
		t.Fatalf("CLIENT SETNAME = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "worker-1" {
		t.Fatalf("CLIENT GETNAME = %v, want worker-1", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "SETNAME", ""); got != "OK" {
		t.Fatalf("CLIENT SETNAME empty = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "" {
		t.Fatalf("cleared CLIENT GETNAME = %v, want empty bulk", got)
	}
}

// TestClientSetNameRejectsSpaces checks a name with a space is refused, matching
// redis, and that the connection's name is unchanged after the reject.
func TestClientSetNameRejectsSpaces(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CLIENT", "SETNAME", "bad name").(errorReply); !ok {
		t.Fatalf("CLIENT SETNAME with a space did not error")
	}
	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "" {
		t.Fatalf("name changed after a rejected SETNAME: %v", got)
	}
}

// TestClientSetInfo checks SETINFO accepts the two library attributes and
// rejects an unknown one, without retaining either value.
func TestClientSetInfo(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "CLIENT", "SETINFO", "lib-name", "go-redis"); got != "OK" {
		t.Fatalf("CLIENT SETINFO lib-name = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "SETINFO", "LIB-VER", "9.0.0"); got != "OK" {
		t.Fatalf("CLIENT SETINFO LIB-VER = %v, want OK", got)
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "SETINFO", "nope", "x").(errorReply); !ok {
		t.Fatalf("CLIENT SETINFO with an unknown option did not error")
	}
}

// TestClientFlagToggles checks NO-EVICT and NO-TOUCH accept ON/OFF and reject a
// bad argument, and that GETREDIR reports no redirection.
func TestClientFlagToggles(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "CLIENT", "NO-EVICT", "ON"); got != "OK" {
		t.Fatalf("CLIENT NO-EVICT ON = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "NO-TOUCH", "off"); got != "OK" {
		t.Fatalf("CLIENT NO-TOUCH off = %v, want OK", got)
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "NO-EVICT", "maybe").(errorReply); !ok {
		t.Fatalf("CLIENT NO-EVICT maybe did not error")
	}
	if got, ok := sendCmd(t, br, nc, "CLIENT", "GETREDIR").(int64); !ok || got != -1 {
		t.Fatalf("CLIENT GETREDIR = %v, want -1", got)
	}
}

// TestClientBadSubcommand checks an unmodeled subcommand errors rather than
// answering a misleading OK.
func TestClientBadSubcommand(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CLIENT", "PAUSE", "100").(errorReply); !ok {
		t.Fatalf("CLIENT PAUSE did not error")
	}
}

// TestHelloBare checks a bare HELLO returns the RESP2 handshake map with the
// connection's own id, proto 2, and the standalone/master facts.
func TestHelloBare(t *testing.T) {
	_, nc, br := startServer(t)

	id, _ := sendCmd(t, br, nc, "CLIENT", "ID").(int64)
	m := helloFields(t, sendCmd(t, br, nc, "HELLO"))
	if m["server"] != "aki" {
		t.Fatalf("HELLO server = %v, want aki", m["server"])
	}
	if m["proto"] != int64(2) {
		t.Fatalf("HELLO proto = %v, want 2", m["proto"])
	}
	if m["id"] != id {
		t.Fatalf("HELLO id = %v, want the connection id %d", m["id"], id)
	}
	if m["mode"] != "standalone" || m["role"] != "master" {
		t.Fatalf("HELLO mode/role = %v/%v, want standalone/master", m["mode"], m["role"])
	}
	mods, ok := m["modules"].([]any)
	if !ok || len(mods) != 0 {
		t.Fatalf("HELLO modules = %v, want empty array", m["modules"])
	}
}

// TestHelloTwoWithSetName checks HELLO 2 SETNAME confirms RESP2 and applies the
// name, so a later GETNAME reads it back.
func TestHelloTwoWithSetName(t *testing.T) {
	_, nc, br := startServer(t)
	m := helloFields(t, sendCmd(t, br, nc, "HELLO", "2", "SETNAME", "conn-a"))
	if m["proto"] != int64(2) {
		t.Fatalf("HELLO 2 proto = %v, want 2", m["proto"])
	}
	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "conn-a" {
		t.Fatalf("GETNAME after HELLO SETNAME = %v, want conn-a", got)
	}
}

// TestHelloThreeDeclined checks HELLO 3 answers NOPROTO rather than switching to
// RESP3, which f3 does not speak yet.
func TestHelloThreeDeclined(t *testing.T) {
	_, nc, br := startServer(t)
	reply, ok := sendCmd(t, br, nc, "HELLO", "3").(errorReply)
	if !ok {
		t.Fatalf("HELLO 3 = %v, want a NOPROTO error", reply)
	}
	if len(reply) < 7 || string(reply[:7]) != "NOPROTO" {
		t.Fatalf("HELLO 3 error = %q, want a NOPROTO prefix", string(reply))
	}
}

// TestHelloAuthDeclined checks HELLO AUTH is declined on a passwordless server,
// the honest redis answer, rather than a faked success.
func TestHelloAuthDeclined(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "HELLO", "2", "AUTH", "default", "").(errorReply); !ok {
		t.Fatalf("HELLO AUTH did not decline on a passwordless server")
	}
}

// helloFields turns a flat HELLO reply array into a field map for assertions.
func helloFields(t *testing.T, reply any) map[string]any {
	t.Helper()
	arr, ok := reply.([]any)
	if !ok || len(arr)%2 != 0 {
		t.Fatalf("HELLO reply = %v, want an even-length array", reply)
	}
	m := map[string]any{}
	for i := 0; i < len(arr); i += 2 {
		key, ok := arr[i].(string)
		if !ok {
			t.Fatalf("HELLO field key %v is not a string", arr[i])
		}
		m[key] = arr[i+1]
	}
	return m
}
