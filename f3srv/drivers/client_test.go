package drivers

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
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

// TestHelloThreeAccepted checks HELLO 3 switches the connection to RESP3 and
// answers the handshake as a RESP3 map with proto 3, and that a following
// map-shaped reply (CONFIG GET) then arrives with the RESP3 map framing.
func TestHelloThreeAccepted(t *testing.T) {
	_, nc, br := startServer(t)
	m := helloFields(t, sendCmd(t, br, nc, "HELLO", "3"))
	if m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	if m["server"] != "aki" {
		t.Fatalf("HELLO 3 server = %v, want aki", m["server"])
	}
	// A map reply now uses the RESP3 map framing; readRESP flattens it to pairs.
	cfg := helloFields(t, sendCmd(t, br, nc, "CONFIG", "GET", "maxmemory"))
	if cfg["maxmemory"] != "0" {
		t.Fatalf("CONFIG GET maxmemory over RESP3 = %v, want 0", cfg["maxmemory"])
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

// TestQuit checks QUIT acknowledges with +OK and then closes the connection, so
// a following read reaches EOF.
func TestQuit(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "QUIT"); got != "OK" {
		t.Fatalf("QUIT = %v, want OK", got)
	}
	// The +OK landed; the server now closes, so the next read is EOF.
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("read after QUIT = %v, want EOF", err)
	}
}

// TestResetClearsName checks RESET replies +RESET and clears a CLIENT SETNAME
// label, so a following GETNAME reads empty.
func TestResetClearsName(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "CLIENT", "SETNAME", "before-reset"); got != "OK" {
		t.Fatalf("CLIENT SETNAME = %v, want OK", got)
	}
	if got := sendCmd(t, br, nc, "RESET"); got != "RESET" {
		t.Fatalf("RESET = %v, want RESET", got)
	}
	if got := sendCmd(t, br, nc, "CLIENT", "GETNAME"); got != "" {
		t.Fatalf("GETNAME after RESET = %v, want empty", got)
	}
}

// TestResetClearsSubscribeMode checks RESET takes the connection out of
// subscribe mode: after SUBSCRIBE a plain GET is refused, and after RESET the
// same GET runs.
func TestResetClearsSubscribeMode(t *testing.T) {
	_, nc, br := startServer(t)

	// SUBSCRIBE confirms with a three-element array; read and discard it.
	if _, ok := sendCmd(t, br, nc, "SUBSCRIBE", "ch").([]any); !ok {
		t.Fatalf("SUBSCRIBE did not confirm")
	}
	// In subscribe mode a plain GET is refused.
	if _, ok := sendCmd(t, br, nc, "GET", "k").(errorReply); !ok {
		t.Fatalf("GET in subscribe mode was not refused")
	}
	if got := sendCmd(t, br, nc, "RESET"); got != "RESET" {
		t.Fatalf("RESET = %v, want RESET", got)
	}
	// Out of subscribe mode, GET runs and answers the null bulk for a missing key.
	if got := sendCmd(t, br, nc, "GET", "k"); got != nil {
		t.Fatalf("GET after RESET = %v, want null", got)
	}
}

// TestAuthDeclined checks AUTH always declines on a passwordless server, in both
// the one-argument and two-argument forms.
func TestAuthDeclined(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "AUTH", "secret").(errorReply); !ok {
		t.Fatalf("AUTH password did not decline")
	}
	if _, ok := sendCmd(t, br, nc, "AUTH", "default", "secret").(errorReply); !ok {
		t.Fatalf("AUTH username password did not decline")
	}
}

// clientInfoFields parses a CLIENT INFO bulk line ("k=v k=v ...") into a field
// map so a test can assert on the fields without pinning the whole line.
func clientInfoFields(t *testing.T, reply any) map[string]string {
	t.Helper()
	line, ok := reply.(string)
	if !ok {
		t.Fatalf("CLIENT INFO = %T, want a bulk string", reply)
	}
	m := map[string]string{}
	for _, field := range strings.Fields(line) {
		eq := strings.IndexByte(field, '=')
		if eq < 0 {
			t.Fatalf("CLIENT INFO field %q has no '='", field)
		}
		m[field[:eq]] = field[eq+1:]
	}
	return m
}

// TestClientInfo checks CLIENT INFO describes this connection: the id matches
// CLIENT ID, the endpoints are real, the fixed standalone facts are present, and
// the name and subscription count track the connection's state.
func TestClientInfo(t *testing.T) {
	_, nc, br := startServer(t)

	id, _ := sendCmd(t, br, nc, "CLIENT", "ID").(int64)
	if got := sendCmd(t, br, nc, "CLIENT", "SETNAME", "inspector"); got != "OK" {
		t.Fatalf("CLIENT SETNAME = %v, want OK", got)
	}

	m := clientInfoFields(t, sendCmd(t, br, nc, "CLIENT", "INFO"))
	if m["id"] != strconv.FormatInt(id, 10) {
		t.Fatalf("CLIENT INFO id = %q, want the connection id %d", m["id"], id)
	}
	if m["name"] != "inspector" {
		t.Fatalf("CLIENT INFO name = %q, want inspector", m["name"])
	}
	if m["addr"] == "" || !strings.Contains(m["addr"], ":") {
		t.Fatalf("CLIENT INFO addr = %q, want a real ip:port", m["addr"])
	}
	if m["laddr"] == "" || !strings.Contains(m["laddr"], ":") {
		t.Fatalf("CLIENT INFO laddr = %q, want a real ip:port", m["laddr"])
	}
	for k, want := range map[string]string{
		"db": "0", "resp": "2", "redir": "-1", "multi": "-1",
		"sub": "0", "psub": "0", "cmd": "client|info", "user": "default",
	} {
		if m[k] != want {
			t.Fatalf("CLIENT INFO %s = %q, want %q", k, m[k], want)
		}
	}
	if _, ok := m["tot-cmds"]; !ok {
		t.Fatalf("CLIENT INFO missing tot-cmds field")
	}
}

// TestClientInfoTracksSubscribe checks the sub= count reflects a live
// subscription, read from this connection's own reader-owned state.
func TestClientInfoTracksSubscribe(t *testing.T) {
	_, nc, br := startServer(t)

	if _, ok := sendCmd(t, br, nc, "SUBSCRIBE", "ch1", "ch2").([]any); !ok {
		t.Fatalf("SUBSCRIBE did not confirm")
	}
	// SUBSCRIBE to two channels emits one confirmation per channel; the first rode
	// back with the sendCmd read, drain the second before the next command.
	readRESP(t, br)

	// CLIENT INFO is allowed in subscribe mode (it is a connection verb), so the
	// sub= field reads the two live subscriptions.
	m := clientInfoFields(t, sendCmd(t, br, nc, "CLIENT", "INFO"))
	if m["sub"] != "2" {
		t.Fatalf("CLIENT INFO sub = %q, want 2", m["sub"])
	}
}

// clientListLines splits a CLIENT LIST bulk reply into its per-connection lines,
// dropping the trailing empty split after the final newline.
func clientListLines(t *testing.T, reply any) []map[string]string {
	t.Helper()
	blob, ok := reply.(string)
	if !ok {
		t.Fatalf("CLIENT LIST = %T, want a bulk string", reply)
	}
	var lines []map[string]string
	for _, raw := range strings.Split(blob, "\n") {
		if raw == "" {
			continue
		}
		m := map[string]string{}
		for _, field := range strings.Fields(raw) {
			eq := strings.IndexByte(field, '=')
			if eq < 0 {
				t.Fatalf("CLIENT LIST field %q has no '='", field)
			}
			m[field[:eq]] = field[eq+1:]
		}
		lines = append(lines, m)
	}
	return lines
}

// lineByID finds the CLIENT LIST line with the given id= value.
func lineByID(lines []map[string]string, id int64) map[string]string {
	want := strconv.FormatInt(id, 10)
	for _, m := range lines {
		if m["id"] == want {
			return m
		}
	}
	return nil
}

// TestClientList checks CLIENT LIST returns one line per live connection, that a
// second connection shows up, and that the requesting connection's line reports
// cmd=client|list while the other reports cmd=NULL (f3 keeps no per-connection
// last-command record).
func TestClientList(t *testing.T) {
	s, nc, br := startServer(t)
	id1, _ := sendCmd(t, br, nc, "CLIENT", "ID").(int64)

	nc2, br2 := dial(t, s)
	id2, _ := sendCmd(t, br2, nc2, "CLIENT", "ID").(int64)

	lines := clientListLines(t, sendCmd(t, br, nc, "CLIENT", "LIST"))
	if len(lines) < 2 {
		t.Fatalf("CLIENT LIST returned %d lines, want at least 2", len(lines))
	}
	self := lineByID(lines, id1)
	other := lineByID(lines, id2)
	if self == nil || other == nil {
		t.Fatalf("CLIENT LIST missing a connection: self=%v other=%v", self, other)
	}
	if self["cmd"] != "client|list" {
		t.Fatalf("requesting line cmd = %q, want client|list", self["cmd"])
	}
	if other["cmd"] != "NULL" {
		t.Fatalf("other line cmd = %q, want NULL", other["cmd"])
	}
	if self["addr"] == "" || other["addr"] == "" {
		t.Fatalf("CLIENT LIST line missing addr: self=%q other=%q", self["addr"], other["addr"])
	}
}

// TestClientListFilters checks the ID and TYPE filters: ID narrows to the named
// connections, TYPE pubsub keeps only subscribed connections, and TYPE master
// (no replication in f3) returns nothing.
func TestClientListFilters(t *testing.T) {
	s, nc, br := startServer(t)
	id1, _ := sendCmd(t, br, nc, "CLIENT", "ID").(int64)

	nc2, br2 := dial(t, s)
	id2, _ := sendCmd(t, br2, nc2, "CLIENT", "ID").(int64)
	// Put the second connection into subscribe mode so the TYPE filter can split.
	if _, ok := sendCmd(t, br2, nc2, "SUBSCRIBE", "room").([]any); !ok {
		t.Fatalf("SUBSCRIBE did not confirm")
	}

	byID := clientListLines(t, sendCmd(t, br, nc, "CLIENT", "LIST", "ID", strconv.FormatInt(id2, 10)))
	if len(byID) != 1 || byID[0]["id"] != strconv.FormatInt(id2, 10) {
		t.Fatalf("CLIENT LIST ID %d = %v, want just that connection", id2, byID)
	}

	pubsub := clientListLines(t, sendCmd(t, br, nc, "CLIENT", "LIST", "TYPE", "pubsub"))
	if len(pubsub) != 1 || pubsub[0]["id"] != strconv.FormatInt(id2, 10) {
		t.Fatalf("CLIENT LIST TYPE pubsub = %v, want only the subscribed connection", pubsub)
	}

	normal := clientListLines(t, sendCmd(t, br, nc, "CLIENT", "LIST", "TYPE", "normal"))
	if lineByID(normal, id1) == nil || lineByID(normal, id2) != nil {
		t.Fatalf("CLIENT LIST TYPE normal = %v, want the unsubscribed connection only", normal)
	}

	master := clientListLines(t, sendCmd(t, br, nc, "CLIENT", "LIST", "TYPE", "master"))
	if len(master) != 0 {
		t.Fatalf("CLIENT LIST TYPE master = %v, want empty", master)
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
