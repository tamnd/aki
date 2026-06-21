package command

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readBulk reads a bulk-string reply whose header line was already returned by
// sendLine. It returns the body, which for CLIENT LIST holds embedded newlines,
// so a plain ReadString would stop short.
func readBulk(t *testing.T, r *bufio.Reader, header string) string {
	t.Helper()
	if !strings.HasPrefix(header, "$") {
		t.Fatalf("expected bulk header, got %q", header)
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil {
		t.Fatalf("bad bulk header %q: %v", header, err)
	}
	if n < 0 {
		return ""
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read bulk body: %v", err)
	}
	return string(buf[:n])
}

func TestClientID(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "CLIENT ID")
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("CLIENT ID = %q", got)
	}
	if n, _ := strconv.Atoi(got[1:]); n <= 0 {
		t.Fatalf("CLIENT ID value = %q", got)
	}
}

func TestClientName(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT GETNAME"); got != "$0" {
		t.Fatalf("empty GETNAME = %q", got)
	}
	// $0 reply has an empty body line still pending.
	_ = sendLineRead(t, r)
	if got := sendLine(t, r, c, "CLIENT SETNAME app"); got != "+OK" {
		t.Fatalf("SETNAME = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT GETNAME"); got != "$3" {
		t.Fatalf("GETNAME header = %q", got)
	}
	if got := sendLineRead(t, r); got != "app" {
		t.Fatalf("GETNAME body = %q", got)
	}
}

func TestClientSetNameBad(t *testing.T) {
	r, c := start(t, Config{})
	// A name with a space arrives as two inline tokens, which the arity check
	// rejects before the name validator runs, matching the wire behavior.
	got := sendLine(t, r, c, "CLIENT SETNAME bad name")
	if !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("SETNAME bad = %q", got)
	}
}

func TestClientSetInfo(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT SETINFO lib-name mylib"); got != "+OK" {
		t.Fatalf("SETINFO lib-name = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT SETINFO lib-ver 1.2.3"); got != "+OK" {
		t.Fatalf("SETINFO lib-ver = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT SETINFO bogus x"); !strings.HasPrefix(got, "-ERR Unrecognized option") {
		t.Fatalf("SETINFO bogus = %q", got)
	}
	header := sendLine(t, r, c, "CLIENT INFO")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "lib-name=mylib") || !strings.Contains(body, "lib-ver=1.2.3") {
		t.Fatalf("CLIENT INFO lib fields = %q", body)
	}
}

func TestClientInfoFields(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "CLIENT INFO")
	body := readBulk(t, r, header)
	for _, field := range []string{"id=", "addr=", "laddr=", "db=", "resp=2", "cmd=client|info"} {
		if !strings.Contains(body, field) {
			t.Fatalf("CLIENT INFO missing %q in %q", field, body)
		}
	}
}

func TestClientList(t *testing.T) {
	r, c := start(t, Config{})
	// Run a command first so cmd= is populated, then list.
	_ = sendLine(t, r, c, "CLIENT ID")
	header := sendLine(t, r, c, "CLIENT LIST")
	body := readBulk(t, r, header)
	if !strings.Contains(body, "id=") || !strings.HasSuffix(body, "\n") {
		t.Fatalf("CLIENT LIST = %q", body)
	}
	lines := strings.Count(body, "\n")
	if lines < 1 {
		t.Fatalf("CLIENT LIST line count = %d", lines)
	}
}

func TestClientListBadType(t *testing.T) {
	r, c := start(t, Config{})
	header := sendLine(t, r, c, "CLIENT LIST TYPE master")
	body := readBulk(t, r, header)
	if body != "" {
		t.Fatalf("CLIENT LIST TYPE master = %q", body)
	}
}

func TestClientNoEvictTouch(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT NO-EVICT on"); got != "+OK" {
		t.Fatalf("NO-EVICT on = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT NO-TOUCH off"); got != "+OK" {
		t.Fatalf("NO-TOUCH off = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT NO-EVICT maybe"); !strings.HasPrefix(got, "-ERR syntax error") {
		t.Fatalf("NO-EVICT maybe = %q", got)
	}
}

func TestClientGetRedir(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT GETREDIR"); got != ":-1" {
		t.Fatalf("GETREDIR = %q", got)
	}
}

func TestClientPause(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT PAUSE 50"); got != "+OK" {
		t.Fatalf("PAUSE = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT UNPAUSE"); got != "+OK" {
		t.Fatalf("UNPAUSE = %q", got)
	}
	if got := sendLine(t, r, c, "CLIENT PAUSE notanumber"); !strings.HasPrefix(got, "-ERR timeout") {
		t.Fatalf("PAUSE bad = %q", got)
	}
}

func TestClientKill(t *testing.T) {
	r, c := start(t, Config{})
	// Old form against a missing address.
	if got := sendLine(t, r, c, "CLIENT KILL 1.2.3.4:5"); !strings.HasPrefix(got, "-ERR No such client") {
		t.Fatalf("KILL missing addr = %q", got)
	}
	// Filter form with SKIPME default (yes) and a non-matching id kills nothing.
	if got := sendLine(t, r, c, "CLIENT KILL ID 999999"); got != ":0" {
		t.Fatalf("KILL ID 999999 = %q", got)
	}
}

func TestClientKillSelf(t *testing.T) {
	r, c := start(t, Config{})
	id := sendLine(t, r, c, "CLIENT ID")
	cid := id[1:]
	// SKIPME no with our own id closes the connection after the reply is sent.
	got := sendLine(t, r, c, "CLIENT KILL ID "+cid+" SKIPME no")
	if got != ":1" {
		t.Fatalf("KILL self = %q", got)
	}
	// The next read should hit EOF since the server closed the socket.
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := r.ReadString('\n'); err == nil {
		t.Fatal("expected connection close after self kill")
	}
}

func TestClientHelp(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CLIENT HELP"); !strings.HasPrefix(got, "*") {
		t.Fatalf("CLIENT HELP = %q", got)
	}
}
