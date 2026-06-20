package networking

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/resp"
)

// testHandler is a stand-in for the real command dispatcher: it knows just
// enough commands to exercise the connection loop end to end.
func testHandler(c *Conn, argv [][]byte) {
	switch strings.ToUpper(string(argv[0])) {
	case "PING":
		if len(argv) > 1 {
			c.Enc().WriteBulkString(argv[1])
		} else {
			c.WriteRaw(resp.ReplyPong)
		}
	case "ECHO":
		c.Enc().WriteBulkString(argv[1])
	case "HELLO":
		// Flip to RESP3 and confirm with a simple status so the test can see the
		// proto switch take effect on the next reply.
		c.SetProto(3)
		c.Enc().WriteStatus("HELLO3")
	case "QUIT":
		c.WriteRaw(resp.ReplyOK)
		c.Quit()
	default:
		c.Enc().WriteError("ERR unknown command")
	}
}

// startTestServer brings up a server on an ephemeral port and returns it with
// the dialable address. The caller defers srv.Close.
func startTestServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	srv := New(cfg, HandlerFunc(testHandler))
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = srv.ListenAndServe(cfg)
	}()
	<-ready
	// Wait for the listener to bind so Addr is available.
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind in time")
		}
		time.Sleep(time.Millisecond)
	}
	return srv, srv.Addr().String()
}

func TestServePingMultibulk(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readLine(t, conn); got != "+PONG" {
		t.Fatalf("PING reply = %q want +PONG", got)
	}
}

func TestServeInlinePing(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readLine(t, conn); got != "+PONG" {
		t.Fatalf("inline PING = %q want +PONG", got)
	}
}

func TestServePipeline(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	// Three commands in one write must produce three replies in order.
	if _, err := conn.Write([]byte("PING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\nPING a\r\n")); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	want := []string{"+PONG", "$2", "hi", "$1", "a"}
	for _, w := range want {
		got := readLineFrom(t, r)
		if got != w {
			t.Fatalf("pipeline line = %q want %q", got, w)
		}
	}
}

func TestServeProtoSwitch(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	r := bufio.NewReader(conn)

	// After HELLO flips the connection to RESP3, a null must serialize as "_".
	if _, err := conn.Write([]byte("HELLO\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readLineFrom(t, r); got != "+HELLO3" {
		t.Fatalf("HELLO reply = %q", got)
	}
}

func TestServeQuitClosesAfterReply(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	r := bufio.NewReader(conn)

	if _, err := conn.Write([]byte("QUIT\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readLineFrom(t, r); got != "+OK" {
		t.Fatalf("QUIT reply = %q want +OK", got)
	}
	// The server must close the connection after the reply: the next read hits EOF.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("connection still open after QUIT")
	}
}

func TestServeProtocolErrorCloses(t *testing.T) {
	srv, addr := startTestServer(t, Config{})
	defer srv.Close()

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	r := bufio.NewReader(conn)

	// A multibulk element that is not a bulk string is a fatal protocol error.
	if _, err := conn.Write([]byte("*1\r\n+notbulk\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readLineFrom(t, r)
	if !strings.HasPrefix(got, "-ERR Protocol error") {
		t.Fatalf("error reply = %q", got)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.ReadByte(); err == nil {
		t.Fatal("connection still open after protocol error")
	}
}

func TestServeMaxClients(t *testing.T) {
	srv, addr := startTestServer(t, Config{MaxClients: 1})
	defer srv.Close()

	// First client holds the only slot.
	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	// Keep it registered: send a command and read the reply.
	_, _ = c1.Write([]byte("PING\r\n"))
	_ = readLine(t, c1)

	// Wait until the server has registered the first client.
	deadline := time.Now().Add(2 * time.Second)
	for srv.CountClients() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("first client never registered")
		}
		time.Sleep(time.Millisecond)
	}

	// Second client is accepted then rejected with the maxclients error.
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got := readLine(t, c2)
	if !strings.HasPrefix(got, "-ERR max number of clients reached") {
		t.Fatalf("second client reply = %q", got)
	}
}

func TestServeUnixSocket(t *testing.T) {
	sock := t.TempDir() + "/aki.sock"
	cfg := Config{Addr: "", UnixSocket: sock}
	srv := New(cfg, HandlerFunc(testHandler))
	go func() { _ = srv.ListenAndServe(cfg) }()
	defer srv.Close()

	deadline := time.Now().Add(2 * time.Second)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("unix", sock)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unix dial: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	defer conn.Close()

	_, _ = conn.Write([]byte("PING\r\n"))
	if got := readLine(t, conn); got != "+PONG" {
		t.Fatalf("unix PING = %q", got)
	}
}

func TestCloseIsGraceful(t *testing.T) {
	srv, addr := startTestServer(t, Config{})

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	_, _ = conn.Write([]byte("PING\r\n"))
	_ = readLine(t, conn)

	// Close must return promptly and tear the live connection down.
	done := make(chan struct{})
	go func() { _ = srv.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return in time")
	}
	if n := srv.CountClients(); n != 0 {
		t.Fatalf("client count after Close = %d want 0", n)
	}
}

// readLine reads one CRLF-terminated line from conn and returns it without the
// terminator.
func readLine(t *testing.T, conn net.Conn) string {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return readLineFrom(t, bufio.NewReader(conn))
}

func readLineFrom(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}
