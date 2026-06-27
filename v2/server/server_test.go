package server

import (
	"bufio"
	"net"
	"testing"

	"github.com/tamnd/aki/v2/store"
)

func dialServer(t *testing.T) (*Server, net.Conn) {
	t.Helper()
	s, err := store.New(store.DefaultTunables())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	srv := New(s)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.ln = ln
	go srv.serve()
	t.Cleanup(func() { srv.Close(); s.Close() })
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return srv, c
}

func send(t *testing.T, c net.Conn, parts ...string) {
	t.Helper()
	w := bufio.NewWriter(c)
	w.WriteByte('*')
	w.WriteString(itoa(len(parts)))
	w.WriteString("\r\n")
	for _, p := range parts {
		w.WriteByte('$')
		w.WriteString(itoa(len(p)))
		w.WriteString("\r\n")
		w.WriteString(p)
		w.WriteString("\r\n")
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func readReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	switch line[0] {
	case '+', '-', ':':
		return line[:len(line)-2]
	case '$':
		if line[1] == '-' {
			return "(nil)"
		}
		n := 0
		for _, c := range line[1 : len(line)-2] {
			n = n*10 + int(c-'0')
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	}
	t.Fatalf("unexpected reply: %q", line)
	return ""
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := r.Read(buf[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

func TestServerSetGetPing(t *testing.T) {
	_, c := dialServer(t)
	r := bufio.NewReader(c)

	send(t, c, "PING")
	if got := readReply(t, r); got != "+PONG" {
		t.Fatalf("PING = %q", got)
	}
	send(t, c, "SET", "foo", "bar")
	if got := readReply(t, r); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	send(t, c, "GET", "foo")
	if got := readReply(t, r); got != "bar" {
		t.Fatalf("GET = %q", got)
	}
	send(t, c, "GET", "missing")
	if got := readReply(t, r); got != "(nil)" {
		t.Fatalf("GET missing = %q", got)
	}
}

func TestServerPipeline(t *testing.T) {
	_, c := dialServer(t)
	r := bufio.NewReader(c)
	// Send three commands back to back, then read three replies.
	send(t, c, "SET", "k1", "v1")
	send(t, c, "SET", "k2", "v2")
	send(t, c, "GET", "k1")
	if got := readReply(t, r); got != "+OK" {
		t.Fatalf("reply 1 = %q", got)
	}
	if got := readReply(t, r); got != "+OK" {
		t.Fatalf("reply 2 = %q", got)
	}
	if got := readReply(t, r); got != "v1" {
		t.Fatalf("reply 3 = %q", got)
	}
}
