package sqlo1b

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/tamnd/aki/engine/sqlo1"
)

// TestServerFlushDurability pins the clean-shutdown contract over the
// real file store: every write acked by the server survives listener
// close, Server.Flush, store close, and a cold reopen. The load is a
// stream long enough to cross the flat fence cap and at least one
// fresh fence page mint, because that path flushes pages before the
// root that references them; without the final Flush a polite exit
// would keep only the prefix up to that barrier, which is exactly the
// undercount the xcatchup lab's replay oracle caught.
func TestServerFlushDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "flush.aki")
	const n = 10000
	pad := make([]byte, 64)
	for i := range pad {
		pad[i] = 'x'
	}

	db, err := CreateStore(path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	srv, l, done := flushTestServe(t, db)
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	w, r := bufio.NewWriter(c), bufio.NewReader(c)
	pending := 0
	for range n {
		flushTestCmd(t, w, "XADD", "s", "*", "f", string(pad))
		if pending++; pending == 256 {
			flushTestDrain(t, w, r, pending)
			pending = 0
		}
	}
	flushTestDrain(t, w, r, pending)
	if got := flushTestInt(t, w, r, "XLEN", "s"); got != n {
		t.Fatalf("live XLEN = %d, want %d", got, n)
	}
	c.Close()
	l.Close()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if err := srv.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = OpenStore(path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, l, done = flushTestServe(t, db)
	c, err = net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	w, r = bufio.NewWriter(c), bufio.NewReader(c)
	if got := flushTestInt(t, w, r, "XLEN", "s"); got != n {
		t.Fatalf("reopened XLEN = %d, want %d", got, n)
	}
	flushTestCmd(t, w, "XRANGE", "s", "-", "+", "COUNT", strconv.Itoa(n))
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	kind, line, cnt := flushTestReply(t, r)
	if kind != '*' || cnt != n {
		t.Fatalf("reopened XRANGE = %c %q %d entries, want %d", kind, line, cnt, n)
	}
	c.Close()
	l.Close()
	if err := <-done; err != nil {
		t.Fatalf("serve after reopen: %v", err)
	}
}

// flushTestServe starts a server for db on an ephemeral port.
func flushTestServe(t *testing.T, db *Store) (*sqlo1.Server, net.Listener, chan error) {
	t.Helper()
	srv, err := sqlo1.NewServer(db)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	return srv, l, done
}

// flushTestCmd buffers one command in RESP array form.
func flushTestCmd(t *testing.T, w *bufio.Writer, args ...string) {
	t.Helper()
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a)
	}
}

// flushTestReply reads one reply, failing the test on an error reply,
// and returns its type byte, first line, and array element count.
func flushTestReply(t *testing.T, r *bufio.Reader) (byte, string, int) {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if len(line) < 3 {
		t.Fatalf("short reply line %q", line)
	}
	kind, body := line[0], line[1:len(line)-2]
	switch kind {
	case '-':
		t.Fatalf("error reply: %s", body)
	case '+', ':':
	case '$':
		m, err := strconv.Atoi(body)
		if err != nil {
			t.Fatal(err)
		}
		if m >= 0 {
			buf := make([]byte, m+2)
			for got := 0; got < len(buf); {
				k, err := r.Read(buf[got:])
				if err != nil {
					t.Fatal(err)
				}
				got += k
			}
		}
	case '*':
		m, err := strconv.Atoi(body)
		if err != nil {
			t.Fatal(err)
		}
		for range m {
			flushTestReply(t, r)
		}
		return kind, body, m
	default:
		t.Fatalf("unknown reply type %q", line)
	}
	return kind, body, 0
}

// flushTestDrain flushes the buffered commands and reads that many
// replies.
func flushTestDrain(t *testing.T, w *bufio.Writer, r *bufio.Reader, k int) {
	t.Helper()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for range k {
		flushTestReply(t, r)
	}
}

// flushTestInt runs one command and returns its integer reply.
func flushTestInt(t *testing.T, w *bufio.Writer, r *bufio.Reader, args ...string) int {
	t.Helper()
	flushTestCmd(t, w, args...)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	kind, body, _ := flushTestReply(t, r)
	if kind != ':' {
		t.Fatalf("%s: reply %c %q, want integer", args[0], kind, body)
	}
	v, err := strconv.Atoi(body)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
