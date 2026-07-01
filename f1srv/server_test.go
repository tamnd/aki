package f1srv

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"
)

// dialTestServer starts a server on an ephemeral port and returns a connected
// buffered reader/writer plus a cleanup. The store is sized small; the tests only
// exercise correctness, not scale. Setting F1SRV_TEST_NET=reactor runs the whole suite
// through the epoll driver on Linux, so the reactor's net path is covered by the same
// command tests as the goroutine path with one environment switch.
func dialTestServer(t *testing.T) (*bufio.ReadWriter, func()) {
	t.Helper()
	return dialTestServerMode(t, os.Getenv("F1SRV_TEST_NET"))
}

// dialTestServerMode is dialTestServer with an explicit net mode, so a Linux-only test
// can force the reactor regardless of the environment.
func dialTestServerMode(t *testing.T, netMode string) (*bufio.ReadWriter, func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: netMode}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	cleanup := func() {
		conn.Close()
		srv.Close()
	}
	return rw, cleanup
}

// cmd writes a RESP multibulk command and flushes it.
func cmd(t *testing.T, rw *bufio.ReadWriter, args ...string) {
	t.Helper()
	rw.WriteByte('*')
	rw.WriteString(itoa(len(args)))
	rw.WriteString("\r\n")
	for _, a := range args {
		rw.WriteByte('$')
		rw.WriteString(itoa(len(a)))
		rw.WriteString("\r\n")
		rw.WriteString(a)
		rw.WriteString("\r\n")
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
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

// readReply reads one full RESP reply and returns it as a normalized string:
// "+OK", "-ERR ...", ":N", "$<bytes>", or "$-1" for nil.
func readReply(t *testing.T, rw *bufio.ReadWriter) string {
	t.Helper()
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line = line[:len(line)-2] // strip CRLF
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		if line == "$-1" {
			return "$-1"
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rw, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return "$" + string(buf[:n])
	case '*':
		return line // caller reads elements itself
	}
	t.Fatalf("bad reply: %q", line)
	return ""
}

func readFull(rw *bufio.ReadWriter, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := rw.Read(buf[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

func expect(t *testing.T, rw *bufio.ReadWriter, want string) {
	t.Helper()
	if got := readReply(t, rw); got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestStringPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "PING")
	expect(t, rw, "+PONG")

	cmd(t, rw, "SET", "k", "v1")
	expect(t, rw, "+OK")

	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v1")

	cmd(t, rw, "GET", "missing")
	expect(t, rw, "$-1")

	cmd(t, rw, "SET", "k", "v2")
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v2")

	cmd(t, rw, "EXISTS", "k", "missing")
	expect(t, rw, ":1")

	cmd(t, rw, "DEL", "k")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$-1")
}

func TestIncrFamily(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "INCR", "n")
	expect(t, rw, ":1")
	cmd(t, rw, "INCR", "n")
	expect(t, rw, ":2")
	cmd(t, rw, "INCRBY", "n", "10")
	expect(t, rw, ":12")
	cmd(t, rw, "DECR", "n")
	expect(t, rw, ":11")
	cmd(t, rw, "DECRBY", "n", "5")
	expect(t, rw, ":6")

	cmd(t, rw, "SET", "s", "notanint")
	expect(t, rw, "+OK")
	cmd(t, rw, "INCR", "s")
	expect(t, rw, "-ERR value is not an integer or out of range")
}

func TestExpireSmokeShape(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXPIRE", "k", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "EXPIRE", "missing", "100")
	expect(t, rw, ":0")
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":-1")
	cmd(t, rw, "TTL", "missing")
	expect(t, rw, ":-2")
}

func TestMSetMGetAndFlush(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "a", "1", "b", "2", "c", "3")
	expect(t, rw, "+OK")

	cmd(t, rw, "MGET", "a", "b", "x", "c")
	// *4 header then bulks: $1,$2,$-1,$3
	expect(t, rw, "*4")
	expect(t, rw, "$1")
	expect(t, rw, "$2")
	expect(t, rw, "$-1")
	expect(t, rw, "$3")

	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":3")
	cmd(t, rw, "FLUSHALL")
	expect(t, rw, "+OK")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":0")
}

func TestPipeline(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Three commands in one write, three replies read back in order.
	cmd(t, rw, "SET", "p", "1")
	cmd(t, rw, "INCR", "p")
	cmd(t, rw, "GET", "p")
	expect(t, rw, "+OK")
	expect(t, rw, ":2")
	expect(t, rw, "$2")
}
