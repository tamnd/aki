package f1srv

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
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

// bcmd buffers a RESP multibulk command into the writer without flushing, for the send side of
// pipeDrain. bufio streams the buffer to the socket on its own as it fills, so a large pipeline goes
// out incrementally rather than in one final flush.
func bcmd(rw *bufio.ReadWriter, args ...string) {
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
}

// pipeDrain writes n commands with send while concurrently reading their n replies with read, so the
// reply stream drains as it is produced. A send-all-then-read-all loop piles the whole reply pipeline
// into the kernel socket buffers and deadlocks once it outgrows them: the server blocks flushing
// replies the client is not yet reading, so it stops reading requests, so the client blocks sending
// the rest of the batch, and neither side moves. A real pipelining client reads replies as it streams
// requests, and so does this, which keeps the beyond-arena tests deadlock-free for any batch size on
// any runner. send runs on a separate goroutine and must only write (never call t.Fatalf, which is
// illegal off the test goroutine); read runs here and may fail the test.
func pipeDrain(t *testing.T, rw *bufio.ReadWriter, n int, send func(i int), read func(i int)) {
	t.Helper()
	werr := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			send(i)
		}
		werr <- rw.Flush()
	}()
	for i := 0; i < n; i++ {
		read(i)
	}
	if err := <-werr; err != nil {
		t.Fatalf("pipeline write: %v", err)
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
	// The TTL is real now: a key with a 100s expiry reads 100, not -1.
	cmd(t, rw, "TTL", "k")
	expect(t, rw, ":100")
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

// dialColdServer starts a server with the larger-than-memory cold tier engaged, its
// value log in the test's temp dir and a low threshold so a modest value separates.
func dialColdServer(t *testing.T, threshold int) (*bufio.ReadWriter, func()) {
	t.Helper()
	cfg := Config{
		Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20,
		ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: os.Getenv("F1SRV_TEST_NET"),
		ColdPath: filepath.Join(t.TempDir(), "cold.vlog"), SepThreshold: threshold,
	}
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
	return rw, func() { conn.Close(); srv.Close() }
}

// TestColdTierWirePath drives the larger-than-memory tier end to end over RESP: a
// value past the threshold separates to the cold log and reads back byte-identical,
// while a small value stays inline. Both must be transparent to the client.
func TestColdTierWirePath(t *testing.T) {
	rw, cleanup := dialColdServer(t, 64)
	defer cleanup()

	small := "tiny"
	cmd(t, rw, "SET", "s", small)
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "s")
	expect(t, rw, "$"+small)

	big := strings.Repeat("Z", 4096)
	cmd(t, rw, "SET", "b", big)
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$"+big)

	// Overwrite the separated key with a small value: it must flip inline and read back.
	cmd(t, rw, "SET", "b", small)
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$"+small)

	// A large value under a fresh key, then delete, then absent.
	cmd(t, rw, "SET", "c", big)
	expect(t, rw, "+OK")
	cmd(t, rw, "DEL", "c")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "c")
	expect(t, rw, "$-1")
}

// TestStringValueOps covers the raw string-value operators STRLEN, APPEND, GETRANGE, SUBSTR,
// and SETRANGE. Every reply was captured from live Redis 8.8.0.
func TestStringValueOps(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// STRLEN: length, 0 for missing, WRONGTYPE for a non-string.
	cmd(t, rw, "SET", "s", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "STRLEN", "s")
	expect(t, rw, ":5")
	cmd(t, rw, "STRLEN", "missing")
	expect(t, rw, ":0")
	cmd(t, rw, "RPUSH", "lst", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "STRLEN", "lst")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "STRLEN")
	expect(t, rw, "-ERR wrong number of arguments for 'strlen' command")

	// APPEND: creates then grows, keeps a set TTL, WRONGTYPE on a non-string.
	cmd(t, rw, "APPEND", "a", "hello")
	expect(t, rw, ":5")
	cmd(t, rw, "APPEND", "a", "world")
	expect(t, rw, ":10")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$helloworld")
	cmd(t, rw, "EXPIRE", "a", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "APPEND", "a", "!")
	expect(t, rw, ":11")
	cmd(t, rw, "TTL", "a")
	expect(t, rw, ":100")
	cmd(t, rw, "APPEND", "lst", "x")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "APPEND", "a")
	expect(t, rw, "-ERR wrong number of arguments for 'append' command")

	// GETRANGE and its SUBSTR alias, with the negative-index and out-of-range clamping.
	cmd(t, rw, "SET", "g", "Hello World")
	expect(t, rw, "+OK")
	cmd(t, rw, "GETRANGE", "g", "0", "4")
	expect(t, rw, "$Hello")
	cmd(t, rw, "GETRANGE", "g", "-5", "-1")
	expect(t, rw, "$World")
	cmd(t, rw, "GETRANGE", "g", "0", "-1")
	expect(t, rw, "$Hello World")
	cmd(t, rw, "GETRANGE", "g", "100", "200")
	expect(t, rw, "$")
	cmd(t, rw, "GETRANGE", "g", "-100", "-200")
	expect(t, rw, "$")
	cmd(t, rw, "GETRANGE", "g", "-100", "-11")
	expect(t, rw, "$H")
	cmd(t, rw, "GETRANGE", "g", "0", "-100")
	expect(t, rw, "$H")
	cmd(t, rw, "GETRANGE", "missing", "0", "-1")
	expect(t, rw, "$")
	cmd(t, rw, "SUBSTR", "g", "0", "4")
	expect(t, rw, "$Hello")
	cmd(t, rw, "GETRANGE", "lst", "0", "1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "GETRANGE", "g", "0")
	expect(t, rw, "-ERR wrong number of arguments for 'getrange' command")
	cmd(t, rw, "SUBSTR")
	expect(t, rw, "-ERR wrong number of arguments for 'substr' command")

	// SETRANGE: pad-with-zero on a fresh key, overwrite in place, empty-value no-op,
	// negative offset error, keeps a set TTL, WRONGTYPE on a non-string.
	cmd(t, rw, "SETRANGE", "sr", "5", "hello")
	expect(t, rw, ":10")
	cmd(t, rw, "GET", "sr")
	expect(t, rw, "$\x00\x00\x00\x00\x00hello")
	cmd(t, rw, "SET", "sr2", "Hello World")
	expect(t, rw, "+OK")
	cmd(t, rw, "SETRANGE", "sr2", "6", "Redis")
	expect(t, rw, ":11")
	cmd(t, rw, "GET", "sr2")
	expect(t, rw, "$Hello Redis")
	cmd(t, rw, "SETRANGE", "empty", "0", "")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "empty")
	expect(t, rw, ":0")
	cmd(t, rw, "SETRANGE", "sr2", "-1", "x")
	expect(t, rw, "-ERR offset is out of range")
	cmd(t, rw, "EXPIRE", "sr2", "100")
	expect(t, rw, ":1")
	cmd(t, rw, "SETRANGE", "sr2", "0", "J")
	expect(t, rw, ":11")
	cmd(t, rw, "TTL", "sr2")
	expect(t, rw, ":100")
	cmd(t, rw, "SETRANGE", "lst", "0", "x")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "SETRANGE", "sr2")
	expect(t, rw, "-ERR wrong number of arguments for 'setrange' command")
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
