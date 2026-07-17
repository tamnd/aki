package drivers

import (
	"bufio"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// testConnShape is the suite-wide shape override: AKI_CONN_SHAPE=pair reruns
// every driver test on the pair shape, which is how CI and the lab 15 A/B
// cover both shapes without doubling the test code. Empty means the single
// default.
func testConnShape() string { return os.Getenv("AKI_CONN_SHAPE") }

// testNetDriver is the same suite-wide override for the network driver:
// AKI_NET=reactor or AKI_NET=uring reruns every driver test on that event
// loop, which is how the ubuntu CI legs cover them without doubling the test
// code. Empty means the goroutine default.
func testNetDriver() string { return os.Getenv("AKI_NET") }

// wantNetDriver is the driver a server built with testNetDriver should
// report: the requested event loop where it exists (for uring, where the
// kernel probe also passes), the logged goroutine fallback everywhere else.
func wantNetDriver() string {
	switch testNetDriver() {
	case NetReactor:
		if runtime.GOOS == "linux" {
			return NetReactor
		}
	case NetURing:
		if runtime.GOOS == "linux" && uringAvailable() {
			return NetURing
		}
	}
	return NetGoroutine
}

// wantEventLoop reports whether the suite is running on an event-loop driver
// (reactor or uring); the tests that pin loop behaviors (wake batching,
// stream stepping under a shared loop, the mid-cycle output cap) gate on it.
func wantEventLoop() bool {
	d := wantNetDriver()
	return d == NetReactor || d == NetURing
}

func startServer(t *testing.T) (*Server, net.Conn, *bufio.Reader) {
	t.Helper()
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18, ConnShape: testConnShape(), NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Close()
	})
	return srv, nc, bufio.NewReader(nc)
}

func expect(t *testing.T, br *bufio.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	for n := 0; n < len(got); {
		m, err := br.Read(got[n:])
		if err != nil {
			t.Fatalf("read after %q: %v", got[:n], err)
		}
		n += m
	}
	if string(got) != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

// TestSmokePingEcho round-trips PING and ECHO over a raw socket, in both the
// inline and the array RESP forms, which is the M0 smoke contract.
func TestSmokePingEcho(t *testing.T) {
	_, nc, br := startServer(t)

	if _, err := nc.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+PONG\r\n")

	if _, err := nc.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+PONG\r\n")

	if _, err := nc.Write([]byte("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "$5\r\nhello\r\n")

	if _, err := nc.Write([]byte("ECHO inline\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "$6\r\ninline\r\n")

	if _, err := nc.Write([]byte("*2\r\n$4\r\nPING\r\n$3\r\nmsg\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "$3\r\nmsg\r\n")
}

// TestTime round-trips TIME: a two element array whose first bulk is a plausible
// unix second and whose second is the microseconds within that second, and whose
// argument tail is rejected for arity.
func TestTime(t *testing.T) {
	_, nc, br := startServer(t)

	if _, err := nc.Write([]byte("*1\r\n$4\r\nTIME\r\n")); err != nil {
		t.Fatal(err)
	}
	els := readArrayBulks(t, br, 2)
	secs, err := strconv.Atoi(els[0])
	if err != nil {
		t.Fatalf("seconds %q not an integer: %v", els[0], err)
	}
	// Any run of this test is well after 2020, so the clock is at least this far.
	if secs < 1577836800 {
		t.Fatalf("seconds %d implausibly early", secs)
	}
	micros, err := strconv.Atoi(els[1])
	if err != nil {
		t.Fatalf("microseconds %q not an integer: %v", els[1], err)
	}
	if micros < 0 || micros > 999999 {
		t.Fatalf("microseconds %d out of range", micros)
	}

	if _, err := nc.Write([]byte("*2\r\n$4\r\nTIME\r\n$1\r\nx\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "-ERR wrong number of arguments for 'time' command\r\n")
}

// readArrayBulks reads a RESP array header of exactly n elements and returns each
// bulk element's payload as a string.
func readArrayBulks(t *testing.T, br *bufio.Reader, n int) []string {
	t.Helper()
	head, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read array header: %v", err)
	}
	if want := "*" + strconv.Itoa(n) + "\r\n"; head != want {
		t.Fatalf("array header = %q, want %q", head, want)
	}
	out := make([]string, n)
	for i := range out {
		hdr, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read bulk header: %v", err)
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			t.Fatalf("bulk header = %q", hdr)
		}
		blen, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
		if err != nil {
			t.Fatalf("bulk length %q: %v", hdr, err)
		}
		buf := make([]byte, blen+2) // payload plus CRLF
		if _, err := io.ReadFull(br, buf); err != nil {
			t.Fatalf("read bulk payload: %v", err)
		}
		out[i] = string(buf[:blen])
	}
	return out
}

// TestSelect checks the single-keyspace SELECT: index 0 succeeds, any other index
// is out of range, and a non-integer index is refused.
func TestSelect(t *testing.T) {
	_, nc, br := startServer(t)

	if _, err := nc.Write([]byte("*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n")

	if _, err := nc.Write([]byte("*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "-ERR DB index is out of range\r\n")

	if _, err := nc.Write([]byte("*2\r\n$6\r\nSELECT\r\n$3\r\nabc\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
}

// TestLolwut checks LOLWUT answers a non-empty bulk both bare and with the
// optional VERSION tail, which it accepts and ignores rather than erroring.
func TestLolwut(t *testing.T) {
	_, nc, br := startServer(t)

	if _, err := nc.Write([]byte("*1\r\n$6\r\nLOLWUT\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readBulk(t, br); len(got) == 0 {
		t.Fatalf("LOLWUT bulk = %q, want non-empty", got)
	}

	if _, err := nc.Write([]byte("*3\r\n$6\r\nLOLWUT\r\n$7\r\nVERSION\r\n$1\r\n5\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readBulk(t, br); len(got) == 0 {
		t.Fatalf("LOLWUT VERSION 5 bulk = %q, want non-empty", got)
	}
}

// readBulk reads a single RESP bulk string reply and returns its payload.
func readBulk(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	hdr, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk header: %v", err)
	}
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("bulk header = %q", hdr)
	}
	blen, err := strconv.Atoi(strings.TrimSuffix(hdr[1:], "\r\n"))
	if err != nil {
		t.Fatalf("bulk length %q: %v", hdr, err)
	}
	buf := make([]byte, blen+2) // payload plus CRLF
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return string(buf[:blen])
}

// TestSmokePipeline sends one write with several commands and expects the
// replies back in request order.
func TestSmokePipeline(t *testing.T) {
	_, nc, br := startServer(t)

	req := "PING\r\n*2\r\n$4\r\nECHO\r\n$1\r\na\r\nNOPE\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$1\r\nb\r\n"
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+PONG\r\n$1\r\na\r\n-ERR unknown command 'NOPE'\r\n+PONG\r\n$1\r\nb\r\n")
}

// TestSmokeUnknownAndArity checks parse-side errors still answer in order.
func TestSmokeUnknownAndArity(t *testing.T) {
	_, nc, br := startServer(t)

	if _, err := nc.Write([]byte("*1\r\n$4\r\nECHO\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "-ERR wrong number of arguments for 'echo' command\r\n+PONG\r\n")
}
