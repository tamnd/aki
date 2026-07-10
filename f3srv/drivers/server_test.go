package drivers

import (
	"bufio"
	"net"
	"os"
	"runtime"
	"testing"
)

// testConnShape is the suite-wide shape override: AKI_CONN_SHAPE=pair reruns
// every driver test on the pair shape, which is how CI and the lab 15 A/B
// cover both shapes without doubling the test code. Empty means the single
// default.
func testConnShape() string { return os.Getenv("AKI_CONN_SHAPE") }

// testNetDriver is the same suite-wide override for the network driver:
// AKI_NET=reactor reruns every driver test on the epoll reactor, which is how
// the ubuntu CI leg covers it without doubling the test code. Empty means the
// goroutine default.
func testNetDriver() string { return os.Getenv("AKI_NET") }

// wantNetDriver is the driver a server built with testNetDriver should
// report: the reactor where it exists, the logged goroutine fallback
// everywhere else.
func wantNetDriver() string {
	if testNetDriver() == NetReactor && runtime.GOOS == "linux" {
		return NetReactor
	}
	return NetGoroutine
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
