package drivers

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// The blocking verbs at the driver seam (spec 2064/f3/13 M3 slice 8). These
// prove the reader barrier end to end over a real socket: a BLPOP that parks is
// served by a push on another connection, and a command pipelined behind an
// unresolved BLPOP on the same connection does not run until the block clears.
// They run on every shape and event loop the suite overrides select, so the
// handleSingle drain, the handlePair poll, and the reactor/uring throttle folds
// are all covered.

// secondConn dials another client to the running server, for the two-connection
// serve.
func secondConn(t *testing.T, srv *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc, bufio.NewReader(nc)
}

func writeCmd(t *testing.T, nc net.Conn, args ...string) {
	t.Helper()
	var b []byte
	b = append(b, '*')
	b = appendInt(b, len(args))
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = appendInt(b, len(a))
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	if _, err := nc.Write(b); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
}

func appendInt(b []byte, n int) []byte {
	if n == 0 {
		return append(b, '0')
	}
	var tmp [20]byte
	i := len(tmp)
	for n > 0 {
		i--
		tmp[i] = byte('0' + n%10)
		n /= 10
	}
	return append(b, tmp[i:]...)
}

// TestBlpopServedAcrossConns blocks one connection on a missing key and serves
// it with a push on another. The push reply reports the length before the serve,
// and the blocked client receives the two-element array. The assertion holds
// whether the BLPOP parks first or the push lands first, so it is stable without
// depending on the scheduler; the sleep just steers the common case through the
// park-and-wake path.
func TestBlpopServedAcrossConns(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	writeCmd(t, c1, "BLPOP", "bk", "0")
	time.Sleep(50 * time.Millisecond) // let the BLPOP park

	writeCmd(t, c2, "RPUSH", "bk", "hello")
	expect(t, br2, ":1\r\n")
	expect(t, br1, "*2\r\n$2\r\nbk\r\n$5\r\nhello\r\n")
}

// TestBlpopPipelineBarrier pins the reader barrier: a PING pipelined behind a
// parked BLPOP on the same connection is held, not answered, until the BLPOP is
// served, and then both replies arrive in request order.
func TestBlpopPipelineBarrier(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	// BLPOP bk 0 then PING, pipelined in one write.
	writeCmd(t, c1, "BLPOP", "bk", "0")
	writeCmd(t, c1, "PING")

	// The PING must not be answered while the BLPOP blocks.
	c1.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := br1.ReadByte(); err == nil {
		t.Fatal("a reply arrived before the BLPOP was served")
	}
	c1.SetReadDeadline(time.Time{})

	// Serve the BLPOP from the other connection; now both replies land in order.
	writeCmd(t, c2, "RPUSH", "bk", "v")
	expect(t, br2, ":1\r\n")
	expect(t, br1, "*2\r\n$2\r\nbk\r\n$1\r\nv\r\n+PONG\r\n")
}
