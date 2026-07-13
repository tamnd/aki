package drivers

import (
	"bufio"
	"strconv"
	"strings"
	"testing"
)

// readReply reads one whole top-level RESP reply and returns it as a string. It
// understands the shapes PFDEBUG uses: simple string, error, integer, bulk, and
// a flat array of integers (it consumes exactly the declared element count).
func readReply(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return line
		}
		body := make([]byte, n+2)
		if _, err := readFull(br, body); err != nil {
			t.Fatalf("read bulk body: %v", err)
		}
		return "$" + string(body[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n; i++ {
			readReply(t, br)
		}
		return "*" + strconv.Itoa(n)
	}
	t.Fatalf("unexpected reply %q", line)
	return ""
}

func readFull(br *bufio.Reader, b []byte) (int, error) {
	for n := 0; n < len(b); {
		m, err := br.Read(b[n:])
		if err != nil {
			return n, err
		}
		n += m
	}
	return len(b), nil
}

// TestPFDebugEncodingAndToDense walks ENCODING across the sparse-to-dense
// boundary and TODENSE forcing the promotion, over a socket.
func TestPFDebugEncodingAndToDense(t *testing.T) {
	_, nc, br := startServer(t)

	// A small sketch stays sparse.
	send(t, nc, "PFADD", "h", "a", "b", "c")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFDEBUG", "ENCODING", "h")
	expect(t, br, "+sparse\r\n")

	// TODENSE forces it dense and reports OK; ENCODING then reads dense.
	send(t, nc, "PFDEBUG", "TODENSE", "h")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFDEBUG", "ENCODING", "h")
	expect(t, br, "+dense\r\n")

	// Still a string sketch, and its count survives the promotion.
	send(t, nc, "TYPE", "h")
	expect(t, br, "+string\r\n")
	send(t, nc, "PFCOUNT", "h")
	expect(t, br, ":3\r\n")

	// TODENSE on an already dense sketch is an idempotent OK.
	send(t, nc, "PFDEBUG", "TODENSE", "h")
	expect(t, br, "+OK\r\n")
}

// TestPFDebugGetReg checks GETREG returns 16384 register values and promotes a
// sparse sketch to dense as a side effect, the Redis behavior.
func TestPFDebugGetReg(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "PFADD", "h", "x", "y")
	expect(t, br, ":1\r\n")

	// The reply is an array of exactly HLL_REGISTERS integers.
	send(t, nc, "PFDEBUG", "GETREG", "h")
	if got := readReply(t, br); got != "*16384" {
		t.Fatalf("GETREG reply = %s, want *16384", got)
	}

	// GETREG converted the sketch to dense.
	send(t, nc, "PFDEBUG", "ENCODING", "h")
	expect(t, br, "+dense\r\n")
}

// TestPFDebugDecode checks DECODE returns the sparse opcode stream as a bulk
// string and refuses a dense sketch.
func TestPFDebugDecode(t *testing.T) {
	_, nc, br := startServer(t)

	// A fresh sketch decodes to a single XZERO covering all registers.
	send(t, nc, "PFADD", "fresh")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFDEBUG", "DECODE", "fresh")
	if got := readReply(t, br); got != "$Z:16384" {
		t.Fatalf("DECODE fresh = %s, want $Z:16384", got)
	}

	// After a few adds the stream carries VAL and ZERO/XZERO tokens; assert it is
	// a non-empty bulk of space-joined tokens.
	send(t, nc, "PFADD", "h", "a", "b", "c")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFDEBUG", "DECODE", "h")
	got := readReply(t, br)
	if !strings.HasPrefix(got, "$") || len(got) < 2 {
		t.Fatalf("DECODE reply not a bulk string: %s", got)
	}
	for _, tok := range strings.Fields(got[1:]) {
		if tok[0] != 'z' && tok[0] != 'Z' && tok[0] != 'v' {
			t.Fatalf("DECODE token %q has unexpected opcode tag", tok)
		}
	}

	// DECODE on a dense sketch is an error.
	send(t, nc, "PFDEBUG", "TODENSE", "h")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFDEBUG", "DECODE", "h")
	expect(t, br, "-ERR HLL encoding is not sparse\r\n")
}

// TestPFDebugCorners covers the missing-key error, the WRONGTYPE refusal, and an
// unknown subcommand.
func TestPFDebugCorners(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key is an error, not an empty sketch.
	send(t, nc, "PFDEBUG", "ENCODING", "nope")
	expect(t, br, "-ERR The specified key does not exist\r\n")

	// A plain string is refused as not a valid HLL.
	send(t, nc, "SET", "s", "plain")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFDEBUG", "ENCODING", "s")
	expect(t, br, "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n")

	// An unknown subcommand is refused.
	send(t, nc, "PFADD", "h", "a")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFDEBUG", "BOGUS", "h")
	expect(t, br, "-ERR unknown PFDEBUG subcommand or wrong number of arguments\r\n")
}

// TestPFSelfTest runs the estimator-and-encoding self check over the wire.
func TestPFSelfTest(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "PFSELFTEST")
	expect(t, br, "+OK\r\n")
}
