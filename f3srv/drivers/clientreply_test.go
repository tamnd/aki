package drivers

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// expectQuiet asserts the connection has no more reply bytes pending: nothing
// already buffered and nothing arriving within a short window. It is how the
// CLIENT REPLY tests prove a reply was dropped rather than merely delayed. The
// deadline is generous enough to catch a reply the writer wrongly emitted, and
// it is cleared on return so the connection stays usable.
func expectQuiet(t *testing.T, nc net.Conn, br *bufio.Reader) {
	t.Helper()
	if n := br.Buffered(); n > 0 {
		b, _ := br.Peek(n)
		t.Fatalf("expected no reply, got buffered bytes %q", b)
	}
	if err := nc.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() { _ = nc.SetReadDeadline(time.Time{}) }()
	if b, err := br.ReadByte(); err == nil {
		t.Fatalf("expected no reply, got a byte %q", string([]byte{b}))
	}
}

// TestClientReplyOff checks CLIENT REPLY OFF mutes every reply until CLIENT
// REPLY ON, that OFF and ON's own replies follow the redis contract (OFF is
// silent, ON answers +OK even though the connection was muted), and that the
// muted commands still took effect (the GET after ON reads back the value a
// muted SET wrote).
func TestClientReplyOff(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "CLIENT", "REPLY", "OFF")
	writeCmd(t, nc, "SET", "a", "1")
	writeCmd(t, nc, "SET", "b", "2")
	// Still muted: none of the three above may answer. Prove it before re-enabling.
	expectQuiet(t, nc, br)

	writeCmd(t, nc, "CLIENT", "REPLY", "ON")
	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("CLIENT REPLY ON = %v, want OK", got)
	}
	// The muted SET a really ran: its value is readable now that replies are on.
	if got := sendCmd(t, br, nc, "GET", "a"); got != "1" {
		t.Fatalf("GET a = %v, want 1 (muted SET must still take effect)", got)
	}
	if got := sendCmd(t, br, nc, "GET", "b"); got != "2" {
		t.Fatalf("GET b = %v, want 2", got)
	}
	expectQuiet(t, nc, br)
}

// TestClientReplySkip checks CLIENT REPLY SKIP mutes exactly the one command
// that follows it (and answers nothing for itself), while the command after that
// replies normally.
func TestClientReplySkip(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "CLIENT", "REPLY", "SKIP")
	writeCmd(t, nc, "SET", "s", "1") // this one's +OK is skipped
	writeCmd(t, nc, "PING")          // this one answers +PONG
	if got := readRESP(t, br); got != "PONG" {
		t.Fatalf("first reply = %v, want PONG (SKIP must drop the SET reply, not the PING)", got)
	}
	expectQuiet(t, nc, br)

	// The skipped SET still ran.
	if got := sendCmd(t, br, nc, "GET", "s"); got != "1" {
		t.Fatalf("GET s = %v, want 1 (skipped SET must still take effect)", got)
	}
}

// TestClientReplySkipOnlyOne checks SKIP is a one-shot: it mutes a single
// following command, and the command after that is not affected.
func TestClientReplySkipOnlyOne(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "CLIENT", "REPLY", "SKIP")
	writeCmd(t, nc, "SET", "x", "1") // skipped
	writeCmd(t, nc, "SET", "y", "2") // not skipped, answers +OK
	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("reply after the skipped command = %v, want OK", got)
	}
	expectQuiet(t, nc, br)
}

// TestClientReplyOnWhileOn checks CLIENT REPLY ON on a connection that is not
// muted still answers +OK, the redis idempotent-switch behaviour.
func TestClientReplyOnWhileOn(t *testing.T) {
	_, nc, br := startServer(t)
	if got := sendCmd(t, br, nc, "CLIENT", "REPLY", "ON"); got != "OK" {
		t.Fatalf("CLIENT REPLY ON (already on) = %v, want OK", got)
	}
}

// TestClientReplyBadArg checks a bad CLIENT REPLY option is the syntax error
// redis gives, and a missing option is the arity error.
func TestClientReplyBadArg(t *testing.T) {
	_, nc, br := startServer(t)
	if _, ok := sendCmd(t, br, nc, "CLIENT", "REPLY", "MAYBE").(errorReply); !ok {
		t.Fatalf("CLIENT REPLY MAYBE did not error")
	}
	if _, ok := sendCmd(t, br, nc, "CLIENT", "REPLY").(errorReply); !ok {
		t.Fatalf("CLIENT REPLY with no option did not error")
	}
}

// TestClientReplyOffPipeline drives the whole sequence as one pipelined write,
// the ordering crux: a single batch carries OFF, several muted commands, ON, and
// a trailing GET, and the drain must emit only ON's +OK and the GET's value in
// order. This exercises the reader stamping each command's suppression at
// dispatch time rather than at drain time, when ON has already re-enabled.
func TestClientReplyOffPipeline(t *testing.T) {
	_, nc, br := startServer(t)

	var pipe []byte
	add := func(args ...string) {
		pipe = append(pipe, '*')
		pipe = appendInt(pipe, len(args))
		pipe = append(pipe, '\r', '\n')
		for _, a := range args {
			pipe = append(pipe, '$')
			pipe = appendInt(pipe, len(a))
			pipe = append(pipe, '\r', '\n')
			pipe = append(pipe, a...)
			pipe = append(pipe, '\r', '\n')
		}
	}
	add("CLIENT", "REPLY", "OFF")
	add("SET", "p", "42")
	add("INCR", "n")
	add("CLIENT", "REPLY", "ON")
	add("GET", "p")
	if _, err := nc.Write(pipe); err != nil {
		t.Fatalf("write pipeline: %v", err)
	}

	if got := readRESP(t, br); got != "OK" {
		t.Fatalf("pipeline reply 1 = %v, want OK (from CLIENT REPLY ON)", got)
	}
	if got := readRESP(t, br); got != "42" {
		t.Fatalf("pipeline reply 2 = %v, want 42 (from GET p)", got)
	}
	expectQuiet(t, nc, br)
}
