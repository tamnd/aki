package drivers

import (
	"net"
	"testing"
	"time"
)

// The WAIT and WAITAOF suite pins the doc 04 section 3.3 barriers over a
// socket. Every reply shape, parse order, and error text below was probed
// against a real redis-server 8.8.0 first, including the appendonly-on runs
// that fixed the achieved-local honesty and the argument check order.

// TestWaitReplies covers WAIT on a plain server: the standby count is zero
// this generation, so an ask at or under zero answers 0 immediately and a
// positive ask waits out its timeout and answers the achieved count of 0.
func TestWaitReplies(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "WAIT", "0", "0")
	expect(t, br, ":0\r\n")
	send(t, nc, "WAIT", "-1", "100")
	expect(t, br, ":0\r\n")
	send(t, nc, "WAIT", "0", "100")
	expect(t, br, ":0\r\n")

	send(t, nc, "WAIT", "abc", "100")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "WAIT", "1", "abc")
	expect(t, br, "-ERR timeout is not an integer or out of range\r\n")
	send(t, nc, "WAIT", "1", "-5")
	expect(t, br, "-ERR timeout is negative\r\n")
	send(t, nc, "WAIT", "1")
	expect(t, br, "-ERR wrong number of arguments for 'wait' command\r\n")
	send(t, nc, "WAIT", "1", "2", "3")
	expect(t, br, "-ERR wrong number of arguments for 'wait' command\r\n")

	// A positive ask parks until the timer delivers the achieved 0.
	start := time.Now()
	send(t, nc, "WAIT", "1", "150")
	expect(t, br, ":0\r\n")
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("WAIT 1 150 answered after %v, want the full timeout", elapsed)
	}

	// The park holds the reader barrier: a pipelined PING answers after
	// the WAIT's timer, in order.
	if _, err := nc.Write([]byte(cmd("WAIT", "1", "120") + cmd("PING"))); err != nil {
		t.Fatal(err)
	}
	expect(t, br, ":0\r\n")
	expect(t, br, "+PONG\r\n")
}

// TestWaitAOFVolatile covers WAITAOF on a volatile server, obs1's
// appendonly-disabled shape: a numlocal ask is refused in Redis's words,
// the refusal comes after every argument parse exactly as Redis orders it,
// and an ask of nothing answers the honest [0, 0].
func TestWaitAOFVolatile(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "WAITAOF", "1", "0", "0")
	expect(t, br, "-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n")
	// The timeout parses before the appendonly check.
	send(t, nc, "WAITAOF", "1", "0", "-5")
	expect(t, br, "-ERR timeout is negative\r\n")
	send(t, nc, "WAITAOF", "1", "abc", "100")
	expect(t, br, "-ERR value is out of range, must be positive\r\n")

	send(t, nc, "WAITAOF", "abc", "0", "0")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "WAITAOF", "2", "0", "0")
	expect(t, br, "-ERR value is out of range, value must between 0 and 1\r\n")
	send(t, nc, "WAITAOF", "-1", "0", "0")
	expect(t, br, "-ERR value is out of range, value must between 0 and 1\r\n")
	send(t, nc, "WAITAOF", "0", "-1", "0")
	expect(t, br, "-ERR value is out of range, must be positive\r\n")
	send(t, nc, "WAITAOF", "0", "0", "abc")
	expect(t, br, "-ERR timeout is not an integer or out of range\r\n")
	send(t, nc, "WAITAOF", "0", "0", "-5")
	expect(t, br, "-ERR timeout is negative\r\n")
	send(t, nc, "WAITAOF", "0", "0")
	expect(t, br, "-ERR wrong number of arguments for 'waitaof' command\r\n")
	send(t, nc, "WAITAOF", "0", "0", "0", "0")
	expect(t, br, "-ERR wrong number of arguments for 'waitaof' command\r\n")

	send(t, nc, "WAITAOF", "0", "0", "0")
	expect(t, br, "*2\r\n:0\r\n:0\r\n")
}

// TestWaitAOFRoundTrip covers WAITAOF on a logged server with the chain
// live: the local barrier answers [1, 0] once the log commits, and an
// unattainable replica ask waits out its timeout with the honest local 1.
func TestWaitAOFRoundTrip(t *testing.T) {
	_, _, nc, br, _ := startLoggedServer(t, false)

	// A fresh log has nothing to cover, so even an ask of nothing reports
	// the local verdict as achieved, the Redis appendonly-on shape.
	send(t, nc, "WAITAOF", "0", "0", "0")
	expect(t, br, "*2\r\n:1\r\n:0\r\n")

	send(t, nc, "SET", "alpha", "v1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WAITAOF", "1", "0", "0")
	expect(t, br, "*2\r\n:1\r\n:0\r\n")

	// One standby short forever: the timer delivers, local still honest.
	start := time.Now()
	send(t, nc, "WAITAOF", "0", "1", "150")
	expect(t, br, "*2\r\n:1\r\n:0\r\n")
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("WAITAOF 0 1 150 answered after %v, want the full timeout", elapsed)
	}
}

// TestWaitAOFStrictHold pins the barrier's reach with the chain gated: a
// WAITAOF pipelined behind a relaxed SET must stay parked until that SET
// commits, proof the commit snapshot covers writes the connection enqueued
// ahead of it on other shards, while a second connection's ask of nothing
// answers the honest not-yet [0, 0] immediately.
func TestWaitAOFStrictHold(t *testing.T) {
	_, _, nc, r, release := startLoggedServer(t, true)

	if _, err := nc.Write([]byte(cmd("SET", "alpha", "v1") + cmd("WAITAOF", "1", "0", "0"))); err != nil {
		t.Fatal(err)
	}
	// The single shape flushes a pipelined round at its boundary, so the
	// SET's +OK rides behind the WAITAOF's park: total silence here means
	// the barrier snapshot really covered the SET's frame. Had the
	// snapshot missed it, the barrier would have fired at once, the round
	// would have drained, and both replies would be on the wire already.
	if err := nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Peek(1); err == nil {
		t.Fatal("the round answered with the chain gated")
	}
	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	// A second connection asks for nothing and gets the current verdict.
	nc2, err := net.Dial("tcp", nc.RemoteAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	r2 := br(nc2)
	send(t, nc2, "WAITAOF", "0", "0", "0")
	expect(t, r2, "*2\r\n:0\r\n:0\r\n")

	release()
	expect(t, r, "+OK\r\n*2\r\n:1\r\n:0\r\n")
}

// TestWaitAOFTimeoutUncommitted runs the timer against a gated chain: the
// local barrier can never fire, so the timeout delivers the honest [0, 0].
func TestWaitAOFTimeoutUncommitted(t *testing.T) {
	_, _, nc, r, _ := startLoggedServer(t, true)

	send(t, nc, "SET", "alpha", "v1")
	expect(t, r, "+OK\r\n")
	start := time.Now()
	send(t, nc, "WAITAOF", "1", "0", "150")
	expect(t, r, "*2\r\n:0\r\n:0\r\n")
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("WAITAOF 1 0 150 answered after %v, want the full timeout", elapsed)
	}
}
