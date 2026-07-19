package drivers

import (
	"bufio"
	"strings"
	"testing"
)

// MONITOR streams every command the server processes to the issuing connection
// (monitor.go). These drive the real pair-shaped server across two connections:
// one monitors, the other runs commands, and the feed lines arrive unsolicited on
// the monitor's socket. The pair shape is required for the same reason pub/sub
// needs it, an idle monitor has no reader draining its own pushes, so the delivery
// rides the standalone writer.

// readMonitorLine reads one monitor feed entry, a RESP simple string, and returns
// it without the leading '+' or trailing CRLF.
func readMonitorLine(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read monitor line: %v", err)
	}
	if len(line) < 3 || line[0] != '+' {
		t.Fatalf("monitor line = %q, want a +simple string", line)
	}
	return strings.TrimRight(line[1:], "\r\n")
}

// TestMonitorFeed checks the core path: a connection issues MONITOR and gets +OK,
// a second connection runs commands, and each command shows up on the monitor's
// wire with the bracketed db and address prefix and the quoted arguments.
func TestMonitorFeed(t *testing.T) {
	srv := startPubsubServer(t)
	monNc, monBr := dialPubsub(t, srv)
	cmdNc, cmdBr := dialPubsub(t, srv)

	send(t, monNc, "MONITOR")
	expect(t, monBr, "+OK\r\n")

	// A SET on the other connection is fed to the monitor. The command's own
	// reply still lands on its own connection, unchanged.
	send(t, cmdNc, "SET", "k", "v")
	expect(t, cmdBr, "+OK\r\n")
	line := readMonitorLine(t, monBr)
	if !strings.Contains(line, `"SET" "k" "v"`) {
		t.Fatalf("monitor line = %q, want it to carry \"SET\" \"k\" \"v\"", line)
	}
	// The prefix is <ts> [<db> <addr>]: a unix timestamp with a dotted
	// microsecond fraction, then the database zero and the client address.
	if !strings.Contains(line, " [0 ") || !strings.Contains(line, ".") {
		t.Fatalf("monitor line = %q, want a <ts>.<usec> [0 <addr>] prefix", line)
	}

	// A GET is fed too, so the feed is per-command not one-shot.
	send(t, cmdNc, "GET", "k")
	expect(t, cmdBr, "$1\r\nv\r\n")
	line = readMonitorLine(t, monBr)
	if !strings.Contains(line, `"GET" "k"`) {
		t.Fatalf("monitor line = %q, want it to carry \"GET\" \"k\"", line)
	}
}

// TestMonitorArgQuoting checks the value quoting: an argument holding a space, a
// quote, and a newline is rendered with the newline escaped so the whole feed
// entry stays one simple-string frame with no embedded terminator.
func TestMonitorArgQuoting(t *testing.T) {
	srv := startPubsubServer(t)
	monNc, monBr := dialPubsub(t, srv)
	cmdNc, cmdBr := dialPubsub(t, srv)

	send(t, monNc, "MONITOR")
	expect(t, monBr, "+OK\r\n")

	send(t, cmdNc, "SET", "k", "a b\"\n")
	expect(t, cmdBr, "+OK\r\n")
	// The reader stops at the first LF; a raw newline in the value would split
	// the frame early and this read would miss the closing quote. The escaped
	// form keeps it on one line.
	line := readMonitorLine(t, monBr)
	if !strings.Contains(line, `"a b\"\n"`) {
		t.Fatalf("monitor line = %q, want the value quoted as \"a b\\\"\\n\"", line)
	}
}

// TestMonitorReset checks that RESET takes a connection out of monitor mode: after
// it, a command on the other connection reaches the ex-monitor no more, so its
// next read is the RESET reply alone with nothing queued behind it.
func TestMonitorReset(t *testing.T) {
	srv := startPubsubServer(t)
	monNc, monBr := dialPubsub(t, srv)
	cmdNc, cmdBr := dialPubsub(t, srv)

	send(t, monNc, "MONITOR")
	expect(t, monBr, "+OK\r\n")

	send(t, cmdNc, "SET", "k", "v")
	expect(t, cmdBr, "+OK\r\n")
	readMonitorLine(t, monBr) // the SET feed line

	// RESET clears monitor mode and replies +RESET.
	send(t, monNc, "RESET")
	expect(t, monBr, "+RESET\r\n")

	// A later command must not reach the ex-monitor. Issue one, wait for its own
	// reply so the feed (if any) would already be queued, then a PING on the
	// ex-monitor answers +PONG with no stray feed line ahead of it.
	send(t, cmdNc, "SET", "k2", "v2")
	expect(t, cmdBr, "+OK\r\n")
	send(t, monNc, "PING")
	expect(t, monBr, "+PONG\r\n")
}

// TestMonitorNoSelfEcho checks that a monitor does not see its own commands. A
// lone monitor that runs a command (here PING, allowed off the shard hop) must
// not receive a feed line for it, so its next read is the command's reply alone.
func TestMonitorNoSelfEcho(t *testing.T) {
	srv := startPubsubServer(t)
	monNc, monBr := dialPubsub(t, srv)

	send(t, monNc, "MONITOR")
	expect(t, monBr, "+OK\r\n")

	// PING on the monitor answers +PONG and is not fed back to itself: if it
	// were, this read would see the feed line instead of the pong.
	send(t, monNc, "PING")
	expect(t, monBr, "+PONG\r\n")
}
