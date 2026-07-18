package drivers

import (
	"bufio"
	"net"
	"strconv"
	"testing"
)

// startPubsubServer runs a server pinned to the pair shape, the goroutine
// driver's shape with a standalone writer that delivers to an idle subscriber
// (the single shape blocks its one goroutine in Read with nobody on the waker,
// so a message push waits for the client's next byte; the reactor delivers like
// the pair through its eventfd). testNetDriver still applies, so the ubuntu CI
// legs cover the reactor path too.
func startPubsubServer(t *testing.T) *Server {
	t.Helper()
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18, ConnShape: ShapePair, NetDriver: testNetDriver()})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv
}

func dialPubsub(t *testing.T, srv *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc, bufio.NewReader(nc)
}

// TestPubsubExactChannel drives the exact-channel path across two connections:
// one subscribes, the other publishes, and the message arrives on the
// subscriber's socket unsolicited. It checks the subscribe and unsubscribe
// confirmations, the delivered message, the receiver count, and that a publish
// to a channel with no subscribers reaches nobody.
func TestPubsubExactChannel(t *testing.T) {
	srv := startPubsubServer(t)
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	// Subscribe to two channels: each confirmation names the channel and the
	// connection's running subscription count.
	send(t, subNc, "SUBSCRIBE", "news", "sports")
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "news" || n != 1 {
		t.Fatalf("first confirm = %q %q %d, want subscribe news 1", k, ch, n)
	}
	if k, ch, n := readSubConfirm(t, subBr); k != "subscribe" || ch != "sports" || n != 2 {
		t.Fatalf("second confirm = %q %q %d, want subscribe sports 2", k, ch, n)
	}

	// Publish to a subscribed channel: the publisher gets the receiver count and
	// the subscriber gets the message push.
	send(t, pubNc, "PUBLISH", "news", "hello")
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("PUBLISH receivers = %d, want 1", n)
	}
	if k, ch, msg := readMessage(t, subBr); k != "message" || ch != "news" || msg != "hello" {
		t.Fatalf("delivered = %q %q %q, want message news hello", k, ch, msg)
	}

	// A publish to a channel this subscriber does not hold reaches nobody.
	send(t, pubNc, "PUBLISH", "weather", "rain")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("PUBLISH to empty channel = %d, want 0", n)
	}

	// PUBSUB introspection sees the two live channels and their counts.
	send(t, pubNc, "PUBSUB", "NUMSUB", "news", "weather")
	readArrayHeader(t, pubBr, 4)
	if ch := readBulkFrom(t, pubBr); ch != "news" {
		t.Fatalf("NUMSUB channel = %q, want news", ch)
	}
	if n := readIntFrom(t, pubBr); n != 1 {
		t.Fatalf("NUMSUB news = %d, want 1", n)
	}
	if ch := readBulkFrom(t, pubBr); ch != "weather" {
		t.Fatalf("NUMSUB channel = %q, want weather", ch)
	}
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("NUMSUB weather = %d, want 0", n)
	}

	// Unsubscribe from one channel: the confirmation reports the remaining count.
	send(t, subNc, "UNSUBSCRIBE", "news")
	if k, ch, n := readSubConfirm(t, subBr); k != "unsubscribe" || ch != "news" || n != 1 {
		t.Fatalf("unsubscribe confirm = %q %q %d, want unsubscribe news 1", k, ch, n)
	}

	// After the unsubscribe a publish to that channel reaches nobody.
	send(t, pubNc, "PUBLISH", "news", "again")
	if n := readIntFrom(t, pubBr); n != 0 {
		t.Fatalf("PUBLISH after unsubscribe = %d, want 0", n)
	}
}

// TestPubsubChannelsAndUnsubscribeAll covers PUBSUB CHANNELS under a glob and the
// bare UNSUBSCRIBE that drops every held channel, including the nil-channel
// confirmation a connection holding none answers.
func TestPubsubChannelsAndUnsubscribeAll(t *testing.T) {
	srv := startPubsubServer(t)
	subNc, subBr := dialPubsub(t, srv)
	qNc, qBr := dialPubsub(t, srv)

	send(t, subNc, "SUBSCRIBE", "news.tech", "news.world", "sports")
	for i := 1; i <= 3; i++ {
		readSubConfirm(t, subBr)
	}

	// CHANNELS with a pattern lists only the matching live channels.
	send(t, qNc, "PUBSUB", "CHANNELS", "news.*")
	got := map[string]bool{}
	readArrayHeader(t, qBr, 2)
	got[readBulkFrom(t, qBr)] = true
	got[readBulkFrom(t, qBr)] = true
	if !got["news.tech"] || !got["news.world"] {
		t.Fatalf("CHANNELS news.* = %v, want news.tech and news.world", got)
	}

	// A bare UNSUBSCRIBE drops all three, one confirmation each, ending at zero.
	send(t, subNc, "UNSUBSCRIBE")
	last := int64(-1)
	for i := 0; i < 3; i++ {
		_, _, n := readSubConfirm(t, subBr)
		last = n
	}
	if last != 0 {
		t.Fatalf("final unsubscribe count = %d, want 0", last)
	}

	// A second bare UNSUBSCRIBE on a connection holding nothing answers one
	// confirmation with a nil channel and count zero.
	send(t, subNc, "UNSUBSCRIBE")
	expect(t, subBr, "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n")
}

// TestPubsubSubscribeModeRestriction checks that a subscribed RESP2 connection
// may not run an ordinary command, while PING still answers.
func TestPubsubSubscribeModeRestriction(t *testing.T) {
	srv := startPubsubServer(t)
	nc, br := dialPubsub(t, srv)

	send(t, nc, "SUBSCRIBE", "ch")
	readSubConfirm(t, br)

	// A GET is refused in subscribe context.
	send(t, nc, "GET", "k")
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read GET reply: %v", err)
	}
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("GET in subscribe mode = %q, want an error", line)
	}

	// PING is allowed and answers normally.
	send(t, nc, "PING")
	expect(t, br, "+PONG\r\n")
}

// readSubConfirm reads a subscribe or unsubscribe confirmation: a three-element
// array of the kind bulk, the channel bulk, and the integer count.
func readSubConfirm(t *testing.T, br *bufio.Reader) (kind, channel string, count int64) {
	t.Helper()
	readArrayHeader(t, br, 3)
	kind = readBulkFrom(t, br)
	channel = readBulkFrom(t, br)
	count = readIntFrom(t, br)
	return kind, channel, count
}

// readMessage reads a delivered message push: "message", the channel, the
// payload, all bulks.
func readMessage(t *testing.T, br *bufio.Reader) (kind, channel, payload string) {
	t.Helper()
	readArrayHeader(t, br, 3)
	kind = readBulkFrom(t, br)
	channel = readBulkFrom(t, br)
	payload = readBulkFrom(t, br)
	return kind, channel, payload
}

// readArrayHeader reads and checks a RESP array header of the wanted length.
func readArrayHeader(t *testing.T, br *bufio.Reader, want int) {
	t.Helper()
	head, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read array header: %v", err)
	}
	if got := "*" + strconv.Itoa(want) + "\r\n"; head != got {
		t.Fatalf("array header = %q, want %q", head, got)
	}
}
