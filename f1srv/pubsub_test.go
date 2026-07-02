package f1srv

import (
	"bufio"
	"testing"
)

// readFrame reads one RESP array reply and returns its elements as a flat slice of strings.
// A bulk element becomes its string value ("" preserved), a nil bulk becomes "<nil>", and an
// integer element becomes ":N". Unlike the bulk-only readArray helper it tolerates the integer
// count and simple-string verbs a pub/sub frame carries.
func readFrame(t *testing.T, rw *bufio.ReadWriter) []string {
	t.Helper()
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read array header: %v", err)
	}
	line = line[:len(line)-2]
	if line[0] != '*' {
		t.Fatalf("want array, got %q", line)
	}
	n := 0
	for _, ch := range line[1:] {
		n = n*10 + int(ch-'0')
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, readArrayElem(t, rw))
	}
	return out
}

// readArrayElem reads a single reply element used inside a pub/sub array: a simple string,
// an integer, or a bulk string (possibly nil).
func readArrayElem(t *testing.T, rw *bufio.ReadWriter) string {
	t.Helper()
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read elem: %v", err)
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '+':
		return line[1:]
	case ':':
		return ":" + line[1:]
	case '$':
		if line == "$-1" {
			return "<nil>"
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rw, buf); err != nil {
			t.Fatalf("read bulk body: %v", err)
		}
		return string(buf[:n])
	}
	t.Fatalf("bad array element: %q", line)
	return ""
}

func expectArray(t *testing.T, rw *bufio.ReadWriter, want ...string) {
	t.Helper()
	if got := readFrame(t, rw); !eqStrs(got, want) {
		t.Fatalf("array = %v, want %v", got, want)
	}
}

// TestSubscribeConfirmCounts covers that each SUBSCRIBE/PSUBSCRIBE confirmation carries the
// verb, the name, and the running count, with channels and patterns sharing one count.
func TestSubscribeConfirmCounts(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SUBSCRIBE", "ch1", "ch2")
	expectArray(t, rw, "subscribe", "ch1", ":1")
	expectArray(t, rw, "subscribe", "ch2", ":2")

	cmd(t, rw, "PSUBSCRIBE", "news.*")
	expectArray(t, rw, "psubscribe", "news.*", ":3")

	// A repeat subscribe is idempotent but still confirmed with the unchanged count.
	cmd(t, rw, "SUBSCRIBE", "ch1")
	expectArray(t, rw, "subscribe", "ch1", ":3")
}

// TestPublishDelivery covers cross-connection message and pmessage delivery, the receiver
// count PUBLISH returns, and PING's subscribe-context array form.
func TestPublishDelivery(t *testing.T) {
	sub, pub, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, sub, "SUBSCRIBE", "ch1")
	expectArray(t, sub, "subscribe", "ch1", ":1")
	cmd(t, sub, "PSUBSCRIBE", "news.*")
	expectArray(t, sub, "psubscribe", "news.*", ":2")

	cmd(t, pub, "PUBLISH", "ch1", "hello")
	expect(t, pub, ":1")
	expectArray(t, sub, "message", "ch1", "hello")

	cmd(t, pub, "PUBLISH", "news.tech", "story")
	expect(t, pub, ":1")
	expectArray(t, sub, "pmessage", "news.*", "news.tech", "story")

	// A channel with no subscriber delivers to nobody.
	cmd(t, pub, "PUBLISH", "nope", "x")
	expect(t, pub, ":0")

	// PING in subscribe context returns a two-element array, not +PONG.
	cmd(t, sub, "PING")
	expectArray(t, sub, "pong", "")
	cmd(t, sub, "PING", "hey")
	expectArray(t, sub, "pong", "hey")
}

// TestPublishMultiPatternCount covers that a client subscribed to a channel and to two
// matching patterns counts three deliveries, and receives three separate frames.
func TestPublishMultiPatternCount(t *testing.T) {
	sub, pub, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, sub, "SUBSCRIBE", "news.tech")
	expectArray(t, sub, "subscribe", "news.tech", ":1")
	cmd(t, sub, "PSUBSCRIBE", "news.*")
	expectArray(t, sub, "psubscribe", "news.*", ":2")
	cmd(t, sub, "PSUBSCRIBE", "*.tech")
	expectArray(t, sub, "psubscribe", "*.tech", ":3")

	cmd(t, pub, "PUBLISH", "news.tech", "m")
	expect(t, pub, ":3")

	// The direct message arrives, then one pmessage per matching pattern. Pattern iteration
	// order is unspecified, so collect the three frames and check the set.
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		f := readFrame(t, sub)
		got[f[0]+"|"+f[1]] = true
	}
	for _, want := range []string{"message|news.tech", "pmessage|news.*", "pmessage|*.tech"} {
		if !got[want] {
			t.Fatalf("missing frame %q, got %v", want, got)
		}
	}
}

// TestSubscribeModeRestriction covers that a subscribed connection refuses a non-pub/sub
// command with the Redis-shaped error and stays usable afterward.
func TestSubscribeModeRestriction(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SUBSCRIBE", "ch1")
	expectArray(t, rw, "subscribe", "ch1", ":1")

	cmd(t, rw, "GET", "k")
	expect(t, rw, "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")

	// Still subscribed and functional: SUBSCRIBE and UNSUBSCRIBE work, and after leaving
	// subscribe context an ordinary command runs again.
	cmd(t, rw, "UNSUBSCRIBE", "ch1")
	expectArray(t, rw, "unsubscribe", "ch1", ":0")
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$-1")
}

// TestUnsubscribeCounts covers explicit and bare unsubscribe, including the empty-registry
// bare form that still replies once with a null channel and a zero count.
func TestUnsubscribeCounts(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SUBSCRIBE", "a", "b", "c")
	expectArray(t, rw, "subscribe", "a", ":1")
	expectArray(t, rw, "subscribe", "b", ":2")
	expectArray(t, rw, "subscribe", "c", ":3")

	cmd(t, rw, "UNSUBSCRIBE", "b")
	expectArray(t, rw, "unsubscribe", "b", ":2")

	// Bare unsubscribe drops every remaining channel, one confirmation each; order is
	// unspecified so collect the names and check the set, with the final count zero.
	cmd(t, rw, "UNSUBSCRIBE")
	names := map[string]bool{}
	var lastCount string
	for i := 0; i < 2; i++ {
		f := readFrame(t, rw)
		names[f[1]] = true
		lastCount = f[2]
	}
	if !names["a"] || !names["c"] {
		t.Fatalf("bare unsubscribe names = %v, want a and c", names)
	}
	if lastCount != ":0" {
		t.Fatalf("final unsubscribe count = %q, want :0", lastCount)
	}

	// Bare unsubscribe with nothing subscribed still replies once, null channel, zero count.
	cmd(t, rw, "UNSUBSCRIBE")
	expectArray(t, rw, "unsubscribe", "<nil>", ":0")
}

// TestPubSubIntrospection covers PUBSUB CHANNELS with and without a glob, NUMSUB, and NUMPAT.
func TestPubSubIntrospection(t *testing.T) {
	sub, other, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, sub, "SUBSCRIBE", "news.tech", "news.biz", "sports")
	expectArray(t, sub, "subscribe", "news.tech", ":1")
	expectArray(t, sub, "subscribe", "news.biz", ":2")
	expectArray(t, sub, "subscribe", "sports", ":3")
	cmd(t, sub, "PSUBSCRIBE", "news.*")
	expectArray(t, sub, "psubscribe", "news.*", ":4")

	// CHANNELS with a glob lists only active exact channels that match; patterns excluded.
	cmd(t, other, "PUBSUB", "CHANNELS", "news.*")
	chans := readFrame(t, other)
	set := map[string]bool{}
	for _, c := range chans {
		set[c] = true
	}
	if len(chans) != 2 || !set["news.tech"] || !set["news.biz"] {
		t.Fatalf("PUBSUB CHANNELS news.* = %v, want news.tech and news.biz", chans)
	}

	// NUMSUB reports exact-channel counts (patterns not counted), zero for an unknown channel.
	cmd(t, other, "PUBSUB", "NUMSUB", "news.tech", "ghost")
	expectArray(t, other, "news.tech", ":1", "ghost", ":0")

	// NUMPAT counts distinct patterns with at least one subscriber.
	cmd(t, other, "PUBSUB", "NUMPAT")
	expect(t, other, ":1")
}

// TestShardPubSub covers that shard channels are a separate namespace: SSUBSCRIBE counts on
// its own, SPUBLISH reaches only shard subscribers, and a regular PUBLISH never reaches a
// shard channel.
func TestShardPubSub(t *testing.T) {
	sub, pub, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, sub, "SSUBSCRIBE", "shard1")
	expectArray(t, sub, "ssubscribe", "shard1", ":1")

	cmd(t, pub, "SPUBLISH", "shard1", "hi")
	expect(t, pub, ":1")
	expectArray(t, sub, "smessage", "shard1", "hi")

	// A regular PUBLISH to the same name does not reach the shard subscriber.
	cmd(t, pub, "PUBLISH", "shard1", "x")
	expect(t, pub, ":0")

	// SHARDCHANNELS and SHARDNUMSUB read the shard namespace.
	cmd(t, pub, "PUBSUB", "SHARDCHANNELS")
	expectArray(t, pub, "shard1")
	cmd(t, pub, "PUBSUB", "SHARDNUMSUB", "shard1")
	expectArray(t, pub, "shard1", ":1")

	cmd(t, sub, "SUNSUBSCRIBE", "shard1")
	expectArray(t, sub, "sunsubscribe", "shard1", ":0")
}

// TestSubscribeInTransactionAborts covers that a subscribe verb inside MULTI is refused and
// flags the transaction so EXEC aborts, matching Redis.
func TestSubscribeInTransactionAborts(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "SUBSCRIBE", "ch1")
	expect(t, rw, "-ERR SUBSCRIBE is not allowed in transactions")
	cmd(t, rw, "EXEC")
	expect(t, rw, "-EXECABORT Transaction discarded because of previous errors.")
}
