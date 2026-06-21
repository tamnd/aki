package command

import (
	"strings"
	"testing"
)

func TestSubscribeConfirm(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*3", "$9", "subscribe", "$4", "news", ":1"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("SUBSCRIBE confirm = %v want %v", got, want)
	}
}

func TestSubscribeMultiple(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("SUBSCRIBE a b\r\n")); err != nil {
		t.Fatal(err)
	}
	first := readResp(t, r)
	if first[len(first)-1] != ":1" {
		t.Fatalf("first confirm count = %v", first)
	}
	second := readResp(t, r)
	if second[len(second)-1] != ":2" {
		t.Fatalf("second confirm count = %v", second)
	}
}

func TestPublishDelivers(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1) // subscribe confirmation

	if got := sendLine(t, r2, c2, "PUBLISH news hello"); got != ":1" {
		t.Fatalf("PUBLISH = %q want :1", got)
	}
	got := readResp(t, r1)
	want := []string{"*3", "$7", "message", "$4", "news", "$5", "hello"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("delivered = %v want %v", got, want)
	}
}

func TestPublishNoSubscribers(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "PUBLISH ghost hi"); got != ":0" {
		t.Fatalf("PUBLISH no subs = %q want :0", got)
	}
}

func TestPatternDelivers(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("PSUBSCRIBE news.*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1) // psubscribe confirmation

	if got := sendLine(t, r2, c2, "PUBLISH news.tech boom"); got != ":1" {
		t.Fatalf("PUBLISH = %q want :1", got)
	}
	got := readResp(t, r1)
	want := []string{"*4", "$8", "pmessage", "$6", "news.*", "$9", "news.tech", "$4", "boom"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("pmessage = %v want %v", got, want)
	}
}

func TestPublishBothChannelAndPattern(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	// One connection holds both a direct and a matching pattern subscription.
	if _, err := c1.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	if _, err := c1.Write([]byte("PSUBSCRIBE ne*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)

	// Both deliveries count, so PUBLISH returns 2.
	if got := sendLine(t, r2, c2, "PUBLISH news hi"); got != ":2" {
		t.Fatalf("PUBLISH = %q want :2", got)
	}
	// Drain the two pushes the subscriber received.
	_ = readResp(t, r1)
	_ = readResp(t, r1)
}

func TestUnsubscribeAll(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("SUBSCRIBE a b\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r)
	_ = readResp(t, r)
	if _, err := c.Write([]byte("UNSUBSCRIBE\r\n")); err != nil {
		t.Fatal(err)
	}
	// Two confirmations, one per channel, in sorted order, count counting down.
	first := readResp(t, r)
	second := readResp(t, r)
	if first[4] != "a" || first[len(first)-1] != ":1" {
		t.Fatalf("first unsub = %v", first)
	}
	if second[4] != "b" || second[len(second)-1] != ":0" {
		t.Fatalf("second unsub = %v", second)
	}
}

func TestUnsubscribeNoneWhenEmpty(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("UNSUBSCRIBE\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*3", "$11", "unsubscribe", "$-1", ":0"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("empty UNSUBSCRIBE = %v want %v", got, want)
	}
}

func TestSubscriberModeRestriction(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r)
	if got := sendLine(t, r, c, "GET k"); got != "-ERR Command not allowed inside a subscription context. Please use RESET." {
		t.Fatalf("GET in subscriber mode = %q", got)
	}
}

func TestSubscriberModePing(t *testing.T) {
	r, c := startData(t)
	if _, err := c.Write([]byte("SUBSCRIBE news\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r)
	if _, err := c.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	want := []string{"*2", "$4", "pong", "$0", ""}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("PING in subscriber mode = %v want %v", got, want)
	}
}

func TestSubscribeInMultiRejected(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "MULTI")
	if got := sendLine(t, r, c, "SUBSCRIBE news"); got != "-ERR subscribe is not allowed in transactions" {
		t.Fatalf("SUBSCRIBE in MULTI = %q", got)
	}
	if got := sendLine(t, r, c, "EXEC"); got != "-EXECABORT Transaction discarded because of previous errors." {
		t.Fatalf("EXEC = %q", got)
	}
}

func TestPubsubChannels(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("SUBSCRIBE alpha beta\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	_ = readResp(t, r1)

	// The non-subscriber connection asks for the active channels.
	got := array(t, r2, c2, "PUBSUB CHANNELS")
	if strings.Join(got, "|") != "alpha|beta" {
		t.Fatalf("PUBSUB CHANNELS = %v", got)
	}
	got = array(t, r2, c2, "PUBSUB CHANNELS al*")
	if strings.Join(got, "|") != "alpha" {
		t.Fatalf("PUBSUB CHANNELS al* = %v", got)
	}
}

func TestPubsubNumSub(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("SUBSCRIBE alpha\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)

	if _, err := c2.Write([]byte("PUBSUB NUMSUB alpha ghost\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r2)
	want := []string{"*4", "$5", "alpha", ":1", "$5", "ghost", ":0"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("PUBSUB NUMSUB = %v want %v", got, want)
	}
}

func TestPubsubNumPat(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("PSUBSCRIBE a.* b.*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1)
	_ = readResp(t, r1)

	if got := sendLine(t, r2, c2, "PUBSUB NUMPAT"); got != ":2" {
		t.Fatalf("PUBSUB NUMPAT = %q want :2", got)
	}
}

func TestShardPublishDelivers(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c1.Write([]byte("SSUBSCRIBE shard1\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r1) // ssubscribe confirmation

	if got := sendLine(t, r2, c2, "SPUBLISH shard1 data"); got != ":1" {
		t.Fatalf("SPUBLISH = %q want :1", got)
	}
	got := readResp(t, r1)
	want := []string{"*3", "$8", "smessage", "$6", "shard1", "$4", "data"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("smessage = %v want %v", got, want)
	}
}

func TestPubsubHelp(t *testing.T) {
	r, c := startData(t)
	got := array(t, r, c, "PUBSUB HELP")
	if len(got) == 0 || !strings.HasPrefix(got[0], "PUBSUB") {
		t.Fatalf("PUBSUB HELP = %v", got)
	}
}
