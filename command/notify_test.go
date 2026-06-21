package command

import (
	"slices"
	"testing"
)

func TestNotifyConfigRoundtrip(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	// The stored value is canonical: K and E sort after the A shorthand.
	if _, err := c.Write([]byte("CONFIG GET notify-keyspace-events\r\n")); err != nil {
		t.Fatal(err)
	}
	got := readResp(t, r)
	if !slices.Contains(got, "AKE") {
		t.Fatalf("CONFIG GET = %v, want it to contain AKE", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET notify-keyspace-events Q"); got == "+OK" {
		t.Fatalf("CONFIG SET with bad flag should fail, got %q", got)
	}
}

func TestNotifyKeyevent(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)

	// One client subscribes to the keyevent channel for SET in db 0.
	if _, err := c2.Write([]byte("SUBSCRIBE __keyevent@0__:set\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2) // drain the subscribe confirmation

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "message") || !slices.Contains(msg, "__keyevent@0__:set") || !slices.Contains(msg, "foo") {
		t.Fatalf("keyevent push = %v", msg)
	}
}

func TestNotifyKeyspace(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)

	// The other form: subscribe to the keyspace channel for a key and watch the
	// event name arrive as the message.
	if _, err := c2.Write([]byte("SUBSCRIBE __keyspace@0__:counter\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "INCR counter"); got != ":1" {
		t.Fatalf("INCR = %q", got)
	}

	msg := readResp(t, r2)
	if !slices.Contains(msg, "__keyspace@0__:counter") || !slices.Contains(msg, "incrby") {
		t.Fatalf("keyspace push = %v", msg)
	}
}

func TestNotifyDisabled(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)

	// With notifications off (the default), a write fires nothing. PING travels the
	// same connection, so if a notification had been delivered it would arrive
	// before the PONG.
	if got := sendLine(t, r1, c1, "SET foo bar"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	if _, err := c2.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	// In RESP2 subscriber mode PING replies with a two-element pong array. If a
	// notification had been delivered it would arrive on this read first.
	reply := readResp(t, r2)
	if slices.Contains(reply, "message") {
		t.Fatalf("notification fired while disabled: %v", reply)
	}
	if !slices.Contains(reply, "pong") {
		t.Fatalf("expected pong reply, got %v", reply)
	}
}

func TestParseNotifyFlags(t *testing.T) {
	if _, ok := parseNotifyFlags("KEA"); !ok {
		t.Fatal("KEA should parse")
	}
	if _, ok := parseNotifyFlags("Kx"); !ok {
		t.Fatal("Kx should parse")
	}
	if _, ok := parseNotifyFlags("KQ"); ok {
		t.Fatal("KQ should not parse")
	}
	if s := canonicalNotifyFlags(notifyKeyspace | notifyKeyevent | notifyAll); s != "AKE" {
		t.Fatalf("canonical = %q, want AKE", s)
	}
	if s := canonicalNotifyFlags(notifyKeyspace | notifyExpired); s != "xK" {
		t.Fatalf("canonical = %q, want xK", s)
	}
}
