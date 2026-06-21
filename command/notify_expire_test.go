package command

import (
	"slices"
	"testing"
	"time"
)

// TestNotifyExpiredLazyGet checks that a read which trips lazy expiry fires the
// expired keyevent for the key it removed.
func TestNotifyExpiredLazyGet(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:expired\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET k v PX 1"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	time.Sleep(20 * time.Millisecond)

	// GET trips lazy expiry, returns nil, and fires the expired event.
	if got := sendLine(t, r1, c1, "GET k"); got != "$-1" {
		t.Fatalf("GET = %q want $-1", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:expired") || !slices.Contains(msg, "k") {
		t.Fatalf("expired push = %v", msg)
	}
}

// TestNotifyExpiredKeyspaceForm checks the keyspace-channel form of the expired
// event, where the channel names the key and the payload is the event name.
func TestNotifyExpiredKeyspaceForm(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyspace@0__:*\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	if got := sendLine(t, r1, c1, "SET k v PX 1"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	// Drain the keyspace event from the SET itself.
	_ = readResp(t, r2)
	time.Sleep(20 * time.Millisecond)

	if got := sendLine(t, r1, c1, "EXISTS k"); got != ":0" {
		t.Fatalf("EXISTS = %q want :0", got)
	}
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyspace@0__:k") || !slices.Contains(msg, "expired") {
		t.Fatalf("expired keyspace push = %v", msg)
	}
}
