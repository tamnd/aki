package drivers

import (
	"testing"
)

// Client-side caching at the driver seam (spec 2064/f3/17; redis's CLIENT
// TRACKING). A tracking connection over RESP3 records every key it reads through a
// cacheable command; the first write to such a key, on any connection, pushes one
// RESP3 invalidate message and then forgets the key so the next read re-arms it.
// These tests drive the enable switch, the write-path push, the once-per-cycle
// discipline, the RESP2 refusal, and the TRACKINGINFO/GETREDIR introspection.

// TestTrackingInvalidatePush enables tracking on a RESP3 connection, reads a key, and
// checks that a write to that key on another connection pushes one invalidate frame
// naming the key.
func TestTrackingInvalidatePush(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	// The tracking connection must be RESP3 to carry the out-of-band push.
	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON"); r != "OK" {
		t.Fatalf("CLIENT TRACKING ON = %v, want OK", r)
	}

	// Seed the key over the writer, then read it on the tracking connection so the
	// key is recorded, then overwrite it: the overwrite is the first write to a
	// recorded key and must push one invalidate.
	send(t, wc, "SET", "foo", "bar")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "GET", "foo"); r != "bar" {
		t.Fatalf("GET foo = %v, want bar", r)
	}
	send(t, wc, "SET", "foo", "baz")
	expect(t, wbr, "+OK\r\n")

	push, ok := readRESP(t, tbr).([]any)
	if !ok || len(push) != 2 {
		t.Fatalf("invalidate push = %v, want a 2-element frame", push)
	}
	if push[0] != "invalidate" {
		t.Fatalf("push[0] = %v, want invalidate", push[0])
	}
	keys, ok := push[1].([]any)
	if !ok || len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("push keys = %v, want [foo]", push[1])
	}
}

// TestTrackingOncePerCycle checks the invalidation fires once per caching cycle: a
// second write to the same key, with no intervening read, pushes nothing. The test
// re-reads the key to re-arm it and then sees a second push.
func TestTrackingOncePerCycle(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON")

	send(t, wc, "SET", "k", "1")
	expect(t, wbr, "+OK\r\n")
	sendCmd(t, tbr, tc, "GET", "k")

	// First write: one invalidate.
	send(t, wc, "SET", "k", "2")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("first write push = %v, want invalidate", push)
	}

	// Second write with no re-read: the key was forgotten, so no push. Prove the
	// socket is quiet by round-tripping a PING on the tracking connection; if a
	// stray push were queued it would arrive before the PONG.
	send(t, wc, "SET", "k", "3")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("after forgotten-key write, next reply = %v, want PONG (no stray push)", r)
	}

	// Re-read re-arms the key, so the next write pushes again.
	sendCmd(t, tbr, tc, "GET", "k")
	send(t, wc, "SET", "k", "4")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("re-armed write push = %v, want invalidate", push)
	}
}

// TestTrackingOffStopsPush checks CLIENT TRACKING OFF disarms the connection: a
// write to a previously recorded key pushes nothing.
func TestTrackingOffStopsPush(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON")
	send(t, wc, "SET", "g", "1")
	expect(t, wbr, "+OK\r\n")
	sendCmd(t, tbr, tc, "GET", "g")

	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "OFF"); r != "OK" {
		t.Fatalf("CLIENT TRACKING OFF = %v, want OK", r)
	}
	send(t, wc, "SET", "g", "2")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("after TRACKING OFF, next reply = %v, want PONG (no push)", r)
	}
}

// TestTrackingRequiresResp3 checks default-mode tracking refuses a RESP2 connection:
// with no redirection target a RESP2 client has no way to carry the push, so CLIENT
// TRACKING ON is an error and GETREDIR stays -1.
func TestTrackingRequiresResp3(t *testing.T) {
	srv := startPubsubServer(t)
	nc, br := dialPubsub(t, srv)

	r := sendCmd(t, br, nc, "CLIENT", "TRACKING", "ON")
	if _, ok := r.(errorReply); !ok {
		t.Fatalf("CLIENT TRACKING ON over RESP2 = %v, want an error", r)
	}
	if r := sendCmd(t, br, nc, "CLIENT", "GETREDIR"); r != int64(-1) {
		t.Fatalf("GETREDIR with tracking off = %v, want -1", r)
	}
}

// TestTrackingInfoAndRedir checks the introspection surface: TRACKINGINFO reports
// off before enable and on after, and GETREDIR reads -1 then 0.
func TestTrackingInfoAndRedir(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}

	// Off: flags [off], redirect -1.
	info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	if flags, ok := info["flags"].([]any); !ok || len(flags) != 1 || flags[0] != "off" {
		t.Fatalf("TRACKINGINFO flags before enable = %v, want [off]", info["flags"])
	}
	if info["redirect"] != int64(-1) {
		t.Fatalf("TRACKINGINFO redirect before enable = %v, want -1", info["redirect"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "GETREDIR"); r != int64(-1) {
		t.Fatalf("GETREDIR before enable = %v, want -1", r)
	}

	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON")

	// On: flags [on], redirect 0, prefixes [].
	info = flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	if flags, ok := info["flags"].([]any); !ok || len(flags) != 1 || flags[0] != "on" {
		t.Fatalf("TRACKINGINFO flags after enable = %v, want [on]", info["flags"])
	}
	if info["redirect"] != int64(0) {
		t.Fatalf("TRACKINGINFO redirect after enable = %v, want 0", info["redirect"])
	}
	if prefixes, ok := info["prefixes"].([]any); !ok || len(prefixes) != 0 {
		t.Fatalf("TRACKINGINFO prefixes = %v, want []", info["prefixes"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "GETREDIR"); r != int64(0) {
		t.Fatalf("GETREDIR after enable = %v, want 0", r)
	}
}

// TestTrackingCachingRejected checks CLIENT CACHING is refused outside OPTIN/OPTOUT
// mode, the only modes it is meaningful in, which this slice does not run.
func TestTrackingCachingRejected(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)

	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "CACHING", "YES").(errorReply); !ok {
		t.Fatalf("CLIENT CACHING YES outside OPTIN/OPTOUT should be an error")
	}
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "CACHING", "MAYBE").(errorReply); !ok {
		t.Fatalf("CLIENT CACHING MAYBE should be a syntax error")
	}
}

// flattenPairs turns a flattened key/value reply (the shape readRESP gives a RESP3
// map) into a name->value map for field lookups.
func flattenPairs(t *testing.T, reply any) map[string]any {
	t.Helper()
	arr, ok := reply.([]any)
	if !ok || len(arr)%2 != 0 {
		t.Fatalf("expected an even-length pair array, got %v", reply)
	}
	out := make(map[string]any, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k, ok := arr[i].(string)
		if !ok {
			t.Fatalf("pair key %v is not a string", arr[i])
		}
		out[k] = arr[i+1]
	}
	return out
}
