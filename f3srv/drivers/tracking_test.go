package drivers

import (
	"strconv"
	"testing"
	"time"
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

// TestTrackingFlushNullPush checks a FLUSHALL invalidates every tracking cache at
// once with a single null-payload push, and does so even when a different
// connection issued the flush.
func TestTrackingFlushNullPush(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON")

	send(t, wc, "SET", "a", "1")
	expect(t, wbr, "+OK\r\n")
	send(t, wc, "SET", "b", "2")
	expect(t, wbr, "+OK\r\n")
	sendCmd(t, tbr, tc, "GET", "a")
	sendCmd(t, tbr, tc, "GET", "b")

	// A flush from the writer connection invalidates the tracking connection's whole
	// cache with one null-payload push, not one per cached key.
	send(t, wc, "FLUSHALL")
	expect(t, wbr, "+OK\r\n")

	push, ok := readRESP(t, tbr).([]any)
	if !ok || len(push) != 2 {
		t.Fatalf("flush push = %v, want a 2-element frame", push)
	}
	if push[0] != "invalidate" {
		t.Fatalf("flush push[0] = %v, want invalidate", push[0])
	}
	if push[1] != nil {
		t.Fatalf("flush push payload = %v, want a null", push[1])
	}

	// The cache was cleared, so a write to a now-forgotten key pushes nothing until
	// it is read again.
	send(t, wc, "SET", "a", "3")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("after flush, write to forgotten key = %v, want PONG (no stray push)", r)
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

// TestTrackingOptin checks OPTIN mode caches only the reads of a command preceded
// by CLIENT CACHING YES: an unmarked read is not cached (a write pushes nothing),
// and a CACHING YES-marked read is (a write pushes).
func TestTrackingOptin(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "OPTIN"); r != "OK" {
		t.Fatalf("CLIENT TRACKING ON OPTIN = %v, want OK", r)
	}

	send(t, wc, "SET", "u", "1")
	expect(t, wbr, "+OK\r\n")

	// An unmarked read in OPTIN mode is not cached, so the write pushes nothing.
	sendCmd(t, tbr, tc, "GET", "u")
	send(t, wc, "SET", "u", "2")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("optin unmarked read then write = %v, want PONG (no push)", r)
	}

	// CACHING YES marks the next read, so it is cached and the write pushes.
	if r := sendCmd(t, tbr, tc, "CLIENT", "CACHING", "YES"); r != "OK" {
		t.Fatalf("CLIENT CACHING YES = %v, want OK", r)
	}
	sendCmd(t, tbr, tc, "GET", "u")
	send(t, wc, "SET", "u", "3")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("optin marked read then write = %v, want invalidate", push)
	}

	// The mark governs one command only: a second read after it is not cached.
	sendCmd(t, tbr, tc, "GET", "u")
	send(t, wc, "SET", "u", "4")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("optin mark expired after one command = %v, want PONG (no push)", r)
	}
}

// TestTrackingOptout checks OPTOUT mode caches every read except one preceded by
// CLIENT CACHING NO.
func TestTrackingOptout(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "OPTOUT")

	send(t, wc, "SET", "o", "1")
	expect(t, wbr, "+OK\r\n")

	// An unmarked read in OPTOUT mode is cached, so the write pushes.
	sendCmd(t, tbr, tc, "GET", "o")
	send(t, wc, "SET", "o", "2")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("optout unmarked read then write = %v, want invalidate", push)
	}

	// CACHING NO opts the next read out, so the write pushes nothing.
	if r := sendCmd(t, tbr, tc, "CLIENT", "CACHING", "NO"); r != "OK" {
		t.Fatalf("CLIENT CACHING NO = %v, want OK", r)
	}
	sendCmd(t, tbr, tc, "GET", "o")
	send(t, wc, "SET", "o", "3")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("optout opted-out read then write = %v, want PONG (no push)", r)
	}
}

// TestTrackingNoloop checks NOLOOP suppresses the invalidation for a key the
// tracking connection wrote itself, while a write by a different connection to the
// same cached key still pushes. A self-write still forgets the key (once per
// caching cycle), so the tracking connection re-reads to re-arm before the foreign
// write is expected to push.
func TestTrackingNoloop(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "NOLOOP"); r != "OK" {
		t.Fatalf("CLIENT TRACKING ON NOLOOP = %v, want OK", r)
	}

	// TRACKINGINFO renders the noloop flag.
	info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	flags, ok := info["flags"].([]any)
	if !ok || len(flags) != 2 || flags[0] != "on" || flags[1] != "noloop" {
		t.Fatalf("TRACKINGINFO flags with NOLOOP = %v, want [on noloop]", info["flags"])
	}

	// Seed and read the key so it is cached, then write it on the tracking
	// connection itself: NOLOOP suppresses the self-invalidation.
	send(t, wc, "SET", "n", "1")
	expect(t, wbr, "+OK\r\n")
	sendCmd(t, tbr, tc, "GET", "n")
	if r := sendCmd(t, tbr, tc, "SET", "n", "2"); r != "OK" {
		t.Fatalf("self-write SET n = %v, want OK", r)
	}
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("NOLOOP self-write then PING = %v, want PONG (no self-push)", r)
	}

	// Re-read to re-arm (the self-write still forgot the key), then a foreign write
	// on another connection must push, since NOLOOP only exempts the writer itself.
	sendCmd(t, tbr, tc, "GET", "n")
	send(t, wc, "SET", "n", "3")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("NOLOOP foreign write push = %v, want invalidate", push)
	}
}

// TestTrackingBcast checks broadcast mode: a PREFIX registration pushes an
// invalidate for every write to a key under the prefix, with no prior read, and
// fires on every such write (stateless, not once per caching cycle). A write to a
// key outside the prefix pushes nothing.
func TestTrackingBcast(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:"); r != "OK" {
		t.Fatalf("CLIENT TRACKING ON BCAST PREFIX user: = %v, want OK", r)
	}

	// TRACKINGINFO renders the bcast flag and the registered prefix.
	info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	flags, ok := info["flags"].([]any)
	if !ok || len(flags) != 2 || flags[0] != "on" || flags[1] != "bcast" {
		t.Fatalf("TRACKINGINFO flags in BCAST = %v, want [on bcast]", info["flags"])
	}
	prefixes, ok := info["prefixes"].([]any)
	if !ok || len(prefixes) != 1 || prefixes[0] != "user:" {
		t.Fatalf("TRACKINGINFO prefixes = %v, want [user:]", info["prefixes"])
	}

	// A write to a key under the prefix pushes an invalidate naming the key, with no
	// prior read by the tracking connection.
	send(t, wc, "SET", "user:1", "a")
	expect(t, wbr, "+OK\r\n")
	push, ok := readRESP(t, tbr).([]any)
	if !ok || len(push) != 2 || push[0] != "invalidate" {
		t.Fatalf("bcast prefix write push = %v, want a 2-element invalidate", push)
	}
	if keys, ok := push[1].([]any); !ok || len(keys) != 1 || keys[0] != "user:1" {
		t.Fatalf("bcast push keys = %v, want [user:1]", push[1])
	}

	// Broadcast is stateless: a second write to the same key, with no intervening
	// read, pushes again.
	send(t, wc, "SET", "user:1", "b")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("bcast second write push = %v, want invalidate (stateless)", push)
	}

	// A write to a key outside the prefix pushes nothing.
	send(t, wc, "SET", "other:1", "c")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("bcast non-prefix write = %v, want PONG (no push)", r)
	}
}

// TestTrackingBcastAllKeys checks BCAST with no PREFIX matches every key.
func TestTrackingBcastAllKeys(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "BCAST")

	send(t, wc, "SET", "anything", "1")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("bcast-all write push = %v, want invalidate", push)
	}
}

// TestTrackingBcastNoloop checks NOLOOP suppresses a broadcast connection's push for
// a matching key it wrote itself, while another connection's write still pushes.
func TestTrackingBcastNoloop(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)
	wc, wbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "BCAST", "NOLOOP", "PREFIX", "k:")

	// The tracking connection writes a matching key itself: NOLOOP suppresses the push.
	if r := sendCmd(t, tbr, tc, "SET", "k:1", "a"); r != "OK" {
		t.Fatalf("bcast self-write = %v, want OK", r)
	}
	if r := sendCmd(t, tbr, tc, "PING"); r != "PONG" {
		t.Fatalf("bcast NOLOOP self-write = %v, want PONG (no self-push)", r)
	}

	// A different connection's write to a matching key still pushes.
	send(t, wc, "SET", "k:2", "b")
	expect(t, wbr, "+OK\r\n")
	if push, ok := readRESP(t, tbr).([]any); !ok || push[0] != "invalidate" {
		t.Fatalf("bcast NOLOOP foreign write push = %v, want invalidate", push)
	}
}

// TestTrackingBcastErrors checks BCAST validation: BCAST with OPTIN is an error,
// PREFIX without BCAST is an error, and overlapping prefixes are an error.
func TestTrackingBcastErrors(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}

	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "BCAST", "OPTIN").(errorReply); !ok {
		t.Fatalf("TRACKING ON BCAST OPTIN should be an error")
	}
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "PREFIX", "x").(errorReply); !ok {
		t.Fatalf("TRACKING ON PREFIX without BCAST should be an error")
	}
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "foo", "PREFIX", "foobar").(errorReply); !ok {
		t.Fatalf("TRACKING ON BCAST with overlapping prefixes should be an error")
	}
}

// TestTrackingModeErrors checks the mode validation: OPTIN and OPTOUT together is an
// error, CACHING outside a mode is an error, and CACHING with the wrong YES/NO for
// the mode is an error. Also checks TRACKINGINFO renders the mode token.
func TestTrackingModeErrors(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)

	if m := helloFields(t, sendCmd(t, tbr, tc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}

	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT").(errorReply); !ok {
		t.Fatalf("TRACKING ON OPTIN OPTOUT should be an error")
	}
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT").(errorReply); !ok {
		t.Fatalf("TRACKING ON REDIRECT should be a not-supported error")
	}

	// Enable OPTIN; a CACHING NO is then the wrong selector for the mode.
	sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "OPTIN")
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "CACHING", "NO").(errorReply); !ok {
		t.Fatalf("CLIENT CACHING NO in OPTIN mode should be an error")
	}
	info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	flags, ok := info["flags"].([]any)
	if !ok || len(flags) != 2 || flags[0] != "on" || flags[1] != "optin" {
		t.Fatalf("TRACKINGINFO flags in OPTIN = %v, want [on optin]", info["flags"])
	}
}

// TestTrackingRedirect drives REDIRECT, the RESP2 client-side-caching mechanism: a
// tracking connection points its invalidations at a second connection subscribed to
// __redis__:invalidate. The tracking connection stays RESP2 (REDIRECT waives the
// RESP3 requirement), records a key, and a foreign write delivers the invalidation to
// the target as a pub/sub message frame on __redis__:invalidate carrying the key. A
// FLUSHALL delivers the whole-cache-gone signal as a message with a null payload.
func TestTrackingRedirect(t *testing.T) {
	srv := startPubsubServer(t)
	// The redirect target: a RESP2 connection subscribed to the invalidation channel.
	// Its client id is read before it subscribes, since subscribe mode bars CLIENT.
	target, tgtBr := dialPubsub(t, srv)
	id, ok := sendCmd(t, tgtBr, target, "CLIENT", "ID").(int64)
	if !ok {
		t.Fatalf("CLIENT ID did not return an integer")
	}
	send(t, target, "SUBSCRIBE", invalidateChannel)
	if k, ch, n := readSubConfirm(t, tgtBr); k != "subscribe" || ch != invalidateChannel || n != 1 {
		t.Fatalf("subscribe confirm = %q %q %d, want subscribe %s 1", k, ch, n, invalidateChannel)
	}

	// The tracking connection stays RESP2: REDIRECT lets it cache without RESP3.
	tc, tbr := dialPubsub(t, srv)
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(id, 10)); r != "OK" {
		t.Fatalf("CLIENT TRACKING ON REDIRECT = %v, want OK", r)
	}
	// GETREDIR and TRACKINGINFO both report the target id.
	if r := sendCmd(t, tbr, tc, "CLIENT", "GETREDIR"); r != id {
		t.Fatalf("GETREDIR = %v, want %d", r, id)
	}
	info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
	if info["redirect"] != id {
		t.Fatalf("TRACKINGINFO redirect = %v, want %d", info["redirect"], id)
	}

	// Seed and read a key so it is cached, then a foreign write invalidates it: the
	// target (not the tracking connection) receives the message frame.
	wc, wbr := dialPubsub(t, srv)
	send(t, wc, "SET", "foo", "bar")
	expect(t, wbr, "+OK\r\n")
	if r := sendCmd(t, tbr, tc, "GET", "foo"); r != "bar" {
		t.Fatalf("GET foo = %v, want bar", r)
	}
	send(t, wc, "SET", "foo", "baz")
	expect(t, wbr, "+OK\r\n")

	msg, ok := readRESP(t, tgtBr).([]any)
	if !ok || len(msg) != 3 || msg[0] != "message" || msg[1] != invalidateChannel {
		t.Fatalf("redirect delivery = %v, want a message on %s", msg, invalidateChannel)
	}
	if keys, ok := msg[2].([]any); !ok || len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("redirect message payload = %v, want [foo]", msg[2])
	}

	// A FLUSHALL delivers the whole-cache-gone signal as a message with a null payload.
	if r := sendCmd(t, tbr, tc, "GET", "foo"); r != "baz" {
		t.Fatalf("re-read GET foo = %v, want baz", r)
	}
	send(t, wc, "FLUSHALL")
	expect(t, wbr, "+OK\r\n")
	msg, ok = readRESP(t, tgtBr).([]any)
	if !ok || len(msg) != 3 || msg[0] != "message" || msg[1] != invalidateChannel {
		t.Fatalf("flush redirect delivery = %v, want a message on %s", msg, invalidateChannel)
	}
	if msg[2] != nil {
		t.Fatalf("flush redirect payload = %v, want a null", msg[2])
	}
}

// TestTrackingRedirectRESP2Waiver checks a RESP2 connection may enable tracking when
// it names a REDIRECT target (the RESP3 requirement is waived), and that REDIRECT 0
// (the explicit no-redirect) does not waive it.
func TestTrackingRedirectRESP2Waiver(t *testing.T) {
	srv := startPubsubServer(t)
	target, tgtBr := dialPubsub(t, srv)
	id := sendCmd(t, tgtBr, target, "CLIENT", "ID").(int64)
	send(t, target, "SUBSCRIBE", invalidateChannel)
	readSubConfirm(t, tgtBr)

	tc, tbr := dialPubsub(t, srv)
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(id, 10)); r != "OK" {
		t.Fatalf("RESP2 TRACKING ON REDIRECT = %v, want OK", r)
	}
	// REDIRECT 0 is the explicit no-redirect, so a RESP2 connection still cannot enable.
	rc, rbr := dialPubsub(t, srv)
	if _, ok := sendCmd(t, rbr, rc, "CLIENT", "TRACKING", "ON", "REDIRECT", "0").(errorReply); !ok {
		t.Fatalf("RESP2 TRACKING ON REDIRECT 0 should be an error (RESP3 not waived)")
	}
}

// TestTrackingRedirectMissing checks REDIRECT to a client id that names no live
// connection is refused, and an unparseable id is a syntax-level error.
func TestTrackingRedirectMissing(t *testing.T) {
	srv := startPubsubServer(t)
	tc, tbr := dialPubsub(t, srv)

	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT", "999999").(errorReply); !ok {
		t.Fatalf("REDIRECT to a nonexistent client id should be an error")
	}
	if _, ok := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT", "notanumber").(errorReply); !ok {
		t.Fatalf("REDIRECT with a non-numeric id should be an error")
	}
}

// TestTrackingRedirectBroken checks that when a redirect target disconnects, the
// tracking connection's redirection is marked broken: TRACKINGINFO reports the
// broken_redirect flag while GETREDIR still names the (now-gone) target id.
func TestTrackingRedirectBroken(t *testing.T) {
	srv := startPubsubServer(t)
	target, tgtBr := dialPubsub(t, srv)
	id := sendCmd(t, tgtBr, target, "CLIENT", "ID").(int64)
	send(t, target, "SUBSCRIBE", invalidateChannel)
	readSubConfirm(t, tgtBr)

	tc, tbr := dialPubsub(t, srv)
	if r := sendCmd(t, tbr, tc, "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(id, 10)); r != "OK" {
		t.Fatalf("TRACKING ON REDIRECT = %v, want OK", r)
	}

	// Drop the target. The server's teardown marks every dependent's redirection
	// broken; that runs on the target's reader goroutine, so poll until it lands.
	target.Close()
	deadline := time.Now().Add(2 * time.Second)
	var broke bool
	for time.Now().Before(deadline) {
		info := flattenPairs(t, sendCmd(t, tbr, tc, "CLIENT", "TRACKINGINFO"))
		flags, _ := info["flags"].([]any)
		if hasFlag(flags, "broken_redirect") {
			broke = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !broke {
		t.Fatalf("TRACKINGINFO never reported broken_redirect after target disconnect")
	}
	// The id is retained after the break, matching redis: GETREDIR still names it.
	if r := sendCmd(t, tbr, tc, "CLIENT", "GETREDIR"); r != id {
		t.Fatalf("GETREDIR after broken redirect = %v, want %d", r, id)
	}
}

// hasFlag reports whether a TRACKINGINFO flags array contains the given token.
func hasFlag(flags []any, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}
	return false
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
