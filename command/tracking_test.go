package command

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// recvValue reads one full RESP2 or RESP3 value, enough to parse the invalidation
// pushes and tracking maps these tests assert on. Aggregates come back as []any,
// nulls and null arrays as nil.
func recvValue(t *testing.T, r *bufio.Reader) any {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatalf("empty reply line")
	}
	switch line[0] {
	case '+', ',':
		return line[1:]
	case '-':
		return cmdErr(line[1:])
	case '#':
		return line[1:] == "t"
	case ':':
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n
	case '_':
		return nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*', '>', '~':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = recvValue(t, r)
		}
		return out
	case '%':
		n, _ := strconv.Atoi(line[1:])
		out := make([]any, 0, n*2)
		for i := 0; i < n*2; i++ {
			out = append(out, recvValue(t, r))
		}
		return out
	default:
		t.Fatalf("unexpected reply type %q", line)
		return nil
	}
}

// trkSend writes a command and reads one reply, refreshing the read deadline so a
// test that has been waiting on pushes does not trip the dial deadline.
func trkSend(t *testing.T, r *bufio.Reader, c net.Conn, parts ...string) any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	rawCmd(t, c, parts...)
	return recvValue(t, r)
}

// recvPush waits for one invalidation message on c. It refreshes the deadline
// first so an idle connection can still receive a delayed push.
func recvPush(t *testing.T, r *bufio.Reader, c net.Conn) []any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	v := recvValue(t, r)
	a, ok := v.([]any)
	if !ok {
		t.Fatalf("expected push array, got %T (%v)", v, v)
	}
	return a
}

// invalKeys returns the key list carried by an invalidation push, or nil for a
// flush. The push is the RESP3 form [channel, keys] or the redirect form
// [message, channel, keys].
func invalKeys(t *testing.T, push []any) []any {
	t.Helper()
	var channel string
	var payload any
	switch len(push) {
	case 2:
		channel, _ = push[0].(string)
		payload = push[1]
	case 3:
		if m, _ := push[0].(string); m != "message" {
			t.Fatalf("redirect push kind = %q want message", m)
		}
		channel, _ = push[1].(string)
		payload = push[2]
	default:
		t.Fatalf("push has %d elements", len(push))
	}
	if channel != "__redis__:invalidate" {
		t.Fatalf("push channel = %q", channel)
	}
	if payload == nil {
		return nil
	}
	keys, ok := payload.([]any)
	if !ok {
		t.Fatalf("push payload not an array: %T", payload)
	}
	return keys
}

// TestTrackingRESP3Push checks a RESP3 tracking client gets an inline push when a
// key it read is later changed by another client.
func TestTrackingRESP3Push(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	if v := trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON"); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON = %v", v)
	}

	sendArgs(t, rc, cc, "SET", "foo", "v0")
	if v := trkSend(t, ra, ca, "GET", "foo"); v != "v0" {
		t.Fatalf("GET foo = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v1")

	keys := invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("invalidation keys = %v want [foo]", keys)
	}
}

// TestTrackingRedirect checks a RESP2 client redirecting invalidations to another
// client: the target receives them as a Pub/Sub message on __redis__:invalidate.
func TestTrackingRedirect(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	addr := net.JoinHostPort(host, port)
	rb, cb := dial(t, addr)
	rc, cc := dial(t, addr)

	bid, ok := sendArgs(t, rb, cb, "CLIENT", "ID").(int64)
	if !ok {
		t.Fatalf("CLIENT ID did not return an integer")
	}

	if v := sendArgs(t, ra, ca, "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(bid, 10)); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON REDIRECT = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v0")
	if v := sendArgs(t, ra, ca, "GET", "foo"); v != "v0" {
		t.Fatalf("GET foo = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v1")

	keys := invalKeys(t, recvPush(t, rb, cb))
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("redirect invalidation keys = %v want [foo]", keys)
	}
}

// TestTrackingBcast checks BCAST mode: any write under a tracked prefix produces
// an invalidation without the client having read the key, and writes outside the
// prefix produce nothing.
func TestTrackingBcast(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	if v := trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "foo:"); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON BCAST = %v", v)
	}

	sendArgs(t, rc, cc, "SET", "foo:1", "x")
	keys := invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo:1" {
		t.Fatalf("bcast keys = %v want [foo:1]", keys)
	}

	// A write outside the prefix must not push. The next push the client sees is
	// the one for foo:2, which proves bar:1 produced nothing.
	sendArgs(t, rc, cc, "SET", "bar:1", "x")
	sendArgs(t, rc, cc, "SET", "foo:2", "x")
	keys = invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo:2" {
		t.Fatalf("bcast keys after bar write = %v want [foo:2]", keys)
	}
}

// TestTrackingNoLoop checks NOLOOP suppresses the invalidation for a key the
// tracking client changed itself.
func TestTrackingNoLoop(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	if v := trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON", "NOLOOP"); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON NOLOOP = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v0")
	if v := trkSend(t, ra, ca, "GET", "foo"); v != "v0" {
		t.Fatalf("GET foo = %v", v)
	}

	// The client's own write must not push back to it, so the very next thing it
	// reads is the SET reply, not an invalidation.
	if v := trkSend(t, ra, ca, "SET", "foo", "v1"); v != "OK" {
		t.Fatalf("own SET reply = %v want OK (a push leaked through NOLOOP)", v)
	}

	// The write cleared foo from the table the same as any invalidation, so the
	// client re-reads to re-arm tracking before checking an external write does
	// reach it.
	if v := trkSend(t, ra, ca, "GET", "foo"); v != "v1" {
		t.Fatalf("re-read GET foo = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v2")
	keys := invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("invalidation keys = %v want [foo]", keys)
	}
}

// TestTrackingFlushAll checks FLUSHALL sends a flush invalidation with a null key
// array.
func TestTrackingFlushAll(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON")
	sendArgs(t, rc, cc, "SET", "foo", "v0")
	trkSend(t, ra, ca, "GET", "foo")

	sendArgs(t, rc, cc, "FLUSHALL")
	keys := invalKeys(t, recvPush(t, ra, ca))
	if keys != nil {
		t.Fatalf("flush invalidation keys = %v want null", keys)
	}
}

// TestTrackingExpiryInvalidates checks a key expiring produces an invalidation.
func TestTrackingExpiryInvalidates(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON")
	sendArgs(t, rc, cc, "SET", "foo", "v0")
	trkSend(t, ra, ca, "GET", "foo")

	// Set the key in the past so the next access lazily expires it. The reader's
	// GET drives lazy expiry which fires the invalidation.
	sendArgs(t, rc, cc, "PEXPIRE", "foo", "1")
	time.Sleep(20 * time.Millisecond)
	sendArgs(t, rc, cc, "GET", "foo")

	keys := invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("expiry invalidation keys = %v want [foo]", keys)
	}
}

// TestTrackingCachingOptIn checks OPTIN mode: a read is tracked only when it is
// preceded by CLIENT CACHING YES.
func TestTrackingCachingOptIn(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rc, cc := dial(t, net.JoinHostPort(host, port))

	trkSend(t, ra, ca, "HELLO", "3")
	if v := trkSend(t, ra, ca, "CLIENT", "TRACKING", "ON", "OPTIN"); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON OPTIN = %v", v)
	}

	sendArgs(t, rc, cc, "SET", "foo", "v0")
	// Not opted in, so this read is not tracked and the following write must not
	// push. The assertions below would read that stray push instead of the
	// expected replies if it leaked.
	if v := trkSend(t, ra, ca, "GET", "foo"); v != "v0" {
		t.Fatalf("GET foo = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v1")

	if v := trkSend(t, ra, ca, "CLIENT", "CACHING", "YES"); v != "OK" {
		t.Fatalf("CLIENT CACHING YES = %v", v)
	}
	if v := trkSend(t, ra, ca, "GET", "foo"); v != "v1" {
		t.Fatalf("opted-in GET foo = %v", v)
	}
	sendArgs(t, rc, cc, "SET", "foo", "v2")

	keys := invalKeys(t, recvPush(t, ra, ca))
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("opted-in invalidation keys = %v want [foo]", keys)
	}
}

// TestTrackingInfo checks CLIENT TRACKINGINFO reports the active flags, redirect,
// and prefixes.
func TestTrackingInfo(t *testing.T) {
	r, c, _, _ := startDataAddr(t)

	trkSend(t, r, c, "HELLO", "3")
	if v := trkSend(t, r, c, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:", "PREFIX", "b:", "NOLOOP"); v != "OK" {
		t.Fatalf("CLIENT TRACKING ON = %v", v)
	}
	info := asArray(t, trkSend(t, r, c, "CLIENT", "TRACKINGINFO"))
	if len(info) != 6 {
		t.Fatalf("trackinginfo = %v want 6 elements", info)
	}
	flags := asArray(t, info[1])
	want := map[string]bool{"on": true, "bcast": true, "noloop": true}
	for _, f := range flags {
		delete(want, f.(string))
	}
	if len(want) != 0 {
		t.Fatalf("trackinginfo flags = %v missing %v", flags, want)
	}
	if info[3] != int64(-1) {
		t.Fatalf("redirect = %v want -1", info[3])
	}
	prefixes := asArray(t, info[5])
	if len(prefixes) != 2 || prefixes[0] != "a:" || prefixes[1] != "b:" {
		t.Fatalf("prefixes = %v want [a: b:]", prefixes)
	}
}

// TestTrackingGetRedir checks CLIENT GETREDIR reports -1 with no redirect and the
// target id once a redirect is set.
func TestTrackingGetRedir(t *testing.T) {
	ra, ca, host, port := startDataAddr(t)
	rb, cb := dial(t, net.JoinHostPort(host, port))

	if v := sendArgs(t, ra, ca, "CLIENT", "GETREDIR"); v != int64(-1) {
		t.Fatalf("GETREDIR with no tracking = %v want -1", v)
	}
	bid := sendArgs(t, rb, cb, "CLIENT", "ID").(int64)
	sendArgs(t, ra, ca, "CLIENT", "TRACKING", "ON", "REDIRECT", strconv.FormatInt(bid, 10))
	if v := sendArgs(t, ra, ca, "CLIENT", "GETREDIR"); v != bid {
		t.Fatalf("GETREDIR = %v want %d", v, bid)
	}
}

// TestTrackingErrors checks the option-validation errors.
func TestTrackingErrors(t *testing.T) {
	r, c := startData(t)

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT"}, "both OPTIN and OPTOUT"},
		{[]string{"CLIENT", "TRACKING", "ON", "PREFIX", "x:"}, "PREFIX option requires BCAST"},
		{[]string{"CLIENT", "TRACKING", "ON", "REDIRECT", "999999"}, "does not exist"},
		{[]string{"CLIENT", "TRACKING", "ON"}, "RESP3 mode"},
		{[]string{"CLIENT", "CACHING", "YES"}, "tracking mode"},
	}
	for _, tc := range cases {
		got := sendArgs(t, r, c, tc.args...)
		e, ok := got.(cmdErr)
		if !ok || !strings.Contains(string(e), tc.want) {
			t.Fatalf("%v = %v want error containing %q", tc.args, got, tc.want)
		}
	}
}
