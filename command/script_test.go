package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// sendArgs writes a command as a proper RESP array, so arguments that contain
// spaces (a Lua script body) survive intact. It returns the parsed reply.
func sendArgs(t *testing.T, r *bufio.Reader, c net.Conn, args ...string) any {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(b.String())); err != nil {
		t.Fatalf("write: %v", err)
	}
	return readReplyValue(t, r)
}

func TestEvalReturnsTypes(t *testing.T) {
	r, c := startData(t)

	if got := sendArgs(t, r, c, "EVAL", "return 1", "0"); got != int64(1) {
		t.Fatalf("return 1 = %v", got)
	}
	if got := sendArgs(t, r, c, "EVAL", "return 'hello'", "0"); got != "hello" {
		t.Fatalf("return 'hello' = %v", got)
	}
	// A Lua true becomes 1 and false becomes nil on RESP2.
	if got := sendArgs(t, r, c, "EVAL", "return true", "0"); got != int64(1) {
		t.Fatalf("return true = %v", got)
	}
	if got := sendArgs(t, r, c, "EVAL", "return false", "0"); got != nil {
		t.Fatalf("return false = %v", got)
	}
	// A float is truncated to an integer.
	if got := sendArgs(t, r, c, "EVAL", "return 3.7", "0"); got != int64(3) {
		t.Fatalf("return 3.7 = %v", got)
	}
}

func TestEvalKeysAndArgv(t *testing.T) {
	r, c := startData(t)
	reply := sendArgs(t, r, c, "EVAL", "return {KEYS[1], KEYS[2], ARGV[1]}", "2", "k1", "k2", "first")
	arr := asArray(t, reply)
	if len(arr) != 3 || arr[0] != "k1" || arr[1] != "k2" || arr[2] != "first" {
		t.Fatalf("keys/argv = %v", arr)
	}
}

func TestEvalStatusAndErrorReply(t *testing.T) {
	r, c := startData(t)
	if got := sendArgs(t, r, c, "EVAL", "return redis.status_reply('TEST')", "0"); got != "TEST" {
		t.Fatalf("status_reply = %v", got)
	}
	got := sendArgs(t, r, c, "EVAL", "return redis.error_reply('my error')", "0")
	e, ok := got.(cmdErr)
	if !ok || string(e) != "ERR my error" {
		t.Fatalf("error_reply = %v (%T)", got, got)
	}
}

func TestEvalRedisCall(t *testing.T) {
	r, c := startData(t)
	if got := sendArgs(t, r, c, "EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "foo", "bar"); got != "OK" {
		t.Fatalf("redis.call SET = %v", got)
	}
	// The write is visible to a normal GET.
	if got := bulk(t, r, c, "GET foo"); got != "bar" {
		t.Fatalf("GET foo = %q", got)
	}
	if got := sendArgs(t, r, c, "EVAL", "return redis.call('GET', KEYS[1])", "1", "foo"); got != "bar" {
		t.Fatalf("redis.call GET = %v", got)
	}
}

func TestEvalRedisCallErrorRaises(t *testing.T) {
	r, c := startData(t)
	// WRONGTYPE: GET on a list. redis.call raises, so EVAL fails.
	sendArgs(t, r, c, "RPUSH", "mylist", "a")
	got := sendArgs(t, r, c, "EVAL", "return redis.call('GET', KEYS[1])", "1", "mylist")
	if _, ok := got.(cmdErr); !ok {
		t.Fatalf("expected error from redis.call on wrong type, got %v (%T)", got, got)
	}
}

func TestEvalRedisPcallReturnsError(t *testing.T) {
	r, c := startData(t)
	sendArgs(t, r, c, "RPUSH", "mylist", "a")
	// pcall returns the error as a table, which the script can inspect.
	got := sendArgs(t, r, c, "EVAL", "local ok = redis.pcall('GET', KEYS[1]); if ok.err then return 'caught' end; return 'no'", "1", "mylist")
	if got != "caught" {
		t.Fatalf("pcall error handling = %v", got)
	}
}

func TestEvalReadOnlyRejectsWrite(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL_RO", "return redis.call('SET', KEYS[1], '1')", "1", "k")
	e, ok := got.(cmdErr)
	if !ok || !strings.Contains(string(e), "Write commands are not allowed") {
		t.Fatalf("EVAL_RO write = %v (%T)", got, got)
	}
}

func TestEvalNumkeysErrors(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVAL", "return 1", "-1")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR Number of keys can't be negative" {
		t.Fatalf("negative numkeys = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", "return 1", "5", "onlyone")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR Number of keys can't be greater than number of args" {
		t.Fatalf("too many keys = %v", got)
	}
	got = sendArgs(t, r, c, "EVAL", "return 1", "notanumber")
	if e, ok := got.(cmdErr); !ok || string(e) != "ERR value is not an integer or out of range" {
		t.Fatalf("bad numkeys = %v", got)
	}
}

func TestScriptLoadAndEvalsha(t *testing.T) {
	r, c := startData(t)
	sha := sendArgs(t, r, c, "SCRIPT", "LOAD", "return 42")
	shaStr, ok := sha.(string)
	if !ok || len(shaStr) != 40 {
		t.Fatalf("SCRIPT LOAD = %v", sha)
	}
	if got := sendArgs(t, r, c, "EVALSHA", shaStr, "0"); got != int64(42) {
		t.Fatalf("EVALSHA = %v", got)
	}
	// Uppercase digest still resolves.
	if got := sendArgs(t, r, c, "EVALSHA", strings.ToUpper(shaStr), "0"); got != int64(42) {
		t.Fatalf("EVALSHA upper = %v", got)
	}
}

func TestEvalshaMissing(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "EVALSHA", "0000000000000000000000000000000000000000", "0")
	e, ok := got.(cmdErr)
	if !ok || !strings.HasPrefix(string(e), "NOSCRIPT") {
		t.Fatalf("EVALSHA missing = %v (%T)", got, got)
	}
}

func TestScriptExists(t *testing.T) {
	r, c := startData(t)
	sha := sendArgs(t, r, c, "SCRIPT", "LOAD", "return 1").(string)
	reply := asArray(t, sendArgs(t, r, c, "SCRIPT", "EXISTS", sha, "ffffffffffffffffffffffffffffffffffffffff"))
	if reply[0] != int64(1) || reply[1] != int64(0) {
		t.Fatalf("SCRIPT EXISTS = %v", reply)
	}
}

func TestScriptFlush(t *testing.T) {
	r, c := startData(t)
	sha := sendArgs(t, r, c, "SCRIPT", "LOAD", "return 1").(string)
	if got := sendArgs(t, r, c, "SCRIPT", "FLUSH"); got != "OK" {
		t.Fatalf("SCRIPT FLUSH = %v", got)
	}
	reply := asArray(t, sendArgs(t, r, c, "SCRIPT", "EXISTS", sha))
	if reply[0] != int64(0) {
		t.Fatalf("after flush EXISTS = %v", reply)
	}
}

func TestScriptLoadCompileError(t *testing.T) {
	r, c := startData(t)
	got := sendArgs(t, r, c, "SCRIPT", "LOAD", "this is not lua )(")
	if _, ok := got.(cmdErr); !ok {
		t.Fatalf("expected compile error, got %v (%T)", got, got)
	}
}

func TestEvalSha1hex(t *testing.T) {
	r, c := startData(t)
	// Known SHA1 of the empty string.
	if got := sendArgs(t, r, c, "EVAL", "return redis.sha1hex('')", "0"); got != "da39a3ee5e6b4b0d3255bfef95601890afd80709" {
		t.Fatalf("sha1hex empty = %v", got)
	}
}

func TestEvalPcallTableFields(t *testing.T) {
	r, c := startData(t)
	// status_reply table flows back through redis.call as {ok=...} and reads back.
	got := sendArgs(t, r, c, "EVAL", "local s = redis.call('SET', KEYS[1], 'v'); if s.ok == 'OK' then return 'yes' end; return 'no'", "1", "k")
	if got != "yes" {
		t.Fatalf("status table field = %v", got)
	}
}
