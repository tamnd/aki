package sqlo1

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// scanServerRig is dispatchServer with the server handle exposed, so
// keyspace tests can push keys through the tier transitions between
// commands.
func scanServerRig(t *testing.T) (*Server, func(args ...string) string) {
	t.Helper()
	s, err := NewServer(NewMemStore())
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() int64 { return 1_000_000 }
	return s, func(args ...string) string {
		bs := make([][]byte, len(args))
		for i, a := range args {
			bs[i] = []byte(a)
		}
		return string(s.dispatch(nil, bs))
	}
}

// parseScanReply splits a SCAN reply into its cursor and key list.
// Test keys never contain CRLF, so line splitting is safe.
func parseScanReply(t *testing.T, raw string) (uint64, []string) {
	t.Helper()
	lines := strings.Split(raw, "\r\n")
	if len(lines) < 4 || lines[0] != "*2" {
		t.Fatalf("not a scan reply: %q", raw)
	}
	cur, err := strconv.ParseUint(lines[2], 10, 64)
	if err != nil {
		t.Fatalf("cursor %q in %q: %v", lines[2], raw, err)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(lines[3], "*"))
	if err != nil {
		t.Fatalf("element count in %q: %v", raw, err)
	}
	keys := make([]string, 0, n)
	for i := 5; i < len(lines) && len(keys) < n; i += 2 {
		keys = append(keys, lines[i])
	}
	if len(keys) != n {
		t.Fatalf("reply %q promised %d keys, carried %d", raw, n, len(keys))
	}
	return cur, keys
}

// scanCollect drives SCAN to cursor 0 and returns the sorted union.
func scanCollect(t *testing.T, do func(...string) string, extra ...string) []string {
	t.Helper()
	seen := map[string]bool{}
	cur := "0"
	for step := 0; ; step++ {
		if step > 10_000 {
			t.Fatal("SCAN does not terminate")
		}
		args := append([]string{"SCAN", cur}, extra...)
		next, keys := parseScanReply(t, do(args...))
		for _, k := range keys {
			seen[k] = true
		}
		if next == 0 {
			break
		}
		cur = strconv.FormatUint(next, 10)
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func wantKeys(t *testing.T, got, want []string, what string) {
	t.Helper()
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	}
}

// TestServerScanWalk drives SCAN over a keyspace spread across the
// tiers and the six types, with MATCH and TYPE filters on top.
func TestServerScanWalk(t *testing.T) {
	s, do := scanServerRig(t)
	ctx := context.Background()

	strKeys := []string{}
	for _, k := range []string{"k00", "k01", "k02", "k03", "k04", "k10", "k11", "k12"} {
		if got := do("SET", k, "v"); got != "+OK\r\n" {
			t.Fatal(got)
		}
		strKeys = append(strKeys, k)
	}
	do("HSET", "h1", "f", "v")
	do("RPUSH", "l1", "a")
	do("SADD", "s1", "a")
	do("ZADD", "z1", "1", "m")
	do("XADD", "x1", "*", "f", "v")

	// Everything above goes cold; the rest of the traffic runs over it.
	if err := s.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.t.EvictAllForTest()

	if got := do("SET", "hot1", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	do("SET", "k01", "rewritten") // blind overwrite of a cold key
	if got := do("DEL", "k00"); got != ":1\r\n" {
		t.Fatal(got)
	}
	strKeys = append(strKeys[1:], "hot1")

	all := append([]string{}, strKeys...)
	all = append(all, "h1", "l1", "s1", "z1", "x1")

	wantKeys(t, scanCollect(t, do, "COUNT", "3"), all, "SCAN COUNT 3")
	wantKeys(t, scanCollect(t, do, "COUNT", "1000"), all, "SCAN COUNT 1000")
	wantKeys(t, scanCollect(t, do, "MATCH", "k0*", "COUNT", "4"), []string{"k01", "k02", "k03", "k04"}, "SCAN MATCH k0*")
	wantKeys(t, scanCollect(t, do, "TYPE", "string", "COUNT", "4"), strKeys, "SCAN TYPE string")
	for ty, k := range map[string]string{"hash": "h1", "list": "l1", "set": "s1", "zset": "z1", "stream": "x1"} {
		wantKeys(t, scanCollect(t, do, "TYPE", ty), []string{k}, "SCAN TYPE "+ty)
	}
	// An unknown type name matches nothing but the walk still runs to
	// cursor 0, per Redis.
	wantKeys(t, scanCollect(t, do, "TYPE", "vector"), nil, "SCAN TYPE vector")
	// Options are case-insensitive and repeat with last-wins.
	wantKeys(t, scanCollect(t, do, "match", "zzz*", "Match", "h*"), []string{"h1", "hot1"}, "SCAN match last-wins")

	// KEYS is the same walk driven to completion in one reply.
	lines := strings.Split(do("KEYS", "*"), "\r\n")
	if want := "*" + strconv.Itoa(len(all)); lines[0] != want {
		t.Fatalf("KEYS * header = %q, want %q", lines[0], want)
	}
	got := []string{}
	for i := 2; i < len(lines)-1; i += 2 {
		got = append(got, lines[i])
	}
	sort.Strings(got)
	wantKeys(t, got, all, "KEYS *")
	if reply := do("KEYS", "nomatch*"); reply != "*0\r\n" {
		t.Fatalf("KEYS nomatch* = %q", reply)
	}
}

// TestServerScanGrammar pins the option doors: cursor parsing, COUNT
// bounds, unknown options, and arity, matching the collection scans'
// grammar.
func TestServerScanGrammar(t *testing.T) {
	_, do := scanServerRig(t)

	if got := do("SCAN"); !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("SCAN arity = %q", got)
	}
	if got := do("SCAN", "abc"); got != "-ERR invalid cursor\r\n" {
		t.Fatalf("SCAN abc = %q", got)
	}
	if got := do("SCAN", "-1"); got != "-ERR invalid cursor\r\n" {
		t.Fatalf("SCAN -1 = %q", got)
	}
	if got := do("SCAN", "0", "COUNT", "0"); got != "-ERR syntax error\r\n" {
		t.Fatalf("COUNT 0 = %q", got)
	}
	if got := do("SCAN", "0", "COUNT", "abc"); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatalf("COUNT abc = %q", got)
	}
	if got := do("SCAN", "0", "NOPE"); got != "-ERR syntax error\r\n" {
		t.Fatalf("unknown option = %q", got)
	}
	if got := do("SCAN", "0", "MATCH"); got != "-ERR syntax error\r\n" {
		t.Fatalf("dangling MATCH = %q", got)
	}
	if got := do("SCAN", "0", "TYPE"); got != "-ERR syntax error\r\n" {
		t.Fatalf("dangling TYPE = %q", got)
	}
	if got := do("KEYS"); !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("KEYS arity = %q", got)
	}
	if got := do("KEYS", "a", "b"); !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("KEYS 2-arg arity = %q", got)
	}

	// An empty keyspace answers cursor 0 straight away once the walk
	// covers the empty shadow and the empty index.
	if keys := scanCollect(t, do); len(keys) != 0 {
		t.Fatalf("empty keyspace SCAN = %v", keys)
	}
}
