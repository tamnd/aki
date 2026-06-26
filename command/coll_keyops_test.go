package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// These tests guard against a regression where EXPIRE, PERSIST, RENAME,
// RENAMENX, MOVE, and COPY corrupted a btree-backed (coll-form) collection.
// Those commands used to carry the key by reading its value and rewriting it
// through Set. For a coll key the "value" is only a 32-byte metadata row, and
// Set drops the element sub-tree, so after any of these ops the collection
// reported a single truncated element. A collection only reaches coll form past
// the listpack threshold (128 entries by default), so each test fills past it.

const collFill = 200

// fillHash builds a coll-form hash with n fields f0..f(n-1) -> v0..v(n-1).
func fillHash(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	var b strings.Builder
	b.WriteString("HSET " + key)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, " f%d v%d", i, i)
	}
	if got := sendLine(t, r, c, b.String()); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HSET %s = %q, want :%d", key, got, n)
	}
}

// assertHashIntact checks the hash at key still has n fields and a sample field
// reads back its value, proving the element sub-tree survived.
func assertHashIntact(t *testing.T, r *bufio.Reader, c net.Conn, key string, n int) {
	t.Helper()
	if got := sendLine(t, r, c, "HLEN "+key); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HLEN %s = %q, want :%d", key, got, n)
	}
	if got := bulk(t, r, c, fmt.Sprintf("HGET %s f%d", key, n/2)); got != fmt.Sprintf("v%d", n/2) {
		t.Fatalf("HGET %s f%d = %q, want v%d", key, n/2, got, n/2)
	}
}

func TestCollHashSurvivesExpire(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "EXPIRE h 1000"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}
	assertHashIntact(t, r, c, "h", collFill)
	// The TTL was actually recorded on the meta header.
	if got := sendLine(t, r, c, "TTL h"); got == ":-1" || got == ":-2" {
		t.Fatalf("TTL after EXPIRE = %q, want a positive TTL", got)
	}
}

func TestCollHashSurvivesPersist(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "EXPIRE h 1000"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}
	if got := sendLine(t, r, c, "PERSIST h"); got != ":1" {
		t.Fatalf("PERSIST = %q", got)
	}
	assertHashIntact(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "TTL h"); got != ":-1" {
		t.Fatalf("TTL after PERSIST = %q, want :-1", got)
	}
}

func TestCollHashSurvivesRename(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "EXPIRE h 1000"); got != ":1" {
		t.Fatalf("EXPIRE = %q", got)
	}
	if got := sendLine(t, r, c, "RENAME h h2"); got != "+OK" {
		t.Fatalf("RENAME = %q", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS h after rename = %q, want :0", got)
	}
	assertHashIntact(t, r, c, "h2", collFill)
	// The source TTL rode along to the destination.
	if got := sendLine(t, r, c, "TTL h2"); got == ":-1" || got == ":-2" {
		t.Fatalf("TTL h2 after rename = %q, want a positive TTL", got)
	}
}

func TestCollHashSurvivesRenameNX(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "RENAMENX h h3"); got != ":1" {
		t.Fatalf("RENAMENX = %q", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS h after renamenx = %q, want :0", got)
	}
	assertHashIntact(t, r, c, "h3", collFill)
}

func TestCollHashSurvivesCopy(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "COPY h h4"); got != ":1" {
		t.Fatalf("COPY = %q", got)
	}
	// Both the source and the copy are whole and independent.
	assertHashIntact(t, r, c, "h", collFill)
	assertHashIntact(t, r, c, "h4", collFill)
	// Mutating the copy must not touch the source.
	if got := sendLine(t, r, c, "HDEL h4 f0"); got != ":1" {
		t.Fatalf("HDEL h4 = %q", got)
	}
	if got := sendLine(t, r, c, "HLEN h4"); got != fmt.Sprintf(":%d", collFill-1) {
		t.Fatalf("HLEN h4 after HDEL = %q", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("HLEN h after HDEL on copy = %q, want :%d (source unchanged)", got, collFill)
	}
}

func TestCollHashSurvivesCopyReplace(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	fillHash(t, r, c, "dst", collFill/2)
	if got := sendLine(t, r, c, "COPY h dst"); got != ":0" {
		t.Fatalf("COPY without REPLACE onto existing = %q, want :0", got)
	}
	if got := sendLine(t, r, c, "COPY h dst REPLACE"); got != ":1" {
		t.Fatalf("COPY REPLACE = %q", got)
	}
	assertHashIntact(t, r, c, "dst", collFill)
}

func TestCollHashSurvivesMove(t *testing.T) {
	r, c := startData(t)
	fillHash(t, r, c, "h", collFill)
	if got := sendLine(t, r, c, "MOVE h 1"); got != ":1" {
		t.Fatalf("MOVE = %q", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS h in db0 after move = %q, want :0", got)
	}
	if got := sendLine(t, r, c, "SELECT 1"); got != "+OK" {
		t.Fatalf("SELECT 1 = %q", got)
	}
	assertHashIntact(t, r, c, "h", collFill)
}

// TestCollTypesSurviveRename covers the other three coll types through RENAME,
// the shared deep-copy path, so set/zset/list metadata and elements all ride
// across intact.
func TestCollTypesSurviveRename(t *testing.T) {
	r, c := startData(t)

	// Set.
	var sb strings.Builder
	sb.WriteString("SADD s")
	for i := 0; i < collFill; i++ {
		fmt.Fprintf(&sb, " m%d", i)
	}
	if got := sendLine(t, r, c, sb.String()); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("SADD = %q", got)
	}
	if got := sendLine(t, r, c, "RENAME s s2"); got != "+OK" {
		t.Fatalf("RENAME s = %q", got)
	}
	if got := sendLine(t, r, c, "SCARD s2"); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("SCARD s2 = %q", got)
	}
	if got := sendLine(t, r, c, "SISMEMBER s2 m100"); got != ":1" {
		t.Fatalf("SISMEMBER s2 m100 = %q", got)
	}

	// Sorted set.
	var zb strings.Builder
	zb.WriteString("ZADD z")
	for i := 0; i < collFill; i++ {
		fmt.Fprintf(&zb, " %d e%d", i, i)
	}
	if got := sendLine(t, r, c, zb.String()); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("ZADD = %q", got)
	}
	if got := sendLine(t, r, c, "RENAME z z2"); got != "+OK" {
		t.Fatalf("RENAME z = %q", got)
	}
	if got := sendLine(t, r, c, "ZCARD z2"); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("ZCARD z2 = %q", got)
	}
	if got := bulk(t, r, c, "ZSCORE z2 e100"); got != "100" {
		t.Fatalf("ZSCORE z2 e100 = %q", got)
	}

	// List.
	var lb strings.Builder
	lb.WriteString("RPUSH l")
	for i := 0; i < collFill; i++ {
		fmt.Fprintf(&lb, " x%d", i)
	}
	if got := sendLine(t, r, c, lb.String()); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("RPUSH = %q", got)
	}
	if got := sendLine(t, r, c, "RENAME l l2"); got != "+OK" {
		t.Fatalf("RENAME l = %q", got)
	}
	if got := sendLine(t, r, c, "LLEN l2"); got != fmt.Sprintf(":%d", collFill) {
		t.Fatalf("LLEN l2 = %q", got)
	}
	if got := bulk(t, r, c, "LINDEX l2 100"); got != "x100" {
		t.Fatalf("LINDEX l2 100 = %q", got)
	}
}
