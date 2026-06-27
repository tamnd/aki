package command

import (
	"bufio"
	"net"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// startHybrid brings up a server whose string point path always runs on the
// hybrid-log store, so the integrated GET/SET fast path is exercised regardless of
// the AKI_TEST_HYBRID env toggle the rest of the suite reads.
func startHybrid(t *testing.T) (*bufio.Reader, net.Conn) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p, keyspace.WithHybridLog(store.Tunables{
		Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: "",
	}))
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return start(t, Config{Engine: NewEngine(ks)})
}

// TestFastPathGetSet checks the fast path serves a plain GET and SET with the same
// replies the full dispatch path gives: OK on the write, the stored value back, a
// null for an absent key, and WRONGTYPE when GET lands on a non-string.
func TestFastPathGetSet(t *testing.T) {
	r, c := startHybrid(t)

	if got := sendArgs(t, r, c, "GET", "missing"); got != nil {
		t.Fatalf("GET missing = %v want nil", got)
	}
	if got := sendArgs(t, r, c, "SET", "k", "hello"); got != "OK" {
		t.Fatalf("SET = %v want OK", got)
	}
	if got := sendArgs(t, r, c, "GET", "k"); got != "hello" {
		t.Fatalf("GET k = %v want hello", got)
	}
	// Overwrite goes through the same fast path.
	if got := sendArgs(t, r, c, "SET", "k", "world"); got != "OK" {
		t.Fatalf("SET overwrite = %v want OK", got)
	}
	if got := sendArgs(t, r, c, "GET", "k"); got != "world" {
		t.Fatalf("GET k after overwrite = %v want world", got)
	}

	// A GET that lands on a list must report WRONGTYPE, not the fast string read.
	if got := sendArgs(t, r, c, "RPUSH", "lst", "a"); got != int64(1) {
		t.Fatalf("RPUSH = %v", got)
	}
	got := sendArgs(t, r, c, "GET", "lst")
	e, ok := got.(cmdErr)
	if !ok || !strings.HasPrefix(string(e), "WRONGTYPE") {
		t.Fatalf("GET lst = %v (%T) want WRONGTYPE", got, got)
	}
}

// TestFastPathIncr checks the INCR/INCRBY/DECR/DECRBY fast path produces the same
// replies as the general increment path: a zero base for a missing key, the running
// total back, TTL preserved across an increment, WRONGTYPE on a non-string, not-int
// on a non-numeric value, and overflow at the int64 ceiling.
func TestFastPathIncr(t *testing.T) {
	r, c := startHybrid(t)

	if got := sendArgs(t, r, c, "INCR", "n"); got != int64(1) {
		t.Fatalf("INCR n = %v want 1", got)
	}
	if got := sendArgs(t, r, c, "INCRBY", "n", "10"); got != int64(11) {
		t.Fatalf("INCRBY n 10 = %v want 11", got)
	}
	if got := sendArgs(t, r, c, "DECR", "n"); got != int64(10) {
		t.Fatalf("DECR n = %v want 10", got)
	}
	if got := sendArgs(t, r, c, "DECRBY", "n", "4"); got != int64(6) {
		t.Fatalf("DECRBY n 4 = %v want 6", got)
	}
	if got := sendArgs(t, r, c, "GET", "n"); got != "6" {
		t.Fatalf("GET n = %v want 6", got)
	}

	// TTL must survive an increment.
	if got := sendArgs(t, r, c, "SET", "t", "1", "EX", "1000"); got != "OK" {
		t.Fatalf("SET t = %v", got)
	}
	if got := sendArgs(t, r, c, "INCR", "t"); got != int64(2) {
		t.Fatalf("INCR t = %v want 2", got)
	}
	if got := sendArgs(t, r, c, "TTL", "t"); got == int64(-1) || got == int64(-2) {
		t.Fatalf("TTL t = %v want a positive remaining TTL", got)
	}

	// WRONGTYPE on a list.
	if got := sendArgs(t, r, c, "RPUSH", "lst", "a"); got != int64(1) {
		t.Fatalf("RPUSH = %v", got)
	}
	if got := sendArgs(t, r, c, "INCR", "lst"); !isErrPrefix(got, "WRONGTYPE") {
		t.Fatalf("INCR lst = %v want WRONGTYPE", got)
	}

	// Not an integer.
	if got := sendArgs(t, r, c, "SET", "s", "abc"); got != "OK" {
		t.Fatalf("SET s = %v", got)
	}
	if got := sendArgs(t, r, c, "INCR", "s"); !isErrPrefix(got, "ERR value is not an integer") {
		t.Fatalf("INCR s = %v want not-integer error", got)
	}

	// Overflow at the int64 ceiling.
	if got := sendArgs(t, r, c, "SET", "max", "9223372036854775807"); got != "OK" {
		t.Fatalf("SET max = %v", got)
	}
	if got := sendArgs(t, r, c, "INCR", "max"); !isErrPrefix(got, "ERR increment or decrement would overflow") {
		t.Fatalf("INCR max = %v want overflow error", got)
	}
}

// isErrPrefix reports whether a reply is an error reply beginning with prefix.
func isErrPrefix(got any, prefix string) bool {
	e, ok := got.(cmdErr)
	return ok && strings.HasPrefix(string(e), prefix)
}

// TestFastPathIncrStats confirms the increment fast path counts each verb against
// its own commandstats entry, so INFO commandstats stays accurate.
func TestFastPathIncrStats(t *testing.T) {
	r, c := startHybrid(t)

	for range 4 {
		sendArgs(t, r, c, "INCR", "n")
	}
	for range 2 {
		sendArgs(t, r, c, "DECR", "n")
	}
	incr := infoField(t, r, c, "commandstats", "cmdstat_incr")
	if !strings.Contains(incr, "calls=4") {
		t.Fatalf("cmdstat_incr = %q want calls=4", incr)
	}
	decr := infoField(t, r, c, "commandstats", "cmdstat_decr")
	if !strings.Contains(decr, "calls=2") {
		t.Fatalf("cmdstat_decr = %q want calls=2", decr)
	}
}

// TestFastPathCommandStats confirms the fast path still counts GET and SET against
// their commandstats entries, so INFO commandstats stays accurate when the bypass
// serves the calls.
func TestFastPathCommandStats(t *testing.T) {
	r, c := startHybrid(t)

	for range 3 {
		if got := sendArgs(t, r, c, "SET", "k", "v"); got != "OK" {
			t.Fatalf("SET = %v", got)
		}
	}
	for range 5 {
		if got := sendArgs(t, r, c, "GET", "k"); got != "v" {
			t.Fatalf("GET = %v", got)
		}
	}

	set := infoField(t, r, c, "commandstats", "cmdstat_set")
	if !strings.Contains(set, "calls=3") {
		t.Fatalf("cmdstat_set = %q want calls=3", set)
	}
	get := infoField(t, r, c, "commandstats", "cmdstat_get")
	if !strings.Contains(get, "calls=5") {
		t.Fatalf("cmdstat_get = %q want calls=5", get)
	}
}

// TestFastPathSkippedInMulti confirms the fast path does not fire mid-transaction:
// a SET between MULTI and EXEC is queued, not applied, and EXEC runs both in order.
func TestFastPathSkippedInMulti(t *testing.T) {
	r, c := startHybrid(t)

	if got := sendArgs(t, r, c, "MULTI"); got != "OK" {
		t.Fatalf("MULTI = %v", got)
	}
	if got := sendArgs(t, r, c, "SET", "k", "queued"); got != "QUEUED" {
		t.Fatalf("SET in MULTI = %v want QUEUED", got)
	}
	if got := sendArgs(t, r, c, "GET", "k"); got != "QUEUED" {
		t.Fatalf("GET in MULTI = %v want QUEUED", got)
	}
	reply := sendArgs(t, r, c, "EXEC")
	arr := asArray(t, reply)
	if len(arr) != 2 || arr[0] != "OK" || arr[1] != "queued" {
		t.Fatalf("EXEC = %v want [OK queued]", arr)
	}
}
