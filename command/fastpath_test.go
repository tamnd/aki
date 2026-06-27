package command

import (
	"bufio"
	"net"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/v2/store"
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
