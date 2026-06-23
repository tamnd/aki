package command

import (
	"bufio"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// startActiveExpiry brings up a server over a shared keyspace and returns a writer
// connection, a subscriber connection, and the dispatcher itself so a test can
// drive the active expiry cycle directly instead of waiting on the cron timer.
func startActiveExpiry(t *testing.T) (*bufio.Reader, net.Conn, *bufio.Reader, net.Conn, *Dispatcher) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	d := New(Config{Engine: NewEngine(ks)})
	srv := networking.New(networking.Config{Addr: "127.0.0.1:0"}, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(networking.Config{Addr: "127.0.0.1:0"}) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	dial := func() (*bufio.Reader, net.Conn) {
		conn, err := net.Dial("tcp", srv.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		return bufio.NewReader(conn), conn
	}
	r1, c1 := dial()
	r2, c2 := dial()
	return r1, c1, r2, c2, d
}

// TestActiveExpireFiresEvent checks that the background cycle removes a key whose
// TTL has passed even though nothing read it, and fires the expired event.
func TestActiveExpireFiresEvent(t *testing.T) {
	r1, c1, r2, c2, d := startActiveExpiry(t)
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

	// No read touches the key, so only the active cycle can remove it.
	d.runActiveExpire()
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:expired") || !slices.Contains(msg, "k") {
		t.Fatalf("expired push = %v", msg)
	}
	if got := sendLine(t, r1, c1, "DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE after active expiry = %q want :0", got)
	}
}

// TestDebugSetActiveExpireDisables checks that DEBUG SET-ACTIVE-EXPIRE 0 stops the
// cycle from removing expired keys, and that re-enabling it resumes the cleanup.
func TestDebugSetActiveExpireDisables(t *testing.T) {
	r1, c1, _, _, d := startActiveExpiry(t)
	if got := sendLine(t, r1, c1, "DEBUG SET-ACTIVE-EXPIRE 0"); got != "+OK" {
		t.Fatalf("DEBUG SET-ACTIVE-EXPIRE 0 = %q", got)
	}
	// Use PX 200 so the key outlives any reasonable command-path latency on slow
	// CI runners (a PX 1 TTL can expire before db.set runs the B-tree write).
	if got := sendLine(t, r1, c1, "SET k v PX 200"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}
	time.Sleep(300 * time.Millisecond)

	// With the cycle off the expired key stays counted in the keyspace.
	d.runActiveExpire()
	if got := sendLine(t, r1, c1, "DBSIZE"); got != ":1" {
		t.Fatalf("DBSIZE with active expiry off = %q want :1", got)
	}

	// Turning it back on lets the next cycle reclaim the key.
	if got := sendLine(t, r1, c1, "DEBUG SET-ACTIVE-EXPIRE 1"); got != "+OK" {
		t.Fatalf("DEBUG SET-ACTIVE-EXPIRE 1 = %q", got)
	}
	d.runActiveExpire()
	if got := sendLine(t, r1, c1, "DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE after re-enable = %q want :0", got)
	}
}

// TestStartBackgroundExpiresKeys checks the cron path end to end: with the
// background loop running, a key with a short TTL is reclaimed without any client
// touching it.
func TestStartBackgroundExpiresKeys(t *testing.T) {
	r1, c1, _, _, d := startActiveExpiry(t)
	d.conf.set("hz", "100")
	d.StartBackground()
	t.Cleanup(d.StopBackground)
	if got := sendLine(t, r1, c1, "SET k v PX 1"); got != "+OK" {
		t.Fatalf("SET = %q", got)
	}

	// 10 s covers the -race overhead in CI (race detection slows goroutine
	// scheduling enough that the default 2 s is too tight).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := sendLine(t, r1, c1, "DBSIZE"); got == ":0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background cycle did not reclaim the key")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
