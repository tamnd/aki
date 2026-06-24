package command

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// TestWritesBlockedByBgsaveError checks the gate logic directly: off until a save
// fails, on once lastStatus is "err" with save points and the flag set, and off
// again when any of those conditions stops holding.
func TestWritesBlockedByBgsaveError(t *testing.T) {
	d := New(Config{})

	// Fresh server: no save has run, so the gate is open.
	if d.writesBlockedByBgsaveError() {
		t.Fatal("gate should be open before any save")
	}

	// A failed save with the default flag and save points closes the gate.
	d.persist.setLastStatus("err")
	if !d.writesBlockedByBgsaveError() {
		t.Fatal("gate should be closed after a failed save")
	}

	// Turning the flag off opens it even though the last save failed.
	d.conf.set("stop-writes-on-bgsave-error", "no")
	if d.writesBlockedByBgsaveError() {
		t.Fatal("gate should be open with stop-writes-on-bgsave-error no")
	}
	d.conf.set("stop-writes-on-bgsave-error", "yes")

	// With no save points configured the gate is open regardless of the flag.
	d.conf.set("save", "")
	if d.writesBlockedByBgsaveError() {
		t.Fatal("gate should be open with no save points")
	}
	d.conf.set("save", "3600 1")

	// A later successful save flips the status back and reopens the gate.
	d.persist.setLastStatus("ok")
	if d.writesBlockedByBgsaveError() {
		t.Fatal("gate should be open after a successful save")
	}
}

// startStopWrites brings up the full server pipeline like startData but keeps a
// handle on the dispatcher so the test can inject a failed-save state, which has
// no command to trigger on demand.
func startStopWrites(t *testing.T) (*bufio.Reader, net.Conn, *Dispatcher) {
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
	ncfg := networking.Config{Addr: "127.0.0.1:0"}
	srv := networking.New(ncfg, d)
	d.SetServer(srv)
	go func() { _ = srv.ListenAndServe(ncfg) }()
	t.Cleanup(func() { _ = srv.Close() })

	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("server did not bind")
		}
		time.Sleep(time.Millisecond)
	}
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return bufio.NewReader(conn), conn, d
}

// TestStopWritesOnBgsaveErrorGate checks the MISCONF rejection on the wire: after
// a failed save writes are refused but reads still work, and a later good save
// clears the gate.
func TestStopWritesOnBgsaveErrorGate(t *testing.T) {
	r, c, d := startStopWrites(t)

	if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
		t.Fatalf("SET before gate = %q", got)
	}

	// Simulate the state left by a failed background save.
	d.persist.mu.Lock()
	d.persist.setLastStatus("err")
	d.persist.mu.Unlock()

	got := sendLine(t, r, c, "SET k v2")
	if got != "-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error." {
		t.Fatalf("SET under gate = %q want MISCONF", got)
	}

	// Reads stay available while writes are blocked.
	h := sendLine(t, r, c, "GET k")
	if v := readBulk(t, r, h); v != "v" {
		t.Fatalf("GET under gate = %q want v", v)
	}

	// A later successful save clears the failure and writes resume.
	d.persist.mu.Lock()
	d.persist.setLastStatus("ok")
	d.persist.mu.Unlock()
	if got := sendLine(t, r, c, "SET k v3"); got != "+OK" {
		t.Fatalf("SET after recovery = %q", got)
	}
}
