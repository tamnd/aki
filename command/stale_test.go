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

// TestDenyStaleData checks the gate logic directly: off by default, off on a
// master, off for stale-safe commands, and on only when a replica has lost its
// master link with replica-serve-stale-data set to no.
func TestDenyStaleData(t *testing.T) {
	d := New(Config{})
	data := &CmdDesc{}                 // a plain data command, not stale-safe
	safe := &CmdDesc{Flags: FlagStale} // INFO, CONFIG, PING and friends

	// Default config serves stale data, so the gate is off even on a replica.
	d.repl.role = "slave"
	d.repl.link = "connect"
	if d.denyStaleData(data) {
		t.Fatal("gate should be open with replica-serve-stale-data yes")
	}

	d.conf.set("replica-serve-stale-data", "no")

	// A replica with a down link refuses a data command.
	if !d.denyStaleData(data) {
		t.Fatal("gate should be closed on a stale replica")
	}
	// Stale-safe commands are always allowed.
	if d.denyStaleData(safe) {
		t.Fatal("stale-safe command should never be gated")
	}
	// Once the link is up the gate opens again.
	d.repl.link = "connected"
	if d.denyStaleData(data) {
		t.Fatal("gate should be open when the link is connected")
	}
	// A master is never affected.
	d.repl.role = "master"
	d.repl.link = "connect"
	if d.denyStaleData(data) {
		t.Fatal("gate should be open on a master")
	}
}

// startStale brings up the full server pipeline and keeps a handle on the
// dispatcher so the test can force the replica link state, which no command sets
// without a real master to connect to.
func startStale(t *testing.T) (*bufio.Reader, net.Conn, *Dispatcher) {
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

// TestServeStaleDataGate checks the MASTERDOWN rejection on the wire: a replica
// with a down link and replica-serve-stale-data no refuses data commands while
// stale-safe commands still answer, and turning the flag back on restores reads.
func TestServeStaleDataGate(t *testing.T) {
	r, c, d := startStale(t)

	if got := sendLine(t, r, c, "SET k v"); got != "+OK" {
		t.Fatalf("SET before gate = %q", got)
	}

	// Become a replica with a down master link and stop serving stale data.
	d.repl.mu.Lock()
	d.repl.role = "slave"
	d.repl.link = "connect"
	d.repl.mu.Unlock()
	if got := sendLine(t, r, c, "CONFIG SET replica-serve-stale-data no"); got != "+OK" {
		t.Fatalf("CONFIG SET replica-serve-stale-data = %q", got)
	}

	// A data command is refused with MASTERDOWN.
	if got := sendLine(t, r, c, "GET k"); got != "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'." {
		t.Fatalf("GET under gate = %q want MASTERDOWN", got)
	}

	// A stale-safe command still answers.
	if got := sendLine(t, r, c, "SELECT 0"); got != "+OK" {
		t.Fatalf("SELECT under gate = %q want +OK", got)
	}

	// Turning the flag back on serves the stale data again.
	if got := sendLine(t, r, c, "CONFIG SET replica-serve-stale-data yes"); got != "+OK" {
		t.Fatalf("CONFIG SET replica-serve-stale-data yes = %q", got)
	}
	h := sendLine(t, r, c, "GET k")
	if v := readBulk(t, r, h); v != "v" {
		t.Fatalf("GET after flag on = %q want v", v)
	}
}
