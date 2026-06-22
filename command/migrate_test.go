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

// startDataAddr brings up a data-backed server like startData but also returns
// the host and port it bound to, so another instance can target it over MIGRATE.
func startDataAddr(t *testing.T) (*bufio.Reader, net.Conn, string, string) {
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
	host, port, err := net.SplitHostPort(srv.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return bufio.NewReader(conn), conn, host, port
}

// TestMigrateMovesKey ships a key to a second instance and checks the source no
// longer has it while the target does.
func TestMigrateMovesKey(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)

	_ = sendLine(t, sr, sc, "SET foo bar")
	got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" foo 0 5000")
	if got != "+OK" {
		t.Fatalf("MIGRATE = %q want +OK", got)
	}
	if got := bulk(t, sr, sc, "GET foo"); got != "<nil>" {
		t.Fatalf("source GET foo after MIGRATE = %q want nil", got)
	}

	// Read the key back from the target through a fresh client.
	dr, dc := dial(t, net.JoinHostPort(dstHost, dstPort))
	if got := bulk(t, dr, dc, "GET foo"); got != "bar" {
		t.Fatalf("target GET foo = %q want bar", got)
	}
}

// TestMigrateNoKey returns +NOKEY when the key does not exist locally.
func TestMigrateNoKey(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)
	got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" missing 0 5000")
	if got != "+NOKEY" {
		t.Fatalf("MIGRATE missing = %q want +NOKEY", got)
	}
}

// TestMigrateCopyKeepsLocal keeps the source copy when COPY is given.
func TestMigrateCopyKeepsLocal(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)

	_ = sendLine(t, sr, sc, "SET k v")
	got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" k 0 5000 COPY")
	if got != "+OK" {
		t.Fatalf("MIGRATE COPY = %q want +OK", got)
	}
	if got := bulk(t, sr, sc, "GET k"); got != "v" {
		t.Fatalf("source GET k after COPY = %q want v", got)
	}
	dr, dc := dial(t, net.JoinHostPort(dstHost, dstPort))
	if got := bulk(t, dr, dc, "GET k"); got != "v" {
		t.Fatalf("target GET k = %q want v", got)
	}
}

// TestMigrateBusyKey rejects an existing target key without REPLACE and accepts
// it with REPLACE.
func TestMigrateBusyKey(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)
	dr, dc := dial(t, net.JoinHostPort(dstHost, dstPort))

	_ = sendLine(t, sr, sc, "SET k source")
	_ = sendLine(t, dr, dc, "SET k target")

	got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" k 0 5000")
	if got != "-BUSYKEY Target key name already exists" {
		t.Fatalf("MIGRATE busy = %q", got)
	}
	// The source key stays put when the migration is rejected.
	if got := bulk(t, sr, sc, "GET k"); got != "source" {
		t.Fatalf("source GET k after BUSYKEY = %q want source", got)
	}

	got = sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" k 0 5000 REPLACE")
	if got != "+OK" {
		t.Fatalf("MIGRATE REPLACE = %q want +OK", got)
	}
	if got := bulk(t, dr, dc, "GET k"); got != "source" {
		t.Fatalf("target GET k after REPLACE = %q want source", got)
	}
}

// TestMigrateKeysVariant moves several keys in one call with the KEYS option.
func TestMigrateKeysVariant(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)

	_ = sendLine(t, sr, sc, "SET k1 a")
	_ = sendLine(t, sr, sc, "SET k2 b")
	_ = sendLine(t, sr, sc, "SET k3 c")

	got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" \"\" 0 5000 KEYS k1 k2 k3")
	if got != "+OK" {
		t.Fatalf("MIGRATE KEYS = %q want +OK", got)
	}
	dr, dc := dial(t, net.JoinHostPort(dstHost, dstPort))
	for _, kv := range [][2]string{{"k1", "a"}, {"k2", "b"}, {"k3", "c"}} {
		if got := bulk(t, dr, dc, "GET "+kv[0]); got != kv[1] {
			t.Fatalf("target GET %s = %q want %q", kv[0], got, kv[1])
		}
		if got := bulk(t, sr, sc, "GET "+kv[0]); got != "<nil>" {
			t.Fatalf("source GET %s after MIGRATE = %q want nil", kv[0], got)
		}
	}
}

// TestMigrateConnError reports an IO error when the target cannot be reached.
func TestMigrateConnError(t *testing.T) {
	sr, sc := startData(t)
	_ = sendLine(t, sr, sc, "SET k v")
	// Port 1 is reserved and will refuse the connection.
	got := sendLine(t, sr, sc, "MIGRATE 127.0.0.1 1 k 0 200")
	if got != "-IOERR error or timeout connecting to target instance" {
		t.Fatalf("MIGRATE to dead target = %q", got)
	}
	// The key survives a failed migration.
	if got := bulk(t, sr, sc, "GET k"); got != "v" {
		t.Fatalf("source GET k after failed MIGRATE = %q want v", got)
	}
}

// TestMigratePreservesTTL carries the remaining TTL across to the target.
func TestMigratePreservesTTL(t *testing.T) {
	sr, sc := startData(t)
	_, _, dstHost, dstPort := startDataAddr(t)

	_ = sendLine(t, sr, sc, "SET k v EX 1000")
	if got := sendLine(t, sr, sc, "MIGRATE "+dstHost+" "+dstPort+" k 0 5000"); got != "+OK" {
		t.Fatalf("MIGRATE = %q want +OK", got)
	}
	dr, dc := dial(t, net.JoinHostPort(dstHost, dstPort))
	ttl := sendLine(t, dr, dc, "TTL k")
	if ttl == ":-1" || ttl == ":-2" {
		t.Fatalf("target TTL k = %q want a positive ttl", ttl)
	}
}

// dial opens a second client to an already running server.
func dial(t *testing.T, addr string) (*bufio.Reader, net.Conn) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	return bufio.NewReader(conn), conn
}
