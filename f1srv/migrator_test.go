package f1srv

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dialMigratorServer starts a server with the larger-than-memory string tier engaged (a
// segmented arena, a cold record region, and the background migrator) on a deliberately small
// arena, then returns a connected client plus a cleanup. It is the server-level analog of the
// engine's churnSegColdStore: the arena is far smaller than the dataset the test writes, so the
// migrator has to sink full segments cold for every write to keep landing.
func dialMigratorServer(t *testing.T) (*bufio.ReadWriter, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		Addr:               "127.0.0.1:0",
		IndexBuckets:       1 << 14,
		ArenaBytes:         4 << 20, // 4 MiB: smaller than the dataset below, so the migrator must drain
		ReadBufSize:        16 << 10,
		IncrStripes:        64,
		ArenaSegmented:     true,
		ArenaSegmentBytes:  256 << 10, // small segments so the 4 MiB arena tiles into many drainable segments
		ArenaOverflowBytes: 256 << 10,
		ColdRecordsPath:    filepath.Join(dir, "f1raw-cold.recs"),
		Migrator:           true,
	}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	cleanup := func() {
		conn.Close()
		srv.Close()
	}
	return rw, cleanup
}

// TestServerMigratorServesBeyondArena is the M3 slice-6b gate: with the migrator wired into the
// server, a string dataset larger than the resident arena is served correctly over the wire. It
// writes many more distinct keys than the 2 MiB arena can hold at once, so the migrator sinks
// full segments into the cold record region as they fill, and every SET must still return OK
// (the D12 backpressure waits on the migrator rather than returning an arena-full error). Then
// it reads every key back and checks its exact value, most now served from cold. Without the
// wiring the arena fills and SET errors after a few thousand keys.
func TestServerMigratorServesBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// Each value is 200 bytes, so the whole set is several arenas' worth of record bytes and the
	// arena cannot hold it at once. Pipeline the writes so the test is not one round trip per key.
	const n = 20000
	val := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		cmd(t, rw, "SET", fmt.Sprintf("k%08d", i), migVal(i, val))
	}
	for i := 0; i < n; i++ {
		expect(t, rw, "+OK")
	}

	// Every distinct key reads back its exact value, whether it ended up resident or cold.
	for i := 0; i < n; i++ {
		cmd(t, rw, "GET", fmt.Sprintf("k%08d", i))
	}
	for i := 0; i < n; i++ {
		want := "$" + migVal(i, val)
		if got := readReply(t, rw); got != want {
			t.Fatalf("GET k%08d = %q, want %q", i, got, want)
		}
	}
}

// TestServerMigratorConfigError checks that asking for the migrator without the segmented arena
// it needs is a clean configuration error surfaced by Listen, not a panic from EnableMigrator.
func TestServerMigratorConfigError(t *testing.T) {
	cfg := Config{
		Addr:            "127.0.0.1:0",
		IndexBuckets:    1 << 12,
		ArenaBytes:      1 << 20,
		ReadBufSize:     4 << 10,
		IncrStripes:     64,
		Migrator:        true, // but ArenaSegmented is false and ColdRecordsPath is empty
		ColdRecordsPath: "",
	}
	srv := New(cfg)
	if err := srv.Listen(); err == nil {
		srv.Close()
		t.Fatal("Listen accepted a migrator config with no segmented arena; want an error")
	}
}

// migVal tags the shared value body with the key index so a misrouted read (wrong key served)
// shows up as a value mismatch, not just a length match.
func migVal(i int, body string) string {
	return fmt.Sprintf("v%08d-%s", i, body)
}
