package drivers

import (
	"bufio"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

// startDurableServer stands up a server backed by the shared durable .aki file at
// path and dials one connection to it. Unlike startServer it does not close the
// server in cleanup, so a test can stop the first run explicitly (to land the
// durable bytes) and reopen the same path for the restart run; each returned server
// is closed once by the test. Only the connection is torn down in cleanup.
func startDurableServer(t *testing.T, path string) (*Server, net.Conn, *bufio.Reader) {
	t.Helper()
	srv, err := Listen(Options{
		Addr: "127.0.0.1:0", Shards: 2, ArenaBytes: 4 << 20, SegBytes: 1 << 18,
		AkiPath: path, ConnShape: testConnShape(), NetDriver: testNetDriver(),
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return srv, nc, bufio.NewReader(nc)
}

// TestDurableRecoversAllTypesAcrossRestart is the end-to-end proof of the M8
// durable arc at the server level. A first server opens the shared .aki file,
// takes one write of every type through the real command surface over a socket,
// and closes, which joins the group-commit writer and lands the durable bytes. A
// second server opens the same path, and every key, the string and all five
// collection types, is back before the first command. This is the row the durable
// engine already had unit tests for per type but that no server ever exercised:
// before the -aki flag and the RecoverColl wiring the string would return and the
// set, zset, hash, list, and stream would be silently gone.
func TestDurableRecoversAllTypesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.aki")

	// First run: one key of each type through the command surface.
	srv1, nc1, br1 := startDurableServer(t, path)
	if got := sendCmd(t, br1, nc1, "SET", "str", "hello"); got != "OK" {
		t.Fatalf("SET = %v, want OK", got)
	}
	if got := sendCmd(t, br1, nc1, "SADD", "set", "a", "b", "c"); got != int64(3) {
		t.Fatalf("SADD = %v, want 3", got)
	}
	if got := sendCmd(t, br1, nc1, "ZADD", "zs", "1.5", "m1", "2.5", "m2"); got != int64(2) {
		t.Fatalf("ZADD = %v, want 2", got)
	}
	if got := sendCmd(t, br1, nc1, "HSET", "h", "f1", "v1", "f2", "v2"); got != int64(2) {
		t.Fatalf("HSET = %v, want 2", got)
	}
	if got := sendCmd(t, br1, nc1, "RPUSH", "lst", "x", "y", "z"); got != int64(3) {
		t.Fatalf("RPUSH = %v, want 3", got)
	}
	if got := sendCmd(t, br1, nc1, "XADD", "stm", "1-1", "field", "val"); got != "1-1" {
		t.Fatalf("XADD = %v, want 1-1", got)
	}
	// Close the connection before the server so Close's conns.Wait returns, then
	// stop the server, which joins the group-commit writer and lands the bytes.
	nc1.Close()
	srv1.Close()

	// Second run: reopen the same file. Recovery ran during open, so every key is
	// live before the first command.
	srv2, nc2, br2 := startDurableServer(t, path)
	// Close the connection before the server: Close's conns.Wait blocks on a live
	// connection, and t.Cleanup (which closes nc2) runs after this deferred Close.
	defer func() { nc2.Close(); srv2.Close() }()

	if got := sendCmd(t, br2, nc2, "GET", "str"); got != "hello" {
		t.Fatalf("GET str after restart = %v, want hello", got)
	}
	if got := sendCmd(t, br2, nc2, "SCARD", "set"); got != int64(3) {
		t.Fatalf("SCARD after restart = %v, want 3", got)
	}
	if got := sendCmd(t, br2, nc2, "SISMEMBER", "set", "b"); got != int64(1) {
		t.Fatalf("SISMEMBER b after restart = %v, want 1", got)
	}
	if got := sendCmd(t, br2, nc2, "ZSCORE", "zs", "m2"); got != "2.5" {
		t.Fatalf("ZSCORE m2 after restart = %v, want 2.5", got)
	}
	if got := sendCmd(t, br2, nc2, "HGET", "h", "f2"); got != "v2" {
		t.Fatalf("HGET f2 after restart = %v, want v2", got)
	}
	got := sendCmd(t, br2, nc2, "LRANGE", "lst", "0", "-1")
	arr, ok := got.([]any)
	if !ok || len(arr) != 3 || arr[0] != "x" || arr[1] != "y" || arr[2] != "z" {
		t.Fatalf("LRANGE after restart = %v, want [x y z]", got)
	}
	if got := sendCmd(t, br2, nc2, "XLEN", "stm"); got != int64(1) {
		t.Fatalf("XLEN after restart = %v, want 1", got)
	}
	// A key never written stays absent: recovery invents nothing.
	if got := sendCmd(t, br2, nc2, "EXISTS", "ghost"); got != int64(0) {
		t.Fatalf("EXISTS ghost after restart = %v, want 0", got)
	}
}

// TestDurableRecoversSeparatedValue is the large-value half of the restart proof: a
// value past the inline max (separated band) written through the socket must survive
// a restart. The server runs uncapped, the full-durability shape, so before the
// durable-mode spill floor the value stayed arena-resident and the reopen failed the
// whole open on an arena-resident word it could not deref. Now the value spills to
// the shared value region on write and reads back whole.
func TestDurableRecoversSeparatedValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.aki")
	val := strings.Repeat("z", 4000) // 1024 < 4000 < 64KiB: one separated run

	srv1, nc1, br1 := startDurableServer(t, path)
	if got := sendCmd(t, br1, nc1, "SET", "big", val); got != "OK" {
		t.Fatalf("SET big = %v, want OK", got)
	}
	nc1.Close()
	srv1.Close()

	srv2, nc2, br2 := startDurableServer(t, path)
	defer func() { nc2.Close(); srv2.Close() }()
	if got := sendCmd(t, br2, nc2, "GET", "big"); got != val {
		gs, _ := got.(string)
		t.Fatalf("GET big after restart = %d bytes, want %d", len(gs), len(val))
	}
	if got := sendCmd(t, br2, nc2, "STRLEN", "big"); got != int64(len(val)) {
		t.Fatalf("STRLEN big after restart = %v, want %d", got, len(val))
	}
}

// TestDurableRecoversChunkedValue is the multi-chunk half of the restart proof: a
// value past the chunk min, so it lives as a directory of value-log runs, written
// through the socket must survive a restart whole. Before the chunked recovery
// branch one value this size failed the whole reopen closed, so a single large key
// bricked a durable restart. Now the record row carries the durable chunk directory
// and recovery reassembles the value from the value log.
func TestDurableRecoversChunkedValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "durable.aki")
	val := strings.Repeat("q", 200000) // > 64KiB: spans several chunks

	srv1, nc1, br1 := startDurableServer(t, path)
	if got := sendCmd(t, br1, nc1, "SET", "huge", val); got != "OK" {
		t.Fatalf("SET huge = %v, want OK", got)
	}
	nc1.Close()
	srv1.Close()

	srv2, nc2, br2 := startDurableServer(t, path)
	defer func() { nc2.Close(); srv2.Close() }()
	if got := sendCmd(t, br2, nc2, "GET", "huge"); got != val {
		gs, _ := got.(string)
		t.Fatalf("GET huge after restart = %d bytes, want %d", len(gs), len(val))
	}
	if got := sendCmd(t, br2, nc2, "STRLEN", "huge"); got != int64(len(val)) {
		t.Fatalf("STRLEN huge after restart = %v, want %d", got, len(val))
	}
}
