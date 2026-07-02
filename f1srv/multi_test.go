package f1srv

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"
)

// dialTwoTestServers starts one server and returns two independent client connections to
// it, so a test can watch a key on one connection and mutate it from the other, the way a
// real optimistic-lock race plays out. The cleanup closes both connections and the server.
func dialTwoTestServers(t *testing.T) (*bufio.ReadWriter, *bufio.ReadWriter, func()) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64, NetMode: os.Getenv("F1SRV_TEST_NET")}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	dial := func() (net.Conn, *bufio.ReadWriter) {
		conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	}
	c1, rw1 := dial()
	c2, rw2 := dial()
	cleanup := func() {
		c1.Close()
		c2.Close()
		srv.Close()
	}
	return rw1, rw2, cleanup
}

// TestMultiExec covers a plain transaction: MULTI opens, each command queues, and EXEC runs
// them back to back and frames the replies as one array. The queued writes take effect only
// at EXEC, so a GET issued after EXEC sees the final state.
func TestMultiExec(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "INCR", "n")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "GET", "k")
	expect(t, rw, "+QUEUED")

	cmd(t, rw, "EXEC")
	expect(t, rw, "*3")
	expect(t, rw, "+OK") // SET
	expect(t, rw, ":1")  // INCR n
	expect(t, rw, "$v")  // GET k

	// After EXEC the writes are durable in the keyspace.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v")
	cmd(t, rw, "GET", "n")
	expect(t, rw, "$1")
}

// TestMultiDiscard queues commands then throws them away; none of them run.
func TestMultiDiscard(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "DISCARD")
	expect(t, rw, "+OK")

	// The queued SET never ran.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$-1")

	// A fresh MULTI works after a DISCARD.
	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "PING")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "EXEC")
	expect(t, rw, "*1")
	expect(t, rw, "+PONG")
}

// TestMultiExecAbort covers EXECABORT: queueing an unknown command flags the transaction,
// and EXEC then refuses to run any of it.
func TestMultiExecAbort(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "NOSUCHCMD", "x")
	expect(t, rw, "-ERR unknown command 'NOSUCHCMD'")
	cmd(t, rw, "EXEC")
	expect(t, rw, "-EXECABORT Transaction discarded because of previous errors.")

	// Nothing ran, and the connection is usable again.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$-1")
	cmd(t, rw, "PING")
	expect(t, rw, "+PONG")
}

// TestMultiRunErrorNotAbort shows a command that queues fine but errors at run time (a
// WRONGTYPE) does not abort the transaction: the error is one element of the EXEC array and
// the other commands still run, matching Redis's no-rollback rule.
func TestMultiRunErrorNotAbort(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "str")
	expect(t, rw, "+OK")

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "LPUSH", "s", "x") // wrong type, but a known command: queues
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "SET", "ok", "1")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "EXEC")
	expect(t, rw, "*2")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	expect(t, rw, "+OK")

	cmd(t, rw, "GET", "ok")
	expect(t, rw, "$1")
}

// TestWatchAbort is the optimistic-lock race: conn1 watches a key, conn2 writes it, and
// conn1's EXEC returns a null array because the watched key moved. The queued write does not
// run.
func TestWatchAbort(t *testing.T) {
	rw1, rw2, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, rw1, "SET", "k", "0")
	expect(t, rw1, "+OK")

	cmd(t, rw1, "WATCH", "k")
	expect(t, rw1, "+OK")

	// A different client touches the watched key.
	cmd(t, rw2, "SET", "k", "changed")
	expect(t, rw2, "+OK")

	cmd(t, rw1, "MULTI")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "SET", "k", "fromtx")
	expect(t, rw1, "+QUEUED")
	cmd(t, rw1, "EXEC")
	expect(t, rw1, "*-1") // aborted: watched key moved

	// The transaction's write never landed; the value is what conn2 set.
	cmd(t, rw1, "GET", "k")
	expect(t, rw1, "$changed")
}

// TestWatchClean is the happy path: nothing touches the watched key, so EXEC runs normally.
func TestWatchClean(t *testing.T) {
	rw1, rw2, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, rw1, "SET", "k", "0")
	expect(t, rw1, "+OK")
	cmd(t, rw2, "SET", "other", "x") // an unrelated key moving does not dirty the watch
	expect(t, rw2, "+OK")

	cmd(t, rw1, "WATCH", "k")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "MULTI")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "SET", "k", "fromtx")
	expect(t, rw1, "+QUEUED")
	cmd(t, rw1, "EXEC")
	expect(t, rw1, "*1")
	expect(t, rw1, "+OK")

	cmd(t, rw1, "GET", "k")
	expect(t, rw1, "$fromtx")
}

// TestUnwatchClearsAbort shows UNWATCH drops the guard, so a later write to the once-watched
// key does not abort a following EXEC.
func TestUnwatchClearsAbort(t *testing.T) {
	rw1, rw2, cleanup := dialTwoTestServers(t)
	defer cleanup()

	cmd(t, rw1, "SET", "k", "0")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "WATCH", "k")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "UNWATCH")
	expect(t, rw1, "+OK")

	cmd(t, rw2, "SET", "k", "changed")
	expect(t, rw2, "+OK")

	cmd(t, rw1, "MULTI")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "SET", "k", "fromtx")
	expect(t, rw1, "+QUEUED")
	cmd(t, rw1, "EXEC")
	expect(t, rw1, "*1")
	expect(t, rw1, "+OK")
	cmd(t, rw1, "GET", "k")
	expect(t, rw1, "$fromtx")
}

// TestTxErrors covers the misuse errors: EXEC/DISCARD without MULTI, nested MULTI, and WATCH
// inside MULTI. Each leaves the connection usable.
func TestTxErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "EXEC")
	expect(t, rw, "-ERR EXEC without MULTI")
	cmd(t, rw, "DISCARD")
	expect(t, rw, "-ERR DISCARD without MULTI")

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "MULTI")
	expect(t, rw, "-ERR MULTI calls can not be nested")
	cmd(t, rw, "WATCH", "k")
	expect(t, rw, "-ERR WATCH inside MULTI is not allowed")
	// The transaction still stands after the two errors; discard it to clean up.
	cmd(t, rw, "DISCARD")
	expect(t, rw, "+OK")
}

// TestReset returns the connection to a clean state mid-transaction and replies +RESET.
func TestReset(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MULTI")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+QUEUED")
	cmd(t, rw, "RESET")
	expect(t, rw, "+RESET")

	// The transaction was dropped: EXEC now errors because no MULTI is open.
	cmd(t, rw, "EXEC")
	expect(t, rw, "-ERR EXEC without MULTI")
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$-1")
}
