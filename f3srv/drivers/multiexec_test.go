package drivers

import "testing"

// TestMultiExecBasic walks a plain transaction: MULTI opens it, each command is
// acknowledged with +QUEUED instead of running, and EXEC runs the queue in one
// step and answers the array of every command's reply.
func TestMultiExecBasic(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:foo", "bar")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "GET", "tx:foo")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*2\r\n+OK\r\n$3\r\nbar\r\n")

	// The effects landed: GET outside the transaction sees the write.
	send(t, nc, "GET", "tx:foo")
	expect(t, br, "$3\r\nbar\r\n")
}

// TestMultiExecEmpty runs an EXEC with an empty queue, which answers the empty
// array.
func TestMultiExecEmpty(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*0\r\n")
}

// TestExecWithoutMulti and its siblings check the standalone-verb errors: EXEC
// and DISCARD outside a transaction each report their own error.
func TestExecWithoutMulti(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "EXEC")
	expect(t, br, "-ERR EXEC without MULTI\r\n")
	send(t, nc, "DISCARD")
	expect(t, br, "-ERR DISCARD without MULTI\r\n")
}

// TestMultiNested checks a second MULTI inside an open transaction is an error
// and does not disturb the queue: the command queued before it still runs.
func TestMultiNested(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:n", "1")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "MULTI")
	expect(t, br, "-ERR MULTI calls can not be nested\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*1\r\n+OK\r\n")
}

// TestDiscard checks DISCARD throws the queue away: the queued write never
// happens.
func TestDiscard(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:d", "gone")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "DISCARD")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "tx:d")
	expect(t, br, "$-1\r\n")

	// After DISCARD the connection is out of MULTI: a plain command runs.
	send(t, nc, "SET", "tx:d", "now")
	expect(t, br, "+OK\r\n")
}

// TestExecAbort checks a queuing error (unknown command or bad arity) flags the
// transaction so EXEC aborts with EXECABORT and runs nothing.
func TestExecAbort(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "NOSUCHCMD", "x")
	expect(t, br, "-ERR unknown command 'NOSUCHCMD'\r\n")
	send(t, nc, "SET", "tx:a")
	expect(t, br, "-ERR wrong number of arguments for 'set' command\r\n")
	// A valid command still queues after the errors, but EXEC aborts anyway.
	send(t, nc, "SET", "tx:a", "1")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "-EXECABORT Transaction discarded because of previous errors.\r\n")

	// The abort ran nothing: the key stays absent.
	send(t, nc, "GET", "tx:a")
	expect(t, br, "$-1\r\n")
}

// TestWatchClean checks a WATCH whose key is untouched lets EXEC run normally.
func TestWatchClean(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "tx:w", "1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WATCH", "tx:w")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "tx:w")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*1\r\n$1\r\n1\r\n")
}

// TestWatchDirty checks a WATCHed key changed by another connection between
// WATCH and EXEC aborts the transaction with the null array and runs nothing.
func TestWatchDirty(t *testing.T) {
	s, nc, br := startServer(t)
	other, obr := dial(t, s)

	send(t, nc, "SET", "tx:wd", "1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WATCH", "tx:wd")
	expect(t, br, "+OK\r\n")

	// A second connection changes the key while the transaction is being built.
	send(t, other, "SET", "tx:wd", "2")
	expect(t, obr, "+OK\r\n")

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:wd", "3")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*-1\r\n")

	// The transaction ran nothing: the other connection's write stands.
	send(t, nc, "GET", "tx:wd")
	expect(t, br, "$1\r\n2\r\n")
}

// TestWatchDeleteDirty checks a WATCHed key deleted after WATCH also aborts
// EXEC: absence-versus-presence is a change even without a value to compare.
func TestWatchDeleteDirty(t *testing.T) {
	s, nc, br := startServer(t)
	other, obr := dial(t, s)

	send(t, nc, "SET", "tx:wdel", "1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WATCH", "tx:wdel")
	expect(t, br, "+OK\r\n")

	send(t, other, "DEL", "tx:wdel")
	expect(t, obr, ":1\r\n")

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:wdel", "3")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*-1\r\n")
}

// TestUnwatch checks UNWATCH forgets the baselines so a later change no longer
// aborts EXEC.
func TestUnwatch(t *testing.T) {
	s, nc, br := startServer(t)
	other, obr := dial(t, s)

	send(t, nc, "SET", "tx:uw", "1")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WATCH", "tx:uw")
	expect(t, br, "+OK\r\n")
	send(t, nc, "UNWATCH")
	expect(t, br, "+OK\r\n")

	send(t, other, "SET", "tx:uw", "2")
	expect(t, obr, "+OK\r\n")

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "tx:uw")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*1\r\n$1\r\n2\r\n")
}

// TestWatchInsideMulti checks WATCH after MULTI is an error and leaves the queue
// intact.
func TestWatchInsideMulti(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "WATCH", "tx:wim")
	expect(t, br, "-ERR WATCH inside MULTI is not allowed\r\n")
	send(t, nc, "PING")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*1\r\n+PONG\r\n")
}

// TestExecBlockingVerb checks a blocking pop queued in a transaction never parks
// under EXEC: an empty source answers the would-block null reply as its array
// element, so EXEC always returns.
func TestExecBlockingVerb(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "BLPOP", "tx:nolist", "0")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*1\r\n*-1\r\n")
}

// TestExecFanCommands checks multi-key fan commands run correctly inside EXEC:
// MSET writes every pair, MGET reads them back in order, DEL sums the deletes,
// each under the one held barrier.
func TestExecFanCommands(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "MSET", "tx:a", "1", "tx:b", "2")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "MGET", "tx:a", "tx:b", "tx:missing")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "DEL", "tx:a", "tx:b")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "EXEC")
	expect(t, br, "*3\r\n"+
		"+OK\r\n"+
		"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"+
		":2\r\n")
}

// TestResetClearsTransaction checks RESET unwinds a half-built MULTI: after it
// the connection is out of MULTI and a plain command runs.
func TestResetClearsTransaction(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MULTI")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "tx:r", "gone")
	expect(t, br, "+QUEUED\r\n")
	send(t, nc, "RESET")
	expect(t, br, "+RESET\r\n")

	// The queue is gone and MULTI is closed: EXEC now errors, a plain SET runs.
	send(t, nc, "EXEC")
	expect(t, br, "-ERR EXEC without MULTI\r\n")
	send(t, nc, "GET", "tx:r")
	expect(t, br, "$-1\r\n")
}
