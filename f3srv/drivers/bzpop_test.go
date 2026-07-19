package drivers

import (
	"testing"
	"time"
)

// The blocking sorted-set pops at the driver seam (spec 2064/f3/12 section 6.7).
// These prove the zset waiter substrate end to end over a real socket: a BZPOPMIN
// that parks is served by a later ZADD on another connection, an immediate serve
// when the key already holds members takes the non-blocking path, and a command
// pipelined behind an unresolved BZPOPMIN is held until the block clears. They all
// block on a single key so the wake ZADD routes to the same owner, staying inside
// the co-located contract this slice serves.

// TestBzpopminImmediate serves BZPOPMIN from a key that already holds members: it
// pops the lowest-scored one and replies [key, member, score] without ever parking.
func TestBzpopminImmediate(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "ZADD", "zs", "1", "a", "2", "b", "3", "c")
	expect(t, br, ":3\r\n")

	// Lowest score first.
	writeCmd(t, nc, "BZPOPMIN", "zs", "0")
	expect(t, br, "*3\r\n$2\r\nzs\r\n$1\r\na\r\n$1\r\n1\r\n")
	// Highest score with BZPOPMAX.
	writeCmd(t, nc, "BZPOPMAX", "zs", "0")
	expect(t, br, "*3\r\n$2\r\nzs\r\n$1\r\nc\r\n$1\r\n3\r\n")
	// One member left.
	writeCmd(t, nc, "ZCARD", "zs")
	expect(t, br, ":1\r\n")
}

// TestBzpopminServedAcrossConns blocks one connection on a missing key and serves
// it with a ZADD on another. The blocked client receives [key, member, score]. The
// assertion holds whether the BZPOPMIN parks first or the ZADD lands first; the
// sleep steers the common case through the park-and-wake path.
func TestBzpopminServedAcrossConns(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	writeCmd(t, c1, "BZPOPMIN", "bz", "0")
	time.Sleep(50 * time.Millisecond) // let the BZPOPMIN park

	writeCmd(t, c2, "ZADD", "bz", "5", "m")
	expect(t, br2, ":1\r\n")
	expect(t, br1, "*3\r\n$2\r\nbz\r\n$1\r\nm\r\n$1\r\n5\r\n")
	// The served ZADD drained the only member, so the key is gone.
	writeCmd(t, c2, "EXISTS", "bz")
	expect(t, br2, ":0\r\n")
}

// TestBzpopmaxServedByZincrby proves ZINCRBY wakes a blocked BZPOPMAX too: the
// increment creates the member, and the waiter pops it off the high end.
func TestBzpopmaxServedByZincrby(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	writeCmd(t, c1, "BZPOPMAX", "iz", "0")
	time.Sleep(50 * time.Millisecond)

	writeCmd(t, c2, "ZINCRBY", "iz", "7", "x")
	expect(t, br2, "$1\r\n7\r\n")
	expect(t, br1, "*3\r\n$2\r\niz\r\n$1\r\nx\r\n$1\r\n7\r\n")
}

// TestBzmpopServedAcrossConns blocks a BZMPOP on a missing key, then grows the key
// past the requested count with one ZADD: the waiter pops up to COUNT members off
// the MIN end and replies [key, [[member, score], ...]].
func TestBzmpopServedAcrossConns(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	writeCmd(t, c1, "BZMPOP", "0", "1", "mz", "MIN", "COUNT", "2")
	time.Sleep(50 * time.Millisecond)

	writeCmd(t, c2, "ZADD", "mz", "1", "a", "2", "b", "3", "c")
	expect(t, br2, ":3\r\n")
	// Two lowest members, each an inner [member, score] pair.
	expect(t, br1, "*2\r\n$2\r\nmz\r\n*2\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n")
	// The third member survived the count-bounded serve.
	writeCmd(t, c2, "ZRANGE", "mz", "0", "-1")
	expect(t, br2, "*1\r\n$1\r\nc\r\n")
}

// TestBzpopminPipelineBarrier pins the reader barrier: a PING pipelined behind a
// parked BZPOPMIN on the same connection is held, not answered, until the pop is
// served, and then both replies arrive in request order.
func TestBzpopminPipelineBarrier(t *testing.T) {
	srv, c1, br1 := startServer(t)
	c2, br2 := secondConn(t, srv)

	writeCmd(t, c1, "BZPOPMIN", "pz", "0")
	writeCmd(t, c1, "PING")

	c1.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if _, err := br1.ReadByte(); err == nil {
		t.Fatal("a reply arrived before the BZPOPMIN was served")
	}
	c1.SetReadDeadline(time.Time{})

	writeCmd(t, c2, "ZADD", "pz", "1", "v")
	expect(t, br2, ":1\r\n")
	expect(t, br1, "*3\r\n$2\r\npz\r\n$1\r\nv\r\n$1\r\n1\r\n+PONG\r\n")
}

// TestBzpopminTimeout parks a BZPOPMIN with a short finite timeout and lets it
// fire: the client gets the RESP2 null array and unblocks, and a command after it
// runs normally.
func TestBzpopminTimeout(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "BZPOPMIN", "tz", "0.1")
	expect(t, br, "*-1\r\n")
	// The connection is live and unblocked after the timeout.
	writeCmd(t, nc, "PING")
	expect(t, br, "+PONG\r\n")
}

// TestBzpopminNegTimeout rejects a negative timeout with Redis's exact error and
// leaves the connection usable.
func TestBzpopminNegTimeout(t *testing.T) {
	_, nc, br := startServer(t)

	writeCmd(t, nc, "BZPOPMIN", "nz", "-1")
	expect(t, br, "-ERR timeout is negative\r\n")
	writeCmd(t, nc, "PING")
	expect(t, br, "+PONG\r\n")
}
