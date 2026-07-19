package drivers

import (
	"testing"
)

// MOVE relocates a key to another numbered database (dispatch/move.go). f3 keeps one
// keyspace, so MOVE has no destination and always declines with an honest error
// rather than being an unknown command. These check it validates the database
// argument the way SELECT does and never moves anything.

// TestMove checks the three declines: database 0 is the current keyspace (the
// source-and-destination error), any other index is out of range, and a non-integer
// is the value error. The key is left in place in every case.
func TestMove(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

	// Database 0 is the current keyspace: source and destination are the same.
	send(t, nc, "MOVE", "k", "0")
	expect(t, br, "-ERR source and destination objects are the same\r\n")

	// Any other index is out of range.
	send(t, nc, "MOVE", "k", "1")
	expect(t, br, "-ERR DB index is out of range\r\n")

	// Non-integer database.
	send(t, nc, "MOVE", "k", "xx")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	// The key never moved.
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")

	// The database check comes before the key, so a missing key answers the same
	// database error, not a not-found or a silent 0.
	send(t, nc, "MOVE", "gone", "0")
	expect(t, br, "-ERR source and destination objects are the same\r\n")
}

// TestSwapdb checks the single-database SWAPDB: swapping database 0 with itself is a
// confirmed no-op, any other index is out of range, and a non-integer is the value
// error.
func TestSwapdb(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")

	// Database 0 with itself: a confirmed no-op, data intact.
	send(t, nc, "SWAPDB", "0", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")

	// Either index non-zero is out of range.
	send(t, nc, "SWAPDB", "0", "1")
	expect(t, br, "-ERR DB index is out of range\r\n")
	send(t, nc, "SWAPDB", "2", "0")
	expect(t, br, "-ERR DB index is out of range\r\n")

	// Non-integer index.
	send(t, nc, "SWAPDB", "x", "0")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
}
