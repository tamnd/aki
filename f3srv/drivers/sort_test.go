package drivers

import "testing"

// TestSortListNumeric drives the plain numeric sort over a list, the default
// order and its DESC reverse, exact RESP through the real dispatch and shard.
func TestSortListNumeric(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "3", "1", "2", "5", "4")
	expect(t, br, ":5\r\n")

	send(t, nc, "SORT", "l")
	expect(t, br, arr("1", "2", "3", "4", "5"))
	send(t, nc, "SORT", "l", "DESC")
	expect(t, br, arr("5", "4", "3", "2", "1"))
	send(t, nc, "SORT", "l", "ASC")
	expect(t, br, arr("1", "2", "3", "4", "5"))
}

// TestSortAlpha sorts lexicographically with ALPHA, over a list of non-numeric
// members that a numeric sort would reject.
func TestSortAlpha(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "banana", "apple", "cherry")
	expect(t, br, ":3\r\n")

	send(t, nc, "SORT", "l", "ALPHA")
	expect(t, br, arr("apple", "banana", "cherry"))
	send(t, nc, "SORT", "l", "ALPHA", "DESC")
	expect(t, br, arr("cherry", "banana", "apple"))

	// Numeric sort of non-numeric members is an error.
	send(t, nc, "SORT", "l")
	expect(t, br, "-ERR One or more scores can't be converted into double\r\n")
}

// TestSortLimit windows the sorted result: LIMIT offset count, count -1 meaning
// to the end, and an offset past the end yielding empty.
func TestSortLimit(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "5", "4", "3", "2", "1")
	expect(t, br, ":5\r\n")

	send(t, nc, "SORT", "l", "LIMIT", "1", "2")
	expect(t, br, arr("2", "3"))
	send(t, nc, "SORT", "l", "LIMIT", "2", "-1")
	expect(t, br, arr("3", "4", "5"))
	send(t, nc, "SORT", "l", "LIMIT", "10", "5")
	expect(t, br, "*0\r\n")
}

// TestSortSetAndZset sorts the other two source types. A set of integers sorts
// numerically; a zset sorts its members and ignores the scores entirely.
func TestSortSetAndZset(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SADD", "s", "3", "1", "2")
	expect(t, br, ":3\r\n")
	send(t, nc, "SORT", "s")
	expect(t, br, arr("1", "2", "3"))

	// Scores 100/5/20 are attached in an order unrelated to the members; SORT
	// sorts the members 3/1/2 numerically, proving the scores are dropped.
	send(t, nc, "ZADD", "z", "100", "3", "5", "1", "20", "2")
	expect(t, br, ":3\r\n")
	send(t, nc, "SORT", "z")
	expect(t, br, arr("1", "2", "3"))
	send(t, nc, "SORT", "z", "ALPHA", "DESC")
	expect(t, br, arr("3", "2", "1"))
}

// TestSortMissingAndWrongType: a missing key sorts to an empty array, a string
// (or any non-collection type) is a WRONGTYPE error.
func TestSortMissingAndWrongType(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SORT", "nope")
	expect(t, br, "*0\r\n")

	send(t, nc, "SET", "str", "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SORT", "str")
	expect(t, br, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestSortByNosort: a BY pattern with no '*' cannot name a key, so it is the
// nosort signal: elements return in stored order, unsorted.
func TestSortByNosort(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "3", "1", "2")
	expect(t, br, ":3\r\n")

	send(t, nc, "SORT", "l", "BY", "weight_none")
	expect(t, br, arr("3", "1", "2"))
	// LIMIT still applies over the stored order.
	send(t, nc, "SORT", "l", "BY", "nosort", "LIMIT", "0", "2")
	expect(t, br, arr("3", "1"))
}

// TestSortRoAndDeferredOptions: SORT_RO shares the plain core; the fan-wave rows
// (BY pattern, GET, STORE) report a clear not-yet error, and STORE on SORT_RO is
// a syntax error since it is not part of that command's grammar.
func TestSortRoAndDeferredOptions(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "2", "1", "3")
	expect(t, br, ":3\r\n")

	send(t, nc, "SORT_RO", "l")
	expect(t, br, arr("1", "2", "3"))
	send(t, nc, "SORT_RO", "l", "ALPHA", "LIMIT", "0", "2")
	expect(t, br, arr("1", "2"))

	send(t, nc, "SORT", "l", "BY", "w_*")
	expect(t, br, "-ERR SORT BY with a key pattern is not yet supported\r\n")
	send(t, nc, "SORT", "l", "GET", "#")
	expect(t, br, "-ERR SORT GET is not yet supported\r\n")
	send(t, nc, "SORT", "l", "STORE", "dest")
	expect(t, br, "-ERR SORT STORE is not yet supported\r\n")
	send(t, nc, "SORT_RO", "l", "STORE", "dest")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SORT", "l", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
}
