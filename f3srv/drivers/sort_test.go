package drivers

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

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

// TestSortRoAndSyntax: SORT_RO shares the plain core and accepts BY/GET (its
// read-only fan) but not STORE, which is not part of its grammar; a bogus token
// and a truncated option are syntax errors on both.
func TestSortRoAndSyntax(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "2", "1", "3")
	expect(t, br, ":3\r\n")

	send(t, nc, "SORT_RO", "l")
	expect(t, br, arr("1", "2", "3"))
	send(t, nc, "SORT_RO", "l", "ALPHA", "LIMIT", "0", "2")
	expect(t, br, arr("1", "2"))
	// GET # is part of the read-only grammar and returns the elements themselves.
	send(t, nc, "SORT_RO", "l", "GET", "#")
	expect(t, br, arr("1", "2", "3"))

	// STORE is a syntax error under SORT_RO.
	send(t, nc, "SORT_RO", "l", "STORE", "dest")
	expect(t, br, "-ERR syntax error\r\n")
	// A bogus token and a truncated GET/STORE are syntax errors.
	send(t, nc, "SORT", "l", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SORT", "l", "GET")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "SORT", "l", "STORE")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestSortByPatternDeref sorts by a dereferenced weight key per element (BY
// pattern with '*'). The weight keys land on arbitrary shards, so this drives the
// read-only owner-hop fan. A missing weight counts as 0 for a numeric sort; ALPHA
// orders the weight strings, with a missing weight sorting first.
func TestSortByPatternDeref(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "1", "2", "3")
	expect(t, br, ":3\r\n")
	send(t, nc, "MSET", "weight_1", "30", "weight_2", "10", "weight_3", "20")
	expect(t, br, "+OK\r\n")

	// Numeric: order by weight 10(2) < 20(3) < 30(1).
	send(t, nc, "SORT", "l", "BY", "weight_*")
	expect(t, br, arr("2", "3", "1"))
	send(t, nc, "SORT", "l", "BY", "weight_*", "DESC")
	expect(t, br, arr("1", "3", "2"))

	// A missing weight (weight_2 deleted) sorts as 0, ahead of the present ones.
	send(t, nc, "DEL", "weight_2")
	expect(t, br, ":1\r\n")
	send(t, nc, "SORT", "l", "BY", "weight_*")
	expect(t, br, arr("2", "3", "1"))
}

// TestSortGetProjection projects one or more GET patterns per element, including
// GET # (the element itself) and a pattern that misses on some elements (a nil
// bulk). The projected keys land on arbitrary shards, driving the projection fan.
func TestSortGetProjection(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "1", "2", "3")
	expect(t, br, ":3\r\n")
	// data_1 and data_3 present, data_2 missing.
	send(t, nc, "MSET", "data_1", "one", "data_3", "three")
	expect(t, br, "+OK\r\n")

	// GET # returns the element; the numeric sort orders 1,2,3.
	send(t, nc, "SORT", "l", "GET", "#")
	expect(t, br, arr("1", "2", "3"))

	// GET data_* projects the value, nil where missing (data_2).
	send(t, nc, "SORT", "l", "GET", "data_*")
	expect(t, br, "*3\r\n"+bulk("one")+"$-1\r\n"+bulk("three"))

	// Two GETs flatten per element: element then its data value.
	send(t, nc, "SORT", "l", "GET", "#", "GET", "data_*")
	expect(t, br, "*6\r\n"+bulk("1")+bulk("one")+bulk("2")+"$-1\r\n"+bulk("3")+bulk("three"))
}

// TestSortByAndGetHashAccess dereferences a hash field with the key->field form,
// for both the BY weight and a GET projection.
func TestSortByAndGetHashAccess(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "1", "2", "3")
	expect(t, br, ":3\r\n")
	send(t, nc, "HSET", "h_1", "w", "30", "name", "alice")
	expect(t, br, ":2\r\n")
	send(t, nc, "HSET", "h_2", "w", "10", "name", "bob")
	expect(t, br, ":2\r\n")
	send(t, nc, "HSET", "h_3", "w", "20", "name", "carol")
	expect(t, br, ":2\r\n")

	// BY h_*->w orders by 10(2) < 20(3) < 30(1); GET h_*->name projects the name.
	send(t, nc, "SORT", "l", "BY", "h_*->w", "GET", "h_*->name")
	expect(t, br, arr("bob", "carol", "alice"))
}

// TestSortStore writes the sorted result to a destination list and returns the
// stored length; a re-read confirms the contents. A store that yields nothing
// deletes the destination and returns 0.
func TestSortStore(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "3", "1", "2")
	expect(t, br, ":3\r\n")

	send(t, nc, "SORT", "l", "STORE", "dest")
	expect(t, br, ":3\r\n")
	send(t, nc, "LRANGE", "dest", "0", "-1")
	expect(t, br, arr("1", "2", "3"))

	// STORE with a GET projection stores the flattened projection, missing as "".
	send(t, nc, "MSET", "data_1", "one", "data_3", "three")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SORT", "l", "GET", "data_*", "STORE", "dest")
	expect(t, br, ":3\r\n")
	send(t, nc, "LRANGE", "dest", "0", "-1")
	expect(t, br, arr("one", "", "three"))

	// A store over an empty source deletes the destination and returns 0.
	send(t, nc, "SORT", "empty", "STORE", "dest")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXISTS", "dest")
	expect(t, br, ":0\r\n")
}

// TestSortStoreEvents pins the keyspace data events STORE fires: a non-empty
// result fires sortstore (list class) on the destination, and a store that
// produces nothing deletes an existing destination and fires the generic del.
func TestSortStoreEvents(t *testing.T) {
	srv := startPubsubServer(t)
	t.Cleanup(func() { shard.SetNotifyFlags(0) })
	subNc, subBr := dialPubsub(t, srv)
	pubNc, pubBr := dialPubsub(t, srv)

	send(t, pubNc, "CONFIG", "SET", "notify-keyspace-events", "KEA")
	expect(t, pubBr, "+OK\r\n")

	send(t, subNc, "PSUBSCRIBE", "__keyevent@0__:*")
	if k, ch, n := readSubConfirm(t, subBr); k != "psubscribe" || ch != "__keyevent@0__:*" || n != 1 {
		t.Fatalf("psubscribe confirm = %q %q %d", k, ch, n)
	}

	send(t, pubNc, "RPUSH", "l", "3", "1", "2")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "rpush", "l")

	// A non-empty store fires sortstore on the destination.
	send(t, pubNc, "SORT", "l", "STORE", "dest")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "sortstore", "dest")

	// A store over an empty source deletes the existing destination: generic del.
	send(t, pubNc, "SORT", "empty", "STORE", "dest")
	readRESP(t, pubBr)
	wantEvent(t, subBr, "del", "dest")
}
