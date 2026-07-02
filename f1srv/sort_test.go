package f1srv

import (
	"bufio"
	"sort"
	"testing"
)

// sortReply reads an array reply and returns each element as its raw readReply string, so a nil
// bulk comes back as "$-1" and a value as "$value". This keeps GET-miss nulls distinguishable
// from empty-string values, which readArray flattens together.
func sortReply(t *testing.T, rw *bufio.ReadWriter) []string {
	t.Helper()
	h := readReply(t, rw)
	if len(h) == 0 || h[0] != '*' {
		t.Fatalf("array header = %q, want an array", h)
	}
	n := 0
	for _, ch := range h[1:] {
		n = n*10 + int(ch-'0')
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = readReply(t, rw)
	}
	return out
}

func eqSort(t *testing.T, rw *bufio.ReadWriter, want []string) {
	t.Helper()
	got := sortReply(t, rw)
	if len(got) != len(want) {
		t.Fatalf("reply = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("reply = %v, want %v", got, want)
		}
	}
}

// TestSortListNumeric sorts a list numerically, which is the default, then reverses it with DESC.
func TestSortListNumeric(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "3", "1", "2", "10", "5")
	expect(t, rw, ":5")
	cmd(t, rw, "SORT", "l")
	eqSort(t, rw, []string{"$1", "$2", "$3", "$5", "$10"})
	cmd(t, rw, "SORT", "l", "DESC")
	eqSort(t, rw, []string{"$10", "$5", "$3", "$2", "$1"})
}

// TestSortAlpha compares as bytes under ALPHA and rejects a non-numeric list without it.
func TestSortAlpha(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "banana", "apple", "cherry")
	expect(t, rw, ":3")
	cmd(t, rw, "SORT", "l", "ALPHA")
	eqSort(t, rw, []string{"$apple", "$banana", "$cherry"})
	cmd(t, rw, "SORT", "l", "ALPHA", "DESC")
	eqSort(t, rw, []string{"$cherry", "$banana", "$apple"})
	cmd(t, rw, "SORT", "l")
	expect(t, rw, "-ERR One or more scores can't be converted into double")
}

// TestSortLimit windows the sorted result, clamps an offset past the end to empty, and treats a
// negative count as "everything from the offset".
func TestSortLimit(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "3", "1", "2", "10", "5")
	expect(t, rw, ":5")
	cmd(t, rw, "SORT", "l", "LIMIT", "1", "2")
	eqSort(t, rw, []string{"$2", "$3"})
	cmd(t, rw, "SORT", "l", "LIMIT", "10", "5")
	eqSort(t, rw, nil)
	cmd(t, rw, "SORT", "l", "LIMIT", "1", "-1")
	eqSort(t, rw, []string{"$2", "$3", "$5", "$10"})
}

// TestSortEmptyElementRejected proves an empty list element fails a numeric sort: the element is
// always present, so it must parse as a number, unlike a missing BY key.
func TestSortEmptyElementRejected(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "", "2", "1")
	expect(t, rw, ":3")
	cmd(t, rw, "SORT", "l")
	expect(t, rw, "-ERR One or more scores can't be converted into double")
}

// TestSortSet sorts a set numerically and lexically. A set has no inherent order, so the sort
// itself supplies the order.
func TestSortSet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "5", "3", "9", "1")
	expect(t, rw, ":4")
	cmd(t, rw, "SORT", "s")
	eqSort(t, rw, []string{"$1", "$3", "$5", "$9"})
	cmd(t, rw, "SADD", "a", "foo", "bar", "baz")
	expect(t, rw, ":3")
	cmd(t, rw, "SORT", "a", "ALPHA")
	eqSort(t, rw, []string{"$bar", "$baz", "$foo"})
}

// TestSortSetNosortStoreForced proves a BY-nosort set is returned unsorted on a read but is
// forced into an ALPHA sort when STORE is present, matching Redis's storekey special case.
func TestSortSetNosortStoreForced(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "foo", "bar", "baz")
	expect(t, rw, ":3")

	// Read form: order is undefined, but the members must all be present.
	cmd(t, rw, "SORT", "s", "BY", "nosort")
	got := sortReply(t, rw)
	sort.Strings(got)
	if len(got) != 3 || got[0] != "$bar" || got[1] != "$baz" || got[2] != "$foo" {
		t.Fatalf("BY nosort members = %v", got)
	}

	// STORE forces an ALPHA sort so the stored list is deterministic.
	cmd(t, rw, "SORT", "s", "BY", "nosort", "STORE", "d")
	expect(t, rw, ":3")
	cmd(t, rw, "LRANGE", "d", "0", "-1")
	eqSort(t, rw, []string{"$bar", "$baz", "$foo"})
}

// TestSortZsetScoreOrder proves a sorted set contributes its members in score order under
// BY-nosort, and rejects a numeric sort of non-numeric members without it.
func TestSortZsetScoreOrder(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "10", "a", "5", "b", "20", "c", "1", "d")
	expect(t, rw, ":4")
	cmd(t, rw, "SORT", "z", "BY", "nosort", "ALPHA")
	eqSort(t, rw, []string{"$d", "$b", "$a", "$c"})
	cmd(t, rw, "SORT", "z", "ALPHA")
	eqSort(t, rw, []string{"$a", "$b", "$c", "$d"})
	cmd(t, rw, "SORT", "z")
	expect(t, rw, "-ERR One or more scores can't be converted into double")
}

// TestSortByWeights sorts by external weight keys. A pattern with no '*' disables sorting, and a
// missing weight key counts as 0 without an error.
func TestSortByWeights(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "MSET", "w_1", "30", "w_2", "10", "w_3", "20")
	expect(t, rw, "+OK")
	cmd(t, rw, "SORT", "l", "BY", "w_*")
	eqSort(t, rw, []string{"$2", "$3", "$1"})
	cmd(t, rw, "SORT", "l", "BY", "w_*", "DESC")
	eqSort(t, rw, []string{"$1", "$3", "$2"})
	// A missing weight key is 0 for every element, so the source order is kept (stable sort).
	cmd(t, rw, "SORT", "l", "BY", "absent_*")
	eqSort(t, rw, []string{"$1", "$2", "$3"})
}

// TestSortByPresentEmptyRejected proves a BY key that exists but holds an empty or whitespace
// value fails the numeric sort, unlike a key that is missing entirely.
func TestSortByPresentEmptyRejected(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "SET", "w_1", "")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "w_2", "5")
	expect(t, rw, "+OK")
	cmd(t, rw, "SORT", "l", "BY", "w_*")
	expect(t, rw, "-ERR One or more scores can't be converted into double")
}

// TestSortGet expands each element through GET patterns: '#' is the element itself, an external
// key is dereferenced, and a miss is a nil reply.
func TestSortGet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "MSET", "w_1", "30", "w_2", "10", "w_3", "20", "d_1", "one", "d_2", "two", "d_3", "three")
	expect(t, rw, "+OK")
	cmd(t, rw, "SORT", "l", "BY", "w_*", "GET", "#", "GET", "d_*")
	eqSort(t, rw, []string{"$2", "$two", "$3", "$three", "$1", "$one"})
	// A GET pattern that misses is a nil bulk.
	cmd(t, rw, "SORT", "l", "GET", "miss_*")
	eqSort(t, rw, []string{"$-1", "$-1", "$-1"})
}

// TestSortByHashField dereferences BY and GET through a hash field with the key->field syntax.
func TestSortByHashField(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "1", "2", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "HSET", "h_1", "w", "3", "d", "x")
	expect(t, rw, ":2")
	cmd(t, rw, "HSET", "h_2", "w", "1", "d", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "HSET", "h_3", "w", "2", "d", "z")
	expect(t, rw, ":2")
	cmd(t, rw, "SORT", "l", "BY", "h_*->w", "GET", "h_*->d")
	eqSort(t, rw, []string{"$y", "$z", "$x"})
	// A missing hash field weight is 0, not an error.
	cmd(t, rw, "SORT", "l", "BY", "h_*->none")
	eqSort(t, rw, []string{"$1", "$2", "$3"})
}

// TestSortStore writes the result into a fresh list, replies with the count, and deletes the
// destination when the result is empty.
func TestSortStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "src", "3", "1", "2")
	expect(t, rw, ":3")
	cmd(t, rw, "SORT", "src", "STORE", "dst")
	expect(t, rw, ":3")
	cmd(t, rw, "TYPE", "dst")
	expect(t, rw, "+list")
	cmd(t, rw, "LRANGE", "dst", "0", "-1")
	eqSort(t, rw, []string{"$1", "$2", "$3"})
	// Storing an empty result deletes the destination and replies 0.
	cmd(t, rw, "SORT", "missing", "STORE", "dst")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "dst")
	expect(t, rw, ":0")
	// A GET miss stores as an empty string, not a nil.
	cmd(t, rw, "SORT", "src", "GET", "miss_*", "STORE", "d2")
	expect(t, rw, ":3")
	cmd(t, rw, "LRANGE", "d2", "0", "-1")
	eqSort(t, rw, []string{"$", "$", "$"})
}

// TestSortRO is SORT with STORE forbidden, so a read-only client can run it.
func TestSortRO(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "3", "1", "2")
	expect(t, rw, ":3")
	cmd(t, rw, "SORT_RO", "l")
	eqSort(t, rw, []string{"$1", "$2", "$3"})
	cmd(t, rw, "SORT_RO", "l", "STORE", "x")
	expect(t, rw, "-ERR syntax error")
}

// TestSortErrors covers the parse and type errors: wrong type, unknown option, a malformed LIMIT,
// a missing clause argument, and the arity check.
func TestSortErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SORT", "str")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "SORT", "nope", "BADOPT")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SORT", "nope", "LIMIT", "1")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SORT", "nope", "LIMIT", "a", "b")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "SORT", "nope", "BY")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SORT")
	expect(t, rw, "-ERR wrong number of arguments for 'sort' command")
	// A missing source is an empty array, and a missing source under STORE stores nothing.
	cmd(t, rw, "SORT", "nope")
	eqSort(t, rw, nil)
	cmd(t, rw, "SORT", "nope", "STORE", "d")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "d")
	expect(t, rw, ":0")
}
