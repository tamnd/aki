package list

import "testing"

// The command surface over the inline band: reply shapes and error texts, all
// the forms verified live against redis-server 8.8.0 and pinned here so a
// refactor cannot drift off Redis.

func TestPushAndLen(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()

	// RPUSH returns the running length and appends in argument order.
	wantInt(t, do(t, c, opRpush, "k", "a", "b", "c"), 3)
	wantInt(t, do(t, c, opLlen, "k"), 3)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "b", "c")

	// LPUSH prepends each element in turn, so the tail order reverses.
	wantInt(t, do(t, c, opLpush, "k", "x", "y"), 5)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "y", "x", "a", "b", "c")

	// LLEN on a missing key is 0.
	wantInt(t, do(t, c, opLlen, "missing"), 0)
}

func TestPushXOnlyWhenPresent(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()

	// LPUSHX/RPUSHX on a missing key reply 0 and create nothing.
	wantInt(t, do(t, c, opLpushx, "k", "a"), 0)
	wantInt(t, do(t, c, opRpushx, "k", "a"), 0)
	wantInt(t, do(t, c, opLlen, "k"), 0)

	// Once the key exists they extend it.
	wantInt(t, do(t, c, opRpush, "k", "b"), 1)
	wantInt(t, do(t, c, opRpushx, "k", "c"), 2)
	wantInt(t, do(t, c, opLpushx, "k", "a"), 3)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "b", "c")
}

func TestPopShapes(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c", "d", "e")

	// No count: a single bulk.
	wantBulk(t, do(t, c, opLpop, "k"), "a")
	wantBulk(t, do(t, c, opRpop, "k"), "e")

	// Count: an array of up to count.
	wantArray(t, do(t, c, opLpop, "k", "2"), "b", "c")

	// Count 0 on a live key: an empty array.
	wantEmptyArray(t, do(t, c, opLpop, "k", "0"))

	// No count on a missing key: a null bulk.
	wantNil(t, do(t, c, opLpop, "missing"))

	// Count on a missing key: a null array.
	wantNil(t, do(t, c, opRpop, "missing", "3"))

	// A count larger than the list returns what is there and deletes the key.
	wantArray(t, do(t, c, opRpop, "k", "9"), "d")
	wantInt(t, do(t, c, opLlen, "k"), 0)

	// Bad count text.
	wantErr(t, do(t, c, opLpop, "k", "-1"), errPosCount)
	wantErr(t, do(t, c, opLpop, "k", "x"), errPosCount)
}

func TestIndexAndRange(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c", "d")

	wantBulk(t, do(t, c, opLindex, "k", "0"), "a")
	wantBulk(t, do(t, c, opLindex, "k", "-1"), "d")
	wantNil(t, do(t, c, opLindex, "k", "9"))
	wantNil(t, do(t, c, opLindex, "missing", "0"))
	wantErr(t, do(t, c, opLindex, "k", "x"), errNotInt)

	wantArray(t, do(t, c, opLrange, "k", "1", "2"), "b", "c")
	wantArray(t, do(t, c, opLrange, "k", "-2", "-1"), "c", "d")
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "b", "c", "d")
	wantEmptyArray(t, do(t, c, opLrange, "k", "5", "9"))
	wantEmptyArray(t, do(t, c, opLrange, "missing", "0", "-1"))
	wantErr(t, do(t, c, opLrange, "k", "a", "1"), errNotInt)
}

func TestSet(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c")

	wantStatus(t, do(t, c, opLset, "k", "1", "B"), "OK")
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "B", "c")
	wantStatus(t, do(t, c, opLset, "k", "-1", "C"), "OK")
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "B", "C")

	wantErr(t, do(t, c, opLset, "k", "9", "z"), errOutOfRange)
	wantErr(t, do(t, c, opLset, "missing", "0", "z"), errNoSuchKey)
	wantErr(t, do(t, c, opLset, "k", "x", "z"), errNotInt)
}

func TestRem(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "a", "c", "a")

	// Positive count removes from the head.
	wantInt(t, do(t, c, opLrem, "k", "2", "a"), 2)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "b", "c", "a")

	do(t, c, opRpush, "k", "a", "a")
	// Negative count removes from the tail.
	wantInt(t, do(t, c, opLrem, "k", "-1", "a"), 1)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "b", "c", "a", "a")

	// Zero removes every match and empties, deleting the key.
	wantInt(t, do(t, c, opLrem, "k", "0", "a"), 2)
	wantInt(t, do(t, c, opLrem, "k", "0", "b"), 1)
	wantInt(t, do(t, c, opLrem, "k", "0", "c"), 1)
	wantInt(t, do(t, c, opLlen, "k"), 0)

	wantInt(t, do(t, c, opLrem, "missing", "0", "a"), 0)
	do(t, c, opRpush, "k2", "a")
	wantErr(t, do(t, c, opLrem, "k2", "x", "a"), errNotInt)
}

func TestTrimCmd(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c", "d", "e")

	wantStatus(t, do(t, c, opLtrim, "k", "1", "3"), "OK")
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "b", "c", "d")

	// A range that keeps nothing deletes the key.
	wantStatus(t, do(t, c, opLtrim, "k", "5", "9"), "OK")
	wantInt(t, do(t, c, opLlen, "k"), 0)

	wantStatus(t, do(t, c, opLtrim, "missing", "0", "-1"), "OK")
	do(t, c, opRpush, "k2", "a")
	wantErr(t, do(t, c, opLtrim, "k2", "x", "1"), errNotInt)
}

func TestInsertCmd(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c")

	wantInt(t, do(t, c, opLinsert, "k", "BEFORE", "b", "X"), 4)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "X", "b", "c")
	wantInt(t, do(t, c, opLinsert, "k", "after", "c", "Y"), 5)
	wantArray(t, do(t, c, opLrange, "k", "0", "-1"), "a", "X", "b", "c", "Y")

	// Missing pivot: -1.
	wantInt(t, do(t, c, opLinsert, "k", "BEFORE", "nope", "Z"), -1)
	// Missing key: 0.
	wantInt(t, do(t, c, opLinsert, "missing", "BEFORE", "a", "Z"), 0)
	// Bad direction: syntax error.
	wantErr(t, do(t, c, opLinsert, "k", "sideways", "a", "Z"), errSyntax)
}

func TestPosCmd(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	do(t, c, opRpush, "k", "a", "b", "c", "a", "b", "a")

	// No COUNT: the RANK-th match or nil.
	wantInt(t, do(t, c, opLpos, "k", "a"), 0)
	wantInt(t, do(t, c, opLpos, "k", "a", "RANK", "2"), 3)
	wantInt(t, do(t, c, opLpos, "k", "a", "RANK", "-1"), 5)
	wantNil(t, do(t, c, opLpos, "k", "zzz"))
	wantNil(t, do(t, c, opLpos, "missing", "a"))

	// COUNT: an array (0 means all).
	wantArray(t, do(t, c, opLpos, "k", "a", "COUNT", "0"), "0", "3", "5")
	wantArray(t, do(t, c, opLpos, "k", "a", "COUNT", "2"), "0", "3")
	wantArray(t, do(t, c, opLpos, "k", "a", "RANK", "-1", "COUNT", "0"), "5", "3", "0")
	wantEmptyArray(t, do(t, c, opLpos, "k", "zzz", "COUNT", "0"))
	wantEmptyArray(t, do(t, c, opLpos, "missing", "a", "COUNT", "0"))

	// MAXLEN caps the compare window.
	wantArray(t, do(t, c, opLpos, "k", "a", "COUNT", "0", "MAXLEN", "4"), "0", "3")

	// Option errors.
	wantErr(t, do(t, c, opLpos, "k", "a", "RANK", "0"), errRankZero)
	wantErr(t, do(t, c, opLpos, "k", "a", "COUNT", "-1"), errCountNeg)
	wantErr(t, do(t, c, opLpos, "k", "a", "MAXLEN", "-1"), errMaxlenNeg)
	wantErr(t, do(t, c, opLpos, "k", "a", "RANK", "x"), errNotInt)
	wantErr(t, do(t, c, opLpos, "k", "a", "BOGUS", "1"), errSyntax)
}

// Every list read and write on a string key answers WRONGTYPE.
func TestWrongType(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()
	wantStatus(t, do(t, c, opSet, "s", "v"), "OK")

	wantErr(t, do(t, c, opLpush, "s", "a"), wrongType)
	wantErr(t, do(t, c, opRpush, "s", "a"), wrongType)
	wantErr(t, do(t, c, opLpushx, "s", "a"), wrongType)
	wantErr(t, do(t, c, opLpop, "s"), wrongType)
	wantErr(t, do(t, c, opLlen, "s"), wrongType)
	wantErr(t, do(t, c, opLindex, "s", "0"), wrongType)
	wantErr(t, do(t, c, opLrange, "s", "0", "-1"), wrongType)
	wantErr(t, do(t, c, opLset, "s", "0", "x"), wrongType)
	wantErr(t, do(t, c, opLrem, "s", "0", "x"), wrongType)
	wantErr(t, do(t, c, opLtrim, "s", "0", "-1"), wrongType)
	wantErr(t, do(t, c, opLinsert, "s", "BEFORE", "a", "b"), wrongType)
	wantErr(t, do(t, c, opLpos, "s", "a"), wrongType)
}

// OBJECT ENCODING reports the list band and falls through to the string store
// for a non-list key.
func TestObjectEncoding(t *testing.T) {
	rt := newHarness(t)
	c := rt.NewConn()

	do(t, c, opRpush, "small", "a", "b", "c")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "small"), "listpack")

	// Push past the 8 KiB budget to force the quicklist band. 100 x 100B packs
	// to ~10.3 KiB, over the budget, and stays within one command's arg cap.
	big := make([]string, 0, 101)
	big = append(big, "big")
	for i := 0; i < 100; i++ {
		big = append(big, string(make([]byte, 100)))
	}
	do(t, c, opRpush, big...)
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "big"), "quicklist")

	// A string key still answers through the delegated set handler.
	do(t, c, opSet, "s", "hello")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "s"), "embstr")

	// A missing key delegates down the chain to the set OBJECT handler, which
	// answers nil for a key that exists nowhere, matching the redis 8.8.0 build
	// (verified live: OBJECT ENCODING on an absent key returns a null bulk, not an
	// error).
	wantNil(t, doAt(t, c, opObject, 1, "ENCODING", "ghost"))
}
