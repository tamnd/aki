package f1srv

import (
	"bufio"
	"strings"
	"testing"
)

// lrangeCall sends an LRANGE (or any element-array command) and decodes the reply into a flat
// slice of element strings, so a test can compare the window order directly.
func lrangeCall(t *testing.T, rw *bufio.ReadWriter, args ...string) []string {
	t.Helper()
	cmd(t, rw, args...)
	ah := readReply(t, rw)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("%s header = %q, want an array", args[0], ah)
	}
	if ah == "*-1" {
		return nil
	}
	n := 0
	for _, ch := range ah[1:] {
		n = n*10 + int(ch-'0')
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("%s item %d = %q, want a bulk string", args[0], i, b)
		}
		out[i] = b[1:]
	}
	return out
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The list point path is element-per-row on f1raw: each element is one row under an
// order-preserving position key, and the header window (head, tail) makes LLEN O(1) and LINDEX
// a direct positional lookup. This exercises the deque semantics RPUSH/LPUSH grow and
// LPOP/RPOP shrink, and confirms the window stays in list order across mixed end edits.
func TestListPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// RPUSH appends in order, returning the new length each time.
	cmd(t, rw, "RPUSH", "l", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "RPUSH", "l", "d")
	expect(t, rw, ":4")

	// LPUSH prepends per element, so LPUSH x y z leaves z at the head.
	cmd(t, rw, "LPUSH", "l", "z", "y", "x")
	expect(t, rw, ":7")

	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"x", "y", "z", "a", "b", "c", "d"}) {
		t.Fatalf("LRANGE full = %v", got)
	}

	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":7")
	cmd(t, rw, "LLEN", "missing")
	expect(t, rw, ":0")

	// LINDEX: forward and negative indexing, out of range is nil.
	cmd(t, rw, "LINDEX", "l", "0")
	expect(t, rw, "$x")
	cmd(t, rw, "LINDEX", "l", "3")
	expect(t, rw, "$a")
	cmd(t, rw, "LINDEX", "l", "-1")
	expect(t, rw, "$d")
	cmd(t, rw, "LINDEX", "l", "-7")
	expect(t, rw, "$x")
	cmd(t, rw, "LINDEX", "l", "7")
	expect(t, rw, "$-1")
	cmd(t, rw, "LINDEX", "missing", "0")
	expect(t, rw, "$-1")

	// LRANGE window clamping.
	if got := lrangeCall(t, rw, "LRANGE", "l", "1", "3"); !eqStrs(got, []string{"y", "z", "a"}) {
		t.Fatalf("LRANGE 1..3 = %v", got)
	}
	if got := lrangeCall(t, rw, "LRANGE", "l", "-2", "-1"); !eqStrs(got, []string{"c", "d"}) {
		t.Fatalf("LRANGE -2..-1 = %v", got)
	}
	if got := lrangeCall(t, rw, "LRANGE", "l", "5", "1"); len(got) != 0 {
		t.Fatalf("LRANGE inverted = %v, want empty", got)
	}
	if got := lrangeCall(t, rw, "LRANGE", "missing", "0", "-1"); len(got) != 0 {
		t.Fatalf("LRANGE missing = %v, want empty", got)
	}
}

// Pops draw from the right end: LPOP from the head, RPOP from the tail, single form as a bulk
// string and count form as an array in pop order. Draining the last element deletes the key.
func TestListPop(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")

	cmd(t, rw, "LPOP", "l")
	expect(t, rw, "$a")
	cmd(t, rw, "RPOP", "l")
	expect(t, rw, "$e")

	// RPOP with count returns from the tail inward: [d c].
	if got := lrangeCall(t, rw, "RPOP", "l", "2"); !eqStrs(got, []string{"d", "c"}) {
		t.Fatalf("RPOP count = %v", got)
	}
	// One element left.
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":1")
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"b"}) {
		t.Fatalf("LRANGE remainder = %v", got)
	}

	// LPOP with a count larger than the length drains and deletes the key.
	if got := lrangeCall(t, rw, "LPOP", "l", "10"); !eqStrs(got, []string{"b"}) {
		t.Fatalf("LPOP drain = %v", got)
	}
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":0")
	cmd(t, rw, "TYPE", "l")
	expect(t, rw, "+none")

	// Pops on a missing key: nil bulk without a count, null array with one.
	cmd(t, rw, "LPOP", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "RPOP", "missing", "3")
	expect(t, rw, "*-1")

	// A count of zero on an existing key is an empty array.
	cmd(t, rw, "RPUSH", "z", "x")
	expect(t, rw, ":1")
	if got := lrangeCall(t, rw, "LPOP", "z", "0"); len(got) != 0 {
		t.Fatalf("LPOP 0 = %v, want empty", got)
	}
	// A negative count is an error.
	cmd(t, rw, "LPOP", "z", "-1")
	got := readReply(t, rw)
	if !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("LPOP -1 = %q, want error", got)
	}
}

// TYPE and OBJECT ENCODING report list and the listpack/quicklist name Redis would pick for
// the same contents. The encoding is byte-budget only under the default list-max-listpack-size
// (-2): small lists are listpack, a big element flips it to quicklist, and the flip is sticky.
func TestListTypeAndEncoding(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "TYPE", "l")
	expect(t, rw, "+list")
	cmd(t, rw, "OBJECT", "ENCODING", "l")
	expect(t, rw, "$listpack")

	// 200 tiny integers stay listpack: the byte budget is 8192 and there is no element cap,
	// matching the running Redis 8.8 and Valkey 9.1 defaults.
	for i := 0; i < 200; i++ {
		cmd(t, rw, "RPUSH", "ints", "5")
		readReply(t, rw)
	}
	cmd(t, rw, "OBJECT", "ENCODING", "ints")
	expect(t, rw, "$listpack")

	// One element past the byte budget flips to quicklist, and the flip is sticky for the rest
	// of the list's lifetime: it stays quicklist even after the list shrinks back under budget,
	// matching Redis, which does not demote a quicklist while the key lives.
	big := strings.Repeat("x", 9000)
	cmd(t, rw, "RPUSH", "big", big)
	expect(t, rw, ":1")
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$quicklist")
	cmd(t, rw, "RPUSH", "big", "small") // now [big, small], still non-empty
	expect(t, rw, ":2")
	cmd(t, rw, "RPOP", "big") // remove small, leaves [big], well under budget by count
	expect(t, rw, "$small")
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$quicklist")

	// Draining the list to empty deletes the key, so a later push is a brand new object that
	// starts back at listpack, exactly as Redis reports for a freshly created list.
	cmd(t, rw, "LPOP", "big") // removes big, list now empty, key gone
	readReply(t, rw)
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$-1")
	cmd(t, rw, "RPUSH", "big", "tiny")
	expect(t, rw, ":1")
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$listpack")

	// OBJECT ENCODING on a missing key is nil.
	cmd(t, rw, "OBJECT", "ENCODING", "missing")
	expect(t, rw, "$-1")
}

// intArrayCall sends a command and decodes an array-of-integers reply into a flat slice, for
// LPOS with COUNT, whose elements are integer positions rather than bulk strings.
func intArrayCall(t *testing.T, rw *bufio.ReadWriter, args ...string) []string {
	t.Helper()
	cmd(t, rw, args...)
	ah := readReply(t, rw)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("%s header = %q, want an array", args[0], ah)
	}
	if ah == "*-1" {
		return nil
	}
	n := 0
	for _, ch := range ah[1:] {
		n = n*10 + int(ch-'0')
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != ':' {
			t.Fatalf("%s item %d = %q, want an integer", args[0], i, b)
		}
		out[i] = b[1:]
	}
	return out
}

// LSET overwrites one element in place by signed index and leaves every other element and the
// list order untouched, erroring on a missing key or an out-of-range index.
func TestListLSet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "d")
	expect(t, rw, ":4")
	cmd(t, rw, "LSET", "l", "0", "X")
	expect(t, rw, "+OK")
	cmd(t, rw, "LSET", "l", "-1", "Y")
	expect(t, rw, "+OK")
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"X", "b", "c", "Y"}) {
		t.Fatalf("after LSET = %v", got)
	}
	cmd(t, rw, "LSET", "l", "10", "z")
	if got := readReply(t, rw); got != "-ERR index out of range" {
		t.Fatalf("LSET out of range = %q", got)
	}
	cmd(t, rw, "LSET", "missing", "0", "v")
	if got := readReply(t, rw); got != "-ERR no such key" {
		t.Fatalf("LSET missing = %q", got)
	}
}

// LPOS finds element positions in list order: first match, ranked match, tail-first with a
// negative rank, bounded and unbounded COUNT, MAXLEN comparison cap, and the not-found forms.
func TestListLPos(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// positions: a0 b1 c2 1(3) 2(4) 3(5) c6 c7
	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "1", "2", "3", "c", "c")
	expect(t, rw, ":8")

	cmd(t, rw, "LPOS", "l", "c")
	expect(t, rw, ":2")
	cmd(t, rw, "LPOS", "l", "c", "RANK", "2")
	expect(t, rw, ":6")
	cmd(t, rw, "LPOS", "l", "c", "RANK", "-1")
	expect(t, rw, ":7")

	if got := intArrayCall(t, rw, "LPOS", "l", "c", "COUNT", "2"); !eqStrs(got, []string{"2", "6"}) {
		t.Fatalf("LPOS COUNT 2 = %v", got)
	}
	if got := intArrayCall(t, rw, "LPOS", "l", "c", "COUNT", "0"); !eqStrs(got, []string{"2", "6", "7"}) {
		t.Fatalf("LPOS COUNT 0 = %v", got)
	}
	if got := intArrayCall(t, rw, "LPOS", "l", "c", "RANK", "-1", "COUNT", "2"); !eqStrs(got, []string{"7", "6"}) {
		t.Fatalf("LPOS RANK -1 COUNT 2 = %v", got)
	}
	// MAXLEN 3 compares only positions 0,1,2, so only the first c is seen.
	if got := intArrayCall(t, rw, "LPOS", "l", "c", "COUNT", "0", "MAXLEN", "3"); !eqStrs(got, []string{"2"}) {
		t.Fatalf("LPOS COUNT 0 MAXLEN 3 = %v", got)
	}

	cmd(t, rw, "LPOS", "l", "nope")
	expect(t, rw, "$-1")
	if got := intArrayCall(t, rw, "LPOS", "l", "nope", "COUNT", "0"); len(got) != 0 {
		t.Fatalf("LPOS not found COUNT = %v, want empty", got)
	}
	cmd(t, rw, "LPOS", "missing", "x")
	expect(t, rw, "$-1")

	cmd(t, rw, "LPOS", "l", "c", "RANK", "0")
	if got := readReply(t, rw); !strings.HasPrefix(got, "-ERR RANK can't be zero") {
		t.Fatalf("LPOS RANK 0 = %q", got)
	}
}

// LPUSHX and RPUSHX push only onto a list that already exists, replying 0 and creating nothing
// on a missing key, and otherwise behaving exactly like LPUSH and RPUSH.
func TestListPushX(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSHX", "l", "x")
	expect(t, rw, ":0")
	cmd(t, rw, "LPUSHX", "l", "x")
	expect(t, rw, ":0")
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":0")

	cmd(t, rw, "RPUSH", "l", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "RPUSHX", "l", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "LPUSHX", "l", "z")
	expect(t, rw, ":4")
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"z", "a", "b", "c"}) {
		t.Fatalf("after pushx = %v", got)
	}
}

// LTRIM keeps only a positional window and deletes the rest, moving the ends inward and
// deleting the key when the window is empty, with negative indexes counted from the tail.
func TestListLTrim(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")
	cmd(t, rw, "LTRIM", "l", "1", "3")
	expect(t, rw, "+OK")
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"b", "c", "d"}) {
		t.Fatalf("after LTRIM 1 3 = %v", got)
	}
	// A push still extends the trimmed window in order.
	cmd(t, rw, "RPUSH", "l", "f")
	expect(t, rw, ":4")
	if got := lrangeCall(t, rw, "LRANGE", "l", "0", "-1"); !eqStrs(got, []string{"b", "c", "d", "f"}) {
		t.Fatalf("after LTRIM then RPUSH = %v", got)
	}

	// Negative indexes count from the tail.
	cmd(t, rw, "DEL", "l2")
	readReply(t, rw)
	cmd(t, rw, "RPUSH", "l2", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")
	cmd(t, rw, "LTRIM", "l2", "-3", "-1")
	expect(t, rw, "+OK")
	if got := lrangeCall(t, rw, "LRANGE", "l2", "0", "-1"); !eqStrs(got, []string{"c", "d", "e"}) {
		t.Fatalf("LTRIM -3 -1 = %v", got)
	}

	// An out-of-window range empties the list and deletes the key.
	cmd(t, rw, "LTRIM", "l2", "5", "10")
	expect(t, rw, "+OK")
	cmd(t, rw, "LLEN", "l2")
	expect(t, rw, ":0")
	cmd(t, rw, "TYPE", "l2")
	expect(t, rw, "+none")

	// LTRIM on a missing key is a no-op that still replies OK.
	cmd(t, rw, "LTRIM", "missing", "0", "-1")
	expect(t, rw, "+OK")
}

// A list command against a plain string is WRONGTYPE, and a string command against a list is
// too, so the two namespaces never cross-read.
func TestListWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "v")
	expect(t, rw, "+OK")
	for _, args := range [][]string{
		{"LPUSH", "s", "x"},
		{"RPUSH", "s", "x"},
		{"LPOP", "s"},
		{"RPOP", "s"},
		{"LLEN", "s"},
		{"LINDEX", "s", "0"},
		{"LRANGE", "s", "0", "-1"},
		{"LSET", "s", "0", "x"},
		{"LPOS", "s", "x"},
		{"LPUSHX", "s", "x"},
		{"RPUSHX", "s", "x"},
		{"LTRIM", "s", "0", "-1"},
	} {
		cmd(t, rw, args...)
		got := readReply(t, rw)
		if !strings.HasPrefix(got, "-WRONGTYPE") {
			t.Fatalf("%v on string = %q, want WRONGTYPE", args, got)
		}
	}
}
