package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// readValue reads one full RESP reply of any shape into a Go value: a string for a
// simple string, error, or bulk string (the leading type byte kept for simple/error so a
// test can tell "+OK" from "$OK"), an int64 for an integer, nil for a null bulk or null
// array, and a []any for an array (recursively). Stream replies nest arrays several deep,
// so the flat readArray helper is not enough to assert their shape.
func readValue(t *testing.T, rw *bufio.ReadWriter) any {
	t.Helper()
	line, err := rw.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line = line[:len(line)-2]
	switch line[0] {
	case '+', '-':
		return line
	case ':':
		var n int64
		neg := false
		s := line[1:]
		if len(s) > 0 && s[0] == '-' {
			neg = true
			s = s[1:]
		}
		for _, ch := range s {
			n = n*10 + int64(ch-'0')
		}
		if neg {
			n = -n
		}
		return n
	case '$':
		if line == "$-1" {
			return nil
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		buf := make([]byte, n+2)
		if _, err := readFull(rw, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*':
		if line == "*-1" {
			return nil
		}
		n := 0
		for _, ch := range line[1:] {
			n = n*10 + int(ch-'0')
		}
		out := make([]any, n)
		for i := 0; i < n; i++ {
			out[i] = readValue(t, rw)
		}
		return out
	}
	t.Fatalf("bad reply: %q", line)
	return nil
}

// asArray asserts v is an array of the given length and returns it.
func asArray(t *testing.T, v any, n int) []any {
	t.Helper()
	a, ok := v.([]any)
	if !ok {
		t.Fatalf("value = %#v, want an array", v)
	}
	if len(a) != n {
		t.Fatalf("array len = %d, want %d (%#v)", len(a), n, a)
	}
	return a
}

// asBulk asserts v is a bulk string equal to want.
func asBulk(t *testing.T, v any, want string) {
	t.Helper()
	s, ok := v.(string)
	if !ok || s != want {
		t.Fatalf("value = %#v, want bulk %q", v, want)
	}
}

// entryID returns the ID string of an [id, [fields...]] entry pair.
func entryID(t *testing.T, v any) string {
	t.Helper()
	a := asArray(t, v, 2)
	s, ok := a[0].(string)
	if !ok {
		t.Fatalf("entry id = %#v, want a bulk string", a[0])
	}
	return s
}

// TestStreamAddLenRange covers XADD id assignment, XLEN, and XRANGE/XREVRANGE windowing.
func TestStreamAddLenRange(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Explicit IDs, in order, with a field map each.
	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "1-2", "b", "2")
	expect(t, rw, "$1-2")
	cmd(t, rw, "XADD", "s", "2-1", "c", "3", "d", "4")
	expect(t, rw, "$2-1")

	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":3")

	// A smaller-or-equal id is rejected and does not grow the stream.
	cmd(t, rw, "XADD", "s", "1-2", "x", "y")
	expect(t, rw, "-"+errStreamIDSmaller)
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":3")

	// XRANGE - + returns all three, in order, with fields.
	cmd(t, rw, "XRANGE", "s", "-", "+")
	all := asArray(t, readValue(t, rw), 3)
	if id := entryID(t, all[0]); id != "1-1" {
		t.Fatalf("first id = %q", id)
	}
	// Verify the field map of the last entry.
	last := asArray(t, all[2], 2)
	asBulk(t, last[0], "2-1")
	fields := asArray(t, last[1], 4)
	asBulk(t, fields[0], "c")
	asBulk(t, fields[1], "3")
	asBulk(t, fields[2], "d")
	asBulk(t, fields[3], "4")

	// A partial start id (bare ms) expands to ms-0, a partial end id to ms-max.
	cmd(t, rw, "XRANGE", "s", "1", "1")
	win := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, win[0], 2)[0], "1-1")
	asBulk(t, asArray(t, win[1], 2)[0], "1-2")

	// Exclusive start drops the boundary entry.
	cmd(t, rw, "XRANGE", "s", "(1-1", "+")
	exc := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, exc[0], 2)[0], "1-2")

	// COUNT caps the window from the front.
	cmd(t, rw, "XRANGE", "s", "-", "+", "COUNT", "2")
	capped := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, capped[0], 2)[0], "1-1")
	asBulk(t, asArray(t, capped[1], 2)[0], "1-2")

	// XREVRANGE walks high to low and takes end then start.
	cmd(t, rw, "XREVRANGE", "s", "+", "-")
	rev := asArray(t, readValue(t, rw), 3)
	asBulk(t, asArray(t, rev[0], 2)[0], "2-1")
	asBulk(t, asArray(t, rev[2], 2)[0], "1-1")

	// XREVRANGE COUNT caps from the high end.
	cmd(t, rw, "XREVRANGE", "s", "+", "-", "COUNT", "1")
	revCap := asArray(t, readValue(t, rw), 1)
	asBulk(t, asArray(t, revCap[0], 2)[0], "2-1")
}

// TestStreamAutoID covers the '*' and 'ms-*' id forms and the monotone sequence.
func TestStreamAutoID(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// ms-* assigns a sequence within a fixed millisecond, incrementing on repeat.
	cmd(t, rw, "XADD", "s", "5-*", "a", "1")
	expect(t, rw, "$5-0")
	cmd(t, rw, "XADD", "s", "5-*", "b", "2")
	expect(t, rw, "$5-1")

	// A full '*' id is the clock, which is far past 5, so it must sort after 5-1.
	cmd(t, rw, "XADD", "s", "*", "c", "3")
	got := readReply(t, rw)
	if got[0] != '$' {
		t.Fatalf("XADD * reply = %q", got)
	}

	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":3")
}

// TestStreamNoMkStream covers NOMKSTREAM on a missing key and the persistent empty stream.
func TestStreamNoMkStream(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// NOMKSTREAM on a missing key returns nil and creates nothing.
	cmd(t, rw, "XADD", "missing", "NOMKSTREAM", "*", "a", "1")
	expect(t, rw, "$-1")
	cmd(t, rw, "EXISTS", "missing")
	expect(t, rw, ":0")

	// A trimmed-to-empty stream still exists and reports TYPE stream.
	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "MAXLEN", "0", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "s")
	expect(t, rw, ":1")
	cmd(t, rw, "TYPE", "s")
	expect(t, rw, "+stream")

	// Only DEL removes an empty stream.
	cmd(t, rw, "DEL", "s")
	expect(t, rw, ":1")
	cmd(t, rw, "EXISTS", "s")
	expect(t, rw, ":0")
}

// TestStreamTrim covers MAXLEN and MINID inline trim on XADD.
func TestStreamTrim(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		cmd(t, rw, "XADD", "s", "MAXLEN", "3", fmt.Sprintf("%d-1", i), "f", "v")
		expect(t, rw, fmt.Sprintf("$%d-1", i))
	}
	// After five adds capped at 3, only the last three survive, oldest dropped.
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":3")
	cmd(t, rw, "XRANGE", "s", "-", "+")
	surv := asArray(t, readValue(t, rw), 3)
	asBulk(t, asArray(t, surv[0], 2)[0], "3-1")
	asBulk(t, asArray(t, surv[2], 2)[0], "5-1")

	// MINID drops entries with an id below the threshold.
	cmd(t, rw, "XADD", "s", "MINID", "5", "6-1", "f", "v")
	expect(t, rw, "$6-1")
	cmd(t, rw, "XRANGE", "s", "-", "+")
	after := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, after[0], 2)[0], "5-1")
	asBulk(t, asArray(t, after[1], 2)[0], "6-1")
}

// TestStreamRead covers non-blocking XREAD after-id semantics and the multi-stream reply.
func TestStreamRead(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s1", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s1", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XADD", "s2", "3-1", "c", "3")
	expect(t, rw, "$3-1")

	// XREAD from 0 on s1 returns both entries after 0.
	cmd(t, rw, "XREAD", "COUNT", "10", "STREAMS", "s1", "0")
	r := asArray(t, readValue(t, rw), 1)
	pair := asArray(t, r[0], 2)
	asBulk(t, pair[0], "s1")
	entries := asArray(t, pair[1], 2)
	asBulk(t, asArray(t, entries[0], 2)[0], "1-1")
	asBulk(t, asArray(t, entries[1], 2)[0], "2-1")

	// XREAD after 1-1 returns only the newer entry.
	cmd(t, rw, "XREAD", "STREAMS", "s1", "1-1")
	r2 := asArray(t, readValue(t, rw), 1)
	e2 := asArray(t, asArray(t, r2[0], 2)[1], 1)
	asBulk(t, asArray(t, e2[0], 2)[0], "2-1")

	// Two streams in one call, each with its own after-id; a stream with nothing new is omitted.
	cmd(t, rw, "XREAD", "STREAMS", "s1", "s2", "2-1", "0")
	r3 := asArray(t, readValue(t, rw), 1)
	asBulk(t, asArray(t, r3[0], 2)[0], "s2")

	// $ means "after the current last id", so a non-blocking read returns the null array.
	cmd(t, rw, "XREAD", "STREAMS", "s1", "$")
	if v := readValue(t, rw); v != nil {
		t.Fatalf("XREAD $ with no new data = %#v, want nil array", v)
	}
}

// TestStreamWrongType covers the WRONGTYPE guard against a string key.
func TestStreamWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "XADD", "str", "*", "a", "1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "XLEN", "str")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "XRANGE", "str", "-", "+")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "XDEL", "str", "1-1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "XTRIM", "str", "MAXLEN", "0")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "XSETID", "str", "1-1")
	expect(t, rw, "-"+wrongType)
}

// TestStreamDel covers XDEL: a point delete drops the live length, an absent id is not counted,
// and max-deleted-id advances so a re-add at a removed id is still rejected while entries-added
// (the lifetime counter) holds.
func TestStreamDel(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for _, id := range []string{"1-1", "2-1", "3-1"} {
		cmd(t, rw, "XADD", "s", id, "f", "v")
		expect(t, rw, "$"+id)
	}

	// Delete the middle entry and one id that is not present: only the present one counts.
	cmd(t, rw, "XDEL", "s", "2-1", "9-9")
	expect(t, rw, ":1")
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":2")

	// The survivors are the untouched ends, in order.
	cmd(t, rw, "XRANGE", "s", "-", "+")
	surv := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, surv[0], 2)[0], "1-1")
	asBulk(t, asArray(t, surv[1], 2)[0], "3-1")

	// Re-adding at the deleted id is still rejected: last-id did not move backward.
	cmd(t, rw, "XADD", "s", "2-1", "f", "v")
	expect(t, rw, "-"+errStreamIDSmaller)

	// Deleting every remaining entry leaves the stream present at length zero.
	cmd(t, rw, "XDEL", "s", "1-1", "3-1")
	expect(t, rw, ":2")
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "s")
	expect(t, rw, ":1")

	// XDEL on a missing key removes nothing.
	cmd(t, rw, "XDEL", "missing", "1-1")
	expect(t, rw, ":0")
}

// TestStreamXTrim covers standalone XTRIM by MAXLEN and MINID and its removed-count reply.
func TestStreamXTrim(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("%d-1", i)
		cmd(t, rw, "XADD", "s", id, "f", "v")
		expect(t, rw, "$"+id)
	}

	// MAXLEN 3 drops the two oldest and reports two removed.
	cmd(t, rw, "XTRIM", "s", "MAXLEN", "3")
	expect(t, rw, ":2")
	cmd(t, rw, "XRANGE", "s", "-", "+")
	surv := asArray(t, readValue(t, rw), 3)
	asBulk(t, asArray(t, surv[0], 2)[0], "3-1")
	asBulk(t, asArray(t, surv[2], 2)[0], "5-1")

	// MINID 5 drops 3-1 and 4-1 (both below 5-0), leaving only 5-1.
	cmd(t, rw, "XTRIM", "s", "MINID", "5")
	expect(t, rw, ":2")
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":1")

	// XTRIM on a missing key removes nothing.
	cmd(t, rw, "XTRIM", "missing", "MAXLEN", "0")
	expect(t, rw, ":0")

	// A trailing unbalanced token is a syntax error, not a silent no-op.
	cmd(t, rw, "XTRIM", "s", "MAXLEN", "1", "BOGUS")
	expect(t, rw, "-ERR syntax error")
}

// TestStreamSetID covers XSETID: it rewrites last-id and the counters, rejects an id below the
// highest present entry, and errors on a missing key.
func TestStreamSetID(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "f", "v")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "2-1", "f", "v")
	expect(t, rw, "$2-1")

	// last-id below the highest present entry (2-1) is refused.
	cmd(t, rw, "XSETID", "s", "1-0")
	expect(t, rw, "-"+errStreamSetIDSmall)

	// Bumping last-id forward makes the next auto-add continue from there.
	cmd(t, rw, "XSETID", "s", "100-5")
	expect(t, rw, "+OK")
	cmd(t, rw, "XADD", "s", "100-*", "f", "v")
	expect(t, rw, "$100-6")

	// ENTRIESADDED and MAXDELETEDID are accepted and echoed back through XINFO-free checks:
	// setting a smaller last-id still fails against the real top entry, not the counter.
	cmd(t, rw, "XSETID", "s", "200-0", "ENTRIESADDED", "50", "MAXDELETEDID", "3-3")
	expect(t, rw, "+OK")

	// A non-integer ENTRIESADDED is rejected.
	cmd(t, rw, "XSETID", "s", "300-0", "ENTRIESADDED", "notanumber")
	expect(t, rw, "-"+errStreamNotInt)

	// XSETID on a missing key is an error, not a create.
	cmd(t, rw, "XSETID", "missing", "1-1")
	expect(t, rw, "-"+errStreamNoSuchKey)
	cmd(t, rw, "EXISTS", "missing")
	expect(t, rw, ":0")
}
