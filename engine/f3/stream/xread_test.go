package stream

import (
	"testing"
)

// The XREAD suite (spec 2064/f3/14 section 6.3): the non-blocking forward read
// over one and several streams, COUNT, the "$" and "+" special IDs, the
// empty-result null array, and the routing-key extraction the dispatcher relies
// on. Blocking (the BLOCK clause and the waiter sets) is a later slice; this one
// refuses BLOCK rather than mishandling it.

// streamWant is one expected [key, entries] row of an XREAD reply.
type streamWant struct {
	key     string
	entries []entry
}

func sw(key string, es ...entry) streamWant { return streamWant{key: key, entries: es} }

// wantStreams asserts an XREAD reply is exactly want; an empty want expects the
// non-blocking null array.
func wantStreams(t *testing.T, raw []byte, want ...streamWant) {
	t.Helper()
	got := decodeReply(t, raw)
	if len(want) == 0 {
		if got != nil {
			t.Fatalf("reply = %v, want null array", render(got))
		}
		return
	}
	rows, ok := got.([]any)
	if !ok {
		t.Fatalf("reply = %v, want an array", render(got))
	}
	if len(rows) != len(want) {
		t.Fatalf("reply has %d streams, want %d: %v", len(rows), len(want), render(got))
	}
	for i, row := range rows {
		pair, ok := row.([]any)
		if !ok || len(pair) != 2 {
			t.Fatalf("stream %d = %v, want a [key, entries] pair", i, render(row))
		}
		key, ok := pair[0].(string)
		if !ok || key != want[i].key {
			t.Fatalf("stream %d key = %v, want %q", i, render(pair[0]), want[i].key)
		}
		entries, ok := pair[1].([]any)
		if !ok {
			t.Fatalf("stream %d entries = %v, want an array", i, render(pair[1]))
		}
		checkEntries(t, entries, want[i].entries)
	}
}

// checkEntries asserts a decoded entries array matches want, in order.
func checkEntries(t *testing.T, rows []any, want []entry) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("entries = %d, want %d: %v", len(rows), len(want), rows)
	}
	for i, row := range rows {
		pair, ok := row.([]any)
		if !ok || len(pair) != 2 {
			t.Fatalf("entry %d = %v, want an [id, fields] pair", i, render(row))
		}
		id, ok := pair[0].(string)
		if !ok || id != want[i].id {
			t.Fatalf("entry %d id = %v, want %q", i, render(pair[0]), want[i].id)
		}
		fields, ok := pair[1].([]any)
		if !ok {
			t.Fatalf("entry %d fields = %v, want an array", i, render(pair[1]))
		}
		if got := flatten(t, fields); !equal(got, want[i].fields) {
			t.Fatalf("entry %d fields = %v, want %v", i, got, want[i].fields)
		}
	}
}

func TestXreadAfterID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "a", "1")
	do(t, c, opXadd, "s", "2-0", "b", "2")
	do(t, c, opXadd, "s", "3-0", "c", "3")
	// Everything after 1-0.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "1-0"),
		sw("s", e("2-0", "b", "2"), e("3-0", "c", "3")))
	// 0 reads the whole stream.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "0"),
		sw("s", e("1-0", "a", "1"), e("2-0", "b", "2"), e("3-0", "c", "3")))
	// After the tail: nothing, so the null array.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "3-0"))
}

func TestXreadCount(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0", "4-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	wantStreams(t, do(t, c, opXread, "COUNT", "2", "STREAMS", "s", "0"),
		sw("s", e("1-0", "f", "v"), e("2-0", "f", "v")))
	// COUNT 0 is unbounded for XREAD.
	wantStreams(t, do(t, c, opXread, "COUNT", "0", "STREAMS", "s", "0"),
		sw("s", e("1-0", "f", "v"), e("2-0", "f", "v"), e("3-0", "f", "v"), e("4-0", "f", "v")))
}

func TestXreadDollar(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// $ is the current tail, so a non-blocking read above it is empty.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "$"))
	// $ on a missing key is also empty, not an error.
	wantStreams(t, do(t, c, opXread, "STREAMS", "missing", "$"))
}

func TestXreadPlus(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "a", "1")
	do(t, c, opXadd, "s", "2-0", "b", "2")
	// + returns the last entry.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "+"),
		sw("s", e("2-0", "b", "2")))
	// After tombstoning the last, + returns the new last live entry.
	do(t, c, opXdel, "s", "2-0")
	wantStreams(t, do(t, c, opXread, "STREAMS", "s", "+"),
		sw("s", e("1-0", "a", "1")))
}

func TestXreadMultiStream(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s1", "1-0", "a", "1")
	do(t, c, opXadd, "s2", "5-0", "b", "2")
	// Two streams, each with its own after-ID; both have new entries.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s1", "s2", "0", "0"),
		sw("s1", e("1-0", "a", "1")), sw("s2", e("5-0", "b", "2")))
	// A stream with nothing new is omitted from the reply.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s1", "s2", "0", "5-0"),
		sw("s1", e("1-0", "a", "1")))
	// Both exhausted: the null array.
	wantStreams(t, do(t, c, opXread, "STREAMS", "s1", "s2", "1-0", "5-0"))
}

func TestXreadMissingStream(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// A named-but-missing stream contributes nothing; the present one still reads.
	wantStreams(t, do(t, c, opXread, "STREAMS", "missing", "s", "0", "0"),
		sw("s", e("1-0", "f", "v")))
}

func TestXreadBlockUnsupported(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXread, "BLOCK", "100", "STREAMS", "s", "$"),
		"ERR stream blocking is not supported yet")
}

func TestXreadSyntax(t *testing.T) {
	c := newHarness(t).NewConn()
	// No STREAMS token.
	wantErr(t, do(t, c, opXread, "s", "0"), "ERR syntax error")
	// Unbalanced key/id list.
	wantErr(t, do(t, c, opXread, "STREAMS", "s1", "s2", "0"),
		"ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.")
	// A malformed explicit ID.
	wantErr(t, do(t, c, opXread, "STREAMS", "s", "bad"),
		"ERR Invalid stream ID specified as stream command argument")
	// COUNT with a non-integer.
	wantErr(t, do(t, c, opXread, "COUNT", "x", "STREAMS", "s", "0"),
		"ERR value is not an integer or out of range")
}

func TestXreadWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXread, "STREAMS", "k", "0"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}

// TestReadKeyExtraction pins the routing helpers the dispatcher uses to find the
// stream keys after the STREAMS token, across the optional COUNT and BLOCK
// clauses.
func TestReadKeyExtraction(t *testing.T) {
	cases := []struct {
		tail    []string
		keys    []string
		keyAt   int
		badTail bool
	}{
		{tail: []string{"STREAMS", "s", "0"}, keys: []string{"s"}, keyAt: 1},
		{tail: []string{"COUNT", "5", "STREAMS", "s", "0"}, keys: []string{"s"}, keyAt: 3},
		{tail: []string{"BLOCK", "0", "STREAMS", "a", "b", "0", "0"}, keys: []string{"a", "b"}, keyAt: 3},
		{tail: []string{"s", "0"}, badTail: true},
		{tail: []string{"STREAMS", "s1", "s2", "0"}, badTail: true},
		{tail: []string{"COUNT"}, badTail: true},
	}
	for _, tc := range cases {
		tail := bytesOf(tc.tail)
		keys := ReadKeys(tail)
		at := ReadKeyAt(tail)
		if tc.badTail {
			if keys != nil || at != -1 {
				t.Fatalf("tail %v: got keys %v at %d, want malformed", tc.tail, keys, at)
			}
			continue
		}
		if at != tc.keyAt {
			t.Fatalf("tail %v: keyAt = %d, want %d", tc.tail, at, tc.keyAt)
		}
		if len(keys) != len(tc.keys) {
			t.Fatalf("tail %v: %d keys, want %d", tc.tail, len(keys), len(tc.keys))
		}
		for i := range keys {
			if string(keys[i]) != tc.keys[i] {
				t.Fatalf("tail %v: key %d = %q, want %q", tc.tail, i, keys[i], tc.keys[i])
			}
		}
	}
}

func bytesOf(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}
