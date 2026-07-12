package stream

import (
	"fmt"
	"testing"
)

// The range-read suite (spec 2064/f3/14 section 6.3): XRANGE and XREVRANGE over
// both bands, the bound grammar ("-", "+", "(" exclusive, partial IDs), COUNT,
// the reversed argument order, and the seek across a native stream's many
// blocks. The window logic runs the same in both bands; the multi-block cases
// force an upgrade so the directory seek and cross-block decode are exercised.

// entry is an expected reply row: its ID text and its flat field-value list.
type entry struct {
	id     string
	fields []string
}

func e(id string, fv ...string) entry { return entry{id: id, fields: fv} }

// wantEntries asserts a range reply is exactly want, in order.
func wantEntries(t *testing.T, raw []byte, want ...entry) {
	t.Helper()
	got := decodeReply(t, raw)
	rows, ok := got.([]any)
	if !ok {
		t.Fatalf("reply = %v, want an array", render(got))
	}
	if len(rows) != len(want) {
		t.Fatalf("reply has %d entries, want %d: %v", len(rows), len(want), render(got))
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

func flatten(t *testing.T, xs []any) []string {
	t.Helper()
	out := make([]string, len(xs))
	for i, x := range xs {
		s, ok := x.(string)
		if !ok {
			t.Fatalf("field element %d = %v, want a bulk string", i, render(x))
		}
		out[i] = s
	}
	return out
}

func equal(a, b []string) bool {
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

func TestXrangeFull(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "a", "1")
	do(t, c, opXadd, "s", "2-2", "b", "2")
	do(t, c, opXadd, "s", "3-3", "c", "3")
	wantEntries(t, do(t, c, opXrange, "s", "-", "+"),
		e("1-1", "a", "1"), e("2-2", "b", "2"), e("3-3", "c", "3"))
}

func TestXrangeMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantEntries(t, do(t, c, opXrange, "nokey", "-", "+"))
}

func TestXrangeBounds(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0", "4-0", "5-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	// Inclusive window.
	wantEntries(t, do(t, c, opXrange, "s", "2-0", "4-0"),
		e("2-0", "f", "v"), e("3-0", "f", "v"), e("4-0", "f", "v"))
	// A start above every entry is empty.
	wantEntries(t, do(t, c, opXrange, "s", "9-0", "+"))
	// An end below every entry is empty.
	wantEntries(t, do(t, c, opXrange, "s", "-", "0-5"))
}

func TestXrangeExclusive(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	// Exclusive start drops 1-0, exclusive end drops 3-0.
	wantEntries(t, do(t, c, opXrange, "s", "(1-0", "(3-0"),
		e("2-0", "f", "v"))
}

func TestXrangePartialID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "5-1", "f", "v")
	do(t, c, opXadd, "s", "5-2", "f", "v")
	do(t, c, opXadd, "s", "6-0", "f", "v")
	// Bare "5" as start is 5-0, as end is 5-max: the whole millisecond 5.
	wantEntries(t, do(t, c, opXrange, "s", "5", "5"),
		e("5-1", "f", "v"), e("5-2", "f", "v"))
}

func TestXrangeCount(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0", "4-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	wantEntries(t, do(t, c, opXrange, "s", "-", "+", "COUNT", "2"),
		e("1-0", "f", "v"), e("2-0", "f", "v"))
	// COUNT 0 yields nothing.
	wantEntries(t, do(t, c, opXrange, "s", "-", "+", "COUNT", "0"))
}

func TestXrevrange(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "a", "1")
	do(t, c, opXadd, "s", "2-2", "b", "2")
	do(t, c, opXadd, "s", "3-3", "c", "3")
	// Reversed bound order: XREVRANGE key end start.
	wantEntries(t, do(t, c, opXrevrange, "s", "+", "-"),
		e("3-3", "c", "3"), e("2-2", "b", "2"), e("1-1", "a", "1"))
	// A window with COUNT, newest first.
	wantEntries(t, do(t, c, opXrevrange, "s", "3-3", "2-2", "COUNT", "1"),
		e("3-3", "c", "3"))
}

func TestXrangeSkipsTombstones(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	do(t, c, opXdel, "s", "2-0")
	wantEntries(t, do(t, c, opXrange, "s", "-", "+"),
		e("1-0", "f", "v"), e("3-0", "f", "v"))
	wantEntries(t, do(t, c, opXrevrange, "s", "+", "-"),
		e("3-0", "f", "v"), e("1-0", "f", "v"))
}

// TestXrangeAcrossBlocks forces the native band and its directory seek: enough
// entries to close many blocks, then windows that open mid-run and span several
// blocks in both directions.
func TestXrangeAcrossBlocks(t *testing.T) {
	c := newHarness(t).NewConn()
	const n = 500 // well past blockCap (128), so the stream upgrades and spans blocks
	for i := 1; i <= n; i++ {
		do(t, c, opXadd, "s", fmt.Sprintf("%d-0", i), "f", fmt.Sprintf("v%d", i))
	}
	wantInt(t, do(t, c, opXlen, "s"), n)

	// A forward window that opens and closes mid-block.
	want := make([]entry, 0, 5)
	for i := 200; i <= 204; i++ {
		want = append(want, e(fmt.Sprintf("%d-0", i), "f", fmt.Sprintf("v%d", i)))
	}
	wantEntries(t, do(t, c, opXrange, "s", "200-0", "204-0"), want...)

	// The same window in reverse.
	rev := make([]entry, 0, 5)
	for i := 204; i >= 200; i-- {
		rev = append(rev, e(fmt.Sprintf("%d-0", i), "f", fmt.Sprintf("v%d", i)))
	}
	wantEntries(t, do(t, c, opXrevrange, "s", "204-0", "200-0"), rev...)

	// A COUNT-bounded read from the front still stops at the limit across blocks.
	head := make([]entry, 0, 130)
	for i := 1; i <= 130; i++ {
		head = append(head, e(fmt.Sprintf("%d-0", i), "f", fmt.Sprintf("v%d", i)))
	}
	wantEntries(t, do(t, c, opXrange, "s", "-", "+", "COUNT", "130"), head...)
}

func TestXrangeInvalidBound(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	wantErr(t, do(t, c, opXrange, "s", "bad", "+"),
		"ERR Invalid stream ID specified as stream command argument")
	wantErr(t, do(t, c, opXrange, "s", "-", "nope"),
		"ERR Invalid stream ID specified as stream command argument")
}

func TestXrangeBadCount(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-1", "f", "v")
	wantErr(t, do(t, c, opXrange, "s", "-", "+", "COUNT"), "ERR syntax error")
	wantErr(t, do(t, c, opXrange, "s", "-", "+", "LIMIT", "5"), "ERR syntax error")
	wantErr(t, do(t, c, opXrange, "s", "-", "+", "COUNT", "x"), "ERR syntax error")
}

func TestXrangeWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXrange, "k", "-", "+"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	wantErr(t, do(t, c, opXrevrange, "k", "+", "-"),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}
