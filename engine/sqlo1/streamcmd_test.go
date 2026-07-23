package sqlo1

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// xent builds one XRANGE reply entry: the two-array of ID bulk and
// field-value array.
func xent(id string, fv ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(id), id, len(fv))
	for _, f := range fv {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
	}
	return b.String()
}

func xarr(ents ...string) string {
	return "*" + strconv.Itoa(len(ents)) + "\r\n" + strings.Join(ents, "")
}

func TestXaddWire(t *testing.T) {
	do, clock := dispatchServer(t)

	// Auto IDs follow the clock; a burst inside one millisecond bumps
	// seq, an advance resets it, and a backwards step bumps off the
	// last ID.
	if got := do("XADD", "s", "*", "f", "v0"); got != "$9\r\n1000000-0\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "*", "f", "v1"); got != "$9\r\n1000000-1\r\n" {
		t.Fatal(got)
	}
	*clock = 1_000_005
	if got := do("XADD", "s", "*", "f", "v2"); got != "$9\r\n1000005-0\r\n" {
		t.Fatal(got)
	}
	*clock = 999_000
	if got := do("XADD", "s", "*", "f", "v3"); got != "$9\r\n1000005-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s"); got != ":4\r\n" {
		t.Fatal(got)
	}

	// Explicit IDs: full, bare ms meaning ms-0, and the two Redis
	// refusals with their exact texts.
	if got := do("XADD", "s", "2000000-5", "a", "b"); got != "$9\r\n2000000-5\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "3000000", "a", "b"); got != "$9\r\n3000000-0\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s", "3000000", "a", "b"); got != "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s2", "0-0", "a", "b"); got != "-ERR The ID specified in XADD must be greater than 0-0\r\n" {
		t.Fatal(got)
	}
	// 0-* cannot generate the zero ID, so the first entry is 0-1.
	if got := do("XADD", "s2", "0-*", "a", "b"); got != "$3\r\n0-1\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s2", "abc-1", "a", "b"); got != "-ERR Invalid stream ID specified as stream command argument\r\n" {
		t.Fatal(got)
	}
	// An unknown option token falls through to the ID parse.
	if got := do("XADD", "s2", "BADOPT", "a", "b"); got != "-ERR Invalid stream ID specified as stream command argument\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "strim", "MAXLEN", "100", "*", "a", "b"); got != "$8\r\n999000-0\r\n" {
		t.Fatal(got)
	}

	// NOMKSTREAM answers the null bulk and leaves no key behind.
	if got := do("XADD", "nosuch", "NOMKSTREAM", "*", "f", "v"); got != "$-1\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE", "nosuch"); got != "+none\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s2", "NOMKSTREAM", "*", "f", "v"); got != "$8\r\n999000-0\r\n" {
		t.Fatal(got)
	}

	// Arity doors: no pairs, torn pairs, and XLEN's fixed shape.
	if got := do("XADD", "s2", "*"); got != "-ERR wrong number of arguments for 'xadd' command\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s2", "*", "f", "v", "g"); got != "-ERR wrong number of arguments for 'xadd' command\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "s2", "extra"); got != "-ERR wrong number of arguments for 'xlen' command\r\n" {
		t.Fatal(got)
	}
	if got := do("XLEN", "nosuch"); got != ":0\r\n" {
		t.Fatal(got)
	}

	// The type answers and both WRONGTYPE directions.
	if got := do("TYPE", "s"); got != "+stream\r\n" {
		t.Fatal(got)
	}
	if got := do("OBJECT", "ENCODING", "s"); got != "$6\r\nstream\r\n" {
		t.Fatal(got)
	}
	do("SET", "str", "v")
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	if got := do("XADD", "str", "*", "f", "v"); got != wrong {
		t.Fatal(got)
	}
	if got := do("XLEN", "str"); got != wrong {
		t.Fatal(got)
	}
	if got := do("LPUSH", "s", "x"); got != wrong {
		t.Fatal(got)
	}

	// DEL retires the stream whole; a recreate starts from a zero last
	// ID and accepts 1-1 again.
	if got := do("DEL", "s2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XADD", "s2", "1-1", "a", "b"); got != "$3\r\n1-1\r\n" {
		t.Fatal(got)
	}

	// XADD preserves a TTL, the restamp door.
	if got := do("EXPIRE", "s2", "100"); got != ":1\r\n" {
		t.Fatal(got)
	}
	do("XADD", "s2", "2-2", "a", "b")
	if got := do("TTL", "s2"); got != ":100\r\n" {
		t.Fatal(got)
	}
}

func TestXrangeWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("XADD", "s", "1-1", "a", "1")
	do("XADD", "s", "1-2", "a", "2")
	do("XADD", "s", "2-1", "b", "3", "bb", "33")
	do("XADD", "s", "3-0", "c", "4")
	e11 := xent("1-1", "a", "1")
	e12 := xent("1-2", "a", "2")
	e21 := xent("2-1", "b", "3", "bb", "33")
	e30 := xent("3-0", "c", "4")

	// The infinities, explicit bounds, and the bare-ms rule: a bare
	// start covers from seq 0, a bare end through the whole
	// millisecond, so XRANGE s 1 1 spans ms 1.
	if got := do("XRANGE", "s", "-", "+"); got != xarr(e11, e12, e21, e30) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "1", "1"); got != xarr(e11, e12) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "1-2", "2-1"); got != xarr(e12, e21) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "1-2", "2"); got != xarr(e12, e21) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "4", "+"); got != "*0\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "3", "1"); got != "*0\r\n" {
		t.Fatal(got)
	}

	// COUNT caps, and zero or negative answer empty while the type
	// check still runs first.
	if got := do("XRANGE", "s", "-", "+", "COUNT", "2"); got != xarr(e11, e12) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "+", "COUNT", "0"); got != "*0\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "+", "COUNT", "-1"); got != "*0\r\n" {
		t.Fatal(got)
	}

	// Exclusive bounds step inward. A bare exclusive ms excludes only
	// its seq-0 default, Redis's streamIncrID over the parsed ID, so
	// (1 still spans 1-1 up. The un-steppable ends are Redis's
	// interval errors.
	if got := do("XRANGE", "s", "(1-1", "+"); got != xarr(e12, e21, e30) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "(3-0"); got != xarr(e11, e12, e21) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "(1", "+"); got != xarr(e11, e12, e21, e30) {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "(0-0"); got != "-ERR invalid end ID for the interval\r\n" {
		t.Fatal(got)
	}
	max := "18446744073709551615"
	if got := do("XRANGE", "s", "("+max+"-"+max, "+"); got != "-ERR invalid start ID for the interval\r\n" {
		t.Fatal(got)
	}

	// The malformed bounds: garbage, exclusive infinities, and ms past
	// 64 bits all answer the invalid ID text.
	bad := "-ERR Invalid stream ID specified as stream command argument\r\n"
	if got := do("XRANGE", "s", "bad", "+"); got != bad {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "(-", "+"); got != bad {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "(+"); got != bad {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "18446744073709551616", "+"); got != bad {
		t.Fatal(got)
	}

	// The option doors: COUNT needs its value, unknown tokens refuse,
	// and a bad count answers the integer text.
	if got := do("XRANGE", "s", "-", "+", "COUNT"); got != "-ERR syntax error\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "+", "FOO", "1"); got != "-ERR syntax error\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "+", "COUNT", "x"); got != "-ERR value is not an integer or out of range\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s"); got != "-ERR wrong number of arguments for 'xrange' command\r\n" {
		t.Fatal(got)
	}

	// XREVRANGE takes end first and walks backward, with the same
	// bare-ms and exclusive grammar.
	if got := do("XREVRANGE", "s", "+", "-"); got != xarr(e30, e21, e12, e11) {
		t.Fatal(got)
	}
	if got := do("XREVRANGE", "s", "1", "1"); got != xarr(e12, e11) {
		t.Fatal(got)
	}
	if got := do("XREVRANGE", "s", "+", "-", "COUNT", "2"); got != xarr(e30, e21) {
		t.Fatal(got)
	}
	if got := do("XREVRANGE", "s", "+", "(1-2"); got != xarr(e30, e21) {
		t.Fatal(got)
	}
	if got := do("XREVRANGE", "s", "-", "+"); got != "*0\r\n" {
		t.Fatal(got)
	}

	// A missing key is the empty array; a wrong type refuses even at
	// count zero.
	if got := do("XRANGE", "nosuch", "-", "+"); got != "*0\r\n" {
		t.Fatal(got)
	}
	do("SET", "str", "v")
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	if got := do("XRANGE", "str", "-", "+"); got != wrong {
		t.Fatal(got)
	}
	if got := do("XRANGE", "str", "-", "+", "COUNT", "0"); got != wrong {
		t.Fatal(got)
	}
	if got := do("XREVRANGE", "str", "+", "-"); got != wrong {
		t.Fatal(got)
	}
}

// TestXrangeWireMultiRun spans the run cut: 300 entries cut at the
// 128-entry cap, so the ranges cross fence boundaries.
func TestXrangeWireMultiRun(t *testing.T) {
	do, _ := dispatchServer(t)
	ents := make([]string, 301)
	for i := 1; i <= 300; i++ {
		id := fmt.Sprintf("%d-1", i)
		v := strconv.Itoa(i)
		if got := do("XADD", "s", id, "f", v); got != fmt.Sprintf("$%d\r\n%s\r\n", len(id), id) {
			t.Fatalf("XADD %d: %s", i, got)
		}
		ents[i] = xent(id, "f", v)
	}
	if got := do("XLEN", "s"); got != ":300\r\n" {
		t.Fatal(got)
	}
	if got := do("XRANGE", "s", "-", "+"); got != xarr(ents[1:301]...) {
		t.Fatal("full range differs")
	}
	if got := do("XRANGE", "s", "100", "250", "COUNT", "50"); got != xarr(ents[100:150]...) {
		t.Fatal("counted window differs")
	}
	// A window straddling the first cut boundary.
	if got := do("XRANGE", "s", "126", "130"); got != xarr(ents[126:131]...) {
		t.Fatal("boundary window differs")
	}
	rev := make([]string, 0, 300)
	for i := 300; i >= 1; i-- {
		rev = append(rev, ents[i])
	}
	if got := do("XREVRANGE", "s", "+", "-"); got != xarr(rev...) {
		t.Fatal("full rev range differs")
	}
	if got := do("XREVRANGE", "s", "250", "100"); got != xarr(rev[50:201]...) {
		t.Fatal("rev window differs")
	}
}
