package sqlo1

import (
	"strconv"
	"strings"
	"testing"
)

// TestXreadgroupWire pins the XREADGROUP surface against Redis 8.8:
// the option scan, the per-key error order, the meaningless-ID texts,
// the > form's row omission, and the history form's deleted-entry row
// were all recorded live before landing here.
func TestXreadgroupWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	if got := do("XGROUP", "CREATE", "s", "grp", "0"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	// The parse ladder before any key work.
	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s"},
			"-ERR wrong number of arguments for 'xreadgroup' command\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "COUNT", "1", "s"},
			"-ERR syntax error\r\n"},
		{[]string{"XREADGROUP", "COUNT", "1", "NOACK", "STREAMS", "s", ">"},
			"-ERR Missing GROUP option for XREADGROUP\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "0", "noack"},
			"-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "COUNT", "abc", "STREAMS", "s", ">"},
			"-ERR value is not an integer or out of range\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "BLOCK", "abc", "STREAMS", "s", ">"},
			"-ERR timeout is not an integer or out of range\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "BLOCK", "-1", "STREAMS", "s", ">"},
			"-ERR timeout is negative\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "nk", ">"},
			"-NOGROUP No such key 'nk' or consumer group 'grp' in XREADGROUP with GROUP option\r\n"},
		// A bad ID on the first key wins over a missing later key.
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "nk", "2-*", ">"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "$"},
			"-ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.\r\n"},
		{[]string{"XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "+"},
			"-ERR The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.\r\n"},
	}
	for _, tc := range errs {
		if got := do(tc.args...); got != tc.want {
			t.Fatalf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}
	// NOGROUP checks per key before the ID argument parses at all.
	do("XADD", "pk", "1-1", "f", "v")
	if got := do("XREADGROUP", "GROUP", "nog", "c1", "STREAMS", "pk", "2-*"); got != "-NOGROUP No such key 'pk' or consumer group 'nog' in XREADGROUP with GROUP option\r\n" {
		t.Fatal(got)
	}
	// WRONGTYPE outranks NOGROUP on its key.
	do("SET", "str", "x")
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "str", ">"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatal(got)
	}

	// The > form delivers, the empty re-poll answers the null array.
	row := func(key string, ents ...string) string {
		return "*2\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n" + xarr(ents...)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"); got != "*1\r\n"+row("s", xent("1-1", "f", "v1"), xent("2-2", "f", "v2")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"); got != "*-1\r\n" {
		t.Fatal(got)
	}

	// The history form: exclusive start, bare ms meaning ms-0, COUNT,
	// and the always-echoed key even when the consumer has nothing.
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "0"); got != "*1\r\n"+row("s", xent("1-1", "f", "v1"), xent("2-2", "f", "v2")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "1-1"); got != "*1\r\n"+row("s", xent("2-2", "f", "v2")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "1"); got != "*1\r\n"+row("s", xent("1-1", "f", "v1"), xent("2-2", "f", "v2")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "COUNT", "1", "STREAMS", "s", "0"); got != "*1\r\n"+row("s", xent("1-1", "f", "v1")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c2", "STREAMS", "s", "0"); got != "*1\r\n"+row("s") {
		t.Fatal(got)
	}

	// Mixed forms: the history key echoes empty-handed or not, and a >
	// key with nothing new drops its row.
	do("XADD", "s2", "1-1", "f", "v")
	do("XGROUP", "CREATE", "s2", "grp", "$")
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "s2", "1-1", ">"); got != "*1\r\n"+row("s", xent("2-2", "f", "v2")) {
		t.Fatal(got)
	}

	// A pending entry the stream no longer holds echoes [id, nil].
	do("XADD", "s", "3-3", "f", "v3")
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">"); got != "*1\r\n"+row("s", xent("3-3", "f", "v3")) {
		t.Fatal(got)
	}
	if got := do("XTRIM", "s", "MINID", "2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "0"); got != "*1\r\n"+row("s", "*2\r\n$3\r\n1-1\r\n*-1\r\n", xent("2-2", "f", "v2"), xent("3-3", "f", "v3")) {
		t.Fatal(got)
	}

	// NOACK delivers without leaving a pending entry behind.
	do("XADD", "s", "4-4", "f", "v4")
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "NOACK", "STREAMS", "s", ">"); got != "*1\r\n"+row("s", xent("4-4", "f", "v4")) {
		t.Fatal(got)
	}
	if got := do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", "3-3"); got != "*1\r\n"+row("s") {
		t.Fatal(got)
	}
}

// TestXackWire pins XACK: the type check outranks the ID parse, which
// outranks the zero replies a missing key or group answers, duplicates
// count once, and bare ms parses as ms-0.
func TestXackWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	do("XADD", "s", "3-3", "f", "v3")
	do("XGROUP", "CREATE", "s", "grp", "0")
	do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">")
	do("SET", "str", "x")

	if got := do("XACK", "s", "grp"); got != "-ERR wrong number of arguments for 'xack' command\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "str", "grp", "2-*"); got != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
		t.Fatal(got)
	}
	for _, bad := range []string{"2-*", "-", "+", "(1-1", "abc"} {
		if got := do("XACK", "nk", "grp", bad); got != "-ERR Invalid stream ID specified as stream command argument\r\n" {
			t.Fatalf("XACK bad id %q = %q", bad, got)
		}
	}
	if got := do("XACK", "nk", "grp", "1-1"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "s", "nogroup", "1-1"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "s", "grp", "2-2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "s", "grp", "2-2"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "s", "grp", "3-3", "3-3"); got != ":1\r\n" {
		t.Fatal(got)
	}
	// Bare ms is a valid ID, just not a pending one.
	if got := do("XACK", "s", "grp", "5"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("XACK", "s", "grp", "1-1", "9-9"); got != ":1\r\n" {
		t.Fatal(got)
	}
}

// TestXinfoFullPelWire pins the FULL groups array with a live PEL: the
// group's four-element pending rows, the consumers' three-element
// rows, the delivery bookkeeping after a history re-read, and the
// COUNT window over both lists.
func TestXinfoFullPelWire(t *testing.T) {
	do, clock := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	do("XADD", "s", "3-3", "f", "v3")
	do("XGROUP", "CREATE", "s", "grp", "0")
	do("XREADGROUP", "GROUP", "grp", "b", "COUNT", "2", "STREAMS", "s", ">")
	*clock = 1_000_100
	do("XREADGROUP", "GROUP", "grp", "a", "STREAMS", "s", ">")
	// b re-reads 2-2 alone: its delivery count moves to 2 at the new
	// clock while 1-1 keeps the original stamp.
	*clock = 1_000_200
	do("XREADGROUP", "GROUP", "grp", "b", "STREAMS", "s", "1-1")

	b := func(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
	i := func(n int64) string { return ":" + strconv.FormatInt(n, 10) + "\r\n" }
	grow := func(id, cons string, dtime, dcount int64) string {
		return "*4\r\n" + b(id) + b(cons) + i(dtime) + i(dcount)
	}
	crow := func(id string, dtime, dcount int64) string {
		return "*3\r\n" + b(id) + i(dtime) + i(dcount)
	}
	wantGroups := b("groups") + "*1\r\n" +
		"*16\r\n" +
		b("name") + b("grp") +
		b("last-delivered-id") + b("3-3") +
		b("entries-read") + i(3) +
		b("lag") + i(0) +
		b("pel-count") + i(3) +
		b("nacked-count") + i(0) +
		b("pending") + "*3\r\n" +
		grow("1-1", "b", 1_000_000, 1) +
		grow("2-2", "b", 1_000_200, 2) +
		grow("3-3", "a", 1_000_100, 1) +
		b("consumers") + "*2\r\n" +
		"*10\r\n" +
		b("name") + b("a") +
		b("seen-time") + i(1_000_100) +
		b("active-time") + i(1_000_100) +
		b("pel-count") + i(1) +
		b("pending") + "*1\r\n" + crow("3-3", 1_000_100, 1) +
		"*10\r\n" +
		b("name") + b("b") +
		b("seen-time") + i(1_000_200) +
		b("active-time") + i(1_000_000) +
		b("pel-count") + i(2) +
		b("pending") + "*2\r\n" + crow("1-1", 1_000_000, 1) + crow("2-2", 1_000_200, 2)
	got := do("XINFO", "STREAM", "s", "FULL")
	if !strings.HasSuffix(got, wantGroups) {
		t.Fatalf("FULL groups tail = %q, want suffix %q", got, wantGroups)
	}

	// COUNT 1 windows the group rows and each consumer's rows.
	got = do("XINFO", "STREAM", "s", "FULL", "COUNT", "1")
	wantWin := b("pending") + "*1\r\n" + grow("1-1", "b", 1_000_000, 1) +
		b("consumers") + "*2\r\n" +
		"*10\r\n" +
		b("name") + b("a") +
		b("seen-time") + i(1_000_100) +
		b("active-time") + i(1_000_100) +
		b("pel-count") + i(1) +
		b("pending") + "*1\r\n" + crow("3-3", 1_000_100, 1) +
		"*10\r\n" +
		b("name") + b("b") +
		b("seen-time") + i(1_000_200) +
		b("active-time") + i(1_000_000) +
		b("pel-count") + i(2) +
		b("pending") + "*1\r\n" + crow("1-1", 1_000_000, 1)
	if !strings.HasSuffix(got, wantWin) {
		t.Fatalf("FULL COUNT 1 tail = %q, want suffix %q", got, wantWin)
	}
}
