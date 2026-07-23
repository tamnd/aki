package sqlo1

import (
	"strconv"
	"testing"
)

// prow renders one XPENDING extended row.
func prow(id, cons string, idle int64, dcount int) string {
	return "*4\r\n$" + strconv.Itoa(len(id)) + "\r\n" + id + "\r\n" +
		"$" + strconv.Itoa(len(cons)) + "\r\n" + cons + "\r\n" +
		":" + strconv.FormatInt(idle, 10) + "\r\n" +
		":" + strconv.Itoa(dcount) + "\r\n"
}

// TestXpendingWire pins the XPENDING surface against Redis 8.8: the
// summary and extended forms, the four-or-five-argument syntax error,
// the parse-before-key precedence, the ignored trailing arguments,
// and the empty-summary nil shapes were all recorded live.
func TestXpendingWire(t *testing.T) {
	do, clock := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	do("XADD", "s", "3-3", "f", "v3")
	if got := do("XGROUP", "CREATE", "s", "grp", "0"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	do("SET", "str", "x")

	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XPENDING", "s"},
			"-ERR wrong number of arguments for 'xpending' command\r\n"},
		{[]string{"XPENDING", "s", "grp", "-"},
			"-ERR syntax error\r\n"},
		{[]string{"XPENDING", "s", "grp", "-", "+"},
			"-ERR syntax error\r\n"},
		{[]string{"XPENDING", "s", "grp", "IDLE", "10", "-", "+"},
			"-ERR syntax error\r\n"},
		{[]string{"XPENDING", "s", "grp", "IDLE", "abc", "-", "+", "10"},
			"-ERR value is not an integer or out of range\r\n"},
		{[]string{"XPENDING", "s", "grp", "bad", "+", "10"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XPENDING", "s", "grp", "-", "+", "abc"},
			"-ERR value is not an integer or out of range\r\n"},
		// The extended form parses before the key resolves, so the bad
		// ID outranks WRONGTYPE; a clean parse then hits the type.
		{[]string{"XPENDING", "str", "grp", "bad", "+", "10"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XPENDING", "str", "grp", "-", "+", "10"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XPENDING", "str", "grp"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		// The bare NOGROUP text, no XREADGROUP clause.
		{[]string{"XPENDING", "nk", "grp"},
			"-NOGROUP No such key 'nk' or consumer group 'grp'\r\n"},
		{[]string{"XPENDING", "s", "ng", "-", "+", "10"},
			"-NOGROUP No such key 's' or consumer group 'ng'\r\n"},
	}
	for _, tc := range errs {
		if got := do(tc.args...); got != tc.want {
			t.Fatalf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}

	// An empty PEL answers zero with nil window and nil consumers.
	if got := do("XPENDING", "s", "grp"); got != "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n" {
		t.Fatalf("empty summary = %q", got)
	}

	// c1 takes 1-1 and 2-2, then 100ms later c2 takes 3-3.
	do("XREADGROUP", "GROUP", "grp", "c1", "COUNT", "2", "STREAMS", "s", ">")
	*clock += 100
	do("XREADGROUP", "GROUP", "grp", "c2", "STREAMS", "s", ">")

	want := "*4\r\n:3\r\n$3\r\n1-1\r\n$3\r\n3-3\r\n*2\r\n" +
		"*2\r\n$2\r\nc1\r\n$1\r\n2\r\n" +
		"*2\r\n$2\r\nc2\r\n$1\r\n1\r\n"
	if got := do("XPENDING", "s", "grp"); got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}

	full := "*3\r\n" + prow("1-1", "c1", 100, 1) + prow("2-2", "c1", 100, 1) + prow("3-3", "c2", 0, 1)
	if got := do("XPENDING", "s", "grp", "-", "+", "10"); got != full {
		t.Fatalf("extended = %q, want %q", got, full)
	}
	// The IDLE clause, the consumer filter, and the pinned tolerance
	// for arguments after the consumer.
	if got := do("XPENDING", "s", "grp", "IDLE", "50", "-", "+", "10"); got != "*2\r\n"+prow("1-1", "c1", 100, 1)+prow("2-2", "c1", 100, 1) {
		t.Fatalf("idle filter = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10", "c2"); got != "*1\r\n"+prow("3-3", "c2", 0, 1) {
		t.Fatalf("consumer filter = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10", "c2", "junk", "more"); got != "*1\r\n"+prow("3-3", "c2", 0, 1) {
		t.Fatalf("trailing args = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10", "nobody"); got != "*0\r\n" {
		t.Fatalf("missing consumer = %q", got)
	}
	// Exclusive bounds on both ends, and the count edge cases.
	if got := do("XPENDING", "s", "grp", "(1-1", "+", "10"); got != "*2\r\n"+prow("2-2", "c1", 100, 1)+prow("3-3", "c2", 0, 1) {
		t.Fatalf("exclusive start = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "(3-3", "10"); got != "*2\r\n"+prow("1-1", "c1", 100, 1)+prow("2-2", "c1", 100, 1) {
		t.Fatalf("exclusive end = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "0"); got != "*0\r\n" {
		t.Fatalf("count 0 = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "-3"); got != "*0\r\n" {
		t.Fatalf("negative count = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "2"); got != "*2\r\n"+prow("1-1", "c1", 100, 1)+prow("2-2", "c1", 100, 1) {
		t.Fatalf("count 2 = %q", got)
	}
}

// TestXclaimWire pins the XCLAIM surface: the key and group resolve
// before the IDs and options scan, IDs parse greedily until the first
// non-ID token, an option missing its argument falls to the
// unrecognized-option text, and the reply is entries or bare IDs
// under JUSTID, all recorded live against Redis 8.8.
func TestXclaimWire(t *testing.T) {
	do, clock := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	if got := do("XGROUP", "CREATE", "s", "grp", "0"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">")
	do("SET", "str", "x")

	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XCLAIM", "s", "grp", "c1", "0"},
			"-ERR wrong number of arguments for 'xclaim' command\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "abc", "1-1"},
			"-ERR Invalid min-idle-time argument for XCLAIM\r\n"},
		// The key resolves before the IDs, so WRONGTYPE and NOGROUP
		// outrank the malformed ID and the bad option.
		{[]string{"XCLAIM", "str", "grp", "c1", "0", "bad"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XCLAIM", "nk", "grp", "c1", "0", "1-1", "BADOPT"},
			"-NOGROUP No such key 'nk' or consumer group 'grp'\r\n"},
		{[]string{"XCLAIM", "s", "ng", "c1", "0", "1-1"},
			"-NOGROUP No such key 's' or consumer group 'ng'\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "IDLE", "abc"},
			"-ERR Invalid IDLE option argument for XCLAIM\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "TIME", "abc"},
			"-ERR Invalid TIME option argument for XCLAIM\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "RETRYCOUNT", "abc"},
			"-ERR Invalid RETRYCOUNT option argument for XCLAIM\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "LASTID", "bad"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "FOO"},
			"-ERR Unrecognized XCLAIM option 'FOO'\r\n"},
		// A trailing option name with no argument, and an ID after the
		// options began, both read as unrecognized options.
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "IDLE"},
			"-ERR Unrecognized XCLAIM option 'IDLE'\r\n"},
		{[]string{"XCLAIM", "s", "grp", "c1", "0", "1-1", "FORCE", "2-2"},
			"-ERR Unrecognized XCLAIM option '2-2'\r\n"},
	}
	for _, tc := range errs {
		if got := do(tc.args...); got != tc.want {
			t.Fatalf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}

	// No IDs at all is a legal empty claim, pinned.
	if got := do("XCLAIM", "s", "grp", "c2", "0", "FORCE"); got != "*0\r\n" {
		t.Fatalf("zero-ID claim = %q", got)
	}
	// A plain claim answers the entries and bumps the count; JUSTID
	// answers bare IDs and freezes it. Negative min-idle reads as
	// zero, pinned.
	*clock += 50
	if got := do("XCLAIM", "s", "grp", "c2", "-5", "1-1"); got != "*1\r\n"+xent("1-1", "f", "v1") {
		t.Fatalf("plain claim = %q", got)
	}
	if got := do("XCLAIM", "s", "grp", "c2", "0", "2-2", "JUSTID"); got != "*1\r\n$3\r\n2-2\r\n" {
		t.Fatalf("justid claim = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10"); got != "*2\r\n"+prow("1-1", "c2", 0, 2)+prow("2-2", "c2", 0, 1) {
		t.Fatalf("after claims = %q", got)
	}
	// The min-idle filter, an unpending ID, and a bare-ms ID all skip
	// silently.
	if got := do("XCLAIM", "s", "grp", "c1", "500", "1-1"); got != "*0\r\n" {
		t.Fatalf("min-idle skip = %q", got)
	}
	if got := do("XCLAIM", "s", "grp", "c1", "0", "9-9"); got != "*0\r\n" {
		t.Fatalf("unpending claim = %q", got)
	}
	if got := do("XCLAIM", "s", "grp", "c1", "0", "1"); got != "*0\r\n" {
		t.Fatalf("bare-ms claim = %q", got)
	}
	// A pending entry trimmed out of the stream drops from the PEL and
	// never reaches the reply.
	if got := do("XTRIM", "s", "MINID", "2"); got != ":1\r\n" {
		t.Fatalf("trim = %q", got)
	}
	if got := do("XCLAIM", "s", "grp", "c1", "0", "1-1"); got != "*0\r\n" {
		t.Fatalf("deleted claim = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10"); got != "*1\r\n"+prow("2-2", "c2", 0, 1) {
		t.Fatalf("after deleted claim = %q", got)
	}
}

// TestXautoclaimWire pins the XAUTOCLAIM surface: everything parses
// before the key resolves, COUNT rejects zero, negatives, non-integers,
// and the overflow guard with one text, the cursor resumes inclusively
// and answers 0-0 when drained, and trimmed entries land in the third
// reply array, all recorded live against Redis 8.8.
func TestXautoclaimWire(t *testing.T) {
	do, clock := dispatchServer(t)
	do("XADD", "s", "1-1", "f", "v1")
	do("XADD", "s", "2-2", "f", "v2")
	do("XADD", "s", "3-3", "f", "v3")
	if got := do("XGROUP", "CREATE", "s", "grp", "0"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	do("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "s", ">")
	do("SET", "str", "x")

	errs := []struct {
		args []string
		want string
	}{
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0"},
			"-ERR wrong number of arguments for 'xautoclaim' command\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "abc", "0"},
			"-ERR Invalid min-idle-time argument for XAUTOCLAIM\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "bad"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		// Parse before key: the bad start outranks WRONGTYPE.
		{[]string{"XAUTOCLAIM", "str", "grp", "c1", "0", "bad"},
			"-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XAUTOCLAIM", "str", "grp", "c1", "0", "0"},
			"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "0", "COUNT", "abc"},
			"-ERR COUNT must be > 0\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "0", "COUNT", "0"},
			"-ERR COUNT must be > 0\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "0", "COUNT", "-1"},
			"-ERR COUNT must be > 0\r\n"},
		// The count-times-ten attempt budget must not overflow, and
		// Redis answers the same text for the guard.
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "0", "COUNT", "9223372036854775807"},
			"-ERR COUNT must be > 0\r\n"},
		{[]string{"XAUTOCLAIM", "s", "grp", "c1", "0", "0", "JUNK"},
			"-ERR syntax error\r\n"},
		{[]string{"XAUTOCLAIM", "nk", "grp", "c1", "0", "0"},
			"-NOGROUP No such key 'nk' or consumer group 'grp'\r\n"},
		{[]string{"XAUTOCLAIM", "s", "ng", "c1", "0", "0"},
			"-NOGROUP No such key 's' or consumer group 'ng'\r\n"},
	}
	for _, tc := range errs {
		if got := do(tc.args...); got != tc.want {
			t.Fatalf("%v = %q, want %q", tc.args, got, tc.want)
		}
	}

	// A two-entry window parks the cursor on 3-3; the resume from the
	// cursor drains and answers 0-0.
	*clock += 100
	if got := do("XAUTOCLAIM", "s", "grp", "c2", "0", "0", "COUNT", "2"); got != "*3\r\n$3\r\n3-3\r\n*2\r\n"+xent("1-1", "f", "v1")+xent("2-2", "f", "v2")+"*0\r\n" {
		t.Fatalf("window = %q", got)
	}
	if got := do("XAUTOCLAIM", "s", "grp", "c2", "0", "3-3"); got != "*3\r\n$3\r\n0-0\r\n*1\r\n"+xent("3-3", "f", "v3")+"*0\r\n" {
		t.Fatalf("resume = %q", got)
	}
	// JUSTID answers bare IDs without bumping the counts, and the
	// exclusive start is honored.
	if got := do("XAUTOCLAIM", "s", "grp", "c3", "0", "0", "JUSTID"); got != "*3\r\n$3\r\n0-0\r\n*3\r\n$3\r\n1-1\r\n$3\r\n2-2\r\n$3\r\n3-3\r\n*0\r\n" {
		t.Fatalf("justid = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10"); got != "*3\r\n"+prow("1-1", "c3", 0, 2)+prow("2-2", "c3", 0, 2)+prow("3-3", "c3", 0, 2) {
		t.Fatalf("after justid = %q", got)
	}
	if got := do("XAUTOCLAIM", "s", "grp", "c3", "0", "(1-1", "COUNT", "1"); got != "*3\r\n$3\r\n3-3\r\n*1\r\n"+xent("2-2", "f", "v2")+"*0\r\n" {
		t.Fatalf("exclusive start = %q", got)
	}
	// Nothing idle enough: the walk drains with empty arrays.
	if got := do("XAUTOCLAIM", "s", "grp", "c1", "500", "0"); got != "*3\r\n$3\r\n0-0\r\n*0\r\n*0\r\n" {
		t.Fatalf("idle skip = %q", got)
	}
	// A trimmed pending entry lands in the deleted array and leaves
	// the PEL.
	if got := do("XTRIM", "s", "MINID", "2"); got != ":1\r\n" {
		t.Fatalf("trim = %q", got)
	}
	if got := do("XAUTOCLAIM", "s", "grp", "c2", "0", "0"); got != "*3\r\n$3\r\n0-0\r\n*2\r\n"+xent("2-2", "f", "v2")+xent("3-3", "f", "v3")+"*1\r\n$3\r\n1-1\r\n" {
		t.Fatalf("deleted = %q", got)
	}
	if got := do("XPENDING", "s", "grp", "-", "+", "10"); got != "*2\r\n"+prow("2-2", "c2", 0, 4)+prow("3-3", "c2", 0, 3) {
		t.Fatalf("after deleted = %q", got)
	}
}
