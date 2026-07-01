package f1srv

import (
	"bufio"
	"sort"
	"testing"
	"time"
)

// scanOnce issues one SCAN call and returns the next cursor and the key batch. A SCAN reply is
// a two-element array of the cursor bulk and the nested key array, so it reads the outer header,
// the cursor, then the key array with the shared helper.
func scanOnce(t *testing.T, rw *bufio.ReadWriter, args ...string) (string, []string) {
	t.Helper()
	cmd(t, rw, args...)
	h := readReply(t, rw)
	if h != "*2" {
		t.Fatalf("SCAN header = %q, want *2", h)
	}
	cur := readReply(t, rw)
	if len(cur) == 0 || cur[0] != '$' {
		t.Fatalf("SCAN cursor = %q, want a bulk", cur)
	}
	keys := readArray(t, rw)
	return cur[1:], keys
}

// scanAll runs a full SCAN iteration from cursor 0 to the done sentinel and returns every key
// seen, the way a client walks the keyspace. The bounded per-call count forces several rounds
// so the resumable cursor is exercised, not just a single batch.
func scanAll(t *testing.T, rw *bufio.ReadWriter, extra ...string) []string {
	t.Helper()
	var all []string
	cursor := "0"
	for round := 0; ; round++ {
		if round > 100000 {
			t.Fatalf("SCAN did not terminate")
		}
		args := append([]string{"SCAN", cursor, "COUNT", "1"}, extra...)
		next, keys := scanOnce(t, rw, args...)
		all = append(all, keys...)
		if next == "0" {
			break
		}
		cursor = next
	}
	return all
}

func sortedEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// KEYS returns every key whose name matches the glob, across all types, and honours '*', a
// literal, a prefix glob, and a class. The order is implementation-defined, so every assertion
// is on set-equality, matching how Redis 8.8 and Valkey 9.1 leave order unspecified.
func TestKeysGlob(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "one", "1", "two", "2", "three", "3")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "h1", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "s1", "m")
	expect(t, rw, ":1")
	cmd(t, rw, "RPUSH", "l1", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "ZADD", "z1", "1", "m")
	expect(t, rw, ":1")

	cmd(t, rw, "KEYS", "*")
	got := readArray(t, rw)
	want := []string{"one", "two", "three", "h1", "s1", "l1", "z1"}
	if !sortedEqual(got, want) {
		t.Fatalf("KEYS * = %v, want %v", got, want)
	}

	cmd(t, rw, "KEYS", "t*")
	got = readArray(t, rw)
	if !sortedEqual(got, []string{"two", "three"}) {
		t.Fatalf("KEYS t* = %v", got)
	}

	cmd(t, rw, "KEYS", "one")
	got = readArray(t, rw)
	if !sortedEqual(got, []string{"one"}) {
		t.Fatalf("KEYS one = %v", got)
	}

	cmd(t, rw, "KEYS", "[hs]1")
	got = readArray(t, rw)
	if !sortedEqual(got, []string{"h1", "s1"}) {
		t.Fatalf("KEYS [hs]1 = %v", got)
	}

	cmd(t, rw, "KEYS", "nomatch*")
	got = readArray(t, rw)
	if len(got) != 0 {
		t.Fatalf("KEYS nomatch* = %v, want empty", got)
	}
}

// A full SCAN iteration visits every key exactly the set KEYS reports, and the MATCH, COUNT, and
// TYPE options filter that set. The cursor is opaque and order is unspecified, so the test walks
// to the done sentinel and asserts on the collected set.
func TestScanFullIteration(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	want := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		key := "k" + itoaTest(i)
		cmd(t, rw, "SET", key, "v")
		expect(t, rw, "+OK")
		want = append(want, key)
	}

	got := scanAll(t, rw)
	if !sortedEqual(got, want) {
		t.Fatalf("SCAN full iteration returned %d keys, want %d", len(got), len(want))
	}

	// MATCH filters the walk to the glob.
	matched := scanAll(t, rw, "MATCH", "k1*")
	wantMatch := []string{}
	for _, k := range want {
		if globMatch([]byte("k1*"), []byte(k)) {
			wantMatch = append(wantMatch, k)
		}
	}
	if !sortedEqual(matched, wantMatch) {
		t.Fatalf("SCAN MATCH k1* = %v, want %v", matched, wantMatch)
	}
}

// SCAN TYPE keeps only keys of the named type, and completes over the whole keyspace.
func TestScanTypeFilter(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str1", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "str2", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "hash1", "f", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "RPUSH", "list1", "x")
	expect(t, rw, ":1")

	got := scanAll(t, rw, "TYPE", "string")
	if !sortedEqual(got, []string{"str1", "str2"}) {
		t.Fatalf("SCAN TYPE string = %v", got)
	}

	got = scanAll(t, rw, "TYPE", "hash")
	if !sortedEqual(got, []string{"hash1"}) {
		t.Fatalf("SCAN TYPE hash = %v", got)
	}

	got = scanAll(t, rw, "TYPE", "list")
	if !sortedEqual(got, []string{"list1"}) {
		t.Fatalf("SCAN TYPE list = %v", got)
	}
}

// SCAN with a bad cursor is an error, and an out-of-range numeric cursor is a finished
// iteration, not an error.
func TestScanCursorErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SCAN", "abc")
	expect(t, rw, "-ERR invalid cursor")

	cmd(t, rw, "SCAN", "0", "COUNT", "0")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "SCAN", "0", "BOGUS")
	expect(t, rw, "-ERR syntax error")

	// A cursor past the bucket count returns the done sentinel and no keys.
	next, keys := scanOnce(t, rw, "SCAN", "999999999")
	if next != "0" || len(keys) != 0 {
		t.Fatalf("SCAN 999999999 = cursor %q keys %v, want 0 and empty", next, keys)
	}
}

// RANDOMKEY returns a key that exists on a populated keyspace and a nil bulk on an empty one, and
// never returns a key that is not present.
func TestRandomKey(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RANDOMKEY")
	expect(t, rw, "$-1")

	present := map[string]bool{}
	for i := 0; i < 20; i++ {
		key := "rk" + itoaTest(i)
		cmd(t, rw, "SET", key, "v")
		expect(t, rw, "+OK")
		present[key] = true
	}

	for i := 0; i < 100; i++ {
		cmd(t, rw, "RANDOMKEY")
		got := readReply(t, rw)
		if len(got) == 0 || got[0] != '$' || got == "$-1" {
			t.Fatalf("RANDOMKEY = %q, want a bulk key", got)
		}
		if !present[got[1:]] {
			t.Fatalf("RANDOMKEY returned %q, not in the keyspace", got[1:])
		}
	}
}

// TOUCH returns the count of the named keys that exist, counting a repeated key once per
// occurrence, exactly as EXISTS tallies.
func TestTouch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSET", "a", "1", "b", "2")
	expect(t, rw, "+OK")

	cmd(t, rw, "TOUCH", "a", "b", "nope")
	expect(t, rw, ":2")

	// A repeated existing key counts twice.
	cmd(t, rw, "TOUCH", "a", "a", "nope")
	expect(t, rw, ":2")

	cmd(t, rw, "TOUCH", "nope1", "nope2")
	expect(t, rw, ":0")

	cmd(t, rw, "TOUCH")
	expect(t, rw, "-ERR wrong number of arguments for 'touch' command")
}

// A logically-expired key is left out of KEYS, SCAN, RANDOMKEY, and TOUCH, the same as a key
// that was never set.
func TestKeyspaceExpiry(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "live", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "dying", "v", "PX", "1")
	expect(t, rw, "+OK")
	time.Sleep(30 * time.Millisecond)

	cmd(t, rw, "KEYS", "*")
	got := readArray(t, rw)
	if !sortedEqual(got, []string{"live"}) {
		t.Fatalf("KEYS after expiry = %v, want [live]", got)
	}

	all := scanAll(t, rw)
	if !sortedEqual(all, []string{"live"}) {
		t.Fatalf("SCAN after expiry = %v, want [live]", all)
	}

	cmd(t, rw, "TOUCH", "live", "dying")
	expect(t, rw, ":1")
}

// DBSIZE counts logical top-level keys: one per string and one per collection regardless of how
// many elements it holds, not the element rows. It rises on a create, is flat while a collection
// only gains or loses elements, and falls when a key is deleted or a collection empties out.
func TestDBSize(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":0")

	for i := 0; i < 5; i++ {
		cmd(t, rw, "SET", "s"+itoaTest(i), "v")
		expect(t, rw, "+OK")
	}
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":5")

	// One collection of each type is one key each, whatever its cardinality.
	cmd(t, rw, "HSET", "h1", "f1", "v", "f2", "v", "f3", "v")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "set1", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "RPUSH", "list1", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "XADD", "stream1", "*", "f", "v")
	readReply(t, rw) // the generated ID, opaque here
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":10")

	// Growing or shrinking a collection's elements does not change the key count.
	cmd(t, rw, "HSET", "h1", "f4", "v")
	expect(t, rw, ":1")
	cmd(t, rw, "HDEL", "h1", "f4")
	expect(t, rw, ":1")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":10")

	// Emptying a collection drops its key.
	cmd(t, rw, "DEL", "list1")
	expect(t, rw, ":1")
	cmd(t, rw, "SREM", "set1", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":8")

	cmd(t, rw, "FLUSHALL")
	expect(t, rw, "+OK")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":0")
}

// A logically-expired but unreaped key still counts toward DBSIZE, matching Redis, which counts
// the dict entry until lazy or active expiry removes it. A typed access that reaps the key then
// drops the count.
func TestDBSizeExpiry(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "live", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "dying", "v", "PX", "1")
	expect(t, rw, "+OK")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":2")

	time.Sleep(30 * time.Millisecond)

	// A GET on the expired key reaps it, and the count then reflects the reap.
	cmd(t, rw, "GET", "dying")
	expect(t, rw, "$-1")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":1")
}

// itoaTest renders a small non-negative int without pulling strconv into the test's intent.
func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
