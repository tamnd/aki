package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// sscanReply is one SSCAN reply decoded into the resume cursor and the flat member
// array. A set member carries no value, so unlike HSCAN the array is member-only.
type sscanReply struct {
	cursor string
	items  []string
}

// sscanCall sends an SSCAN and decodes the two-element reply: a bulk cursor followed by
// a flat array of bulk members. The cursor is returned without the RESP "$" prefix, so
// a completed iteration reads back as "0".
func sscanCall(t *testing.T, rw *bufio.ReadWriter, args ...string) sscanReply {
	t.Helper()
	cmd(t, rw, args...)
	if got := readReply(t, rw); got != "*2" {
		t.Fatalf("sscan header = %q, want *2", got)
	}
	cur := readReply(t, rw)
	if len(cur) == 0 || cur[0] != '$' {
		t.Fatalf("sscan cursor = %q, want a bulk string", cur)
	}
	ah := readReply(t, rw)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("sscan items header = %q, want an array", ah)
	}
	n := 0
	for _, ch := range ah[1:] {
		n = n*10 + int(ch-'0')
	}
	items := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("sscan item %d = %q, want a bulk string", i, b)
		}
		items[i] = b[1:]
	}
	return sscanReply{cursor: cur[1:], items: items}
}

// readMemberSet reads a RESP array of bulk members into a set, so an order-insensitive
// enumeration can be asserted by membership rather than sequence. Redis leaves SMEMBERS and
// SSCAN order unspecified, and aki enumerates off the dense member vector (spec
// 2064/f1_rewrite_ltm/20), not in sorted key order, so the tests compare member sets.
func readMemberSet(t *testing.T, rw *bufio.ReadWriter, want int) map[string]bool {
	t.Helper()
	ah := readReply(t, rw)
	if ah != fmt.Sprintf("*%d", want) {
		t.Fatalf("array header = %q, want *%d", ah, want)
	}
	got := make(map[string]bool, want)
	for i := 0; i < want; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("member %d = %q, want a bulk string", i, b)
		}
		got[b[1:]] = true
	}
	return got
}

// wantMembers asserts a decoded member set equals exactly the given members.
func wantMembers(t *testing.T, got map[string]bool, members ...string) {
	t.Helper()
	if len(got) != len(members) {
		t.Fatalf("got %d members, want %d", len(got), len(members))
	}
	for _, m := range members {
		if !got[m] {
			t.Fatalf("member %q missing from reply", m)
		}
	}
}

// The set point path is element-per-row on f1raw, the hash with the value stripped:
// SADD writes one empty-valued member row and maintains the header count, SISMEMBER is a
// single lock-free index probe, and SCARD reads the header count with no scan.
func TestSetPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// SADD reports the count of newly added members, not the count written.
	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "s", "a", "d") // a already present, d new
	expect(t, rw, ":1")

	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":4")
	cmd(t, rw, "SCARD", "missing")
	expect(t, rw, ":0")

	cmd(t, rw, "SISMEMBER", "s", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SISMEMBER", "s", "missing")
	expect(t, rw, ":0")
	cmd(t, rw, "SISMEMBER", "missing", "a")
	expect(t, rw, ":0")
}

func TestSetMIsMember(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "SMISMEMBER", "s", "a", "x", "c")
	expect(t, rw, "*3")
	expect(t, rw, ":1")
	expect(t, rw, ":0")
	expect(t, rw, ":1")
}

func TestSetRem(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")

	// SREM returns the count of members actually removed, missing ones do not count.
	cmd(t, rw, "SREM", "s", "a", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":2")
	cmd(t, rw, "SISMEMBER", "s", "a")
	expect(t, rw, ":0")

	// Removing the last members drops the header, so SCARD reports 0 and the set key
	// stops existing.
	cmd(t, rw, "SREM", "s", "b", "c")
	expect(t, rw, ":2")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":0")
}

// A set member row and a string of the same key bytes must not collide: the record kind
// byte keeps the namespaces disjoint, and the set meta kind is distinct from the hash
// meta kind so SCARD and HLEN never cross-read one another's header count.
func TestSetStringNamespaceDisjoint(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "m")
	expect(t, rw, ":1")

	// A plain string keyed by the bare set key is independent of the set.
	cmd(t, rw, "SET", "sk", "strval")
	expect(t, rw, "+OK")
	cmd(t, rw, "SISMEMBER", "sk", "m") // sk is a string, no such set member
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "sk")
	expect(t, rw, "$strval")
	cmd(t, rw, "SISMEMBER", "s", "m")
	expect(t, rw, ":1")
}

// A set and a hash of the same key must not cross-read one another's header count: SCARD
// uses the set meta kind and HLEN the hash meta kind, so each reports only its own type.
func TestSetHashHeaderDisjoint(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "k", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "HSET", "k", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")

	// Each type reports only its own count off its own header kind.
	cmd(t, rw, "SCARD", "k")
	expect(t, rw, ":3")
	cmd(t, rw, "HLEN", "k")
	expect(t, rw, ":2")
}

// SADD against a key that already holds a string is WRONGTYPE, the common type clash.
func TestSetWrongTypeOnString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SADD", "k", "m")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "SREM", "k", "m")
	expect(t, rw, "-"+wrongType)
	// The string is still intact.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v")
}

// SMEMBERS enumerates one set off the dense member vector, framing the RESP array from the
// vector length so it always matches what streams. Order is unspecified (Redis leaves it so),
// so the reply is asserted as a member set.
func TestSetMembers(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "b", "a", "c")
	expect(t, rw, ":3")

	cmd(t, rw, "SMEMBERS", "s")
	wantMembers(t, readMemberSet(t, rw, 3), "a", "b", "c")
}

// A missing set enumerates as an empty array, not an error or nil.
func TestSetMembersMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SMEMBERS", "nope")
	expect(t, rw, "*0")
}

// Enumeration tracks removes and re-adds: a removed member drops out, a re-added member
// reappears once.
func TestSetMembersAfterMutate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "SREM", "s", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "s", "b") // re-add
	expect(t, rw, ":1")

	cmd(t, rw, "SMEMBERS", "s")
	wantMembers(t, readMemberSet(t, rw, 3), "a", "b", "c")
}

// Enumeration must stream more members than one internal scan batch without dropping or
// duplicating any, so the RESP length matches across the batch boundary.
func TestSetMembersManyMembers(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 1000
	args := []string{"SADD", "big"}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("m%05d", i))
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "SCARD", "big")
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "SMEMBERS", "big")
	got := readMemberSet(t, rw, n)
	for i := 0; i < n; i++ {
		if !got[fmt.Sprintf("m%05d", i)] {
			t.Fatalf("member m%05d missing from SMEMBERS", i)
		}
	}
}

// SMEMBERS against a string key is WRONGTYPE, matching the point-path type guard.
func TestSetMembersWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SMEMBERS", "k")
	expect(t, rw, "-"+wrongType)
}

// SSCAN walks a small set in one batch when COUNT covers it: cursor 0 back means the
// iteration is complete, and the results are the flat members (order unspecified).
func TestSetScanOneBatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "b", "a", "c")
	expect(t, rw, ":3")

	// COUNT 100 scans the whole three-member set in one call, so the cursor comes back 0.
	reply := sscanCall(t, rw, "SSCAN", "s", "0", "COUNT", "100")
	if reply.cursor != "0" {
		t.Fatalf("cursor = %q, want 0 (iteration complete)", reply.cursor)
	}
	got := map[string]bool{}
	for _, m := range reply.items {
		got[m] = true
	}
	wantMembers(t, got, "a", "b", "c")
}

// A missing set scans as a completed empty iteration: cursor 0 and no results.
func TestSetScanMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SSCAN", "nope", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*0")
}

// MATCH filters returned members by glob without changing the cursor walk.
func TestSetScanMatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "user:1", "user:2", "post:1")
	expect(t, rw, ":3")

	// Only the user:* members survive.
	reply := sscanCall(t, rw, "SSCAN", "s", "0", "COUNT", "100", "MATCH", "user:*")
	if reply.cursor != "0" {
		t.Fatalf("cursor = %q, want 0 (iteration complete)", reply.cursor)
	}
	got := map[string]bool{}
	for _, m := range reply.items {
		got[m] = true
	}
	wantMembers(t, got, "user:1", "user:2")
}

// A full iteration with a small COUNT returns every member exactly once across batches:
// the client resumes with the returned cursor until it comes back 0, and the union of
// batches is the whole set with no dupes and no drops.
func TestSetScanFullIterationSmallCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 250
	args := []string{"SADD", "big"}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("m%04d", i))
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	seen := map[string]bool{}
	cursor := "0"
	rounds := 0
	for {
		rounds++
		if rounds > 10000 {
			t.Fatal("SSCAN did not terminate")
		}
		reply := sscanCall(t, rw, "SSCAN", "big", cursor, "COUNT", "7")
		cursor = reply.cursor
		for _, m := range reply.items {
			if seen[m] {
				t.Fatalf("member %q returned twice", m)
			}
			seen[m] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("full scan saw %d members, want %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		if !seen[fmt.Sprintf("m%04d", i)] {
			t.Fatalf("member m%04d missing from full scan", i)
		}
	}
}

// SSCAN against a string key is WRONGTYPE, matching the other set reads, and a bad cursor
// or bad COUNT is a clean error rather than a crash.
func TestSetScanErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SSCAN", "k", "0")
	expect(t, rw, "-"+wrongType)

	cmd(t, rw, "SADD", "s", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SSCAN", "s", "zz") // not hex, not "0"
	expect(t, rw, "-ERR invalid cursor")
	cmd(t, rw, "SSCAN", "s", "0", "COUNT", "0")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "SSCAN", "s", "0", "BOGUS")
	expect(t, rw, "-ERR syntax error")
}

// FLUSHALL clears member and header rows along with strings.
func TestSetFlush(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "FLUSHALL")
	expect(t, rw, "+OK")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "SISMEMBER", "s", "a")
	expect(t, rw, ":0")
}
