package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// scanReply is one HSCAN reply decoded into the resume cursor and the flat element
// array (field-then-value, or field-only under NOVALUES).
type scanReply struct {
	cursor string
	items  []string
}

// scanCall sends an HSCAN and decodes the two-element reply: a bulk cursor followed by
// a flat array of bulk elements. The cursor is returned without the RESP "$" prefix, so
// a completed iteration reads back as "0".
func scanCall(t *testing.T, rw *bufio.ReadWriter, args ...string) scanReply {
	t.Helper()
	cmd(t, rw, args...)
	if got := readReply(t, rw); got != "*2" {
		t.Fatalf("scan header = %q, want *2", got)
	}
	cur := readReply(t, rw)
	if len(cur) == 0 || cur[0] != '$' {
		t.Fatalf("scan cursor = %q, want a bulk string", cur)
	}
	ah := readReply(t, rw)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("scan items header = %q, want an array", ah)
	}
	n := 0
	for _, ch := range ah[1:] {
		n = n*10 + int(ch-'0')
	}
	items := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("scan item %d = %q, want a bulk string", i, b)
		}
		items[i] = b[1:]
	}
	return scanReply{cursor: cur[1:], items: items}
}

// The hash point path is element-per-row on f1raw: HSET writes one field row and
// maintains the header count, HGET is a single lock-free probe, and HLEN reads the
// header count with no scan. This exercises the whole slice-1 surface end to end over
// the wire.
func TestHashPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// HSET reports the count of newly created fields, not the count written.
	cmd(t, rw, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")
	cmd(t, rw, "HSET", "h", "f1", "v1b", "f3", "v3")
	expect(t, rw, ":1") // f1 updated, f3 new

	cmd(t, rw, "HGET", "h", "f1")
	expect(t, rw, "$v1b")
	cmd(t, rw, "HGET", "h", "f3")
	expect(t, rw, "$v3")
	cmd(t, rw, "HGET", "h", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "HGET", "missing", "f1")
	expect(t, rw, "$-1")

	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":3")
	cmd(t, rw, "HLEN", "missing")
	expect(t, rw, ":0")

	cmd(t, rw, "HEXISTS", "h", "f2")
	expect(t, rw, ":1")
	cmd(t, rw, "HEXISTS", "h", "missing")
	expect(t, rw, ":0")

	cmd(t, rw, "HSTRLEN", "h", "f1")
	expect(t, rw, ":3") // "v1b"
	cmd(t, rw, "HSTRLEN", "h", "missing")
	expect(t, rw, ":0")
}

func TestHashMGet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")

	cmd(t, rw, "HMGET", "h", "a", "x", "c")
	expect(t, rw, "*3")
	expect(t, rw, "$1")
	expect(t, rw, "$-1")
	expect(t, rw, "$3")
}

func TestHashDel(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")

	// HDEL returns the count of fields actually removed, missing ones do not count.
	cmd(t, rw, "HDEL", "h", "a", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":2")
	cmd(t, rw, "HGET", "h", "a")
	expect(t, rw, "$-1")

	// Deleting the last fields drops the header, so HLEN reports 0 and the hash key
	// stops existing.
	cmd(t, rw, "HDEL", "h", "b", "c")
	expect(t, rw, ":2")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":0")
}

func TestHashSetNX(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSETNX", "h", "f", "first")
	expect(t, rw, ":1")
	cmd(t, rw, "HSETNX", "h", "f", "second")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$first")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")
}

// A hash field row and a string of the same key bytes must not collide: the record
// kind byte keeps the namespaces disjoint. Here a string key equal to the composite
// field-key bytes must not be seen by HGET, and vice versa.
func TestHashStringNamespaceDisjoint(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "hashval")
	expect(t, rw, ":1")

	// A plain string keyed by the bare hash key is independent of the hash.
	cmd(t, rw, "SET", "sk", "strval")
	expect(t, rw, "+OK")
	cmd(t, rw, "HGET", "sk", "f") // sk is a string, no such hash field
	expect(t, rw, "$-1")
	cmd(t, rw, "GET", "sk")
	expect(t, rw, "$strval")
	// The hash is untouched by the string write.
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$hashval")
}

// HSET against a key that already holds a string is WRONGTYPE, the common type clash.
func TestHashWrongTypeOnString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "k", "f", "1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HDEL", "k", "f")
	expect(t, rw, "-"+wrongType)
	// The string is still intact.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v")
}

// HGETALL, HKEYS, and HVALS enumerate one hash in field-key order off the ordered
// index, framing the RESP array from the maintained header count so the length always
// matches what is streamed.
func TestHashEnumerate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "b", "2", "a", "1", "c", "3")
	expect(t, rw, ":3")

	// HKEYS is the field names in byte order.
	cmd(t, rw, "HKEYS", "h")
	expect(t, rw, "*3")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")

	// HVALS is the values in the same order.
	cmd(t, rw, "HVALS", "h")
	expect(t, rw, "*3")
	expect(t, rw, "$1")
	expect(t, rw, "$2")
	expect(t, rw, "$3")

	// HGETALL interleaves field and value, still in field order.
	cmd(t, rw, "HGETALL", "h")
	expect(t, rw, "*6")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
}

// A missing hash enumerates as an empty array, not an error or nil.
func TestHashEnumerateMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HGETALL", "nope")
	expect(t, rw, "*0")
	cmd(t, rw, "HKEYS", "nope")
	expect(t, rw, "*0")
	cmd(t, rw, "HVALS", "nope")
	expect(t, rw, "*0")
}

// Enumeration tracks deletes and overwrites: a deleted field drops out, an overwritten
// field keeps its slot with the fresh value, and a field whose value outgrew its record
// still enumerates with the new value.
func TestHashEnumerateAfterMutate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "HDEL", "h", "b")
	expect(t, rw, ":1")
	// Overwrite a with a much longer value so the record is republished at a new offset.
	cmd(t, rw, "HSET", "h", "a", "a-much-longer-value-than-before")
	expect(t, rw, ":0")

	cmd(t, rw, "HGETALL", "h")
	expect(t, rw, "*4")
	expect(t, rw, "$a")
	expect(t, rw, "$a-much-longer-value-than-before")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
}

// Enumeration must stream more fields than one internal scan batch without dropping or
// duplicating any, so the RESP length matches across the batch boundary.
func TestHashEnumerateManyFields(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 1000
	args := []string{"HSET", "big"}
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%05d", i)
		args = append(args, f, "v")
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "HLEN", "big")
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "HKEYS", "big")
	expect(t, rw, fmt.Sprintf("*%d", n))
	for i := 0; i < n; i++ {
		expect(t, rw, "$"+fmt.Sprintf("f%05d", i))
	}
}

// HGETALL against a string key is WRONGTYPE, matching the point-path type guard.
func TestHashEnumerateWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HGETALL", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HKEYS", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HVALS", "k")
	expect(t, rw, "-"+wrongType)
}

// HSCAN walks a small hash in one batch when COUNT covers it: cursor 0 back means the
// iteration is complete, and the results are the flat field-then-value pairs in field
// order.
func TestHashScanOneBatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "b", "2", "a", "1", "c", "3")
	expect(t, rw, ":3")

	// COUNT 100 scans the whole three-field hash in one call, so the cursor comes back 0.
	cmd(t, rw, "HSCAN", "h", "0", "COUNT", "100")
	expect(t, rw, "*2")
	expect(t, rw, "$0") // cursor "0" means the iteration is complete
	expect(t, rw, "*6")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
}

// A missing hash scans as a completed empty iteration: cursor 0 and no results.
func TestHashScanMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSCAN", "nope", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*0")
}

// NOVALUES returns only field names, a flat array with no interleaved values.
func TestHashScanNoValues(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2")
	expect(t, rw, ":2")

	cmd(t, rw, "HSCAN", "h", "0", "COUNT", "100", "NOVALUES")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*2")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
}

// MATCH filters returned fields by glob without changing the cursor walk.
func TestHashScanMatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "user:1", "a", "user:2", "b", "post:1", "c")
	expect(t, rw, ":3")

	// Only the user:* fields survive, still field then value.
	cmd(t, rw, "HSCAN", "h", "0", "COUNT", "100", "MATCH", "user:*")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*4")
	expect(t, rw, "$user:1")
	expect(t, rw, "$a")
	expect(t, rw, "$user:2")
	expect(t, rw, "$b")
}

// A full iteration with a small COUNT returns every field exactly once across batches:
// the client resumes with the returned cursor until it comes back 0, and the union of
// batches is the whole hash with no dupes and no drops.
func TestHashScanFullIterationSmallCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 250
	args := []string{"HSET", "big"}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("f%04d", i), fmt.Sprintf("v%d", i))
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	seen := map[string]string{}
	cursor := "0"
	rounds := 0
	for {
		rounds++
		if rounds > 10000 {
			t.Fatal("HSCAN did not terminate")
		}
		reply := scanCall(t, rw, "HSCAN", "big", cursor, "COUNT", "7")
		cursor = reply.cursor
		for i := 0; i+1 < len(reply.items); i += 2 {
			f, v := reply.items[i], reply.items[i+1]
			if _, dup := seen[f]; dup {
				t.Fatalf("field %q returned twice", f)
			}
			seen[f] = v
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("full scan saw %d fields, want %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%04d", i)
		if seen[f] != fmt.Sprintf("v%d", i) {
			t.Fatalf("field %q = %q, want v%d", f, seen[f], i)
		}
	}
}

// HSCAN against a string key is WRONGTYPE, matching the other hash reads, and a bad
// cursor is a clean error rather than a crash.
func TestHashScanErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSCAN", "k", "0")
	expect(t, rw, "-"+wrongType)

	cmd(t, rw, "HSET", "h", "a", "1")
	expect(t, rw, ":1")
	cmd(t, rw, "HSCAN", "h", "zz") // not hex, not "0"
	expect(t, rw, "-ERR invalid cursor")
	cmd(t, rw, "HSCAN", "h", "0", "COUNT", "0")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "HSCAN", "h", "0", "BOGUS")
	expect(t, rw, "-ERR syntax error")
}

// FLUSHALL clears field and header rows along with strings.
func TestHashFlush(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "FLUSHALL")
	expect(t, rw, "+OK")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "a")
	expect(t, rw, "$-1")
}
