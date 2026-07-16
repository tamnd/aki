package sqlo1

import (
	"fmt"
	"strings"
	"testing"
)

// respCmd encodes one command as a RESP array of bulk strings.
func respCmd(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.String()
}

func TestServerHashSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Point writes and reads.
	send("HSET", "h", "f1", "v1")
	expect(t, r, ":1\r\n")
	send("HSET", "h", "f1", "v1b", "f2", "v2")
	expect(t, r, ":1\r\n") // f1 updated, f2 created
	send("HGET", "h", "f1")
	expect(t, r, "$3\r\nv1b\r\n")
	send("HGET", "h", "nope")
	expect(t, r, "$-1\r\n")
	send("HGET", "missing", "f")
	expect(t, r, "$-1\r\n")
	send("hmset", "h", "a", "1", "b", "2")
	expect(t, r, "+OK\r\n")
	send("HLEN", "h")
	expect(t, r, ":4\r\n")
	send("HSET", "h", "odd")
	expect(t, r, "-ERR wrong number of arguments for 'hset' command\r\n")
	send("HSET", "h", "odd", "v", "stray")
	expect(t, r, "-ERR wrong number of arguments for 'hset' command\r\n")

	// HMGET keeps order, emits misses, and never drops entries.
	send("HMGET", "h", "f1", "a", "nope")
	expect(t, r, "*3\r\n$3\r\nv1b\r\n$1\r\n1\r\n$-1\r\n")
	send("HMGET", "ghost", "x", "y")
	expect(t, r, "*2\r\n$-1\r\n$-1\r\n")

	// Existence and length probes.
	send("HEXISTS", "h", "a")
	expect(t, r, ":1\r\n")
	send("HEXISTS", "h", "zz")
	expect(t, r, ":0\r\n")
	send("HSTRLEN", "h", "f1")
	expect(t, r, ":3\r\n")
	send("HSTRLEN", "h", "zz")
	expect(t, r, ":0\r\n")

	// HDEL counts only the fields that were there.
	send("HDEL", "h", "f1", "f2", "zz")
	expect(t, r, ":2\r\n")
	send("HLEN", "h")
	expect(t, r, ":2\r\n")

	// HSETNX.
	send("HSETNX", "h", "a", "9")
	expect(t, r, ":0\r\n")
	send("HGET", "h", "a")
	expect(t, r, "$1\r\n1\r\n")
	send("HSETNX", "h", "c", "3")
	expect(t, r, ":1\r\n")

	// The INCR family.
	send("HINCRBY", "h", "n", "5")
	expect(t, r, ":5\r\n")
	send("HINCRBY", "h", "n", "-8")
	expect(t, r, ":-3\r\n")
	send("HINCRBY", "h", "n", "+2")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("HSET", "h", "s", "abc")
	expect(t, r, ":1\r\n")
	send("HINCRBY", "h", "s", "1")
	expect(t, r, "-ERR hash value is not an integer\r\n")
	send("HINCRBY", "h", "n", "-9223372036854775807")
	expect(t, r, "-ERR increment or decrement would overflow\r\n")
	send("HINCRBYFLOAT", "h", "fl", "10.5")
	expect(t, r, "$4\r\n10.5\r\n")
	send("HINCRBYFLOAT", "h", "fl", "0.1")
	expect(t, r, "$4\r\n10.6\r\n")
	send("HINCRBYFLOAT", "h", "s", "1")
	expect(t, r, "-ERR hash value is not a float\r\n")
	send("HINCRBYFLOAT", "h", "fl", "bad")
	expect(t, r, "-ERR value is not a valid float\r\n")

	// HGETDEL and its FIELDS block.
	send("HGETDEL", "h", "FIELDS", "1", "s")
	expect(t, r, "*1\r\n$3\r\nabc\r\n")
	send("HGET", "h", "s")
	expect(t, r, "$-1\r\n")
	send("HGETDEL", "h", "FIELDS", "2", "zz", "a")
	expect(t, r, "*2\r\n$-1\r\n$1\r\n1\r\n")
	send("HGETDEL", "h", "NOPE", "1", "a")
	expect(t, r, "-ERR Mandatory keyword FIELDS is missing or not at the right position\r\n")
	send("HGETDEL", "h", "FIELDS", "0", "a")
	expect(t, r, "-ERR Parameter `numFields` should be greater than 0\r\n")
	send("HGETDEL", "h", "FIELDS", "5", "a", "b")
	expect(t, r, "-ERR Parameter `numFields` is more than number of arguments\r\n")
	send("HGETDEL", "h", "FIELDS", "1", "a", "b")
	expect(t, r, "-ERR syntax error\r\n")

	// HGETEX: read, TTL set, persist, past-expiry delete.
	send("HSET", "h", "t", "vv")
	expect(t, r, ":1\r\n")
	send("HGETEX", "h", "FIELDS", "1", "t")
	expect(t, r, "*1\r\n$2\r\nvv\r\n")
	send("HGETEX", "h", "EX", "100", "FIELDS", "1", "t")
	expect(t, r, "*1\r\n$2\r\nvv\r\n")
	send("HGETEX", "h", "PERSIST", "FIELDS", "1", "t")
	expect(t, r, "*1\r\n$2\r\nvv\r\n")
	send("HGETEX", "h", "EXAT", "1", "FIELDS", "1", "t")
	expect(t, r, "*1\r\n$2\r\nvv\r\n")
	send("HGET", "h", "t")
	expect(t, r, "$-1\r\n")
	send("HGETEX", "h", "EX", "10", "PERSIST", "FIELDS", "1", "a")
	expect(t, r, "-ERR syntax error\r\n")
	send("HGETEX", "h", "EX", "0", "FIELDS", "1", "a")
	expect(t, r, "-ERR invalid expire time in 'hgetex' command\r\n")
	send("HGETEX", "h", "EX", "10")
	expect(t, r, "-ERR wrong number of arguments for 'hgetex' command\r\n")

	// Cross-type doors, both directions.
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("HGET", "str", "f")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("HSET", "str", "f", "v")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("HMGET", "str", "f")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("GET", "h")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("APPEND", "h", "x")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

	// TYPE and OBJECT ENCODING route through the root sniff.
	send("TYPE", "h")
	expect(t, r, "+hash\r\n")
	send("TYPE", "str")
	expect(t, r, "+string\r\n")
	send("TYPE", "ghost")
	expect(t, r, "+none\r\n")
	send("OBJECT", "ENCODING", "h")
	expect(t, r, "$8\r\nlistpack\r\n")

	// Push h past the count threshold: encoding flips to hashtable and
	// the whole point surface keeps answering over segments.
	args := []string{"HSET", "h"}
	for i := range 140 {
		args = append(args, fmt.Sprintf("w%03d", i), fmt.Sprintf("val-%d", i))
	}
	send(args...)
	expect(t, r, ":140\r\n")
	send("OBJECT", "ENCODING", "h")
	expect(t, r, "$9\r\nhashtable\r\n")
	send("TYPE", "h")
	expect(t, r, "+hash\r\n")
	send("HGET", "h", "w077")
	expect(t, r, "$6\r\nval-77\r\n")
	send("HMGET", "h", "w000", "zz", "w139")
	expect(t, r, "*3\r\n$5\r\nval-0\r\n$-1\r\n$7\r\nval-139\r\n")
	send("HINCRBY", "h", "n", "3")
	expect(t, r, ":0\r\n")
	send("HGETDEL", "h", "FIELDS", "1", "w000")
	expect(t, r, "*1\r\n$5\r\nval-0\r\n")
	send("HEXISTS", "h", "w000")
	expect(t, r, ":0\r\n")

	// DEL kills a segmented hash whole.
	send("DEL", "h")
	expect(t, r, ":1\r\n")
	send("TYPE", "h")
	expect(t, r, "+none\r\n")
	send("HLEN", "h")
	expect(t, r, ":0\r\n")
}
