package sqlo1

import (
	"fmt"
	"testing"
)

func TestServerSetSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Point writes: variadic SADD counts created members only.
	send("SADD", "s", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("SADD", "s", "a", "d")
	expect(t, r, ":1\r\n")
	send("sadd", "s")
	expect(t, r, "-ERR wrong number of arguments for 'sadd' command\r\n")
	send("SCARD", "s")
	expect(t, r, ":4\r\n")
	send("SCARD", "ghost")
	expect(t, r, ":0\r\n")
	send("SCARD", "s", "stray")
	expect(t, r, "-ERR wrong number of arguments for 'scard' command\r\n")

	// Membership probes.
	send("SISMEMBER", "s", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "s", "zz")
	expect(t, r, ":0\r\n")
	send("SISMEMBER", "ghost", "a")
	expect(t, r, ":0\r\n")
	send("SISMEMBER", "s")
	expect(t, r, "-ERR wrong number of arguments for 'sismember' command\r\n")
	send("SMISMEMBER", "s", "a", "zz", "d")
	expect(t, r, "*3\r\n:1\r\n:0\r\n:1\r\n")
	send("SMISMEMBER", "ghost", "x", "y")
	expect(t, r, "*2\r\n:0\r\n:0\r\n")
	send("SMISMEMBER", "s")
	expect(t, r, "-ERR wrong number of arguments for 'smismember' command\r\n")

	// Removal counts hits only.
	send("SREM", "s", "d", "zz")
	expect(t, r, ":1\r\n")
	send("SREM", "s")
	expect(t, r, "-ERR wrong number of arguments for 'srem' command\r\n")

	// SMOVE relocates a held member and answers 0 for an absent one.
	send("SMOVE", "s", "moved", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "moved", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "s", "a")
	expect(t, r, ":0\r\n")
	send("SMOVE", "s", "moved", "nope")
	expect(t, r, ":0\r\n")
	send("SMOVE", "s", "moved")
	expect(t, r, "-ERR wrong number of arguments for 'smove' command\r\n")

	// TYPE and OBJECT ENCODING route through the root sniff.
	send("TYPE", "s")
	expect(t, r, "+set\r\n")
	send("OBJECT", "ENCODING", "s")
	expect(t, r, "$8\r\nlistpack\r\n")
	send("SADD", "ints", "1", "2", "3")
	expect(t, r, ":3\r\n")
	send("OBJECT", "ENCODING", "ints")
	expect(t, r, "$6\r\nintset\r\n")

	// Past the inline count threshold the encoding flips to hashtable
	// and the point surface keeps answering over segments.
	args := []string{"SADD", "big"}
	for i := range 140 {
		args = append(args, fmt.Sprintf("w%03d", i))
	}
	send(args...)
	expect(t, r, ":140\r\n")
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$9\r\nhashtable\r\n")
	send("TYPE", "big")
	expect(t, r, "+set\r\n")
	send("SISMEMBER", "big", "w077")
	expect(t, r, ":1\r\n")
	send("SMOVE", "big", "s", "w007")
	expect(t, r, ":1\r\n")
	send("SCARD", "big")
	expect(t, r, ":139\r\n")

	// Cross-type doors, both directions.
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("SADD", "str", "x")
	expect(t, r, wrong)
	send("SCARD", "str")
	expect(t, r, wrong)
	send("SMOVE", "s", "str", "b")
	expect(t, r, wrong)
	send("SMOVE", "str", "s", "x")
	expect(t, r, wrong)
	send("GET", "s")
	expect(t, r, wrong)
}
