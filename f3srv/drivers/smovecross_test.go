package drivers

import (
	"strconv"
	"testing"
)

// Cross-shard SMOVE end to end: the dispatch divert, the arm fan, the intent
// barrier, and the loopback reply all under a raw RESP socket. The shard and
// set suites prove the mechanics; this is the wire-level smoke that the
// route is actually reachable from a client.

func TestSmoveCrossShardWire(t *testing.T) {
	srv, nc, br := startServer(t)

	// One key on each of the two shards, and a co-located pair for contrast.
	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if srv.rt.ShardOf([]byte(k)) == sh {
				return k
			}
		}
	}
	src := keyOn(0, "wsrc")
	dst := keyOn(1, "wdst")
	coDst := keyOn(0, "wcod")

	send(t, nc, "SADD", src, "a", "b", "c")
	expect(t, br, ":3\r\n")

	// Cross-shard move, destination created.
	send(t, nc, "SMOVE", src, dst, "b")
	expect(t, br, ":1\r\n")
	send(t, nc, "SISMEMBER", src, "b")
	expect(t, br, ":0\r\n")
	send(t, nc, "SISMEMBER", dst, "b")
	expect(t, br, ":1\r\n")

	// Absent member replies 0 and moves nothing.
	send(t, nc, "SMOVE", src, dst, "zz")
	expect(t, br, ":0\r\n")

	// The co-located pair stays on the fast path and agrees.
	send(t, nc, "SMOVE", src, coDst, "a")
	expect(t, br, ":1\r\n")
	send(t, nc, "SISMEMBER", coDst, "a")
	expect(t, br, ":1\r\n")

	// WRONGTYPE on the cross-shard path, checked before any write.
	strKey := keyOn(1, "wstr")
	send(t, nc, "SET", strKey, "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SMOVE", src, strKey, "c")
	expect(t, br, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send(t, nc, "SISMEMBER", src, "c")
	expect(t, br, ":1\r\n")

	// Pipelined burst: cross-shard SMOVEs interleaved with point reads keep
	// pipeline order through the loopback reply path.
	burst := ""
	for i := 0; i < 5; i++ {
		burst += cmd("SMOVE", dst, src, "b")
		burst += cmd("SMOVE", src, dst, "b")
		burst += cmd("SISMEMBER", dst, "b")
	}
	if _, err := nc.Write([]byte(burst)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		expect(t, br, ":1\r\n") // dst -> src
		expect(t, br, ":1\r\n") // src -> dst
		expect(t, br, ":1\r\n") // b is back at dst
	}
}
