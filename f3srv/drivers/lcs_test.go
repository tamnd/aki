package drivers

import (
	"strconv"
	"testing"
)

// TestLcs drives LCS on co-located keys through the canonical Redis example
// (ohmytext / mynewtext -> mytext), covering the plain, LEN, IDX, WITHMATCHLEN,
// and MINMATCHLEN forms plus the empty-operand and option-error cases.
func TestLcs(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "{k}1", "ohmytext", "{k}2", "mynewtext")
	expect(t, br, "+OK\r\n")

	// Plain form returns the subsequence itself.
	send(t, nc, "LCS", "{k}1", "{k}2")
	expect(t, br, "$6\r\nmytext\r\n")

	// LEN returns its length.
	send(t, nc, "LCS", "{k}1", "{k}2", "LEN")
	expect(t, br, ":6\r\n")

	// IDX returns the matching ranges, tail-first, with the total length.
	send(t, nc, "LCS", "{k}1", "{k}2", "IDX")
	expect(t, br, "*4\r\n$7\r\nmatches\r\n*2\r\n"+
		"*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n"+
		"*2\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n"+
		"$3\r\nlen\r\n:6\r\n")

	// WITHMATCHLEN tags each range with its length.
	send(t, nc, "LCS", "{k}1", "{k}2", "IDX", "WITHMATCHLEN")
	expect(t, br, "*4\r\n$7\r\nmatches\r\n*2\r\n"+
		"*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n"+
		"*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n"+
		"$3\r\nlen\r\n:6\r\n")

	// MINMATCHLEN drops the shorter range from the IDX reply.
	send(t, nc, "LCS", "{k}1", "{k}2", "IDX", "MINMATCHLEN", "4")
	expect(t, br, "*4\r\n$7\r\nmatches\r\n*1\r\n"+
		"*2\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n"+
		"$3\r\nlen\r\n:6\r\n")

	// A missing key is an empty operand: no common subsequence.
	send(t, nc, "LCS", "{k}1", "{k}absent")
	expect(t, br, "$0\r\n\r\n")
	send(t, nc, "LCS", "{k}1", "{k}absent", "LEN")
	expect(t, br, ":0\r\n")
	send(t, nc, "LCS", "{k}1", "{k}absent", "IDX")
	expect(t, br, "*4\r\n$7\r\nmatches\r\n*0\r\n$3\r\nlen\r\n:0\r\n")

	// LEN and IDX together are refused, and an unknown option is a syntax error.
	send(t, nc, "LCS", "{k}1", "{k}2", "LEN", "IDX")
	expect(t, br, "-ERR If you want both the length and indexes, please just use IDX.\r\n")
	send(t, nc, "LCS", "{k}1", "{k}2", "NOPE")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestLcsCrossShard drives LCS with the two keys on different shards, so the
// reply comes back through the F17 intent route rather than the co-located path.
func TestLcsCrossShard(t *testing.T) {
	srv, nc, br := startServer(t)

	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if srv.rt.ShardOf([]byte(k)) == sh {
				return k
			}
		}
	}
	k1 := keyOn(0, "lcsa")
	k2 := keyOn(1, "lcsb")

	send(t, nc, "SET", k1, "ohmytext")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", k2, "mynewtext")
	expect(t, br, "+OK\r\n")

	send(t, nc, "LCS", k1, k2)
	expect(t, br, "$6\r\nmytext\r\n")
	send(t, nc, "LCS", k1, k2, "IDX", "WITHMATCHLEN")
	expect(t, br, "*4\r\n$7\r\nmatches\r\n*2\r\n"+
		"*3\r\n*2\r\n:4\r\n:7\r\n*2\r\n:5\r\n:8\r\n:4\r\n"+
		"*3\r\n*2\r\n:2\r\n:3\r\n*2\r\n:0\r\n:1\r\n:2\r\n"+
		"$3\r\nlen\r\n:6\r\n")
}
