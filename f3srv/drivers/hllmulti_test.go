package drivers

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
)

// expectAny reads one whole RESP line (through the CRLF) and discards it, for
// replies whose exact value is not being asserted, like a PFADD change flag mid
// bulk load.
func expectAny(t *testing.T, br *bufio.Reader) {
	t.Helper()
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read reply: %v", err)
	}
}

// readInt sends a command and parses its RESP integer reply.
func readInt(t *testing.T, nc net.Conn, br *bufio.Reader, args ...string) int {
	t.Helper()
	send(t, nc, args...)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read int reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 || line[0] != ':' {
		t.Fatalf("reply %q is not an integer", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("bad integer reply %q: %v", line, err)
	}
	return n
}

// keyOnShard returns the first prefix+N key that the runtime routes to shard sh,
// the same placement trick the cross-shard set suites use to force a route.
func keyOnShard(srv *Server, sh int, prefix string) string {
	for i := 0; ; i++ {
		k := prefix + strconv.Itoa(i)
		if srv.rt.ShardOf([]byte(k)) == sh {
			return k
		}
	}
}

// TestHLLMultiCountColocated exercises multi-key PFCOUNT on the co-located fast
// path: both keys on one shard, so the point handler folds them itself. The
// union of two disjoint small sets is exact in the sparse form, so the count is
// the true union size, and a missing key in the set contributes nothing.
func TestHLLMultiCountColocated(t *testing.T) {
	srv, nc, br := startServer(t)

	a := keyOnShard(srv, 0, "uca")
	b := keyOnShard(srv, 0, "ucb")

	send(t, nc, "PFADD", a, "x", "y", "z")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFADD", b, "z", "w") // z overlaps a
	expect(t, br, ":1\r\n")

	// Union {x,y,z,w} = 4.
	send(t, nc, "PFCOUNT", a, b)
	expect(t, br, ":4\r\n")

	// A missing key folds in as empty: the union is unchanged.
	send(t, nc, "PFCOUNT", a, b, "ucmissing")
	expect(t, br, ":4\r\n")

	// PFCOUNT writes nothing, so each key still reports its own count.
	send(t, nc, "PFCOUNT", a)
	expect(t, br, ":3\r\n")
	send(t, nc, "PFCOUNT", b)
	expect(t, br, ":2\r\n")
}

// TestHLLMultiCountCrossShard forces the two keys onto different shards so the
// union rides the F17 intent path. The answer must match the co-located fold.
func TestHLLMultiCountCrossShard(t *testing.T) {
	srv, nc, br := startServer(t)

	a := keyOnShard(srv, 0, "xca")
	b := keyOnShard(srv, 1, "xcb")

	send(t, nc, "PFADD", a, "x", "y", "z")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFADD", b, "z", "w")
	expect(t, br, ":1\r\n")

	send(t, nc, "PFCOUNT", a, b)
	expect(t, br, ":4\r\n")

	// A WRONGTYPE key anywhere in the set is refused on the cross path too.
	strKey := keyOnShard(srv, 1, "xcs")
	send(t, nc, "SET", strKey, "plain")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFCOUNT", a, strKey)
	expect(t, br, "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n")
}

// TestHLLMergeColocated merges sources into a destination on one shard, then
// checks the destination is a dense string sketch holding the union count.
func TestHLLMergeColocated(t *testing.T) {
	srv, nc, br := startServer(t)

	dst := keyOnShard(srv, 0, "mcd")
	a := keyOnShard(srv, 0, "mca")
	b := keyOnShard(srv, 0, "mcb")

	send(t, nc, "PFADD", a, "1", "2", "3")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFADD", b, "3", "4")
	expect(t, br, ":1\r\n")

	send(t, nc, "PFMERGE", dst, a, b)
	expect(t, br, "+OK\r\n")

	// Destination holds the union {1,2,3,4} = 4.
	send(t, nc, "PFCOUNT", dst)
	expect(t, br, ":4\r\n")

	// PFMERGE always writes the dense encoding, and the sketch is still a string.
	send(t, nc, "TYPE", dst)
	expect(t, br, "+string\r\n")

	// Sources are untouched by the merge.
	send(t, nc, "PFCOUNT", a)
	expect(t, br, ":3\r\n")

	// Merging into an existing destination folds it in as well.
	send(t, nc, "PFADD", "mcextra", "9")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFMERGE", dst, "mcextra")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFCOUNT", dst)
	expect(t, br, ":5\r\n")
}

// TestHLLMergeCrossShard spreads the destination and sources over both shards so
// the merge rides the F17 path, then verifies the destination union and its
// dense encoding.
func TestHLLMergeCrossShard(t *testing.T) {
	srv, nc, br := startServer(t)

	dst := keyOnShard(srv, 0, "xmd")
	a := keyOnShard(srv, 1, "xma")
	b := keyOnShard(srv, 1, "xmb")

	send(t, nc, "PFADD", a, "1", "2", "3")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFADD", b, "3", "4", "5")
	expect(t, br, ":1\r\n")

	send(t, nc, "PFMERGE", dst, a, b)
	expect(t, br, "+OK\r\n")

	send(t, nc, "PFCOUNT", dst)
	expect(t, br, ":5\r\n")
	send(t, nc, "TYPE", dst)
	expect(t, br, "+string\r\n")

	// A WRONGTYPE source is refused before the destination is written.
	strKey := keyOnShard(srv, 1, "xms")
	send(t, nc, "SET", strKey, "plain")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFMERGE", "xmd2", a, strKey)
	expect(t, br, "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n")
	send(t, nc, "EXISTS", "xmd2")
	expect(t, br, ":0\r\n")
}

// TestHLLMergeLargeUnion pushes both sources into the dense form (past the sparse
// budget) and checks the merged estimate stays within HLL's error bound of the
// true union, the accuracy contract of the fold on filled registers.
func TestHLLMergeLargeUnion(t *testing.T) {
	srv, nc, br := startServer(t)

	a := keyOnShard(srv, 0, "lua")
	b := keyOnShard(srv, 1, "lub")
	dst := keyOnShard(srv, 0, "lud")

	// 20000 distinct in a, another 20000 in b with 10000 shared: true union 30000.
	// Batches stay small: one command's argument vector must fit the hop node's
	// span budget, so the load is many modest PFADDs, not a few giant ones.
	addRange := func(key string, lo, hi int) {
		const batch = 100
		for i := lo; i < hi; i += batch {
			end := i + batch
			if end > hi {
				end = hi
			}
			cmdArgs := []string{"PFADD", key}
			for j := i; j < end; j++ {
				cmdArgs = append(cmdArgs, "e"+strconv.Itoa(j))
			}
			send(t, nc, cmdArgs...)
			expectAny(t, br)
		}
	}
	addRange(a, 0, 20000)
	addRange(b, 10000, 30000)

	send(t, nc, "PFMERGE", dst, a, b)
	expect(t, br, "+OK\r\n")

	got := readInt(t, nc, br, "PFCOUNT", dst)
	const want = 30000
	// HLL standard error at p=14 is ~0.81%; allow a comfortable 3% envelope.
	lo, hi := want*97/100, want*103/100
	if got < lo || got > hi {
		t.Fatalf("PFCOUNT union = %d, want within [%d,%d] of %d", got, lo, hi, want)
	}
}
