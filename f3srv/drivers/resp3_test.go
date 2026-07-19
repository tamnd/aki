package drivers

import (
	"bufio"
	"net"
	"testing"
)

// The RESP3 wire differential (spec 2064/f3/11): every command whose reply shape
// diverges between the two protocols is exercised on one connection that stayed
// RESP2 and one that sent HELLO 3, and the leading reply byte is asserted on each.
// The leading byte is the whole divergence: a map is '%' where the RESP2 array is
// '*', a set is '~', a double is ',', a boolean is '#', a push is '>', a null is
// '_' where RESP2 sends '$-1'. Asserting it proves the connection carried the
// negotiated framing and, by the RESP2 column staying exactly as it always was,
// that HELLO 3 changed nothing for a client that never asked for it.

// firstByte sends args and returns the leading byte of the reply, then drains the
// rest of the frame through readRESP so the connection is left at a frame boundary
// for the next command.
func firstByte(t *testing.T, br *bufio.Reader, nc net.Conn, args ...string) byte {
	t.Helper()
	writeCmd(t, nc, args...)
	b, err := br.Peek(1)
	if err != nil {
		t.Fatalf("peek reply to %v: %v", args, err)
	}
	lead := b[0]
	readRESP(t, br)
	return lead
}

// resp3Case is one command whose leading reply byte differs by protocol.
type resp3Case struct {
	name  string
	args  []string
	resp2 byte
	resp3 byte
}

// TestResp3Framing seeds a spread of types on a RESP2 connection, then replays the
// diverging commands on both a RESP2 and a RESP3 connection and checks each reply's
// leading byte matches the protocol. The seed writes go over RESP2 so the fixture
// is identical for both readers.
func TestResp3Framing(t *testing.T) {
	srv, nc2, br2 := startServer(t)
	nc3, br3 := dial(t, srv)

	// A second RESP2 connection would do; nc2/br2 is already RESP2 by default.
	// Seed the keyspace over the RESP2 connection.
	for _, seed := range [][]string{
		{"HSET", "h", "f1", "v1", "f2", "v2"},
		{"SADD", "s", "a", "b", "c"},
		{"SADD", "s2", "b", "c", "d"},
		{"ZADD", "z", "1", "m1", "2", "m2"},
		{"SET", "str", "10.5"},
	} {
		writeCmd(t, nc2, seed...)
		readRESP(t, br2)
	}

	// Turn the second connection into a RESP3 one.
	if m := helloFields(t, sendCmd(t, br3, nc3, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}

	cases := []resp3Case{
		{"hgetall-map", []string{"HGETALL", "h"}, '*', '%'},
		{"config-map", []string{"CONFIG", "GET", "maxmemory"}, '*', '%'},
		{"smembers-set", []string{"SMEMBERS", "s"}, '*', '~'},
		{"sunion-set", []string{"SUNION", "s", "s2"}, '*', '~'},
		{"sinter-set", []string{"SINTER", "s", "s2"}, '*', '~'},
		{"sdiff-set", []string{"SDIFF", "s", "s2"}, '*', '~'},
		{"sismember-bool", []string{"SISMEMBER", "s", "a"}, ':', '#'},
		{"smismember-bool", []string{"SMISMEMBER", "s", "a", "z"}, '*', '*'},
		{"zscore-double", []string{"ZSCORE", "z", "m1"}, '$', ','},
		{"zincrby-double", []string{"ZINCRBY", "z", "1", "m1"}, '$', ','},
		{"incrbyfloat-double", []string{"INCRBYFLOAT", "str", "1.5"}, '$', ','},
		{"zrange-plain", []string{"ZRANGE", "z", "0", "-1"}, '*', '*'},
		{"zrange-withscores", []string{"ZRANGE", "z", "0", "-1", "WITHSCORES"}, '*', '*'},
		{"get-missing-null", []string{"GET", "nope"}, '$', '_'},
		{"expire-int", []string{"EXPIRE", "str", "100"}, ':', ':'},
		{"setnx-int", []string{"SETNX", "fresh", "1"}, ':', ':'},
	}

	for _, c := range cases {
		if got := firstByte(t, br2, nc2, c.args...); got != c.resp2 {
			t.Errorf("%s RESP2 lead = %q, want %q", c.name, got, c.resp2)
		}
		if got := firstByte(t, br3, nc3, c.args...); got != c.resp3 {
			t.Errorf("%s RESP3 lead = %q, want %q", c.name, got, c.resp3)
		}
	}
}

// TestResp3WithscoresPairs checks a WITHSCORES range over RESP3 nests each member
// with its score as a 2-element array of [bulk, double], while the RESP2 form stays
// a flat member,score,member,score array. The outer array count is the member count
// on RESP3 and twice that on RESP2.
func TestResp3WithscoresPairs(t *testing.T) {
	srv, nc2, br2 := startServer(t)
	nc3, br3 := dial(t, srv)

	writeCmd(t, nc2, "ZADD", "z", "1", "m1", "2", "m2")
	readRESP(t, br2)

	if m := helloFields(t, sendCmd(t, br3, nc3, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}

	// RESP2: flat array of four bulks.
	r2 := sendCmd(t, br2, nc2, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	arr2, ok := r2.([]any)
	if !ok || len(arr2) != 4 {
		t.Fatalf("RESP2 WITHSCORES = %v, want a flat 4-element array", r2)
	}
	if arr2[0] != "m1" || arr2[1] != "1" || arr2[2] != "m2" || arr2[3] != "2" {
		t.Fatalf("RESP2 WITHSCORES body = %v, want [m1 1 m2 2]", arr2)
	}

	// RESP3: array of two pairs, each [member, score-as-double].
	r3 := sendCmd(t, br3, nc3, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	arr3, ok := r3.([]any)
	if !ok || len(arr3) != 2 {
		t.Fatalf("RESP3 WITHSCORES = %v, want two pairs", r3)
	}
	for i, want := range [][2]string{{"m1", "1"}, {"m2", "2"}} {
		pair, ok := arr3[i].([]any)
		if !ok || len(pair) != 2 {
			t.Fatalf("RESP3 pair %d = %v, want a 2-element array", i, arr3[i])
		}
		if pair[0] != want[0] || pair[1] != want[1] {
			t.Fatalf("RESP3 pair %d = %v, want [%s %s]", i, pair, want[0], want[1])
		}
	}
}
