package drivers

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
)

// The RESP3 double differential for score-bearing replies (spec 2064/f3/11). Every
// command that carries a score, a coordinate, or a distance renders it as a bulk
// string under RESP2 and as a native double (a ',' frame) under RESP3, and the
// commands whose element shape gates on a count or a WITH flag also switch between
// a flat array and one that nests each value with its member. readRESP collapses a
// double and a bulk to the same Go string, so these tests read the raw frame bytes
// where the framing byte itself is the thing under test and fall back to the parsed
// structure where the nesting is.

// readFrameRaw reads one full RESP frame off br and returns its exact bytes,
// container and leaves alike, so a test can assert the ',' double framing that the
// value-collapsing readRESP hides. It recurses into every aggregate the score
// replies use (array, push, set, map) and reads the fixed two-line body of a bulk.
func readFrameRaw(t *testing.T, br *bufio.Reader) []byte {
	t.Helper()
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read frame line: %v", err)
	}
	body := strings.TrimSuffix(line[1:], "\r\n")
	switch line[0] {
	case '+', '-', ':', ',', '#', '_', '(':
		return []byte(line)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			t.Fatalf("bulk length %q: %v", body, err)
		}
		if n < 0 {
			return []byte(line)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			t.Fatalf("bulk payload: %v", err)
		}
		return append([]byte(line), buf...)
	case '*', '>', '~':
		n, err := strconv.Atoi(body)
		if err != nil {
			t.Fatalf("aggregate length %q: %v", body, err)
		}
		out := []byte(line)
		if n < 0 {
			return out
		}
		for i := 0; i < n; i++ {
			out = append(out, readFrameRaw(t, br)...)
		}
		return out
	case '%':
		n, err := strconv.Atoi(body)
		if err != nil {
			t.Fatalf("map length %q: %v", body, err)
		}
		out := []byte(line)
		for i := 0; i < n; i++ {
			out = append(out, readFrameRaw(t, br)...)
			out = append(out, readFrameRaw(t, br)...)
		}
		return out
	default:
		t.Fatalf("unknown RESP prefix %q", line)
		return nil
	}
}

// rawReply sends args and returns the exact bytes of the reply frame.
func rawReply(t *testing.T, br *bufio.Reader, nc net.Conn, args ...string) string {
	t.Helper()
	writeCmd(t, nc, args...)
	return string(readFrameRaw(t, br))
}

// resp3Conn opens a fresh connection and negotiates RESP3 on it.
func resp3Conn(t *testing.T, srv *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, br := dial(t, srv)
	if m := helloFields(t, sendCmd(t, br, nc, "HELLO", "3")); m["proto"] != int64(3) {
		t.Fatalf("HELLO 3 proto = %v, want 3", m["proto"])
	}
	return nc, br
}

// TestResp3ScoreDoubleFraming checks that every score-bearing reply frames its
// numbers as ',' doubles under RESP3 and as '$' bulks under RESP2, by asserting on
// the raw bytes. It only cares that a double frame is present (or absent) in the
// reply, not the surrounding structure, which the nesting tests cover.
func TestResp3ScoreDoubleFraming(t *testing.T) {
	srv, nc2, br2 := startServer(t)
	nc3, br3 := resp3Conn(t, srv)

	for _, seed := range [][]string{
		{"ZADD", "z", "1", "m1", "2", "m2", "3", "m3"},
		{"HSET", "h", "f1", "v1", "f2", "v2"},
		{"GEOADD", "g", "13.361389", "38.115556", "a", "15.087269", "37.502669", "b"},
	} {
		writeCmd(t, nc2, seed...)
		readRESP(t, br2)
	}

	// Each case names a command and whether its RESP3 reply must contain a ',' double
	// frame somewhere in the bytes. The RESP2 reply must never contain one.
	cases := []struct {
		name string
		args []string
	}{
		{"zpopmin-count", []string{"ZPOPMIN", "z", "1"}},
		{"zmscore", []string{"ZMSCORE", "z", "m2", "nope"}},
		{"zrandmember-ws", []string{"ZRANDMEMBER", "z", "-2", "WITHSCORES"}},
		{"zmpop", []string{"ZMPOP", "1", "z", "MIN"}},
		{"geopos", []string{"GEOPOS", "g", "a"}},
		{"geodist", []string{"GEODIST", "g", "a", "b"}},
		{"geosearch-withdist", []string{"GEOSEARCH", "g", "FROMMEMBER", "a", "BYRADIUS", "500", "km", "WITHDIST", "WITHCOORD"}},
	}
	for _, c := range cases {
		// Re-seed z before each pop so the count-gated pops keep members to return.
		writeCmd(t, nc2, "ZADD", "z", "1", "m1", "2", "m2", "3", "m3")
		readRESP(t, br2)

		got2 := rawReply(t, br2, nc2, c.args...)
		if strings.Contains(got2, "\r\n,") || strings.HasPrefix(got2, ",") {
			t.Errorf("%s RESP2 reply carried a double frame: %q", c.name, got2)
		}

		writeCmd(t, nc2, "ZADD", "z", "1", "m1", "2", "m2", "3", "m3")
		readRESP(t, br2)

		got3 := rawReply(t, br3, nc3, c.args...)
		if !strings.Contains(got3, ",") {
			t.Errorf("%s RESP3 reply carried no double frame: %q", c.name, got3)
		}
	}
}

// TestResp3ScoreNesting checks the count/flag-gated commands nest each value with
// its member under RESP3 while staying flat under RESP2, reading the parsed
// structure. ZMSCORE stays a flat array under both (its elements are bare doubles,
// not member/value pairs), and BZPOPMIN's immediate serve stays a flat 3-element
// [key, member, score] under both.
func TestResp3ScoreNesting(t *testing.T) {
	srv, nc2, br2 := startServer(t)
	nc3, br3 := resp3Conn(t, srv)

	seed := func(nc net.Conn, br *bufio.Reader) {
		writeCmd(t, nc, "DEL", "z", "h")
		readRESP(t, br)
		writeCmd(t, nc, "ZADD", "z", "1", "m1", "2", "m2")
		readRESP(t, br)
		writeCmd(t, nc, "HSET", "h", "f1", "v1", "f2", "v2")
		readRESP(t, br)
	}

	// ZPOPMIN with a count nests under RESP3, stays flat under RESP2.
	seed(nc2, br2)
	if arr := sendCmd(t, br2, nc2, "ZPOPMIN", "z", "2").([]any); len(arr) != 4 {
		t.Errorf("RESP2 ZPOPMIN count = %v, want flat 4 elements", arr)
	}
	seed(nc3, br3)
	arr3 := sendCmd(t, br3, nc3, "ZPOPMIN", "z", "2").([]any)
	if len(arr3) != 2 {
		t.Fatalf("RESP3 ZPOPMIN count = %v, want two pairs", arr3)
	}
	if pair, ok := arr3[0].([]any); !ok || len(pair) != 2 {
		t.Errorf("RESP3 ZPOPMIN pair 0 = %v, want a 2-element pair", arr3[0])
	}

	// ZMSCORE stays flat under both: an array whose absent member is a null.
	seed(nc2, br2)
	if arr := sendCmd(t, br2, nc2, "ZMSCORE", "z", "m1", "nope").([]any); len(arr) != 2 || arr[0] != "1" || arr[1] != nil {
		t.Errorf("RESP2 ZMSCORE = %v, want [1 <nil>]", arr)
	}
	seed(nc3, br3)
	if arr := sendCmd(t, br3, nc3, "ZMSCORE", "z", "m1", "nope").([]any); len(arr) != 2 || arr[0] != "1" || arr[1] != nil {
		t.Errorf("RESP3 ZMSCORE = %v, want [1 <nil>]", arr)
	}

	// HRANDFIELD WITHVALUES nests under RESP3 (values stay bulk strings), flat under RESP2.
	seed(nc2, br2)
	if arr := sendCmd(t, br2, nc2, "HRANDFIELD", "h", "-2", "WITHVALUES").([]any); len(arr) != 4 {
		t.Errorf("RESP2 HRANDFIELD withvalues = %v, want flat 4 elements", arr)
	}
	seed(nc3, br3)
	arrh := sendCmd(t, br3, nc3, "HRANDFIELD", "h", "-2", "WITHVALUES").([]any)
	if len(arrh) != 2 {
		t.Fatalf("RESP3 HRANDFIELD withvalues = %v, want two pairs", arrh)
	}
	if pair, ok := arrh[0].([]any); !ok || len(pair) != 2 {
		t.Errorf("RESP3 HRANDFIELD pair 0 = %v, want a 2-element pair", arrh[0])
	}

	// BZPOPMIN's immediate serve stays a flat [key, member, score] under both.
	seed(nc2, br2)
	if arr := sendCmd(t, br2, nc2, "BZPOPMIN", "z", "0").([]any); len(arr) != 3 || arr[0] != "z" || arr[1] != "m1" {
		t.Errorf("RESP2 BZPOPMIN = %v, want [z m1 1]", arr)
	}
	seed(nc3, br3)
	if arr := sendCmd(t, br3, nc3, "BZPOPMIN", "z", "0").([]any); len(arr) != 3 || arr[0] != "z" || arr[1] != "m1" {
		t.Errorf("RESP3 BZPOPMIN = %v, want [z m1 1]", arr)
	}

	// ZMPOP nests its inner member/score pairs under both protocols; only the score
	// framing changes, which the raw-bytes test covers.
	seed(nc2, br2)
	mp2 := sendCmd(t, br2, nc2, "ZMPOP", "1", "z", "MIN", "COUNT", "2").([]any)
	if len(mp2) != 2 {
		t.Fatalf("RESP2 ZMPOP = %v, want [key, members]", mp2)
	}
	if inner, ok := mp2[1].([]any); !ok || len(inner) != 2 {
		t.Fatalf("RESP2 ZMPOP members = %v, want two pairs", mp2[1])
	} else if pair, ok := inner[0].([]any); !ok || len(pair) != 2 {
		t.Errorf("RESP2 ZMPOP pair 0 = %v, want a 2-element pair", inner[0])
	}
}
