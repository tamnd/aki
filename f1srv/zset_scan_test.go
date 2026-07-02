package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// zscanReply is one ZSCAN reply decoded into the resume cursor and the flat member+score
// array: unlike SSCAN each member is followed by its score, the same shape HSCAN uses for
// field+value. The cursor is returned without the RESP "$" prefix, so a completed iteration
// reads back as "0".
type zscanReply struct {
	cursor string
	items  []string // member, score, member, score, ...
}

func zscanCall(t *testing.T, rw *bufio.ReadWriter, args ...string) zscanReply {
	t.Helper()
	cmd(t, rw, args...)
	if got := readReply(t, rw); got != "*2" {
		t.Fatalf("zscan header = %q, want *2", got)
	}
	cur := readReply(t, rw)
	if len(cur) == 0 || cur[0] != '$' {
		t.Fatalf("zscan cursor = %q, want a bulk string", cur)
	}
	ah := readReply(t, rw)
	if len(ah) == 0 || ah[0] != '*' {
		t.Fatalf("zscan items header = %q, want an array", ah)
	}
	n := 0
	for _, ch := range ah[1:] {
		n = n*10 + int(ch-'0')
	}
	items := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("zscan item %d = %q, want a bulk string", i, b)
		}
		items[i] = b[1:]
	}
	return zscanReply{cursor: cur[1:], items: items}
}

// pairs folds the flat member/score array into a member->score map for order-independent
// assertions. The walk order is member-byte order, which differs from Redis's internal
// iteration, so tests compare the collected set rather than the sequence.
func (r zscanReply) pairs(t *testing.T) map[string]string {
	t.Helper()
	if len(r.items)%2 != 0 {
		t.Fatalf("zscan items not paired: %d elements", len(r.items))
	}
	m := map[string]string{}
	for i := 0; i < len(r.items); i += 2 {
		m[r.items[i]] = r.items[i+1]
	}
	return m
}

// ZSCAN returns member+score pairs and completes in one batch when COUNT covers the zset;
// the score column is formatted exactly like ZSCORE.
func TestZScanOneBatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "10", "a", "5", "b", "3.5", "c")
	expect(t, rw, ":3")

	reply := zscanCall(t, rw, "ZSCAN", "z", "0", "COUNT", "100")
	if reply.cursor != "0" {
		t.Fatalf("cursor = %q, want 0 (complete)", reply.cursor)
	}
	got := reply.pairs(t)
	want := map[string]string{"a": "10", "b": "5", "c": "3.5"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pairs = %v, want %v", got, want)
	}
}

// A missing zset scans as a completed empty iteration: cursor 0 and no results.
func TestZScanMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZSCAN", "nope", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*0")
}

// MATCH filters returned members by glob without changing the score column or the walk.
func TestZScanMatch(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "user:1", "2", "user:2", "3", "post:1")
	expect(t, rw, ":3")

	reply := zscanCall(t, rw, "ZSCAN", "z", "0", "COUNT", "100", "MATCH", "user:*")
	if reply.cursor != "0" {
		t.Fatalf("cursor = %q, want 0", reply.cursor)
	}
	got := reply.pairs(t)
	want := map[string]string{"user:1": "1", "user:2": "2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pairs = %v, want %v", got, want)
	}
}

// A full iteration with a small COUNT returns every member exactly once across batches, each
// paired with its correct score.
func TestZScanFullIterationSmallCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 250
	args := []string{"ZADD", "big"}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("%d", i), fmt.Sprintf("m%04d", i))
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	seen := map[string]string{}
	cursor := "0"
	rounds := 0
	for {
		rounds++
		if rounds > 10000 {
			t.Fatal("ZSCAN did not terminate")
		}
		reply := zscanCall(t, rw, "ZSCAN", "big", cursor, "COUNT", "7")
		cursor = reply.cursor
		for m, s := range reply.pairs(t) {
			if _, dup := seen[m]; dup {
				t.Fatalf("member %q returned twice", m)
			}
			seen[m] = s
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("full scan saw %d members, want %d", len(seen), n)
	}
	for i := 0; i < n; i++ {
		m := fmt.Sprintf("m%04d", i)
		if seen[m] != fmt.Sprintf("%d", i) {
			t.Fatalf("member %q score = %q, want %d", m, seen[m], i)
		}
	}
}

// ZSCAN validates its key first, like Redis: a missing key returns the empty-scan sentinel
// before options are parsed, so a bad option on a missing key is not an error; on an existing
// key the same option is rejected. A non-int COUNT reports the integer error, COUNT < 1 and
// unknown tokens (including NOSCORES, which ZSCAN does not have) are syntax errors.
func TestZScanErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "ZSCAN", "k", "0")
	expect(t, rw, "-"+wrongType)

	cmd(t, rw, "ZADD", "z", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZSCAN", "z", "zz") // not hex, not "0"
	expect(t, rw, "-ERR invalid cursor")
	cmd(t, rw, "ZSCAN", "z", "0", "COUNT", "0")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "ZSCAN", "z", "0", "COUNT", "-1")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "ZSCAN", "z", "0", "COUNT", "abc")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "ZSCAN", "z", "0", "MATCH")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "ZSCAN", "z", "0", "NOSCORES") // not a ZSCAN option
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "ZSCAN", "z", "0", "BOGUS")
	expect(t, rw, "-ERR syntax error")

	// Missing key short-circuits option parsing: bad options do not error.
	cmd(t, rw, "ZSCAN", "gone", "0", "BOGUS")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*0")
	cmd(t, rw, "ZSCAN", "gone", "0", "COUNT", "0")
	expect(t, rw, "*2")
	expect(t, rw, "$0")
	expect(t, rw, "*0")

	// Wrong arity.
	cmd(t, rw, "ZSCAN", "z")
	expect(t, rw, "-ERR wrong number of arguments for 'zscan' command")
}
