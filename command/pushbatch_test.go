package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readArrayReply reads a RESP array of bulk strings and returns the elements.
// It supports just the shape LRANGE produces in these tests.
func readArrayReply(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read array header: %v", err)
	}
	if line[0] != '*' {
		t.Fatalf("want array reply, got %q", line)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rep := readTaggedReply(t, r)
		if !strings.HasPrefix(rep, "str:") {
			t.Fatalf("array elem %d = %q want a bulk string", i, rep)
		}
		out = append(out, rep[len("str:"):])
	}
	return out
}

// makeCollList sends one RPUSH with enough elements to promote the key past the
// listpack threshold into the btree-backed (coll) form, the form whose pipelined
// pushes take the batched hand-off. It returns once the reply is read, so the
// promotion is durable and a later push's coll probe sees it.
func makeCollList(t *testing.T, conn net.Conn, r *bufio.Reader, key string, n int) {
	t.Helper()
	args := make([]string, 0, n+2)
	args = append(args, "RPUSH", key)
	for i := 0; i < n; i++ {
		args = append(args, "seed"+strconv.Itoa(i))
	}
	if _, err := conn.Write([]byte(encodeCmd(args...))); err != nil {
		t.Fatal(err)
	}
	if got := readTaggedReply(t, r); got != "int:"+strconv.Itoa(n) {
		t.Fatalf("seed RPUSH reply = %q want int:%d", got, n)
	}
}

// A pipeline of RPUSH on one coll-form key runs through the batched hand-off and
// must return increasing lengths in order, with the elements landing at the tail.
func TestPushBatchRPushCollForm(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const seed = 200
	makeCollList(t, conn, r, "k", seed)

	const pushes = 50
	var pipe string
	for i := 0; i < pushes; i++ {
		pipe += encodeCmd("RPUSH", "k", "v"+strconv.Itoa(i))
	}
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < pushes; i++ {
		want := "int:" + strconv.Itoa(seed+i+1)
		if got := readTaggedReply(t, r); got != want {
			t.Fatalf("RPUSH reply %d = %q want %q", i, got, want)
		}
	}

	// The last three pushed values must be at the tail in push order.
	if _, err := conn.Write([]byte(encodeCmd("LRANGE", "k", "-3", "-1"))); err != nil {
		t.Fatal(err)
	}
	got := readArrayReply(t, r)
	want := []string{"v47", "v48", "v49"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("tail = %v want %v", got, want)
	}
}

// A pipeline of single-value LPUSH on a coll-form key must prepend each value, so
// the head ends up in reverse push order. This guards the head-side window write
// through the batch.
func TestPushBatchLPushReversed(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const seed = 200
	makeCollList(t, conn, r, "k", seed)

	for _, v := range []string{"a", "b", "c"} {
		if _, err := conn.Write([]byte(encodeCmd("LPUSH", "k", v))); err == nil {
			continue
		} else {
			t.Fatal(err)
		}
	}
	// Read the three deferred LPUSH replies (lengths 201, 202, 203).
	for i, want := range []string{"int:201", "int:202", "int:203"} {
		if got := readTaggedReply(t, r); got != want {
			t.Fatalf("LPUSH reply %d = %q want %q", i, got, want)
		}
	}
	if _, err := conn.Write([]byte(encodeCmd("LRANGE", "k", "0", "2"))); err != nil {
		t.Fatal(err)
	}
	got := readArrayReply(t, r)
	want := []string{"c", "b", "a"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("head = %v want %v", got, want)
	}
}

// A read interleaved with deferred pushes must flush the pending batch first, so
// the read sees every push before it and the replies stay in pipeline order. This
// also exercises the push-then-read read-after-write within one drain.
func TestPushBatchReadAfterWrite(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const seed = 200
	makeCollList(t, conn, r, "k", seed)

	pipe := encodeCmd("RPUSH", "k", "x") +
		encodeCmd("LLEN", "k") +
		encodeCmd("RPUSH", "k", "y") +
		encodeCmd("LLEN", "k")
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	want := []string{"int:201", "int:201", "int:202", "int:202"}
	for i, w := range want {
		if got := readTaggedReply(t, r); got != w {
			t.Fatalf("reply %d = %q want %q", i, got, w)
		}
	}
}

// RPUSHX on a coll-form key takes the batched path and pushes; the deferred replies
// must carry the growing length. An RPUSHX on an absent key stays on the inline
// path (the coll probe says not-coll) and returns 0.
func TestPushBatchPushXVariants(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)

	const seed = 200
	makeCollList(t, conn, r, "k", seed)

	pipe := encodeCmd("RPUSHX", "k", "p") +
		encodeCmd("RPUSHX", "k", "q") +
		encodeCmd("RPUSHX", "missing", "z")
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	want := []string{"int:201", "int:202", "int:0"}
	for i, w := range want {
		if got := readTaggedReply(t, r); got != w {
			t.Fatalf("reply %d = %q want %q", i, got, w)
		}
	}
}

// Pushes to distinct coll-form keys spread across shards must each report the right
// length, exercising the per-shard sub-batch grouping in applyPushBatch.
func TestPushBatchDistinctKeys(t *testing.T) {
	_, addr := startIncrBatchServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	r := bufio.NewReader(conn)

	const (
		keys = 16
		seed = 200
	)
	for i := 0; i < keys; i++ {
		makeCollList(t, conn, r, "list:"+strconv.Itoa(i), seed)
	}
	var pipe string
	for i := 0; i < keys; i++ {
		pipe += encodeCmd("RPUSH", "list:"+strconv.Itoa(i), "tail")
	}
	if _, err := conn.Write([]byte(pipe)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keys; i++ {
		if got := readTaggedReply(t, r); got != "int:201" {
			t.Fatalf("key %d reply = %q want int:201", i, got)
		}
	}
}
