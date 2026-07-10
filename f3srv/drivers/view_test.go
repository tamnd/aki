package drivers

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
)

// The view-lifetime regressions: GET now replies with a view into the shard
// arena, valid only until the reply builder copies it, and a later command in
// the same drained batch may overwrite the very bytes the view named. These
// tests pin that the copy really happens inside the GET's own execution, by
// sending the whole sequence in one write to a one-shard server so it lands
// as one batch on one owner.

func startOneShard(t *testing.T) (net.Conn, *bufio.Reader) {
	t.Helper()
	srv, err := Listen(Options{Addr: "127.0.0.1:0", Shards: 1, ArenaBytes: 4 << 20, SegBytes: 1 << 18})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Close()
	})
	return nc, bufio.NewReader(nc)
}

func bulk(v string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v) }

// TestGetThenOverwriteSameBatch pipelines GET k right before a same-size SET
// of the same key. The overwrite is in place (same vcap, embedded band), so
// it reuses the exact arena bytes the GET viewed; the GET's reply must still
// be the pre-overwrite value, proving the bytes were copied into the reply
// arena during the GET, not referenced past it.
func TestGetThenOverwriteSameBatch(t *testing.T) {
	nc, br := startOneShard(t)
	a := strings.Repeat("a", 512)
	b := strings.Repeat("b", 512)
	req := cmd("SET", "k", a) + cmd("GET", "k") + cmd("SET", "k", b) + cmd("GET", "k")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n"+bulk(a)+"+OK\r\n"+bulk(b))
}

// TestGetThenOverwriteSepRunSameBatch is the same pin for the separated band:
// a 4KiB value's run lives in the arena and a same-size overwrite republishes
// the record, so the old run's bytes go back to the allocator while the GET's
// view could still name them.
func TestGetThenOverwriteSepRunSameBatch(t *testing.T) {
	nc, br := startOneShard(t)
	a := strings.Repeat("a", 4096)
	b := strings.Repeat("b", 4096)
	req := cmd("SET", "k", a) + cmd("GET", "k") + cmd("SET", "k", b) + cmd("GET", "k")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n"+bulk(a)+"+OK\r\n"+bulk(b))
}

// TestGetThenDelThenAppendSameBatch frees the viewed record and immediately
// allocates a fresh one for the same key in the same batch, the arena-reuse
// shape: the GET's reply bytes must be the pre-delete value whatever the
// allocator did with the space.
func TestGetThenDelThenAppendSameBatch(t *testing.T) {
	nc, br := startOneShard(t)
	a := strings.Repeat("x", 512)
	w := strings.Repeat("y", 512)
	req := cmd("SET", "k", a) + cmd("GET", "k") + cmd("DEL", "k") + cmd("APPEND", "k", w) + cmd("GET", "k")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n"+bulk(a)+":1\r\n:512\r\n"+bulk(w))
}

// TestMGetThenOverwriteSameBatch covers the fan path: MGET's per-key views
// are consumed into the partial one by one, and a following overwrite in the
// same batch must not reach back into an earlier reply.
func TestMGetThenOverwriteSameBatch(t *testing.T) {
	nc, br := startOneShard(t)
	a := strings.Repeat("a", 512)
	b := strings.Repeat("b", 512)
	req := cmd("SET", "k1", a) + cmd("SET", "k2", b) +
		cmd("MGET", "k1", "k2", "nope") + cmd("SET", "k1", b) + cmd("GET", "k1")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n+OK\r\n*3\r\n"+bulk(a)+bulk(b)+"$-1\r\n+OK\r\n"+bulk(b))
}

// TestGetRangeThenOverwriteSameBatch covers the substring read: the clamp
// reslices the view and Bulk copies it, all before the overwrite runs.
func TestGetRangeThenOverwriteSameBatch(t *testing.T) {
	nc, br := startOneShard(t)
	a := strings.Repeat("a", 512)
	b := strings.Repeat("b", 512)
	req := cmd("SET", "k", a) + cmd("GETRANGE", "k", "0", "9") + cmd("SET", "k", b) + cmd("GETRANGE", "k", "0", "9")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n"+bulk(a[:10])+"+OK\r\n"+bulk(b[:10]))
}
