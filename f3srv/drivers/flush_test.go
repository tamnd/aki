package drivers

import (
	"bytes"
	"strings"
	"testing"
)

// TestFlushAllEmptiesEveryBand fills all three value bands on a logged server
// (embedded, separated spilled to the log, chunked), flushes, and checks the
// evidence surface reads empty: DBSIZE zero, every INFO counter zero, and
// used_memory back at the fresh-store baseline. Then it writes again and
// checks the store still serves and the spill lands in a fresh log.
func TestFlushAllEmptiesEveryBand(t *testing.T) {
	// A one-byte resident cap forces every separated and chunked value into
	// the per-shard logs, so the flush has log bytes to drop on both shards.
	nc, br := startLTMServer(t, 1)

	base := readInfo(t, nc, br)

	send(t, nc, "SET", "emb", "hi")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "sep", strings.Repeat("s", 4096))
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "chunk", strings.Repeat("c", 1<<20))
	expect(t, br, "+OK\r\n")

	send(t, nc, "DBSIZE")
	expect(t, br, ":3\r\n")
	full := readInfo(t, nc, br)
	if full["keys"] != 3 || full["vlog_bytes"] == 0 || full["chunked_bytes"] != 1<<20 {
		t.Fatalf("bands not populated: %v", full)
	}

	send(t, nc, "FLUSHALL")
	expect(t, br, "+OK\r\n")

	send(t, nc, "DBSIZE")
	expect(t, br, ":0\r\n")
	after := readInfo(t, nc, br)
	for _, k := range []string{
		"keys", "band_int", "band_embedded", "band_separated", "band_chunked",
		"vlog_bytes", "vlog_dead_bytes", "vlog_runs",
		"arena_used_bytes", "arena_live_bytes", "chunked_bytes",
	} {
		if after[k] != 0 {
			t.Fatalf("%s = %d after FLUSHALL, want 0 (%v)", k, after[k], after)
		}
	}
	if after["used_memory"] != base["used_memory"] {
		t.Fatalf("used_memory after flush = %d, want fresh baseline %d",
			after["used_memory"], base["used_memory"])
	}

	// The flushed keys are gone, not just uncounted.
	send(t, nc, "GET", "sep")
	expect(t, br, "$-1\r\n")

	// A post-flush write reads back and its spill starts a fresh log: one
	// run, no dead bytes, nothing carried over from before the flush.
	v := bytes.Repeat([]byte("w"), 4096)
	send(t, nc, "SET", "again", string(v))
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "again")
	expectBulk(t, br, v)
	fresh := readInfo(t, nc, br)
	if fresh["vlog_runs"] != 1 || fresh["vlog_bytes"] == 0 || fresh["vlog_dead_bytes"] != 0 {
		t.Fatalf("post-flush log not fresh: %v", fresh)
	}
	if fresh["vlog_bytes"] >= full["vlog_bytes"] {
		t.Fatalf("post-flush log carries old bytes: %d, pre-flush %d",
			fresh["vlog_bytes"], full["vlog_bytes"])
	}
}

// TestFlushDBAliasAndTokens checks FLUSHDB flushes the single keyspace and
// the option tail: ASYNC and SYNC in any case are accepted (both run sync),
// anything else is a syntax error, and a longer tail is an arity error.
func TestFlushDBAliasAndTokens(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "FLUSHDB")
	expect(t, br, "+OK\r\n")
	send(t, nc, "DBSIZE")
	expect(t, br, ":0\r\n")

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "FLUSHALL", "ASYNC")
	expect(t, br, "+OK\r\n")
	send(t, nc, "DBSIZE")
	expect(t, br, ":0\r\n")

	send(t, nc, "FLUSHDB", "sync")
	expect(t, br, "+OK\r\n")
	send(t, nc, "flushall", "async")
	expect(t, br, "+OK\r\n")

	send(t, nc, "FLUSHALL", "NOW")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "FLUSHDB", "ASYNC", "extra")
	expect(t, br, "-ERR wrong number of arguments for 'flushdb' command\r\n")
}

// TestFlushPipelineOrder pipelines SET, FLUSHALL, GET in one write on one
// connection and expects the flushed order back: the flush gathers a partial
// from every shard before its +OK, and each shard runs its flush sub-command
// in the connection's per-shard order, so the trailing GET must see the empty
// store.
func TestFlushPipelineOrder(t *testing.T) {
	_, nc, br := startServer(t)

	req := cmd("SET", "k", "v") + cmd("FLUSHALL") + cmd("GET", "k")
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	expect(t, br, "+OK\r\n+OK\r\n$-1\r\n")
}
