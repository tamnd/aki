package f1srv

import (
	"fmt"
	"strings"
	"testing"
)

// TestZRemCoalescedDeferredRankExact removes half a zset's members through the coalesced ZREM
// drain, which defers both ordered-index splices (the member row and its score sidecar) to the
// background folder, then immediately runs the width-based reads. Those reads walk order-statistic
// widths, which count a not-yet-spliced dead node as if it were live, so without the engine's
// SyncPendingRemovals gate at the head of every width descent they would over-count and mis-rank.
// Removing 100 members in one flush keeps tombstones pending when the reads run, so the reads
// exercise the gate rather than racing a folder that already drained. Every read must reflect the
// exact surviving set: members m100..m199 at ranks 0..99.
func TestZRemCoalescedDeferredRankExact(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Seed 200 members with distinct increasing scores and zero-padded names, so rank order,
	// score order, and lexical order all agree and every rank is predictable.
	args := []string{"ZADD", "z"}
	for i := 0; i < 200; i++ {
		args = append(args, fmt.Sprintf("%d", i), fmt.Sprintf("m%03d", i))
	}
	cmd(t, rw, args...)
	expect(t, rw, ":200")

	// Remove m000..m099 as one coalesced run: 100 single-member ZREMs to the same key from one
	// connection fold into a single locked batch that queues 200 tombstones (a member row and a
	// score row each) to the folder.
	for i := 0; i < 100; i++ {
		writeCmd(t, rw, "ZREM", "z", fmt.Sprintf("m%03d", i))
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i := 0; i < 100; i++ {
		expect(t, rw, ":1")
	}

	// Cardinality is the seqlock count word, exact regardless of pending tombstones.
	cmd(t, rw, "ZCARD", "z")
	expect(t, rw, ":100")

	// ZRANK rides rankLocked width arithmetic: the smallest survivor m100 is rank 0, and the
	// largest m199 is rank 99. Over-counting dead nodes would push these ranks out.
	cmd(t, rw, "ZRANK", "z", "m100")
	expect(t, rw, ":0")
	cmd(t, rw, "ZRANK", "z", "m199")
	expect(t, rw, ":99")

	// ZRANGE by index rides selectAtLocked width arithmetic across the whole shrunk set. The full
	// window must be exactly the 100 survivors in rank order, with no dead node leaking in.
	cmd(t, rw, "ZRANGE", "z", "0", "-1")
	got := readArray(t, rw)
	if len(got) != 100 {
		t.Fatalf("ZRANGE returned %d members, want 100", len(got))
	}
	for i, m := range got {
		want := fmt.Sprintf("m%03d", 100+i)
		if m != want {
			t.Fatalf("ZRANGE[%d] = %q, want %q", i, m, want)
		}
	}

	// ZRANDMEMBER with a count of the full cardinality rides selectInPrefix per draw: every
	// returned member must be a survivor, never one of the removed m000..m099.
	cmd(t, rw, "ZRANDMEMBER", "z", "100")
	rand := readArray(t, rw)
	for _, m := range rand {
		var n int
		if _, err := fmt.Sscanf(m, "m%03d", &n); err != nil || n < 100 {
			t.Fatalf("ZRANDMEMBER returned removed member %q", m)
		}
	}
}

// TestCollDeleteCoalesced drives the delete-coalescing drain path: a run of same-key
// named-element deletes (HDEL/SREM/ZREM) from one connection folds into a single locked
// batch, and the folded run replies exactly as the same commands would unfolded. It covers
// the multi-element command, an already-removed element counting zero for a later command in
// the run, a verb change and a foreign command breaking the run, and the WRONGTYPE run.
func TestCollDeleteCoalesced(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Seed a hash with six fields, then delete them across a pipelined run. Command replies are
	// the per-command deleted counts: 1, 2, 1 (the third command re-deletes f2 which the first
	// already removed, so only f5 counts).
	cmd(t, rw, "HSET", "h", "f0", "0", "f1", "1", "f2", "2", "f3", "3", "f4", "4", "f5", "5")
	expect(t, rw, ":6")
	writeCmd(t, rw, "HDEL", "h", "f2")
	writeCmd(t, rw, "HDEL", "h", "f0", "f1")
	writeCmd(t, rw, "HDEL", "h", "f2", "f5")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expect(t, rw, ":1")
	expect(t, rw, ":2")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":2") // f3, f4 remain
	cmd(t, rw, "HGET", "h", "f3")
	expect(t, rw, "$3")

	// A foreign command between two HDELs breaks the run; both deletes still land and the GET
	// runs on its own between them.
	cmd(t, rw, "HSET", "h2", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")
	writeCmd(t, rw, "HDEL", "h2", "a")
	writeCmd(t, rw, "GET", "nope")
	writeCmd(t, rw, "HDEL", "h2", "b")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expect(t, rw, ":1")
	expect(t, rw, "$-1")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "h2")
	expect(t, rw, ":1") // c remains

	// SREM run folds the same way; per-command removed counts 1, 1, 0.
	cmd(t, rw, "SADD", "s", "m0", "m1", "m2")
	expect(t, rw, ":3")
	writeCmd(t, rw, "SREM", "s", "m0")
	writeCmd(t, rw, "SREM", "s", "m1")
	writeCmd(t, rw, "SREM", "s", "m0")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expect(t, rw, ":1")
	expect(t, rw, ":1")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":1") // m2 remains

	// ZREM run folds and keeps the score index consistent: after removing a and b, a ZSCORE on
	// the survivor still resolves and a ZRANK reflects the shrunk set.
	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	writeCmd(t, rw, "ZREM", "z", "a")
	writeCmd(t, rw, "ZREM", "z", "b", "a")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expect(t, rw, ":1")
	expect(t, rw, ":1") // b removed; a already gone
	cmd(t, rw, "ZCARD", "z")
	expect(t, rw, ":1")
	cmd(t, rw, "ZSCORE", "z", "c")
	expect(t, rw, "$3")
	cmd(t, rw, "ZRANK", "z", "c")
	expect(t, rw, ":0")

	// A differing verb breaks the run: the SREM stands alone from an HDEL that follows on a
	// different key, and both apply in order.
	cmd(t, rw, "SADD", "s2", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "HSET", "h3", "p", "1")
	expect(t, rw, ":1")
	writeCmd(t, rw, "SREM", "s2", "x")
	writeCmd(t, rw, "HDEL", "h3", "p")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expect(t, rw, ":1")
	expect(t, rw, ":1")

	// A pipelined delete run against a string key replies WRONGTYPE for every command in the run.
	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	writeCmd(t, rw, "HDEL", "str", "a")
	writeCmd(t, rw, "HDEL", "str", "b")
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i := 0; i < 2; i++ {
		got := readReply(t, rw)
		if !strings.HasPrefix(got, "-WRONGTYPE") {
			t.Fatalf("coalesced delete on string reply %d = %q, want WRONGTYPE", i, got)
		}
	}
}
