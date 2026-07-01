package f1srv

import (
	"testing"
	"time"
)

// firstEntryID digs the id of the first entry out of an XREAD/XREADGROUP reply, which nests
// [ [key, [ [id, [field, value, ...]], ... ]], ... ]. It fails the test on any shape mismatch.
func firstEntryID(t *testing.T, reply any) string {
	t.Helper()
	streams, ok := reply.([]any)
	if !ok || len(streams) == 0 {
		t.Fatalf("reply = %#v, want a non-empty stream array", reply)
	}
	first, ok := streams[0].([]any)
	if !ok || len(first) != 2 {
		t.Fatalf("stream[0] = %#v, want [key, entries]", streams[0])
	}
	entries, ok := first[1].([]any)
	if !ok || len(entries) == 0 {
		t.Fatalf("entries = %#v, want a non-empty entry array", first[1])
	}
	entry, ok := entries[0].([]any)
	if !ok || len(entry) != 2 {
		t.Fatalf("entry[0] = %#v, want [id, fields]", entries[0])
	}
	id, ok := entry[0].(string)
	if !ok {
		t.Fatalf("id = %#v, want a bulk string", entry[0])
	}
	return id
}

// A blocking XREAD on '$' parks until another connection XADDs to the stream, then wakes and
// returns the entry added after the id that was current when the read was issued.
func TestStreamXReadBlockWakeup(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	// Seed one entry so '$' resolves to a known last id (1-1); the blocked read waits for what
	// lands after it.
	cmd(t, rwB, "XADD", "s", "1-1", "f", "v1")
	expect(t, rwB, "$1-1")

	done := make(chan string, 1)
	go func() {
		cmd(t, rwA, "XREAD", "BLOCK", "0", "STREAMS", "s", "$")
		done <- firstEntryID(t, readValue(t, rwA))
	}()

	// Let the reader park before the add, so the wakeup path runs rather than the immediate serve.
	time.Sleep(50 * time.Millisecond)
	cmd(t, rwB, "XADD", "s", "2-2", "f", "v2")
	expect(t, rwB, "$2-2")

	select {
	case got := <-done:
		if got != "2-2" {
			t.Fatalf("woken XREAD id = %q, want 2-2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("XREAD BLOCK did not wake after XADD")
	}
}

// A blocking XREAD whose stream stays quiet for the whole timeout replies with a null array.
func TestStreamXReadBlockTimeout(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "f", "v1")
	expect(t, rw, "$1-1")

	start := time.Now()
	cmd(t, rw, "XREAD", "BLOCK", "100", "STREAMS", "s", "$")
	expect(t, rw, "*-1")
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("XREAD returned after %v, want it to wait out the ~100ms timeout", elapsed)
	}
}

// A blocking XREADGROUP '>' parks until an XADD delivers a new entry, then wakes with the entry
// and records it pending for the consumer.
func TestStreamXReadGroupBlockWakeup(t *testing.T) {
	rwA, rwB, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rwB, "XADD", "s", "1-1", "f", "v1")
	expect(t, rwB, "$1-1")
	cmd(t, rwB, "XGROUP", "CREATE", "s", "g", "$") // group starts after 1-1
	expect(t, rwB, "+OK")

	done := make(chan string, 1)
	go func() {
		cmd(t, rwA, "XREADGROUP", "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", ">")
		done <- firstEntryID(t, readValue(t, rwA))
	}()

	time.Sleep(50 * time.Millisecond)
	cmd(t, rwB, "XADD", "s", "2-2", "f", "v2")
	expect(t, rwB, "$2-2")

	select {
	case got := <-done:
		if got != "2-2" {
			t.Fatalf("woken XREADGROUP id = %q, want 2-2", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("XREADGROUP BLOCK did not wake after XADD")
	}

	// The delivered entry is now pending for consumer c.
	cmd(t, rwB, "XPENDING", "s", "g")
	expect(t, rwB, "*4")
	expect(t, rwB, ":1") // one pending
	expect(t, rwB, "$2-2")
	expect(t, rwB, "$2-2")
	// consumers array: [ [c, 1] ]
	expect(t, rwB, "*1")
	expect(t, rwB, "*2")
	expect(t, rwB, "$c")
	expect(t, rwB, "$1")
}

// A blocking XREADGROUP '>' whose group sees no new entry for the whole timeout replies null.
func TestStreamXReadGroupBlockTimeout(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "f", "v1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "$")
	expect(t, rw, "+OK")

	start := time.Now()
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "BLOCK", "100", "STREAMS", "s", ">")
	expect(t, rw, "*-1")
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("XREADGROUP returned after %v, want it to wait out the ~100ms timeout", elapsed)
	}
}

// An XREADGROUP with an explicit id never blocks, even with BLOCK set: it re-reads the consumer's
// pending entries and returns immediately with a per-stream list, empty when nothing is pending.
func TestStreamXReadGroupExplicitNeverBlocks(t *testing.T) {
	rw, _, cleanup := dialTwoGo(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "f", "v1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "$")
	expect(t, rw, "+OK")

	// BLOCK 0 would park forever on a '>' read, but an explicit id returns at once.
	got := make(chan any, 1)
	go func() {
		cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s", "0")
		got <- readValue(t, rw)
	}()
	select {
	case reply := <-got:
		streams := asArray(t, reply, 1)
		one := asArray(t, streams[0], 2)
		asBulk(t, one[0], "s")
		asArray(t, one[1], 0) // no pending entries for this consumer
	case <-time.After(2 * time.Second):
		t.Fatal("explicit-id XREADGROUP with BLOCK 0 blocked, want immediate reply")
	}
}
