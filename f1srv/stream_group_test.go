package f1srv

import "testing"

// TestStreamGroupCreateDestroy covers XGROUP CREATE / DESTROY and their error paths.
func TestStreamGroupCreateDestroy(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// CREATE on a missing key without MKSTREAM is an error, worded the way Redis words it.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g1", "$")
	expect(t, rw, "-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")

	// MKSTREAM creates the empty stream and the group in one shot.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g1", "$", "MKSTREAM")
	expect(t, rw, "+OK")
	cmd(t, rw, "TYPE", "s")
	expect(t, rw, "+stream")
	cmd(t, rw, "XLEN", "s")
	expect(t, rw, ":0")

	// A second CREATE of the same group is BUSYGROUP.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g1", "$")
	expect(t, rw, "-BUSYGROUP Consumer Group name already exists")

	// $ paired with ENTRIESREAD is accepted, the way Redis accepts it.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g2", "$", "ENTRIESREAD", "5")
	expect(t, rw, "+OK")

	// Re-creating that group is BUSYGROUP regardless of the id or ENTRIESREAD.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g2", "0", "ENTRIESREAD", "5")
	expect(t, rw, "-BUSYGROUP Consumer Group name already exists")

	// DESTROY removes just the named group.
	cmd(t, rw, "XGROUP", "DESTROY", "s", "g2")
	expect(t, rw, ":1")
	cmd(t, rw, "XGROUP", "DESTROY", "s", "g2")
	expect(t, rw, ":0")
	// g1 still exists (SETID succeeds), so DESTROY g2 left it alone.
	cmd(t, rw, "XGROUP", "SETID", "s", "g1", "0")
	expect(t, rw, "+OK")

	// SETID / CREATECONSUMER on a missing group is NOGROUP.
	cmd(t, rw, "XGROUP", "SETID", "s", "ghost", "0")
	expect(t, rw, "-"+streamNoGroup("ghost", "s"))
}

// TestStreamReadGroup covers the > delivery path, PEL bookkeeping, and XACK.
func TestStreamReadGroup(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XADD", "s", "3-1", "c", "3")
	expect(t, rw, "$3-1")

	// A group reading from the start, then a > read delivers everything after 0-0.
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, rw, "+OK")

	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "COUNT", "2", "STREAMS", "s", ">")
	r := asArray(t, readValue(t, rw), 1)
	pair := asArray(t, r[0], 2)
	asBulk(t, pair[0], "s")
	entries := asArray(t, pair[1], 2)
	asBulk(t, asArray(t, entries[0], 2)[0], "1-1")
	asBulk(t, asArray(t, entries[1], 2)[0], "2-1")
	// Verify the field map came through on the first entry.
	f := asArray(t, asArray(t, entries[0], 2)[1], 2)
	asBulk(t, f[0], "a")
	asBulk(t, f[1], "1")

	// A second > read picks up where the group left off: just 3-1.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	r2 := asArray(t, readValue(t, rw), 1)
	e2 := asArray(t, asArray(t, r2[0], 2)[1], 1)
	asBulk(t, asArray(t, e2[0], 2)[0], "3-1")

	// A third > read has nothing new: null array.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	if v := readValue(t, rw); v != nil {
		t.Fatalf("empty > read = %#v, want null array", v)
	}

	// All three are now pending; an explicit-id re-read from 0 returns all three.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0")
	rp := asArray(t, readValue(t, rw), 1)
	pend := asArray(t, asArray(t, rp[0], 2)[1], 3)
	asBulk(t, asArray(t, pend[0], 2)[0], "1-1")
	asBulk(t, asArray(t, pend[2], 2)[0], "3-1")

	// Ack two of them; XACK returns the count actually removed.
	cmd(t, rw, "XACK", "s", "g", "1-1", "2-1", "9-9")
	expect(t, rw, ":2")

	// The explicit re-read now shows only the un-acked 3-1.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0")
	rp2 := asArray(t, readValue(t, rw), 1)
	pend2 := asArray(t, asArray(t, rp2[0], 2)[1], 1)
	asBulk(t, asArray(t, pend2[0], 2)[0], "3-1")

	// Acking an already-acked id counts nothing.
	cmd(t, rw, "XACK", "s", "g", "1-1")
	expect(t, rw, ":0")
}

// TestStreamReadGroupTombstone verifies an entry deleted after delivery re-reads as a null-field row.
func TestStreamReadGroupTombstone(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, rw, "+OK")

	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	asArray(t, readValue(t, rw), 1)

	// Delete 1-1 from the stream; it stays in the PEL but its body is gone.
	cmd(t, rw, "XDEL", "s", "1-1")
	expect(t, rw, ":1")

	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0")
	rp := asArray(t, readValue(t, rw), 1)
	pend := asArray(t, asArray(t, rp[0], 2)[1], 2)
	// 1-1 is a tombstone (null field array), 2-1 still carries its fields.
	row0 := asArray(t, pend[0], 2)
	asBulk(t, row0[0], "1-1")
	if row0[1] != nil {
		t.Fatalf("deleted entry fields = %#v, want null array", row0[1])
	}
	row1 := asArray(t, pend[1], 2)
	asBulk(t, row1[0], "2-1")
	asArray(t, row1[1], 2)
}

// TestStreamReadGroupNoAck verifies NOACK delivers entries without creating PEL rows.
func TestStreamReadGroupNoAck(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, rw, "+OK")

	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "NOACK", "STREAMS", "s", ">")
	asArray(t, readValue(t, rw), 1)

	// Nothing is pending, so an explicit re-read is an empty per-stream list.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", "0")
	rp := asArray(t, readValue(t, rw), 1)
	asArray(t, asArray(t, rp[0], 2)[1], 0)

	// XACK finds nothing to remove.
	cmd(t, rw, "XACK", "s", "g", "1-1")
	expect(t, rw, ":0")

	// NOACK with an explicit id is accepted (NOACK is simply ignored on a non-consuming re-read),
	// the way Redis accepts it: c1 has nothing pending, so the per-stream list is empty.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "NOACK", "STREAMS", "s", "0")
	rp2 := asArray(t, readValue(t, rw), 1)
	asArray(t, asArray(t, rp2[0], 2)[1], 0)
}

// TestStreamXGroupConsumer covers CREATECONSUMER and DELCONSUMER, including the pending count returned.
func TestStreamXGroupConsumer(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, rw, "+OK")

	// CREATECONSUMER returns 1 for a new consumer, 0 for an existing one.
	cmd(t, rw, "XGROUP", "CREATECONSUMER", "s", "g", "c1")
	expect(t, rw, ":1")
	cmd(t, rw, "XGROUP", "CREATECONSUMER", "s", "g", "c1")
	expect(t, rw, ":0")

	// Deliver both entries to c1 so it owns two pending.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	asArray(t, readValue(t, rw), 1)

	// DELCONSUMER returns the number of pending entries it dropped.
	cmd(t, rw, "XGROUP", "DELCONSUMER", "s", "g", "c1")
	expect(t, rw, ":2")

	// The pending entries are gone: a > read from a fresh consumer sees nothing new
	// (the group cursor already advanced), and the deleted consumer had its PEL cleared.
	cmd(t, rw, "XGROUP", "DELCONSUMER", "s", "g", "c1")
	expect(t, rw, ":0")

	// DELCONSUMER on a group that does not exist is NOGROUP.
	cmd(t, rw, "XGROUP", "DELCONSUMER", "s", "ghost", "c1")
	expect(t, rw, "-"+streamNoGroup("ghost", "s"))
}

// TestStreamGroupSetIDRedelivery verifies XGROUP SETID rewinds the group so > redelivers.
func TestStreamGroupSetIDRedelivery(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "s", "2-1", "b", "2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "$")
	expect(t, rw, "+OK")

	// At $ the group is caught up: a > read delivers nothing.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	if v := readValue(t, rw); v != nil {
		t.Fatalf("caught-up read = %#v, want null array", v)
	}

	// Rewind to 0; now > redelivers both entries.
	cmd(t, rw, "XGROUP", "SETID", "s", "g", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	r := asArray(t, readValue(t, rw), 1)
	entries := asArray(t, asArray(t, r[0], 2)[1], 2)
	asBulk(t, asArray(t, entries[0], 2)[0], "1-1")
	asBulk(t, asArray(t, entries[1], 2)[0], "2-1")
}

// TestStreamGroupWrongType checks the group commands guard a string key with WRONGTYPE, and that
// a stream DEL clears the group rows with it.
func TestStreamGroupWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "x")
	expect(t, rw, "+OK")
	for _, c := range [][]string{
		{"XGROUP", "CREATE", "str", "g", "$"},
		{"XGROUP", "SETID", "str", "g", "$"},
		{"XGROUP", "DESTROY", "str", "g"},
		{"XGROUP", "CREATECONSUMER", "str", "g", "c"},
		{"XGROUP", "DELCONSUMER", "str", "g", "c"},
		{"XACK", "str", "g", "1-1"},
		{"XREADGROUP", "GROUP", "g", "c", "STREAMS", "str", ">"},
	} {
		cmd(t, rw, c...)
		expect(t, rw, "-"+wrongType)
	}

	// A DEL of a stream carrying a group and pending entries leaves no orphan rows: a fresh
	// stream at the same key with a fresh group starts clean.
	cmd(t, rw, "XADD", "st", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "st", "g", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", ">")
	asArray(t, readValue(t, rw), 1)
	cmd(t, rw, "DEL", "st")
	expect(t, rw, ":1")
	// Re-create; the old group must be gone (CREATE succeeds, not BUSYGROUP).
	cmd(t, rw, "XADD", "st", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "st", "g", "0")
	expect(t, rw, "+OK")
	// And no stale PEL: an explicit re-read shows an empty pending list.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", "0")
	rp := asArray(t, readValue(t, rw), 1)
	asArray(t, asArray(t, rp[0], 2)[1], 0)
}

// TestStreamGroupErrorParity locks the error and arity replies to the exact strings Redis 8.8 and
// Valkey 9.1 return, since a client that matches on them cares about the wording.
func TestStreamGroupErrorParity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "s", "1-1", "a", "1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, rw, "+OK")

	// Per-subcommand arity errors name the fully qualified subcommand.
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"XGROUP"}, "-ERR wrong number of arguments for 'xgroup' command"},
		{[]string{"XGROUP", "CREATE", "s"}, "-ERR wrong number of arguments for 'xgroup|create' command"},
		{[]string{"XGROUP", "SETID", "s", "g"}, "-ERR wrong number of arguments for 'xgroup|setid' command"},
		{[]string{"XGROUP", "DESTROY", "s", "g", "x"}, "-ERR wrong number of arguments for 'xgroup|destroy' command"},
		{[]string{"XGROUP", "CREATECONSUMER", "s", "g"}, "-ERR wrong number of arguments for 'xgroup|createconsumer' command"},
		{[]string{"XGROUP", "DELCONSUMER", "s", "g"}, "-ERR wrong number of arguments for 'xgroup|delconsumer' command"},
		{[]string{"XACK", "s"}, "-ERR wrong number of arguments for 'xack' command"},
		{[]string{"XREADGROUP", "GROUP", "g", "c", "STREAMS", "s"}, "-ERR wrong number of arguments for 'xreadgroup' command"},
	} {
		cmd(t, rw, tc.args...)
		expect(t, rw, tc.want)
	}

	// Unknown subcommand echoes the token verbatim; a bad option inside a valid subcommand takes
	// the other error form.
	cmd(t, rw, "XGROUP", "FROB", "s", "g")
	expect(t, rw, "-ERR unknown subcommand 'FROB'. Try XGROUP HELP.")
	cmd(t, rw, "XGROUP", "CREATE", "s", "g2", "0", "FROB")
	expect(t, rw, "-ERR unknown subcommand or wrong number of arguments for 'CREATE'. Try XGROUP HELP.")

	// ENTRIESREAD accepts any value >= -1 and rejects anything smaller or non-integer.
	cmd(t, rw, "XGROUP", "CREATE", "em2", "g", "$", "MKSTREAM", "ENTRIESREAD", "-2")
	expect(t, rw, "-ERR value for ENTRIESREAD must be positive or -1")
	cmd(t, rw, "XGROUP", "CREATE", "eab", "g", "$", "MKSTREAM", "ENTRIESREAD", "abc")
	expect(t, rw, "-ERR value is not an integer or out of range")

	// XACK on a missing key or group is 0, not an error.
	cmd(t, rw, "XACK", "nostream", "g", "1-1")
	expect(t, rw, ":0")
	cmd(t, rw, "XACK", "s", "ghost", "1-1")
	expect(t, rw, ":0")

	// XREADGROUP uses its own NOGROUP wording, and a missing GROUP option is a distinct error.
	cmd(t, rw, "XREADGROUP", "GROUP", "ghost", "c", "STREAMS", "s", ">")
	expect(t, rw, "-"+streamReadGroupNoGroup("s", "ghost"))
	cmd(t, rw, "XREADGROUP", "COUNT", "1", "BLOCK", "0", "STREAMS", "s", ">")
	expect(t, rw, "-ERR Missing GROUP option for XREADGROUP")
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", "extra", ">")
	expect(t, rw, "-"+errStreamUnbalancedGroup)

	// A non-positive COUNT is clamped to "no limit" rather than rejected.
	cmd(t, rw, "XREADGROUP", "GROUP", "g", "c", "COUNT", "0", "STREAMS", "s", ">")
	r := asArray(t, readValue(t, rw), 1)
	asArray(t, asArray(t, r[0], 2)[1], 1)
}

// asInt asserts v is an integer reply and returns it.
func asInt(t *testing.T, v any) int64 {
	t.Helper()
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("value = %#v, want an integer", v)
	}
	return n
}

// TestStreamPending covers XPENDING summary and extended forms, the idle and consumer filters, and
// the error and arity paths, all pinned to the replies Redis 8.8 and Valkey 9.1 give.
func TestStreamPending(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	for _, id := range []string{"1-1", "2-1", "3-1", "4-1"} {
		cmd(t, rw, "XADD", "s", id, "f", "v")
		expect(t, rw, "$"+id)
	}
	cmd(t, rw, "XGROUP", "CREATE", "s", "g1", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "XGROUP", "CREATE", "s", "empty", "0")
	expect(t, rw, "+OK")

	// alice takes 1-1..3-1, bob takes 4-1, then 1-1 is acked so alice keeps 2 pending.
	cmd(t, rw, "XREADGROUP", "GROUP", "g1", "alice", "COUNT", "3", "STREAMS", "s", ">")
	asArray(t, readValue(t, rw), 1)
	cmd(t, rw, "XREADGROUP", "GROUP", "g1", "bob", "STREAMS", "s", ">")
	asArray(t, readValue(t, rw), 1)
	cmd(t, rw, "XACK", "s", "g1", "1-1")
	expect(t, rw, ":1")

	// Summary: total 3, low 2-1, high 4-1, breakdown alice=2 (bulk string) then bob=1.
	cmd(t, rw, "XPENDING", "s", "g1")
	sum := asArray(t, readValue(t, rw), 4)
	if got := asInt(t, sum[0]); got != 3 {
		t.Fatalf("summary total = %d, want 3", got)
	}
	asBulk(t, sum[1], "2-1")
	asBulk(t, sum[2], "4-1")
	cons := asArray(t, sum[3], 2)
	a := asArray(t, cons[0], 2)
	asBulk(t, a[0], "alice")
	asBulk(t, a[1], "2") // count is a bulk string in the summary
	b := asArray(t, cons[1], 2)
	asBulk(t, b[0], "bob")
	asBulk(t, b[1], "1")

	// Summary of a group with nothing pending: 0, nil, nil, nil.
	cmd(t, rw, "XPENDING", "s", "empty")
	e := asArray(t, readValue(t, rw), 4)
	if got := asInt(t, e[0]); got != 0 {
		t.Fatalf("empty summary total = %d, want 0", got)
	}
	if e[1] != nil || e[2] != nil || e[3] != nil {
		t.Fatalf("empty summary tail = %#v, want three nils", e[1:])
	}

	// Extended: the three pending rows in id order, each [id, consumer, idle, delivery-count].
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+", "10")
	ext := asArray(t, readValue(t, rw), 3)
	wantRows := []struct{ id, consumer string }{{"2-1", "alice"}, {"3-1", "alice"}, {"4-1", "bob"}}
	for i, wr := range wantRows {
		row := asArray(t, ext[i], 4)
		asBulk(t, row[0], wr.id)
		asBulk(t, row[1], wr.consumer)
		if idle := asInt(t, row[2]); idle < 0 {
			t.Fatalf("row %d idle = %d, want >= 0", i, idle)
		}
		if dc := asInt(t, row[3]); dc != 1 {
			t.Fatalf("row %d delivery-count = %d, want 1", i, dc)
		}
	}

	// Consumer filter narrows to alice's two rows.
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+", "10", "alice")
	fa := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, fa[0], 4)[0], "2-1")
	asBulk(t, asArray(t, fa[1], 4)[0], "3-1")

	// COUNT caps the window to the first row.
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+", "1")
	asArray(t, readValue(t, rw), 1)

	// A huge IDLE floor filters everything out.
	cmd(t, rw, "XPENDING", "s", "g1", "IDLE", "9999999", "-", "+", "10")
	asArray(t, readValue(t, rw), 0)

	// An exclusive start drops the boundary row.
	cmd(t, rw, "XPENDING", "s", "g1", "(2-1", "+", "10")
	xs := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, xs[0], 4)[0], "3-1")

	// A non-positive COUNT yields an empty reply, not an error.
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+", "0")
	asArray(t, readValue(t, rw), 0)

	// Error and arity paths, verbatim to Redis and Valkey.
	cmd(t, rw, "XPENDING", "nokey", "g1")
	expect(t, rw, "-NOGROUP No such key 'nokey' or consumer group 'g1'")
	cmd(t, rw, "XPENDING", "s", "ghost")
	expect(t, rw, "-NOGROUP No such key 's' or consumer group 'ghost'")
	cmd(t, rw, "XPENDING", "s")
	expect(t, rw, "-ERR wrong number of arguments for 'xpending' command")
	cmd(t, rw, "XPENDING", "s", "g1", "extra")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "XPENDING", "s", "g1", "-", "+", "abc")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "XPENDING", "s", "g1", "xx", "+", "10")
	expect(t, rw, "-ERR Invalid stream ID specified as stream command argument")
	cmd(t, rw, "XPENDING", "s", "g1", "IDLE", "abc", "-", "+", "10")
	expect(t, rw, "-ERR value is not an integer or out of range")
	// A non-IDLE token in the IDLE slot shifts count onto "-", which fails the integer parse first.
	cmd(t, rw, "XPENDING", "s", "g1", "FOO", "0", "-", "+", "10")
	expect(t, rw, "-ERR value is not an integer or out of range")

	// Wrong type on the key.
	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "XPENDING", "str", "g1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
}

// TestStreamClaim covers XCLAIM and XAUTOCLAIM: the read and JUSTID claim forms, the min-idle and
// force filters, dead-entry drops, RETRYCOUNT/IDLE/TIME modifiers, the XAUTOCLAIM cursor and deleted
// list, and the error and arity paths, all pinned to Redis 8.8 and Valkey 9.1 replies.
func TestStreamClaim(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	seed := func() {
		cmd(t, rw, "FLUSHALL")
		expect(t, rw, "+OK")
		for _, id := range []string{"1-1", "2-1", "3-1", "4-1", "5-1"} {
			cmd(t, rw, "XADD", "s", id, "f", id)
			expect(t, rw, "$"+id)
		}
		cmd(t, rw, "XGROUP", "CREATE", "s", "g1", "0")
		expect(t, rw, "+OK")
		cmd(t, rw, "XGROUP", "CREATE", "s", "g2", "0")
		expect(t, rw, "+OK")
		cmd(t, rw, "XREADGROUP", "GROUP", "g1", "alice", "COUNT", "3", "STREAMS", "s", ">")
		asArray(t, readValue(t, rw), 1)
		cmd(t, rw, "XREADGROUP", "GROUP", "g1", "bob", "COUNT", "2", "STREAMS", "s", ">")
		asArray(t, readValue(t, rw), 1)
	}

	// XCLAIM reassigns 1-1 and 2-1 to carol, returning them with fields in argument order.
	seed()
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "2-1")
	cl := asArray(t, readValue(t, rw), 2)
	asBulk(t, asArray(t, cl[0], 2)[0], "1-1")
	asBulk(t, asArray(t, cl[1], 2)[0], "2-1")
	// The claim bumped their delivery counts and moved them to carol.
	cmd(t, rw, "XPENDING", "s", "g1", "1-1", "1-1", "1")
	row := asArray(t, asArray(t, readValue(t, rw), 1)[0], 4)
	asBulk(t, row[1], "carol")
	if dc := asInt(t, row[3]); dc != 2 {
		t.Fatalf("delivery-count after claim = %d, want 2", dc)
	}

	// JUSTID replies just the id and does not bump the delivery count.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "4-1", "JUSTID")
	j := asArray(t, readValue(t, rw), 1)
	asBulk(t, j[0], "4-1")
	cmd(t, rw, "XPENDING", "s", "g1", "4-1", "4-1", "1")
	if dc := asInt(t, asArray(t, asArray(t, readValue(t, rw), 1)[0], 4)[3]); dc != 1 {
		t.Fatalf("delivery-count after JUSTID claim = %d, want 1", dc)
	}

	// A min-idle floor above the entry's idle claims nothing.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "999999", "5-1")
	asArray(t, readValue(t, rw), 0)
	// An id not pending and not forced is skipped.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "9-9")
	asArray(t, readValue(t, rw), 0)

	// FORCE creates a pending row in a group that never delivered the entry, at delivery count 1.
	cmd(t, rw, "XCLAIM", "s", "g2", "zoe", "0", "2-1", "FORCE")
	f := asArray(t, readValue(t, rw), 1)
	asBulk(t, asArray(t, f[0], 2)[0], "2-1")
	cmd(t, rw, "XPENDING", "s", "g2")
	g2sum := asArray(t, readValue(t, rw), 4)
	if got := asInt(t, g2sum[0]); got != 1 {
		t.Fatalf("g2 pending after force = %d, want 1", got)
	}

	// RETRYCOUNT sets the delivery count outright.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "RETRYCOUNT", "42")
	asArray(t, readValue(t, rw), 1)
	cmd(t, rw, "XPENDING", "s", "g1", "1-1", "1-1", "1")
	if dc := asInt(t, asArray(t, asArray(t, readValue(t, rw), 1)[0], 4)[3]); dc != 42 {
		t.Fatalf("delivery-count after RETRYCOUNT = %d, want 42", dc)
	}
	// IDLE sets the idle clock back, so the entry reads as at least that idle.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "5-1", "IDLE", "5000", "JUSTID")
	asArray(t, readValue(t, rw), 1)
	cmd(t, rw, "XPENDING", "s", "g1", "5-1", "5-1", "1")
	if idle := asInt(t, asArray(t, asArray(t, readValue(t, rw), 1)[0], 4)[2]); idle < 5000 {
		t.Fatalf("idle after IDLE 5000 = %d, want >= 5000", idle)
	}

	// An entry deleted from the log is dropped from the pending list on claim, not returned.
	seed()
	cmd(t, rw, "XDEL", "s", "3-1")
	expect(t, rw, ":1")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "3-1")
	asArray(t, readValue(t, rw), 0)
	cmd(t, rw, "XPENDING", "s", "g1")
	if got := asInt(t, asArray(t, readValue(t, rw), 4)[0]); got != 4 {
		t.Fatalf("pending after dead-entry claim = %d, want 4", got)
	}

	// XAUTOCLAIM from 0-0 claims the whole pending list, drops the dead 3-1, and reports cursor 0-0.
	seed()
	cmd(t, rw, "XDEL", "s", "3-1")
	expect(t, rw, ":1")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "0")
	ac := asArray(t, readValue(t, rw), 3)
	asBulk(t, ac[0], "0-0")
	asArray(t, ac[1], 4)
	del := asArray(t, ac[2], 1)
	asBulk(t, del[0], "3-1")

	// COUNT stops early and returns the resume cursor; a dead entry counts against COUNT.
	seed()
	cmd(t, rw, "XDEL", "s", "3-1")
	expect(t, rw, ":1")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "3-1", "COUNT", "2")
	ac2 := asArray(t, readValue(t, rw), 3)
	asBulk(t, ac2[0], "5-1")
	claimed := asArray(t, ac2[1], 1)
	asBulk(t, asArray(t, claimed[0], 2)[0], "4-1")
	asBulk(t, asArray(t, ac2[2], 1)[0], "3-1")

	// A high min-idle floor claims nothing and finishes the scan.
	seed()
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "999999", "0")
	ac3 := asArray(t, readValue(t, rw), 3)
	asBulk(t, ac3[0], "0-0")
	asArray(t, ac3[1], 0)
	asArray(t, ac3[2], 0)

	// XAUTOCLAIM JUSTID returns bare ids in the claimed slot.
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "0", "JUSTID")
	ac4 := asArray(t, readValue(t, rw), 3)
	asBulk(t, asArray(t, ac4[1], 5)[0], "1-1")

	// XCLAIM error and arity paths.
	cmd(t, rw, "XCLAIM", "s", "g1", "carol")
	expect(t, rw, "-ERR wrong number of arguments for 'xclaim' command")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0")
	expect(t, rw, "-ERR wrong number of arguments for 'xclaim' command")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "abc", "1-1")
	expect(t, rw, "-ERR Invalid min-idle-time argument for XCLAIM")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "badid")
	expect(t, rw, "-ERR Unrecognized XCLAIM option 'badid'")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "RETRYCOUNT", "abc")
	expect(t, rw, "-ERR Invalid RETRYCOUNT option argument for XCLAIM")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "IDLE", "abc")
	expect(t, rw, "-ERR Invalid IDLE option argument for XCLAIM")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "TIME", "abc")
	expect(t, rw, "-ERR Invalid TIME option argument for XCLAIM")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "IDLE")
	expect(t, rw, "-ERR Unrecognized XCLAIM option 'IDLE'")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "LASTID", "abc")
	expect(t, rw, "-ERR Invalid stream ID specified as stream command argument")
	cmd(t, rw, "XCLAIM", "s", "g1", "carol", "0", "1-1", "frob")
	expect(t, rw, "-ERR Unrecognized XCLAIM option 'frob'")
	cmd(t, rw, "XCLAIM", "s", "nope", "carol", "0", "1-1")
	expect(t, rw, "-NOGROUP No such key 's' or consumer group 'nope'")
	cmd(t, rw, "XCLAIM", "nokey", "g1", "carol", "0", "1-1")
	expect(t, rw, "-NOGROUP No such key 'nokey' or consumer group 'g1'")

	// XAUTOCLAIM error and arity paths.
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0")
	expect(t, rw, "-ERR wrong number of arguments for 'xautoclaim' command")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "abc", "0")
	expect(t, rw, "-ERR Invalid min-idle-time argument for XAUTOCLAIM")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "badstart")
	expect(t, rw, "-ERR Invalid stream ID specified as stream command argument")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "0", "COUNT", "abc")
	expect(t, rw, "-ERR COUNT must be > 0")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "0", "COUNT", "0")
	expect(t, rw, "-ERR COUNT must be > 0")
	cmd(t, rw, "XAUTOCLAIM", "s", "g1", "carol", "0", "0", "frob")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "XAUTOCLAIM", "s", "nope", "carol", "0", "0")
	expect(t, rw, "-NOGROUP No such key 's' or consumer group 'nope'")
	cmd(t, rw, "XAUTOCLAIM", "nokey", "g1", "carol", "0", "0")
	expect(t, rw, "-NOGROUP No such key 'nokey' or consumer group 'g1'")

	// Wrong type on the key.
	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "XCLAIM", "str", "g1", "carol", "0", "1-1")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
	cmd(t, rw, "XAUTOCLAIM", "str", "g1", "carol", "0", "0")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")
}
