package sqlo1

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// TestStreamGroupCodec round-trips a populated group record and rejects
// every non-canonical mutation, the same decode-or-die posture as the
// run codec.
func TestStreamGroupCodec(t *testing.T) {
	g := streamGroup{
		name: []byte("workers"),
		last: streamID{ms: 7, seq: 3},
		read: 42,
		cons: []streamConsumer{
			{name: []byte("alice"), seenMs: 1000, activeMs: 900, pel: 5},
			{name: []byte("bob"), seenMs: 2000, activeMs: -1, pel: 0},
			{name: []byte(""), seenMs: 0, activeMs: 0, pel: 1},
		},
	}
	enc := appendStreamGroup(nil, &g)
	dec, err := decodeStreamGroup(enc, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(dec.name) != "workers" || dec.last != g.last || dec.read != 42 || len(dec.cons) != 3 {
		t.Fatalf("round trip lost fields: %+v", dec)
	}
	for i := range g.cons {
		if string(dec.cons[i].name) != string(g.cons[i].name) ||
			dec.cons[i].seenMs != g.cons[i].seenMs ||
			dec.cons[i].activeMs != g.cons[i].activeMs ||
			dec.cons[i].pel != g.cons[i].pel {
			t.Fatalf("consumer %d round trip: %+v vs %+v", i, dec.cons[i], g.cons[i])
		}
	}
	if re := appendStreamGroup(nil, &dec); string(re) != string(enc) {
		t.Fatalf("re-encode differs: %d vs %d bytes", len(re), len(enc))
	}

	// A read of -1 is the unknown counter and stays legal.
	g2 := streamGroup{name: []byte("g"), read: -1}
	enc2 := appendStreamGroup(nil, &g2)
	if dec2, err := decodeStreamGroup(enc2, nil); err != nil || dec2.read != -1 {
		t.Fatalf("read -1 round trip: read=%d err=%v", dec2.read, err)
	}

	mutate := func(name string, f func(v []byte) []byte) {
		v := append([]byte(nil), enc...)
		v = f(v)
		if _, err := decodeStreamGroup(v, nil); err == nil {
			t.Fatalf("%s decoded clean", name)
		}
	}
	mutate("short header", func(v []byte) []byte { return v[:streamGroupHdrLen-1] })
	mutate("trailing byte", func(v []byte) []byte { return append(v, 0) })
	mutate("truncated consumer", func(v []byte) []byte { return v[:len(v)-1] })
	mutate("read raw -2", func(v []byte) []byte {
		for i := 16; i < 24; i++ {
			v[i] = 0xff
		}
		v[16] = 0xfe
		return v
	})
	mutate("pel fence entries", func(v []byte) []byte { v[26] = 1; return v })
	mutate("consumer count over the payload", func(v []byte) []byte { v[24]++; return v })
	mutate("negative seen", func(v []byte) []byte {
		bad := streamGroup{name: []byte("g"), cons: []streamConsumer{{name: []byte("c"), seenMs: 0, activeMs: -1}}}
		out := appendStreamGroup(v[:0], &bad)
		// seen_ms sits right after the consumer's name length and byte.
		off := streamGroupHdrLen + 4 + 1 + 4 + 1
		for i := range 8 {
			out[off+i] = 0xff
		}
		out[off] = 0xfe
		return out
	})
	mutate("active below -1", func(v []byte) []byte {
		bad := streamGroup{name: []byte("g"), cons: []streamConsumer{{name: []byte("c"), activeMs: -2}}}
		return appendStreamGroup(v[:0], &bad)
	})
	mutate("duplicate consumer", func(v []byte) []byte {
		bad := streamGroup{name: []byte("g"), cons: []streamConsumer{
			{name: []byte("c"), activeMs: -1}, {name: []byte("c"), activeMs: -1},
		}}
		return appendStreamGroup(v[:0], &bad)
	})
}

// groupSnap is a copied-out GroupsInfo row for the oracle assertions,
// since the emitted record dies with its read.
type groupSnap struct {
	name    string
	last    streamID
	read    int64
	pending uint64
	lag     int64
	lagOK   bool
	cons    []string
}

func (r *streamRig) groupSnaps(key string) []groupSnap {
	r.t.Helper()
	var out []groupSnap
	err := r.x.GroupsInfo(context.Background(), []byte(key), func(int) {}, func(g *streamGroup, pending uint64, lag int64, lagOK bool) {
		gs := groupSnap{name: string(g.name), last: g.last, read: g.read, pending: pending, lag: lag, lagOK: lagOK}
		for i := range g.cons {
			gs.cons = append(gs.cons, string(g.cons[i].name))
		}
		out = append(out, gs)
	})
	if err != nil {
		r.t.Fatalf("GroupsInfo(%q): %v", key, err)
	}
	return out
}

// TestStreamGroupOracle drives the group layer through create, setid,
// consumer, and destroy sequences, auditing the persisted rows and the
// destroy compaction against the dense-ordinal design.
func TestStreamGroupOracle(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	x := r.x
	key := []byte("s")

	if err := x.GroupCreate(ctx, key, []byte("g1"), true, streamID{ms: 5}, false, false, -1); !errors.Is(err, errXgroupNoKey) {
		t.Fatalf("create without MKSTREAM on a missing key: %v", err)
	}
	if err := x.GroupCreate(ctx, key, []byte("g1"), true, streamID{}, true, true, -1); err != nil {
		t.Fatalf("MKSTREAM create: %v", err)
	}
	if n, err := x.Len(ctx, key); err != nil || n != 0 {
		t.Fatalf("MKSTREAM stream length = %d err=%v", n, err)
	}
	if gs := r.groupSnaps("s"); len(gs) != 1 || gs[0].name != "g1" || gs[0].last != (streamID{}) || gs[0].read != -1 || !gs[0].lagOK || gs[0].lag != 0 {
		t.Fatalf("fresh MKSTREAM group: %+v", gs)
	}

	// The count 0 root takes the empty-fence append path on the first
	// XADD, and the rig audit proves the whole persisted shape.
	r.add("s", xidExplicit, streamID{ms: 1, seq: 1}, "f", "v")
	r.add("s", xidExplicit, streamID{ms: 2, seq: 2}, "f", "v")

	if err := x.GroupCreate(ctx, key, []byte("g1"), true, streamID{}, false, false, -1); !errors.Is(err, errStreamBusyGroup) {
		t.Fatalf("duplicate create: %v", err)
	}
	if err := x.GroupCreate(ctx, key, []byte("g2"), true, streamID{}, true, false, -1); err != nil {
		t.Fatalf("create g2 at $: %v", err)
	}
	if err := x.GroupCreate(ctx, key, []byte("g3"), true, streamID{ms: 1, seq: 1}, false, false, 100); err != nil {
		t.Fatalf("create g3: %v", err)
	}
	gs := r.groupSnaps("s")
	if len(gs) != 3 || gs[0].name != "g1" || gs[1].name != "g2" || gs[2].name != "g3" {
		t.Fatalf("three groups in name order, got %+v", gs)
	}
	if gs[1].last != (streamID{ms: 2, seq: 2}) {
		t.Fatalf("g2's $ resolved to %v, want the stream last", gs[1].last)
	}
	if gs[2].read != 2 {
		t.Fatalf("g3's ENTRIESREAD 100 stored as %d, want the entries-added clamp 2", gs[2].read)
	}

	if err := x.GroupSetID(ctx, key, []byte("nog"), true, streamID{}, false, -1); !errors.Is(err, errStreamNoGroup) {
		t.Fatalf("setid on a missing group: %v", err)
	}
	if err := x.GroupSetID(ctx, []byte("nk"), []byte("g1"), true, streamID{}, false, -1); !errors.Is(err, errXgroupNoKey) {
		t.Fatalf("setid on a missing key: %v", err)
	}
	if err := x.GroupSetID(ctx, key, []byte("g3"), true, streamID{ms: 1, seq: 1}, false, -1); err != nil {
		t.Fatalf("setid g3: %v", err)
	}
	if gs := r.groupSnaps("s"); gs[2].read != -1 || gs[2].last != (streamID{ms: 1, seq: 1}) {
		t.Fatalf("setid left g3 at %+v, want the counter reset", gs[2])
	}

	// Consumers land in stored order and read back sorted.
	for _, want := range []struct {
		name    string
		created bool
	}{{"b", true}, {"a", true}, {"b", false}} {
		created, err := x.GroupCreateConsumer(ctx, key, []byte("g2"), []byte(want.name), r.nowMs)
		if err != nil || created != want.created {
			t.Fatalf("createconsumer %q = %v err=%v, want %v", want.name, created, err, want.created)
		}
	}
	var consNames []string
	var consSeen, consActive []int64
	err := x.ConsumersInfo(ctx, key, []byte("g2"), func(int) {}, func(c *streamConsumer) {
		consNames = append(consNames, string(c.name))
		consSeen = append(consSeen, c.seenMs)
		consActive = append(consActive, c.activeMs)
	})
	if err != nil {
		t.Fatalf("ConsumersInfo: %v", err)
	}
	if len(consNames) != 2 || consNames[0] != "a" || consNames[1] != "b" {
		t.Fatalf("consumers = %v, want name order", consNames)
	}
	if consSeen[0] != r.nowMs || consActive[0] != -1 {
		t.Fatalf("fresh consumer times seen=%d active=%d", consSeen[0], consActive[0])
	}
	if err := x.ConsumersInfo(ctx, key, []byte("nog"), func(int) {}, func(*streamConsumer) {}); !errors.Is(err, errStreamNoGroup) {
		t.Fatalf("ConsumersInfo on a missing group: %v", err)
	}

	if pending, err := x.GroupDelConsumer(ctx, key, []byte("g2"), []byte("a")); err != nil || pending != 0 {
		t.Fatalf("delconsumer a = %d err=%v", pending, err)
	}
	if pending, err := x.GroupDelConsumer(ctx, key, []byte("g2"), []byte("zz")); err != nil || pending != 0 {
		t.Fatalf("delconsumer of a missing consumer = %d err=%v", pending, err)
	}
	if gs := r.groupSnaps("s"); len(gs[1].cons) != 1 || gs[1].cons[0] != "b" {
		t.Fatalf("g2 consumers after the delete: %v", gs[1].cons)
	}

	// Destroying ordinal 0 moves the tail record (g3) into the hole and
	// drops the vacated tail subkey.
	destroyed, err := x.GroupDestroy(ctx, key, []byte("g1"))
	if err != nil || !destroyed {
		t.Fatalf("destroy g1 = %v err=%v", destroyed, err)
	}
	gs = r.groupSnaps("s")
	if len(gs) != 2 || gs[0].name != "g2" || gs[1].name != "g3" {
		t.Fatalf("groups after the destroy: %+v", gs)
	}
	if gs[1].last != (streamID{ms: 1, seq: 1}) || gs[1].read != -1 {
		t.Fatalf("the moved g3 lost fields: %+v", gs[1])
	}
	if exists, _, err := x.stateOf(ctx, key); err != nil || !exists {
		t.Fatalf("stateOf after destroy: exists=%v err=%v", exists, err)
	}
	var kb [SubkeySize]byte
	putStreamGroupKey(kb[:], x.root.rooth, 2)
	if _, ok, err := r.tr.Get(ctx, kb[:]); err != nil || ok {
		t.Fatalf("vacated tail record still present: ok=%v err=%v", ok, err)
	}
	if destroyed, err := x.GroupDestroy(ctx, key, []byte("g1")); err != nil || destroyed {
		t.Fatalf("destroy of a missing group = %v err=%v", destroyed, err)
	}
	if _, err := x.GroupDestroy(ctx, []byte("nk"), []byte("g1")); !errors.Is(err, errXgroupNoKey) {
		t.Fatalf("destroy on a missing key: %v", err)
	}
	if err := x.GroupsInfo(ctx, []byte("nk"), func(int) {}, nil); !errors.Is(err, errStreamNoKey) {
		t.Fatalf("GroupsInfo on a missing key: %v", err)
	}
	r.check("s")
}

// TestStreamCGLag drives every branch of the lag pair against the
// live-pinned Redis 8.8 rows: the stored counter bounded by the live
// count, the tombstone poisoning, the edge estimates, and the emptied
// stream.
func TestStreamCGLag(t *testing.T) {
	r := newStreamRig(t)
	ctx := context.Background()
	key := []byte("L")
	for i := uint64(1); i <= 5; i++ {
		r.add("L", xidExplicit, streamID{ms: i, seq: i}, "f", "v")
	}
	lag := func(last streamID, read int64) (int64, bool) {
		t.Helper()
		exists, _, err := r.x.stateOf(ctx, key)
		if err != nil || !exists {
			t.Fatalf("stateOf: exists=%v err=%v", exists, err)
		}
		g := streamGroup{last: last, read: read}
		return r.x.cgLag(&g)
	}
	expect := func(name string, last streamID, read, wantLag int64, wantOK bool) {
		t.Helper()
		if got, ok := lag(last, read); got != wantLag || ok != wantOK {
			t.Fatalf("%s: lag(%v, read %d) = %d/%v, want %d/%v", name, last, read, got, ok, wantLag, wantOK)
		}
	}

	// No tombstones: first 1-1, last 5-5, count 5, added 5.
	expect("counter at the last ID", streamID{ms: 5, seq: 5}, 4, 1, true)
	expect("counter mid-stream", streamID{ms: 3, seq: 3}, 2, 3, true)
	expect("counter beats the position estimate", streamID{ms: 1, seq: 1}, 4, 1, true)
	expect("unknown counter below the first", streamID{seq: 5}, -1, 5, true)
	expect("unknown counter on the first", streamID{ms: 1, seq: 1}, -1, 4, true)
	expect("unknown counter mid-stream", streamID{ms: 3, seq: 3}, -1, 0, false)
	expect("unknown counter at the last", streamID{ms: 5, seq: 5}, -1, 0, true)
	expect("unknown counter past the last", streamID{ms: 6}, -1, 0, true)

	// Plant a tombstone at 2-2 via the maxdel root field, inside the
	// live range: at or past the group's position it poisons both the
	// counter and the estimate; strictly below the position the counter
	// stays exact.
	if err := r.x.SetID(ctx, key, streamID{ms: 5, seq: 5}, false, 0, true, streamID{ms: 2, seq: 2}); err != nil {
		t.Fatalf("setid maxdel: %v", err)
	}
	expect("tombstone below the position", streamID{ms: 3, seq: 3}, 2, 3, true)
	expect("tombstone at the position", streamID{ms: 2, seq: 2}, 2, 0, false)
	expect("tombstone past an early position", streamID{ms: 1, seq: 5}, 2, 0, false)
	expect("tombstone kills the below-first estimate", streamID{seq: 5}, -1, 0, false)
	expect("past the last beats the tombstone", streamID{ms: 6}, -1, 0, true)

	// Trim to two entries: first 4-4, count 2, added 5, and the 2-2
	// tombstone now sits below the live range so the counter is valid
	// again, bounded by the live count.
	if _, err := r.x.Trim(ctx, key, false, 2, streamID{}, false, -1); err != nil {
		t.Fatalf("trim: %v", err)
	}
	r.model = r.model[3:]
	r.check("L")
	expect("counter clamps to the live count", streamID{}, 1, 2, true)
	expect("tombstone behind the trim is harmless", streamID{ms: 2, seq: 2}, 2, 2, true)

	// Empty the stream: everything added counts as read, whatever the
	// counter says.
	if _, err := r.x.Trim(ctx, key, false, 0, streamID{}, false, -1); err != nil {
		t.Fatalf("trim to zero: %v", err)
	}
	r.model = r.model[:0]
	r.check("L")
	expect("emptied stream with a counter", streamID{ms: 3, seq: 3}, 2, 0, true)
	expect("emptied stream without a counter", streamID{ms: 3, seq: 3}, -1, 0, true)

	// A never-appended MKSTREAM stream answers 0 through the added==0
	// arm.
	if err := r.x.GroupCreate(ctx, []byte("E"), []byte("g"), true, streamID{}, true, true, -1); err != nil {
		t.Fatalf("MKSTREAM create: %v", err)
	}
	if exists, _, err := r.x.stateOf(ctx, []byte("E")); err != nil || !exists {
		t.Fatalf("stateOf E: %v", err)
	}
	g := streamGroup{last: streamID{}, read: -1}
	if got, ok := r.x.cgLag(&g); got != 0 || !ok {
		t.Fatalf("empty-stream lag = %d/%v", got, ok)
	}
}

// TestXgroupWire pins the XGROUP dispatch against Redis 8.8's observed
// replies: the per-subcommand arity texts, the option scan outranking
// the key checks, the key checks outranking the ID parse, CREATE's ID
// parse outranking BUSYGROUP against SETID's NOGROUP outranking the ID
// parse, and the HELP block verbatim.
func TestXgroupWire(t *testing.T) {
	do, _ := dispatchServer(t)
	do("SET", "plain", "v")
	do("XADD", "ts", "5-5", "f", "v")
	do("XADD", "ts", "7-7", "f", "v")

	noKey := "-ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.\r\n"
	table := []struct {
		args []string
		want string
	}{
		{[]string{"XGROUP"}, "-ERR wrong number of arguments for 'xgroup' command\r\n"},
		{[]string{"XGROUP", "CREATE"}, "-ERR wrong number of arguments for 'xgroup|create' command\r\n"},
		{[]string{"XGROUP", "CREATE", "ts", "g"}, "-ERR wrong number of arguments for 'xgroup|create' command\r\n"},
		{[]string{"XGROUP", "SETID", "ts", "g"}, "-ERR wrong number of arguments for 'xgroup|setid' command\r\n"},
		{[]string{"XGROUP", "DESTROY", "ts"}, "-ERR wrong number of arguments for 'xgroup|destroy' command\r\n"},
		{[]string{"XGROUP", "DESTROY", "ts", "g", "x"}, "-ERR wrong number of arguments for 'xgroup|destroy' command\r\n"},
		{[]string{"XGROUP", "CREATECONSUMER", "ts", "g"}, "-ERR wrong number of arguments for 'xgroup|createconsumer' command\r\n"},
		{[]string{"XGROUP", "DELCONSUMER", "ts", "g", "c", "x"}, "-ERR wrong number of arguments for 'xgroup|delconsumer' command\r\n"},
		{[]string{"XGROUP", "HELP", "x"}, "-ERR wrong number of arguments for 'xgroup|help' command\r\n"},
		{[]string{"XGROUP", "BOGUS"}, "-ERR unknown subcommand 'BOGUS'. Try XGROUP HELP.\r\n"},

		// The option scan's value errors outrank the key checks, and a
		// bad or dangling token echoes the subcommand as typed.
		{[]string{"XGROUP", "CREATE", "nosuch", "g", "1-1", "ENTRIESREAD", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XGROUP", "CREATE", "nosuch", "g", "1-1", "ENTRIESREAD", "-5"}, "-ERR value for ENTRIESREAD must be positive or -1\r\n"},
		{[]string{"XGROUP", "CREATE", "nosuch", "g", "1-1", "ENTRIESREAD"}, "-ERR unknown subcommand or wrong number of arguments for 'CREATE'. Try XGROUP HELP.\r\n"},
		{[]string{"XGROUP", "create", "nosuch", "g", "1-1", "BOGUSOPT"}, "-ERR unknown subcommand or wrong number of arguments for 'create'. Try XGROUP HELP.\r\n"},
		{[]string{"XGROUP", "SETID", "nosuch", "g", "1-1", "MKSTREAM"}, "-ERR unknown subcommand or wrong number of arguments for 'SETID'. Try XGROUP HELP.\r\n"},

		// The key checks outrank the ID parse for every subcommand;
		// WRONGTYPE outranks the missing-key text.
		{[]string{"XGROUP", "CREATE", "nosuch", "g", "abc"}, noKey},
		{[]string{"XGROUP", "SETID", "nosuch", "g", "abc"}, noKey},
		{[]string{"XGROUP", "DESTROY", "nosuch", "g"}, noKey},
		{[]string{"XGROUP", "CREATECONSUMER", "nosuch", "g", "c"}, noKey},
		{[]string{"XGROUP", "DELCONSUMER", "nosuch", "g", "c"}, noKey},
		{[]string{"XGROUP", "CREATE", "plain", "g", "abc"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XGROUP", "SETID", "plain", "g", "1-1"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XGROUP", "CREATE", "nosuch", "g", "abc", "MKSTREAM"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},

		// CREATE parses the ID before the group check; SETID looks the
		// group up first.
		{[]string{"XGROUP", "CREATE", "ts", "g1", "$"}, "+OK\r\n"},
		{[]string{"XGROUP", "CREATE", "ts", "g1", "0"}, "-BUSYGROUP Consumer Group name already exists\r\n"},
		{[]string{"XGROUP", "CREATE", "ts", "g1", "abc"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XGROUP", "SETID", "ts", "nog", "abc"}, "-NOGROUP No such consumer group 'nog' for key name 'ts'\r\n"},
		{[]string{"XGROUP", "SETID", "ts", "g1", "abc"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},

		// Consumer and destroy counts.
		{[]string{"XGROUP", "CREATECONSUMER", "ts", "g1", "c1"}, ":1\r\n"},
		{[]string{"XGROUP", "CREATECONSUMER", "ts", "g1", "c1"}, ":0\r\n"},
		{[]string{"XGROUP", "CREATECONSUMER", "ts", "nog", "c1"}, "-NOGROUP No such consumer group 'nog' for key name 'ts'\r\n"},
		{[]string{"XGROUP", "DELCONSUMER", "ts", "g1", "zz"}, ":0\r\n"},
		{[]string{"XGROUP", "DELCONSUMER", "ts", "g1", "c1"}, ":0\r\n"},
		{[]string{"XGROUP", "DESTROY", "ts", "nog"}, ":0\r\n"},
		{[]string{"XGROUP", "DESTROY", "ts", "g1"}, ":1\r\n"},
	}
	for _, row := range table {
		if got := do(row.args...); got != row.want {
			t.Fatalf("%v = %q, want %q", row.args, got, row.want)
		}
	}

	help := do("XGROUP", "HELP")
	if !strings.HasPrefix(help, "*17\r\n") {
		t.Fatalf("HELP header: %q", help)
	}
	for _, line := range []string{
		"+XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n",
		"+CREATE <key> <groupname> <id|$> [option]\r\n",
		"+    * MKSTREAM\r\n",
		"+      Set the group's entries_read counter (internal use).\r\n",
		"+SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]\r\n",
		"+    Print this help.\r\n",
	} {
		if !strings.Contains(help, line) {
			t.Fatalf("HELP misses %q in %q", line, help)
		}
	}
}

// TestXinfoGroupsWire pins XINFO GROUPS, CONSUMERS, the summary's
// groups counter, and the FULL groups array against the live Redis 8.8
// rows, including the entries-read clamp, the SETID counter reset, and
// the consumer clocks.
func TestXinfoGroupsWire(t *testing.T) {
	do, clockp := dispatchServer(t)
	b := func(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
	num := func(n int) string { return ":" + strconv.Itoa(n) + "\r\n" }

	do("SET", "plain", "v")
	do("XADD", "ts", "5-5", "f", "v")
	do("XADD", "ts", "7-7", "f", "v")

	if got, want := do("XINFO", "GROUPS", "nosuch"), "-ERR no such key\r\n"; got != want {
		t.Fatalf("GROUPS on a missing key = %q", got)
	}
	if got, want := do("XINFO", "CONSUMERS", "nosuch", "g"), "-ERR no such key\r\n"; got != want {
		t.Fatalf("CONSUMERS on a missing key = %q", got)
	}
	if got, want := do("XINFO", "GROUPS", "plain"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; got != want {
		t.Fatalf("GROUPS on a wrongtype key = %q", got)
	}
	if got, want := do("XINFO", "CONSUMERS", "ts", "nog"), "-NOGROUP No such consumer group 'nog' for key name 'ts'\r\n"; got != want {
		t.Fatalf("CONSUMERS on a missing group = %q", got)
	}

	// One group at $ with no counter: entries-read nil, lag 0.
	do("XGROUP", "CREATE", "ts", "gb", "$")
	want := "*1\r\n*12\r\n" +
		b("name") + b("gb") +
		b("consumers") + num(0) +
		b("pending") + num(0) +
		b("last-delivered-id") + b("7-7") +
		b("entries-read") + "$-1\r\n" +
		b("lag") + num(0)
	if got := do("XINFO", "GROUPS", "ts"); got != want {
		t.Fatalf("GROUPS = %q, want %q", got, want)
	}

	// The live lagck pin: ENTRIESREAD 100 clamps to entries-added 2 and
	// the counter path answers lag 0. Rows come back in name order.
	do("XGROUP", "CREATE", "ts", "ga", "5-5", "ENTRIESREAD", "100")
	want = "*2\r\n*12\r\n" +
		b("name") + b("ga") +
		b("consumers") + num(0) +
		b("pending") + num(0) +
		b("last-delivered-id") + b("5-5") +
		b("entries-read") + num(2) +
		b("lag") + num(0) +
		"*12\r\n" +
		b("name") + b("gb") +
		b("consumers") + num(0) +
		b("pending") + num(0) +
		b("last-delivered-id") + b("7-7") +
		b("entries-read") + "$-1\r\n" +
		b("lag") + num(0)
	if got := do("XINFO", "GROUPS", "ts"); got != want {
		t.Fatalf("GROUPS after ga = %q, want %q", got, want)
	}

	// SETID without ENTRIESREAD resets the counter; the estimate
	// answers count-1 on the first entry.
	do("XGROUP", "SETID", "ts", "ga", "5-5")
	if got := do("XINFO", "GROUPS", "ts"); !strings.Contains(got, b("entries-read")+"$-1\r\n"+b("lag")+num(1)) {
		t.Fatalf("GROUPS after the SETID reset = %q", got)
	}

	// The summary's groups counter follows the records.
	if got := do("XINFO", "STREAM", "ts"); !strings.Contains(got, b("groups")+num(2)) {
		t.Fatalf("summary groups counter in %q", got)
	}

	// Consumer clocks: idle runs from seen, inactive is -1 until a
	// delivery.
	do("XGROUP", "CREATECONSUMER", "ts", "ga", "beta")
	do("XGROUP", "CREATECONSUMER", "ts", "ga", "alpha")
	*clockp += 500
	want = "*2\r\n*8\r\n" +
		b("name") + b("alpha") +
		b("pending") + num(0) +
		b("idle") + num(500) +
		b("inactive") + ":-1\r\n" +
		"*8\r\n" +
		b("name") + b("beta") +
		b("pending") + num(0) +
		b("idle") + num(500) +
		b("inactive") + ":-1\r\n"
	if got := do("XINFO", "CONSUMERS", "ts", "ga"); got != want {
		t.Fatalf("CONSUMERS = %q, want %q", got, want)
	}

	// The FULL groups array: eight pairs per group with the empty PEL
	// shapes, consumers nested in name order.
	got := do("XINFO", "STREAM", "ts", "FULL")
	wantGroup := b("groups") + "*2\r\n*16\r\n" +
		b("name") + b("ga") +
		b("last-delivered-id") + b("5-5") +
		b("entries-read") + "$-1\r\n" +
		b("lag") + num(1) +
		b("pel-count") + num(0) +
		b("nacked-count") + num(0) +
		b("pending") + "*0\r\n" +
		b("consumers") + "*2\r\n" +
		"*10\r\n" +
		b("name") + b("alpha") +
		b("seen-time") + num(1_000_000) +
		b("active-time") + ":-1\r\n" +
		b("pel-count") + num(0) +
		b("pending") + "*0\r\n" +
		"*10\r\n" +
		b("name") + b("beta") +
		b("seen-time") + num(1_000_000) +
		b("active-time") + ":-1\r\n" +
		b("pel-count") + num(0) +
		b("pending") + "*0\r\n"
	if !strings.Contains(got, wantGroup) {
		t.Fatalf("FULL groups block missing in %q, want %q", got, wantGroup)
	}
}
