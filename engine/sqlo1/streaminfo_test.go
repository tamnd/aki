package sqlo1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// TestStreamSetIDOracle drives the engine half of the root surface:
// the top-item wall, the entries-added floor, the max-deleted-ID
// apply, the emptied stream's freedom, and Info reading the maintained
// fields back.
func TestStreamSetIDOracle(t *testing.T) {
	rig := newStreamRig(t)
	ctx := context.Background()
	for ms := uint64(1); ms <= 5; ms++ {
		rig.addQuick("k", ms, "f", fmt.Sprintf("v%d", ms))
	}
	rig.check("k")

	// Info answers off the root, no counting.
	info, err := rig.x.Info(ctx, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	sr := rig.streamRootOf("k")
	if info.count != 5 || info.added != 5 || info.last != sr.last || info.maxDel != (streamID{}) || info.groups != 0 {
		t.Fatalf("info = %+v", info)
	}
	if info.geom != int64(len(sr.fence)) {
		t.Fatalf("geom = %d, fence holds %d runs", info.geom, len(sr.fence))
	}

	// The missing key is the surface's one lookup error.
	if _, err := rig.x.Info(ctx, []byte("nosuch")); !errors.Is(err, errStreamNoKey) {
		t.Fatalf("Info(nosuch) = %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("nosuch"), streamID{ms: 9}, false, 0, false, streamID{}); !errors.Is(err, errStreamNoKey) {
		t.Fatalf("SetID(nosuch) = %v", err)
	}

	// The wall is the top entry actually in the stream: below fails,
	// equal passes, and lowering back down to the top after a raise
	// passes too.
	top := streamID{ms: 5, seq: 1}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 5}, false, 0, false, streamID{}); !errors.Is(err, errXsetidTop) {
		t.Fatalf("SetID below top = %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 100}, false, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID raise: %v", err)
	}
	if last := rig.streamRootOf("k").last; last != (streamID{ms: 100}) {
		t.Fatalf("last = %v after raise", last)
	}
	if err := rig.x.SetID(ctx, []byte("k"), top, false, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID lower to top: %v", err)
	}
	if last := rig.streamRootOf("k").last; last != top {
		t.Fatalf("last = %v after lowering to top", last)
	}
	rig.check("k")

	// The entries-added floor is the live count.
	if err := rig.x.SetID(ctx, []byte("k"), top, true, 4, false, streamID{}); !errors.Is(err, errXsetidAdded) {
		t.Fatalf("SetID added below count = %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("k"), top, true, 9, false, streamID{}); err != nil {
		t.Fatalf("SetID added: %v", err)
	}
	if added := rig.streamRootOf("k").added; added != 9 {
		t.Fatalf("added = %d, want 9", added)
	}

	// max-deleted-ID applies alone.
	if err := rig.x.SetID(ctx, []byte("k"), top, false, 0, true, streamID{ms: 3, seq: 3}); err != nil {
		t.Fatalf("SetID maxdel: %v", err)
	}
	if md := rig.streamRootOf("k").maxDel; md != (streamID{ms: 3, seq: 3}) {
		t.Fatalf("maxDel = %v", md)
	}
	rig.check("k")

	// Appends resume above the moved last ID and the audit stays green.
	rig.addQuick("k", 200, "f", "tail")
	rig.check("k")

	// A trim to empty frees the wall: any ID goes, downward included,
	// and ENTRIESADDED 0 is legal against the zero count.
	rig.trim("k", false, 0, streamID{}, false, 0)
	if err := rig.x.SetID(ctx, []byte("k"), streamID{}, false, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID 0-0 on empty: %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 1, seq: 5}, true, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID on empty: %v", err)
	}
	sr = rig.streamRootOf("k")
	if sr.count != 0 || sr.last != (streamID{ms: 1, seq: 5}) || sr.added != 0 {
		t.Fatalf("emptied root = count %d last %v added %d", sr.count, sr.last, sr.added)
	}
	rig.addQuick("k", 7, "f", "fresh")
	rig.check("k")
}

// TestStreamSetIDPagedTop drives the top-item wall over the paged
// fence at dialed caps, the one record read XSETID makes, and Info's
// geometry flip from runs to pages.
func TestStreamSetIDPagedTop(t *testing.T) {
	defer SetStreamFenceCapsForTest(3, 2, 8)()
	rig := newStreamRig(t)
	ctx := context.Background()
	big := strings.Repeat("x", 1500)
	for ms := uint64(1); ms <= 30; ms++ {
		rig.addQuick("k", ms, "f", big)
	}
	rig.check("k")
	sr := rig.streamRootOf("k")
	if !sr.paged {
		t.Fatal("scenario did not page")
	}
	info, err := rig.x.Info(ctx, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if info.geom != int64(len(sr.pidx)) {
		t.Fatalf("paged geom = %d, index holds %d pages", info.geom, len(sr.pidx))
	}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 30}, false, 0, false, streamID{}); !errors.Is(err, errXsetidTop) {
		t.Fatalf("SetID below paged top = %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 30, seq: 1}, false, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID equal to paged top: %v", err)
	}
	if err := rig.x.SetID(ctx, []byte("k"), streamID{ms: 40}, false, 0, false, streamID{}); err != nil {
		t.Fatalf("SetID above paged top: %v", err)
	}
	if last := rig.streamRootOf("k").last; last != (streamID{ms: 40}) {
		t.Fatalf("last = %v", last)
	}
}

// TestXsetidXinfoWire pins the root surface's wire behavior against
// Redis 8.8's observed replies: the XSETID error table with its
// validation order, the XINFO subcommand dispatch with its two error
// families, the sixteen-pair summary with the 8.8 idempotent-producer
// block, and FULL's COUNT window.
func TestXsetidXinfoWire(t *testing.T) {
	do, _ := dispatchServer(t)
	b := func(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
	num := func(n int) string { return ":" + strconv.Itoa(n) + "\r\n" }

	do("SET", "plain", "v")
	do("XADD", "ts", "5-5", "f", "v")
	do("XADD", "ts", "7-7", "f", "v")

	// The XSETID table: the wall is the top existing entry, equal
	// allowed; the maxdel-vs-new-id check is argument level and beats
	// the missing key and WRONGTYPE alike; parse failures beat the
	// missing key; the entries-added floor is post-lookup so no-such-key
	// wins there.
	table := []struct {
		args []string
		want string
	}{
		{[]string{"XSETID"}, "-ERR wrong number of arguments for 'xsetid' command\r\n"},
		{[]string{"XSETID", "ts"}, "-ERR wrong number of arguments for 'xsetid' command\r\n"},
		{[]string{"XSETID", "ts", "6-0"}, "-ERR The ID specified in XSETID is smaller than the target stream top item\r\n"},
		{[]string{"XSETID", "ts", "7-7"}, "+OK\r\n"},
		{[]string{"XSETID", "ts", "abc"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XSETID", "ts", "5-*"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XSETID", "ts", "100", "ENTRIESADDED", "1"}, "-ERR The entries_added specified in XSETID is smaller than the target stream length\r\n"},
		{[]string{"XSETID", "ts", "100", "ENTRIESADDED", "-1"}, "-ERR entries_added must be positive\r\n"},
		{[]string{"XSETID", "ts", "100", "ENTRIESADDED", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XSETID", "ts", "100", "ENTRIESADDED"}, "-ERR syntax error\r\n"},
		{[]string{"XSETID", "ts", "100", "BOGUS", "1"}, "-ERR syntax error\r\n"},
		{[]string{"XSETID", "ts", "100", "MAXDELETEDID", "101"}, "-ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id\r\n"},
		{[]string{"XSETID", "nosuch", "5", "MAXDELETEDID", "6"}, "-ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id\r\n"},
		{[]string{"XSETID", "plain", "5", "MAXDELETEDID", "6"}, "-ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id\r\n"},
		{[]string{"XSETID", "nosuch", "abc"}, "-ERR Invalid stream ID specified as stream command argument\r\n"},
		{[]string{"XSETID", "nosuch", "5", "ENTRIESADDED", "-1"}, "-ERR entries_added must be positive\r\n"},
		{[]string{"XSETID", "nosuch", "5", "ENTRIESADDED", "0"}, "-ERR no such key\r\n"},
		{[]string{"XSETID", "nosuch", "5"}, "-ERR no such key\r\n"},
		{[]string{"XSETID", "plain", "5"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XSETID", "ts", "100", "MAXDELETEDID", "100"}, "+OK\r\n"},
		{[]string{"XSETID", "ts", "300", "ENTRIESADDED", "50", "ENTRIESADDED", "60"}, "+OK\r\n"},
	}
	for _, row := range table {
		if got := do(row.args...); got != row.want {
			t.Fatalf("%v = %q, want %q", row.args, got, row.want)
		}
	}

	// The XSETID applies read back through the summary: the duplicate
	// ENTRIESADDED's last value won and the bare ms parsed as ms-0.
	got := do("XINFO", "STREAM", "ts")
	for _, want := range []string{
		b("last-generated-id") + b("300-0"),
		b("entries-added") + num(60),
		b("max-deleted-entry-id") + b("100-0"),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary %q misses %q", got, want)
		}
	}

	// The XINFO dispatch table: known subcommands with wrong counts
	// answer the container arity errors, a malformed STREAM tail echoes
	// the subcommand token as typed, and an unknown subcommand echoes
	// its own token.
	xinfoTable := []struct {
		args []string
		want string
	}{
		{[]string{"XINFO"}, "-ERR wrong number of arguments for 'xinfo' command\r\n"},
		{[]string{"XINFO", "STREAM"}, "-ERR wrong number of arguments for 'xinfo|stream' command\r\n"},
		{[]string{"XINFO", "GROUPS"}, "-ERR wrong number of arguments for 'xinfo|groups' command\r\n"},
		{[]string{"XINFO", "GROUPS", "ts", "x"}, "-ERR wrong number of arguments for 'xinfo|groups' command\r\n"},
		{[]string{"XINFO", "CONSUMERS", "ts"}, "-ERR wrong number of arguments for 'xinfo|consumers' command\r\n"},
		{[]string{"XINFO", "stream", "ts", "foo"}, "-ERR unknown subcommand or wrong number of arguments for 'stream'. Try XINFO HELP.\r\n"},
		{[]string{"XINFO", "STREAM", "ts", "FULL", "extra"}, "-ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.\r\n"},
		{[]string{"XINFO", "STREAM", "ts", "FULL", "COUNT"}, "-ERR unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.\r\n"},
		{[]string{"XINFO", "STREAM", "ts", "FULL", "COUNT", "abc"}, "-ERR value is not an integer or out of range\r\n"},
		{[]string{"XINFO", "bogus"}, "-ERR unknown subcommand 'bogus'. Try XINFO HELP.\r\n"},
		{[]string{"XINFO", "STREAM", "nosuch"}, "-ERR no such key\r\n"},
		{[]string{"XINFO", "STREAM", "plain"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XINFO", "GROUPS", "ts"}, "*0\r\n"},
		{[]string{"XINFO", "GROUPS", "nosuch"}, "-ERR no such key\r\n"},
		{[]string{"XINFO", "GROUPS", "plain"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
		{[]string{"XINFO", "CONSUMERS", "ts", "g"}, "-NOGROUP No such consumer group 'g' for key name 'ts'\r\n"},
		{[]string{"XINFO", "CONSUMERS", "nosuch", "g"}, "-ERR no such key\r\n"},
	}
	for _, row := range xinfoTable {
		if got := do(row.args...); got != row.want {
			t.Fatalf("%v = %q, want %q", row.args, got, row.want)
		}
	}

	// HELP, nine simple strings, Redis's own unbalanced bracket in the
	// STREAM line verbatim.
	help := "*9\r\n" +
		"+XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:\r\n" +
		"+CONSUMERS <key> <groupname>\r\n" +
		"+    Show consumers of <groupname>.\r\n" +
		"+GROUPS <key>\r\n" +
		"+    Show the stream consumer groups.\r\n" +
		"+STREAM <key> [FULL [COUNT <count>]\r\n" +
		"+    Show information about the stream.\r\n" +
		"+HELP\r\n" +
		"+    Print this help.\r\n"
	if got := do("XINFO", "HELP"); got != help {
		t.Fatalf("HELP = %q", got)
	}

	// The summary, byte exact on a one-entry stream: sixteen pairs in
	// 8.8's order with the static idempotent-producer block and the
	// synthesized radix-tree geometry.
	do("XADD", "sum", "5-5", "f", "v")
	entry := "*2\r\n" + b("5-5") + "*2\r\n" + b("f") + b("v")
	summary := "*32\r\n" +
		b("length") + num(1) +
		b("radix-tree-keys") + num(1) +
		b("radix-tree-nodes") + num(2) +
		b("last-generated-id") + b("5-5") +
		b("max-deleted-entry-id") + b("0-0") +
		b("entries-added") + num(1) +
		b("recorded-first-entry-id") + b("5-5") +
		b("idmp-duration") + num(100) +
		b("idmp-maxsize") + num(100) +
		b("pids-tracked") + num(0) +
		b("iids-tracked") + num(0) +
		b("iids-added") + num(0) +
		b("iids-duplicates") + num(0) +
		b("groups") + num(0) +
		b("first-entry") + entry +
		b("last-entry") + entry
	if got := do("XINFO", "STREAM", "sum"); got != summary {
		t.Fatalf("summary = %q, want %q", got, summary)
	}

	// FULL on the same stream: fifteen pairs, the entries array in
	// place of the peeks and the still-empty groups array.
	fullReply := "*30\r\n" +
		b("length") + num(1) +
		b("radix-tree-keys") + num(1) +
		b("radix-tree-nodes") + num(2) +
		b("last-generated-id") + b("5-5") +
		b("max-deleted-entry-id") + b("0-0") +
		b("entries-added") + num(1) +
		b("recorded-first-entry-id") + b("5-5") +
		b("idmp-duration") + num(100) +
		b("idmp-maxsize") + num(100) +
		b("pids-tracked") + num(0) +
		b("iids-tracked") + num(0) +
		b("iids-added") + num(0) +
		b("iids-duplicates") + num(0) +
		b("entries") + "*1\r\n" + entry +
		b("groups") + "*0\r\n"
	if got := do("XINFO", "STREAM", "sum", "FULL"); got != fullReply {
		t.Fatalf("FULL = %q, want %q", got, fullReply)
	}

	// The emptied stream: null bulk peeks, recorded-first-entry-id
	// 0-0, geometry 0 keys 1 node, entries-added intact, and FULL's
	// entries array empty rather than nil.
	do("XADD", "es", "5-5", "f", "v")
	do("XTRIM", "es", "MAXLEN", "0")
	if got := do("XSETID", "es", "3-3"); got != "+OK\r\n" {
		t.Fatalf("XSETID on emptied stream = %q", got)
	}
	empty := "*32\r\n" +
		b("length") + num(0) +
		b("radix-tree-keys") + num(0) +
		b("radix-tree-nodes") + num(1) +
		b("last-generated-id") + b("3-3") +
		b("max-deleted-entry-id") + b("0-0") +
		b("entries-added") + num(1) +
		b("recorded-first-entry-id") + b("0-0") +
		b("idmp-duration") + num(100) +
		b("idmp-maxsize") + num(100) +
		b("pids-tracked") + num(0) +
		b("iids-tracked") + num(0) +
		b("iids-added") + num(0) +
		b("iids-duplicates") + num(0) +
		b("groups") + num(0) +
		b("first-entry") + "$-1\r\n" +
		b("last-entry") + "$-1\r\n"
	if got := do("XINFO", "STREAM", "es"); got != empty {
		t.Fatalf("empty summary = %q, want %q", got, empty)
	}
	if got := do("XINFO", "STREAM", "es", "FULL"); !strings.HasSuffix(got, b("entries")+"*0\r\n"+b("groups")+"*0\r\n") {
		t.Fatalf("empty FULL tail = %q", got)
	}

	// FULL's COUNT window on a twelve-entry stream: default 10, 0
	// unbounded, negatives folded to the default, an explicit small
	// count honored. Each entry carries exactly one "f" field bulk.
	for ms := 1; ms <= 12; ms++ {
		do("XADD", "fc", fmt.Sprintf("%d-1", ms), "f", "v")
	}
	countRows := []struct {
		args []string
		want int
	}{
		{[]string{"XINFO", "STREAM", "fc", "FULL"}, 10},
		{[]string{"XINFO", "STREAM", "fc", "FULL", "COUNT", "0"}, 12},
		{[]string{"XINFO", "STREAM", "fc", "FULL", "COUNT", "-5"}, 10},
		{[]string{"XINFO", "STREAM", "fc", "FULL", "COUNT", "3"}, 3},
	}
	for _, row := range countRows {
		got := do(row.args...)
		if n := strings.Count(got, b("f")); n != row.want {
			t.Fatalf("%v window holds %d entries, want %d", row.args, n, row.want)
		}
	}
}
