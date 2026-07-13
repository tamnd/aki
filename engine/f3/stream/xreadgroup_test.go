package stream

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The XREADGROUP suite (spec 2064/f3/14 section 7.5): the `>` form delivers new
// entries and records them in the PEL, the explicit-ID form re-reads a consumer's
// own pending history, and NOACK delivers without tracking. The reply mirrors
// XREAD's [key, entries] shape, with [id, nil] for a pending entry the log has
// since dropped.

// gEntry is one decoded reply entry: its ID and its flat field list, or fields nil
// for the [id, nil] deleted-entry form.
type gEntry struct {
	id     string
	fields []string
}

// readGroupReply decodes an XREADGROUP reply into a per-key entry list. A
// null-array reply decodes to a nil map, the no-new-entries answer.
func readGroupReply(t *testing.T, raw []byte) map[string][]gEntry {
	t.Helper()
	got := decodeReply(t, raw)
	if got == nil {
		return nil
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("reply = %v, want an array of streams", render(got))
	}
	out := make(map[string][]gEntry, len(arr))
	for _, pair := range arr {
		kv := pair.([]any)
		if len(kv) != 2 {
			t.Fatalf("stream pair = %v, want [key, entries]", render(pair))
		}
		key := kv[0].(string)
		entries := kv[1].([]any)
		es := make([]gEntry, 0, len(entries))
		for _, e := range entries {
			ea := e.([]any)
			id := ea[0].(string)
			if ea[1] == nil {
				es = append(es, gEntry{id: id})
				continue
			}
			fa := ea[1].([]any)
			fs := make([]string, len(fa))
			for i := range fa {
				fs[i] = fa[i].(string)
			}
			es = append(es, gEntry{id: id, fields: fs})
		}
		out[key] = es
	}
	return out
}

// seedGroup adds n entries 1-0..n-0 and creates group g at the head.
func seedGroup(t *testing.T, c *shard.Conn, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		do(t, c, opXadd, "s", idStr(i), "f", "v"+idStr(i))
	}
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "0"), "OK")
}

func idStr(i int) string { return itoaTest(i) + "-0" }

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestXreadgroupDeliverAndPending(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "COUNT", "2", "STREAMS", "s", ">"))
	es := rep["s"]
	if len(es) != 2 || es[0].id != "1-0" || es[1].id != "2-0" {
		t.Fatalf("delivered = %v, want 1-0 and 2-0", es)
	}
	if len(es[0].fields) != 2 || es[0].fields[0] != "f" || es[0].fields[1] != "v1-0" {
		t.Fatalf("entry 1-0 fields = %v, want [f v1-0]", es[0].fields)
	}
	// The group now tracks two pending entries under one consumer, and the cursor
	// and lag have advanced past the second.
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "1")
	wantField(t, m, "pending", "2")
	wantField(t, m, "last-delivered-id", "2-0")
	wantField(t, m, "entries-read", "2")
	wantField(t, m, "lag", "1")
}

func TestXreadgroupSecondConsumerSharesCursor(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "COUNT", "2", "STREAMS", "s", ">")
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c2", "STREAMS", "s", ">"))
	es := rep["s"]
	if len(es) != 1 || es[0].id != "3-0" {
		t.Fatalf("c2 delivered = %v, want just 3-0", es)
	}
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "2")
	wantField(t, m, "pending", "3")
}

func TestXreadgroupNoAckSkipsPEL(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "NOACK", "STREAMS", "s", ">"))
	if len(rep["s"]) != 2 {
		t.Fatalf("delivered = %v, want 2 entries", rep["s"])
	}
	// Delivered but not tracked: the cursor moved, nothing is pending.
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "pending", "0")
	wantField(t, m, "last-delivered-id", "2-0")
}

func TestXreadgroupNoNewEntries(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	// The cursor is at the tail; a second `>` read finds nothing and replies nil.
	if rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")); rep != nil {
		t.Fatalf("second read = %v, want nil (null array)", rep)
	}
}

func TestXreadgroupHistoryReRead(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">") // delivers all three
	// The explicit-ID form re-reads the consumer's own pending list from 0.
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", "0"))
	es := rep["s"]
	if len(es) != 3 || es[0].id != "1-0" || es[2].id != "3-0" {
		t.Fatalf("history = %v, want 1-0..3-0", es)
	}
	// Acking one shrinks the re-read.
	do(t, c, opXack, "s", "g", "2-0")
	es = readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", "0"))["s"]
	if len(es) != 2 || es[0].id != "1-0" || es[1].id != "3-0" {
		t.Fatalf("history after ack = %v, want 1-0 and 3-0", es)
	}
}

func TestXreadgroupHistoryDeletedEntry(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	do(t, c, opXdel, "s", "1-0") // the PEL outlives the log entry
	es := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", "0"))["s"]
	if len(es) != 2 {
		t.Fatalf("history = %v, want 2 entries", es)
	}
	if es[0].id != "1-0" || es[0].fields != nil {
		t.Fatalf("entry 0 = %v, want 1-0 with nil fields", es[0])
	}
	if es[1].id != "2-0" || len(es[1].fields) != 2 {
		t.Fatalf("entry 1 = %v, want live 2-0", es[1])
	}
}

func TestXreadgroupHistoryDrainedPresent(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	// A consumer that never read still gets its stream in the history reply, an
	// empty entry list, the "you have no pending entries" answer.
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "fresh", "STREAMS", "s", "0"))
	es, ok := rep["s"]
	if !ok || len(es) != 0 {
		t.Fatalf("history = %v, want the key present with no entries", rep)
	}
}

func TestXreadgroupLazyConsumer(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "auto", "STREAMS", "s", ">")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "1")
}

func TestXreadgroupNogroupMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "nokey", ">"),
		"NOGROUP No such key 'nokey' or consumer group 'g' in XREADGROUP with GROUP option")
}

func TestXreadgroupNogroupMissingGroup(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "nope", "c", "STREAMS", "s", ">"),
		"NOGROUP No such key 's' or consumer group 'nope' in XREADGROUP with GROUP option")
}

func TestXreadgroupWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "str", "v")
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "str", ">"), wrongType)
}

func TestXreadgroupMissingGroupKeyword(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXreadgroup, "NOTGROUP", "g", "c", "STREAMS", "s", ">"),
		"Missing GROUP keyword or consumer/group name in XREADGROUP with the GROUP option")
}

func TestXreadgroupSyntaxError(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BOGUS", "STREAMS", "s", ">"), "ERR syntax error")
}

func TestXreadgroupUnbalanced(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s"),
		"ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified.")
}

func TestXreadgroupBadID(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", "notanid"), errInvalidID)
}

func TestXreadgroupBlockEmptyRepliesNull(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "s", ">")
	// The one seeded entry is delivered, so a following `>` read finds the cursor at
	// the tail and parks; a finite BLOCK times out to the null array. The blocking
	// path itself is covered in xreadgroup_block_test.go.
	if rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">")); rep != nil {
		t.Fatalf("blocked empty read = %v, want nil", rep)
	}
}

func TestXreadgroupBadOptions(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1)
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "-5", "STREAMS", "s", ">"), "ERR timeout is negative")
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "BLOCK", "abc", "STREAMS", "s", ">"), "ERR timeout is not an integer or out of range")
	wantErr(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "COUNT", "abc", "STREAMS", "s", ">"), "ERR value is not an integer or out of range")
}

func TestXreadgroupDelConsumerDrainsPending(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "COUNT", "2", "STREAMS", "s", ">")
	do(t, c, opXreadgroup, "GROUP", "g", "c2", "STREAMS", "s", ">")
	// Dropping c1 retires the two entries it owned and reports that count.
	wantInt(t, do(t, c, opXgroup, "DELCONSUMER", "s", "g", "c1"), 2)
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "1")
	wantField(t, m, "pending", "1")
	// Only c2's entry survives in the PEL.
	rows := decodeReply(t, do(t, c, opXpending, "s", "g")).([]any)
	if rows[1].(string) != "3-0" || rows[2].(string) != "3-0" {
		t.Fatalf("summary after drain = %v, want min=max=3-0", render(rows))
	}
}

func TestXreadgroupMultipleStreams(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "a", "1-0", "f", "va")
	do(t, c, opXadd, "b", "1-0", "f", "vb")
	do(t, c, opXgroup, "CREATE", "a", "g", "0")
	do(t, c, opXgroup, "CREATE", "b", "g", "0")
	rep := readGroupReply(t, do(t, c, opXreadgroup, "GROUP", "g", "c", "STREAMS", "a", "b", ">", ">"))
	if len(rep["a"]) != 1 || rep["a"][0].id != "1-0" {
		t.Fatalf("stream a = %v, want 1-0", rep["a"])
	}
	if len(rep["b"]) != 1 || rep["b"][0].id != "1-0" {
		t.Fatalf("stream b = %v, want 1-0", rep["b"])
	}
}
