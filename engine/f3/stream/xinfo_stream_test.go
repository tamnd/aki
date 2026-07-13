package stream

import (
	"strconv"
	"testing"
)

// The XINFO STREAM / STREAM FULL / CONSUMERS suite (spec 2064/f3/14 sections 6.4
// and 7.3). STREAM reads the header and the two end entries; FULL adds a
// COUNT-bounded entry window and every group with a bounded PEL and consumer
// sample; CONSUMERS reports each consumer's pending count and its idle/inactive
// clocks. The clocks are wall-clock derived (NowMs), so the tests assert the sign
// and the -1 never-active sentinel rather than exact millisecond values.

// flatMap folds a RESP2 flat map (an even-length array of alternating key/value)
// into a Go map. Integer and bulk values decode to strings, nested arrays to []any,
// a null to nil, matching decodeReply.
func flatMap(t *testing.T, v any) map[string]any {
	t.Helper()
	kv, ok := v.([]any)
	if !ok || len(kv)%2 != 0 {
		t.Fatalf("value = %v, want an even flat map", render(v))
	}
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			t.Fatalf("map key = %v, want a string", render(kv[i]))
		}
		m[k] = kv[i+1]
	}
	return m
}

// streamInfo decodes an XINFO STREAM (or STREAM FULL) reply into its top-level map.
func streamInfo(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	return flatMap(t, decodeReply(t, raw))
}

// atoi parses a decoded integer field (which arrives as a string) for a numeric
// assertion.
func atoi(t *testing.T, v any) int64 {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("field = %v, want a numeric string", render(v))
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("field %q not an integer: %v", s, err)
	}
	return n
}

func TestXinfoStreamSummary(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v1-0")
	do(t, c, opXadd, "s", "2-0", "f", "v2-0")
	do(t, c, opXadd, "s", "3-0", "f", "v3-0")
	do(t, c, opXgroup, "CREATE", "s", "g", "0")

	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s"))
	wantField(t, m, "length", "3")
	wantField(t, m, "last-generated-id", "3-0")
	wantField(t, m, "max-deleted-entry-id", "0-0")
	wantField(t, m, "entries-added", "3")
	wantField(t, m, "recorded-first-entry-id", "1-0")
	wantField(t, m, "groups", "1")

	// radix-tree-nodes is synthesized one above the key count, monotone and plausible.
	if keys, nodes := atoi(t, m["radix-tree-keys"]), atoi(t, m["radix-tree-nodes"]); nodes != keys+1 {
		t.Fatalf("radix-tree keys=%d nodes=%d, want nodes=keys+1", keys, nodes)
	}

	first := m["first-entry"].([]any)
	if first[0].(string) != "1-0" {
		t.Fatalf("first-entry id = %v, want 1-0", render(first[0]))
	}
	last := m["last-entry"].([]any)
	if last[0].(string) != "3-0" {
		t.Fatalf("last-entry id = %v, want 3-0", render(last[0]))
	}
	// The entry payload is the [field value ...] flat array.
	fv := first[1].([]any)
	if len(fv) != 2 || fv[0].(string) != "f" || fv[1].(string) != "v1-0" {
		t.Fatalf("first-entry fields = %v, want [f v1-0]", render(first[1]))
	}
}

func TestXinfoStreamEmpty(t *testing.T) {
	c := newHarness(t).NewConn()
	// MKSTREAM makes an empty native stream: zero entries, null end peeks.
	do(t, c, opXgroup, "CREATE", "s", "g", "$", "MKSTREAM")

	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s"))
	wantField(t, m, "length", "0")
	wantField(t, m, "recorded-first-entry-id", "0-0")
	wantField(t, m, "groups", "1")
	if m["first-entry"] != nil || m["last-entry"] != nil {
		t.Fatalf("empty stream end peeks = %v / %v, want nil / nil", render(m["first-entry"]), render(m["last-entry"]))
	}
}

func TestXinfoStreamAfterDelete(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v1-0")
	do(t, c, opXadd, "s", "2-0", "f", "v2-0")
	do(t, c, opXadd, "s", "3-0", "f", "v3-0")
	// Delete the front entry: length drops, recorded-first advances, max-deleted set,
	// last-generated-id never moves back.
	do(t, c, opXdel, "s", "1-0")

	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s"))
	wantField(t, m, "length", "2")
	wantField(t, m, "last-generated-id", "3-0")
	wantField(t, m, "max-deleted-entry-id", "1-0")
	wantField(t, m, "recorded-first-entry-id", "2-0")
	first := m["first-entry"].([]any)
	if first[0].(string) != "2-0" {
		t.Fatalf("first-entry after delete = %v, want 2-0", render(first[0]))
	}
}

func TestXinfoStreamMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXinfo, "STREAM", "nope"), errNoSuchKey)
}

func TestXinfoStreamWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXinfo, "STREAM", "k"), wrongType)
}

func TestXinfoStreamBadOpts(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// A stray token, FULL with a trailing non-COUNT word, and a non-integer COUNT are
	// all the shared XINFO error naming the STREAM subcommand.
	wantErr(t, do(t, c, opXinfo, "STREAM", "s", "BOGUS"), unknownXinfo([]byte("STREAM")))
	wantErr(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "NOPE"), unknownXinfo([]byte("STREAM")))
	wantErr(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "COUNT"), unknownXinfo([]byte("STREAM")))
	wantErr(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "COUNT", "abc"), unknownXinfo([]byte("STREAM")))
}

func TestXinfoStreamFullNoDelivery(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2) // group g created, nothing delivered, so its PEL is nil
	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL"))
	g := flatMap(t, m["groups"].([]any)[0])
	wantField(t, g, "pel-count", "0")
	wantField(t, g, "nacked-count", "0")
	if n := len(g["pending"].([]any)); n != 0 {
		t.Fatalf("group pending = %d, want 0", n)
	}
	if n := len(g["consumers"].([]any)); n != 0 {
		t.Fatalf("consumers = %d, want 0", n)
	}
}

func TestXinfoStreamFull(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 3) // entries 1-0..3-0, group g at head
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL"))
	wantField(t, m, "length", "3")

	// The FULL entry window is present and holds every entry.
	entries := m["entries"].([]any)
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}

	groups := m["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	g := flatMap(t, groups[0])
	wantField(t, g, "name", "g")
	wantField(t, g, "pel-count", "3")
	wantField(t, g, "nacked-count", "0")

	// Group PEL sample: three rows [id, owner, delivery-time, delivery-count].
	gpel := g["pending"].([]any)
	if len(gpel) != 3 {
		t.Fatalf("group pending = %d, want 3", len(gpel))
	}
	row0 := gpel[0].([]any)
	if row0[0].(string) != "1-0" || row0[1].(string) != "c1" || row0[3].(string) != "1" {
		t.Fatalf("group pending row0 = %v, want [1-0 c1 _ 1]", render(gpel[0]))
	}

	cons := g["consumers"].([]any)
	if len(cons) != 1 {
		t.Fatalf("consumers = %d, want 1", len(cons))
	}
	con := flatMap(t, cons[0])
	wantField(t, con, "name", "c1")
	wantField(t, con, "pel-count", "3")
	// seen-time is an absolute wall-clock ms; active-time was stamped by the fetch.
	if atoi(t, con["seen-time"]) <= 0 {
		t.Fatalf("seen-time = %v, want a positive absolute ms", render(con["seen-time"]))
	}
	if atoi(t, con["active-time"]) <= 0 {
		t.Fatalf("active-time = %v, want a positive absolute ms after a fetch", render(con["active-time"]))
	}
	// Consumer PEL sample: three rows [id, delivery-time, delivery-count].
	cpel := con["pending"].([]any)
	if len(cpel) != 3 {
		t.Fatalf("consumer pending = %d, want 3", len(cpel))
	}
	if r := cpel[0].([]any); len(r) != 3 || r[0].(string) != "1-0" || r[2].(string) != "1" {
		t.Fatalf("consumer pending row0 = %v, want [1-0 _ 1]", render(cpel[0]))
	}
}

func TestXinfoStreamFullNackedCount(t *testing.T) {
	c := nackSetup(t, 3) // 3 delivered to c1
	// Hand one back to the group: it becomes an unowned NACK.
	wantInt(t, do(t, c, opXnack, "s", "g", "FAIL", "IDS", "1", "2-0"), 1)

	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL"))
	g := flatMap(t, m["groups"].([]any)[0])
	wantField(t, g, "pel-count", "3")
	wantField(t, g, "nacked-count", "1")

	// The nacked row renders an empty owner name; the group still lists it.
	var unowned int
	for _, row := range g["pending"].([]any) {
		r := row.([]any)
		if r[1].(string) == "" {
			unowned++
			if r[0].(string) != "2-0" {
				t.Fatalf("unowned row id = %v, want 2-0", render(r[0]))
			}
		}
	}
	if unowned != 1 {
		t.Fatalf("unowned rows = %d, want 1", unowned)
	}
	// c1 now owns two; its own sample drops the disowned entry.
	con := flatMap(t, g["consumers"].([]any)[0])
	wantField(t, con, "pel-count", "2")
	if n := len(con["pending"].([]any)); n != 2 {
		t.Fatalf("consumer pending = %d, want 2", n)
	}
}

func TestXinfoStreamFullCountBounds(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 5)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")

	// COUNT 2 caps the entry window and every PEL sample at two rows.
	m := streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "COUNT", "2"))
	if n := len(m["entries"].([]any)); n != 2 {
		t.Fatalf("entries under COUNT 2 = %d, want 2", n)
	}
	g := flatMap(t, m["groups"].([]any)[0])
	// pel-count is the true total even when the sample is capped.
	wantField(t, g, "pel-count", "5")
	if n := len(g["pending"].([]any)); n != 2 {
		t.Fatalf("group pending under COUNT 2 = %d, want 2", n)
	}
	con := flatMap(t, g["consumers"].([]any)[0])
	if n := len(con["pending"].([]any)); n != 2 {
		t.Fatalf("consumer pending under COUNT 2 = %d, want 2", n)
	}

	// COUNT 0 is unbounded: the full window and samples.
	m = streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "COUNT", "0"))
	if n := len(m["entries"].([]any)); n != 5 {
		t.Fatalf("entries under COUNT 0 = %d, want 5", n)
	}
	g = flatMap(t, m["groups"].([]any)[0])
	if n := len(g["pending"].([]any)); n != 5 {
		t.Fatalf("group pending under COUNT 0 = %d, want 5", n)
	}

	// A negative COUNT folds to the default 10, so all five entries fit the window.
	m = streamInfo(t, do(t, c, opXinfo, "STREAM", "s", "FULL", "COUNT", "-1"))
	if n := len(m["entries"].([]any)); n != 5 {
		t.Fatalf("entries under COUNT -1 = %d, want 5 (default 10 cap)", n)
	}
}

func TestXinfoConsumers(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 2)
	do(t, c, opXreadgroup, "GROUP", "g", "c1", "STREAMS", "s", ">")
	// c2 exists but has never fetched: created explicitly.
	do(t, c, opXgroup, "CREATECONSUMER", "s", "g", "c2")

	cons := groupInfos(t, do(t, c, opXinfo, "CONSUMERS", "s", "g"))
	if len(cons) != 2 {
		t.Fatalf("consumers = %d, want 2", len(cons))
	}
	// Name order: c1 then c2.
	wantField(t, cons[0], "name", "c1")
	wantField(t, cons[1], "name", "c2")
	// c1 fetched two entries and is active.
	wantField(t, cons[0], "pending", "2")
	if atoi(t, cons[0]["idle"]) < 0 {
		t.Fatalf("c1 idle = %v, want >= 0", render(cons[0]["idle"]))
	}
	if atoi(t, cons[0]["inactive"]) < 0 {
		t.Fatalf("c1 inactive = %v, want >= 0 after a fetch", render(cons[0]["inactive"]))
	}
	// c2 has never fetched: inactive is the -1 never-active sentinel.
	wantField(t, cons[1], "pending", "0")
	wantField(t, cons[1], "inactive", "-1")
	if atoi(t, cons[1]["idle"]) < 0 {
		t.Fatalf("c2 idle = %v, want >= 0", render(cons[1]["idle"]))
	}
}

func TestXinfoConsumersUnknownGroup(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXinfo, "CONSUMERS", "s", "missing"),
		nogroup([]byte("missing"), []byte("s")))
}

func TestXinfoConsumersMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXinfo, "CONSUMERS", "nope", "g"), errNoSuchKey)
}

func TestXinfoConsumersWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXinfo, "CONSUMERS", "k", "g"), wrongType)
}

func TestXinfoConsumersEmpty(t *testing.T) {
	c := newHarness(t).NewConn()
	seedGroup(t, c, 1) // group g with no consumers yet
	got := decodeReply(t, do(t, c, opXinfo, "CONSUMERS", "s", "g"))
	if arr, ok := got.([]any); !ok || len(arr) != 0 {
		t.Fatalf("reply = %v, want an empty array", render(got))
	}
}
