package stream

import (
	"testing"
)

// The XGROUP suite (spec 2064/f3/14 sections 7.8 and 7.2): the consumer-group
// lifecycle, CREATE and SETID moving the cursor, DESTROY dropping a group,
// CREATECONSUMER and DELCONSUMER managing consumer records, plus the XINFO GROUPS
// read that makes the group state observable. The delivery ledger the PEL fields
// report against fills with XREADGROUP (slice 6), so pending is 0 throughout here.

// groupInfos decodes an XINFO GROUPS reply into one field map per group. Integer
// and bulk fields both decode to strings, an absent (nil) field to nil, so a
// caller compares against the string form or checks for nil.
func groupInfos(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	got := decodeReply(t, raw)
	rows, ok := got.([]any)
	if !ok {
		t.Fatalf("reply = %v, want an array of groups", render(got))
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		kv, ok := row.([]any)
		if !ok || len(kv)%2 != 0 {
			t.Fatalf("group %d = %v, want an even flat map", i, render(row))
		}
		m := make(map[string]any, len(kv)/2)
		for j := 0; j+1 < len(kv); j += 2 {
			k, ok := kv[j].(string)
			if !ok {
				t.Fatalf("group %d field %d name = %v, want a string", i, j, render(kv[j]))
			}
			m[k] = kv[j+1]
		}
		out[i] = m
	}
	return out
}

func wantField(t *testing.T, m map[string]any, key string, want any) {
	t.Helper()
	if got := m[key]; got != want {
		t.Fatalf("field %q = %v, want %v", key, render(got), render(want))
	}
}

func TestXgroupCreateAtZero(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXadd, "s", "2-0", "f", "v")
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "0"), "OK")
	gs := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))
	if len(gs) != 1 {
		t.Fatalf("got %d groups, want 1", len(gs))
	}
	m := gs[0]
	wantField(t, m, "name", "g")
	wantField(t, m, "consumers", "0")
	wantField(t, m, "pending", "0")
	wantField(t, m, "last-delivered-id", "0-0")
	// Created at the head: nothing read, so lag is every entry added.
	wantField(t, m, "entries-read", "0")
	wantField(t, m, "lag", "2")
}

func TestXgroupCreateAtDollar(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXadd, "s", "2-0", "f", "v")
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "$"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	// Created at the tail: caught up, so entries-read equals entries-added and lag
	// is zero, and the cursor sits at the last ID.
	wantField(t, m, "last-delivered-id", "2-0")
	wantField(t, m, "entries-read", "2")
	wantField(t, m, "lag", "0")
}

func TestXgroupCreateExplicitPastTail(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// An explicit ID at or past the tail is caught up, the same as "$".
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "9-0"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "last-delivered-id", "9-0")
	wantField(t, m, "entries-read", "1")
	wantField(t, m, "lag", "0")
}

func TestXgroupCreateMidStreamUnknownLag(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	// An explicit ID inside the history has an entries-read the directory-priced
	// cursor of slice 6 must resolve, so entries-read and lag report nil for now.
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "2-0"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "last-delivered-id", "2-0")
	wantField(t, m, "entries-read", nil)
	wantField(t, m, "lag", nil)
}

func TestXgroupCreateEntriesRead(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	// An explicit ENTRIESREAD seeds the lag basis even for a mid-stream start, so
	// lag becomes entries-added minus the given count.
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "2-0", "ENTRIESREAD", "2"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "entries-read", "2")
	wantField(t, m, "lag", "1")
}

func TestXgroupCreateMkstream(t *testing.T) {
	c := newHarness(t).NewConn()
	// MKSTREAM creates an empty native stream and its group in one call.
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "$", "MKSTREAM"), "OK")
	wantInt(t, do(t, c, opXlen, "s"), 0)
	wantBulk(t, do(t, c, opObject, "ENCODING", "s"), "stream")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "name", "g")
	wantField(t, m, "last-delivered-id", "0-0")
	// A brand-new stream added nothing, so a caught-up group has zero lag.
	wantField(t, m, "entries-read", "0")
	wantField(t, m, "lag", "0")
}

func TestXgroupCreateMissingKeyNoMkstream(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g", "$"), errXgroupKey)
}

func TestXgroupCreateBusyGroup(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantStatus(t, do(t, c, opXgroup, "CREATE", "s", "g", "0"), "OK")
	// A second CREATE of the same name is BUSYGROUP, and it does not disturb the
	// first: its cursor stays where CREATE at 0 put it.
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g", "$"), busyGroup)
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "last-delivered-id", "0-0")
}

func TestXgroupCreateWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXgroup, "CREATE", "k", "g", "$"), wrongType)
}

func TestXgroupCreateBadID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g", "not-an-id"), errInvalidID)
}

func TestXgroupCreateSyntax(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// A stray trailing token is a syntax error.
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g", "0", "junk"), "ERR syntax error")
	// A non-integer ENTRIESREAD is the integer error.
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g", "0", "ENTRIESREAD", "x"),
		"ERR value is not an integer or out of range")
	// A short CREATE misses its ID and is the unknown-subcommand-or-arity error.
	wantErr(t, do(t, c, opXgroup, "CREATE", "s", "g"),
		"ERR Unknown XGROUP subcommand or wrong number of arguments for 'CREATE'")
}

func TestXgroupSetID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXadd, "s", "2-0", "f", "v")
	do(t, c, opXgroup, "CREATE", "s", "g", "0")
	// SETID to "$" catches the group up: cursor at the tail, lag zero.
	wantStatus(t, do(t, c, opXgroup, "SETID", "s", "g", "$"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "last-delivered-id", "2-0")
	wantField(t, m, "entries-read", "2")
	wantField(t, m, "lag", "0")
}

func TestXgroupSetIDEntriesRead(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, id := range []string{"1-0", "2-0", "3-0"} {
		do(t, c, opXadd, "s", id, "f", "v")
	}
	do(t, c, opXgroup, "CREATE", "s", "g", "$")
	// SETID back to the head with an explicit ENTRIESREAD 0 resets the lag basis.
	wantStatus(t, do(t, c, opXgroup, "SETID", "s", "g", "0", "ENTRIESREAD", "0"), "OK")
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "last-delivered-id", "0-0")
	wantField(t, m, "entries-read", "0")
	wantField(t, m, "lag", "3")
}

func TestXgroupSetIDBadID(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXgroup, "CREATE", "s", "g", "0")
	wantErr(t, do(t, c, opXgroup, "SETID", "s", "g", "not-an-id"), errInvalidID)
	// A stray SETID option token is a syntax error, and MKSTREAM is a CREATE-only
	// token so it is stray here too.
	wantErr(t, do(t, c, opXgroup, "SETID", "s", "g", "0", "MKSTREAM"), "ERR syntax error")
}

// TestXgroupWrongTypeSubcommands drives every group subcommand at a string key so
// each reports WRONGTYPE before touching a group, through both the CREATE path
// and the shared resolveGroup path.
func TestXgroupWrongTypeSubcommands(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXgroup, "SETID", "k", "g", "$"), wrongType)
	wantErr(t, do(t, c, opXgroup, "DESTROY", "k", "g"), wrongType)
	wantErr(t, do(t, c, opXgroup, "CREATECONSUMER", "k", "g", "c"), wrongType)
	wantErr(t, do(t, c, opXgroup, "DELCONSUMER", "k", "g", "c"), wrongType)
}

func TestXgroupSetIDNoGroup(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXgroup, "SETID", "s", "g", "$"),
		"NOGROUP No such consumer group 'g' for key name 's'")
}

func TestXgroupSetIDMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXgroup, "SETID", "s", "g", "$"), errXgroupKey)
}

func TestXgroupDestroy(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXgroup, "CREATE", "s", "g", "0")
	wantInt(t, do(t, c, opXgroup, "DESTROY", "s", "g"), 1)
	// The group is gone from the introspection view.
	if gs := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s")); len(gs) != 0 {
		t.Fatalf("got %d groups after destroy, want 0", len(gs))
	}
	// A second destroy removes nothing.
	wantInt(t, do(t, c, opXgroup, "DESTROY", "s", "g"), 0)
}

func TestXgroupDestroyMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXgroup, "DESTROY", "s", "g"), errXgroupKey)
}

func TestXgroupConsumers(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXgroup, "CREATE", "s", "g", "0")
	// A new consumer reports created, a repeat reports already present.
	wantInt(t, do(t, c, opXgroup, "CREATECONSUMER", "s", "g", "c1"), 1)
	wantInt(t, do(t, c, opXgroup, "CREATECONSUMER", "s", "g", "c1"), 0)
	wantInt(t, do(t, c, opXgroup, "CREATECONSUMER", "s", "g", "c2"), 1)
	m := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "2")
	// DELCONSUMER reports the pending entries it owned (none here) and drops it.
	wantInt(t, do(t, c, opXgroup, "DELCONSUMER", "s", "g", "c1"), 0)
	m = groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))[0]
	wantField(t, m, "consumers", "1")
	// Deleting an absent consumer removes nothing.
	wantInt(t, do(t, c, opXgroup, "DELCONSUMER", "s", "g", "gone"), 0)
}

func TestXgroupConsumerNoGroup(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXgroup, "CREATECONSUMER", "s", "g", "c"),
		"NOGROUP No such consumer group 'g' for key name 's'")
	wantErr(t, do(t, c, opXgroup, "DELCONSUMER", "s", "g", "c"),
		"NOGROUP No such consumer group 'g' for key name 's'")
}

func TestXgroupUnknownSub(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXgroup, "FROBNICATE", "s", "g"),
		"ERR Unknown XGROUP subcommand or wrong number of arguments for 'FROBNICATE'")
}

func TestXinfoGroupsMissingKey(t *testing.T) {
	c := newHarness(t).NewConn()
	wantErr(t, do(t, c, opXinfo, "GROUPS", "s"), errNoSuchKey)
}

func TestXinfoGroupsEmpty(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	// A stream that exists but holds no group answers the empty array, not an error.
	if gs := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s")); len(gs) != 0 {
		t.Fatalf("got %d groups, want 0", len(gs))
	}
}

func TestXinfoGroupsWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opSet, "k", "v")
	wantErr(t, do(t, c, opXinfo, "GROUPS", "k"), wrongType)
}

func TestXinfoUnknownSub(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	wantErr(t, do(t, c, opXinfo, "STREAM", "s"),
		"ERR Unknown XINFO subcommand or wrong number of arguments for 'STREAM'")
}

func TestXgroupMultipleGroupsSorted(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opXadd, "s", "1-0", "f", "v")
	do(t, c, opXgroup, "CREATE", "s", "gb", "0")
	do(t, c, opXgroup, "CREATE", "s", "ga", "$")
	// XINFO GROUPS lists groups in name order for a deterministic reply.
	gs := groupInfos(t, do(t, c, opXinfo, "GROUPS", "s"))
	if len(gs) != 2 {
		t.Fatalf("got %d groups, want 2", len(gs))
	}
	wantField(t, gs[0], "name", "ga")
	wantField(t, gs[1], "name", "gb")
}
