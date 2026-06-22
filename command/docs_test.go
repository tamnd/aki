package command

import (
	"testing"
)

// flatMapAny turns a RESP2 flat map array (key, value, key, value, ...) into a Go
// map with any values, so nested arrays and maps survive. COMMAND DOCS replies as
// a map, which a RESP2 client receives as this flat array.
func flatMapAny(t *testing.T, v any) map[string]any {
	t.Helper()
	a := asArray(t, v)
	if len(a)%2 != 0 {
		t.Fatalf("flat map has odd length %d", len(a))
	}
	m := make(map[string]any, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		k, ok := a[i].(string)
		if !ok {
			t.Fatalf("map key at %d is not a string: %T", i, a[i])
		}
		m[k] = a[i+1]
	}
	return m
}

// TestCommandDocsGet checks the doc map for a single command carries summary,
// since, group, complexity, and arguments.
func TestCommandDocsGet(t *testing.T) {
	r, c := startData(t)
	top := flatMapAny(t, sendReply(t, r, c, "COMMAND DOCS get"))
	doc, ok := top["get"]
	if !ok {
		t.Fatalf("DOCS get has no get entry: %v", top)
	}
	m := flatMapAny(t, doc)
	if m["summary"] != "Returns the string value of a key." {
		t.Fatalf("summary = %v", m["summary"])
	}
	if m["since"] != "1.0.0" {
		t.Fatalf("since = %v", m["since"])
	}
	if m["group"] != "string" {
		t.Fatalf("group = %v", m["group"])
	}
	if m["complexity"] != "O(1)" {
		t.Fatalf("complexity = %v", m["complexity"])
	}
	args := asArray(t, m["arguments"])
	if len(args) != 1 {
		t.Fatalf("get has %d arguments want 1", len(args))
	}
	arg := flatMapAny(t, args[0])
	if arg["name"] != "key" || arg["type"] != "key" {
		t.Fatalf("arg = %v", arg)
	}
}

// TestCommandDocsSetArguments checks the structured argument tree for SET, which
// has nested oneof groups with tokens.
func TestCommandDocsSetArguments(t *testing.T) {
	r, c := startData(t)
	top := flatMapAny(t, sendReply(t, r, c, "COMMAND DOCS set"))
	m := flatMapAny(t, top["set"])
	args := asArray(t, m["arguments"])
	if len(args) != 5 {
		t.Fatalf("set has %d arguments want 5", len(args))
	}
	// The third argument is the NX/XX condition, a oneof with two pure-token args.
	cond := flatMapAny(t, args[2])
	if cond["type"] != "oneof" {
		t.Fatalf("condition type = %v", cond["type"])
	}
	inner := asArray(t, cond["arguments"])
	if len(inner) != 2 {
		t.Fatalf("condition has %d sub-args want 2", len(inner))
	}
	nx := flatMapAny(t, inner[0])
	if nx["type"] != "pure-token" || nx["token"] != "NX" {
		t.Fatalf("nx arg = %v", nx)
	}
}

// TestCommandDocsContainerSubcommands checks a container command reports its
// subcommands as a nested map.
func TestCommandDocsContainerSubcommands(t *testing.T) {
	r, c := startData(t)
	top := flatMapAny(t, sendReply(t, r, c, "COMMAND DOCS config"))
	m := flatMapAny(t, top["config"])
	subs := flatMapAny(t, m["subcommands"])
	if _, ok := subs["config|get"]; !ok {
		t.Fatalf("config docs missing config|get subcommand: %v", subs)
	}
}

// TestCommandDocsAll checks DOCS with no arguments returns an entry for every
// command, including ones without an overlay summary.
func TestCommandDocsAll(t *testing.T) {
	r, c := startData(t)
	top := flatMapAny(t, sendReply(t, r, c, "COMMAND DOCS"))
	if _, ok := top["get"]; !ok {
		t.Fatalf("DOCS all missing get")
	}
	if _, ok := top["set"]; !ok {
		t.Fatalf("DOCS all missing set")
	}
	// A command without an overlay entry still answers with a derived summary.
	doc, ok := top["object"]
	if !ok {
		t.Fatalf("DOCS all missing object")
	}
	m := flatMapAny(t, doc)
	if _, ok := m["summary"].(string); !ok {
		t.Fatalf("object has no summary: %v", m)
	}
}

// TestCommandDocsUnknown checks an unknown command name is left out of the reply
// rather than answered with a null.
func TestCommandDocsUnknown(t *testing.T) {
	r, c := startData(t)
	top := flatMapAny(t, sendReply(t, r, c, "COMMAND DOCS nosuchcmd"))
	if len(top) != 0 {
		t.Fatalf("DOCS for unknown command should be empty, got %v", top)
	}
}
