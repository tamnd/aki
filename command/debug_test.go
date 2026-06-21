package command

import (
	"strings"
	"testing"
)

func TestDebugObject(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k hello")
	header := sendLine(t, r, c, "DEBUG OBJECT k")
	body := readBulk(t, r, header)
	for _, want := range []string{"refcount:1", "encoding:embstr", "serializedlength:5", "type:string"} {
		if !strings.Contains(body, want) {
			t.Fatalf("DEBUG OBJECT missing %q in %q", want, body)
		}
	}
}

func TestDebugObjectMissing(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "DEBUG OBJECT nope")
	if !strings.HasPrefix(got, "-ERR no such key") {
		t.Fatalf("DEBUG OBJECT missing = %q", got)
	}
}

func TestDebugSleep(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "DEBUG SLEEP 0"); got != "+OK" {
		t.Fatalf("DEBUG SLEEP 0 = %q", got)
	}
	if got := sendLine(t, r, c, "DEBUG SLEEP notafloat"); !strings.HasPrefix(got, "-ERR invalid value") {
		t.Fatalf("DEBUG SLEEP bad = %q", got)
	}
}

func TestDebugStringmatchLen(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "DEBUG STRINGMATCH-LEN h*llo hello"); got != ":1" {
		t.Fatalf("match = %q", got)
	}
	if got := sendLine(t, r, c, "DEBUG STRINGMATCH-LEN h*llo world"); got != ":0" {
		t.Fatalf("no match = %q", got)
	}
	if got := sendLine(t, r, c, "DEBUG STRINGMATCH-LEN HELLO hello nocase"); got != ":1" {
		t.Fatalf("nocase = %q", got)
	}
}

func TestDebugNoOps(t *testing.T) {
	r, c := start(t, Config{})
	for _, cmd := range []string{
		"DEBUG SET-ACTIVE-EXPIRE 0",
		"DEBUG SET-ACTIVE-EXPIRE 1",
		"DEBUG QUICKLIST-PACKED-THRESHOLD 100",
		"DEBUG CHANGE-REPL-ID",
		"DEBUG JMAP",
	} {
		if got := sendLine(t, r, c, cmd); got != "+OK" {
			t.Fatalf("%q = %q", cmd, got)
		}
	}
}

// TestDebugReloadEmpty checks DEBUG RELOAD returns OK on an empty keyspace.
func TestDebugReloadEmpty(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
}

func TestDebugFlushAll(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "DEBUG FLUSHALL"); got != "+OK" {
		t.Fatalf("DEBUG FLUSHALL = %q", got)
	}
	if got := sendLine(t, r, c, "DBSIZE"); got != ":0" {
		t.Fatalf("DBSIZE after flush = %q", got)
	}
}

func TestDebugLoadAOF(t *testing.T) {
	r, c := start(t, Config{})
	got := sendLine(t, r, c, "DEBUG LOADAOF")
	if !strings.HasPrefix(got, "-ERR AOF not enabled") {
		t.Fatalf("DEBUG LOADAOF = %q", got)
	}
}

func TestDebugRejected(t *testing.T) {
	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "DEBUG SEGFAULT"); !strings.HasPrefix(got, "-ERR DEBUG SEGFAULT is disabled") {
		t.Fatalf("DEBUG SEGFAULT = %q", got)
	}
	if got := sendLine(t, r, c, "DEBUG AOFSTATS"); !strings.HasPrefix(got, "-ERR not supported") {
		t.Fatalf("DEBUG AOFSTATS = %q", got)
	}
	if got := sendLine(t, r, c, "DEBUG NOPE"); !strings.HasPrefix(got, "-ERR Unknown DEBUG option") {
		t.Fatalf("DEBUG NOPE = %q", got)
	}
}
