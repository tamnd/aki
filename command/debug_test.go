package command

import (
	"os"
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
	} {
		if got := sendLine(t, r, c, cmd); got != "+OK" {
			t.Fatalf("%q = %q", cmd, got)
		}
	}
}

// TestDebugChangeReplID checks the replid actually rolls and the old one is
// preserved as master_replid2, the way a promotion records it.
func TestDebugChangeReplID(t *testing.T) {
	r, c := start(t, Config{})
	before := infoField(t, r, c, "replication", "master_replid")
	if got := sendLine(t, r, c, "DEBUG CHANGE-REPL-ID"); got != "+OK" {
		t.Fatalf("DEBUG CHANGE-REPL-ID = %q", got)
	}
	after := infoField(t, r, c, "replication", "master_replid")
	if after == before {
		t.Fatalf("master_replid did not change: still %q", after)
	}
	if second := infoField(t, r, c, "replication", "master_replid2"); second != before {
		t.Fatalf("master_replid2 = %q want old replid %q", second, before)
	}
}

// TestDebugJMAP runs DEBUG JMAP in a temp working directory and checks it drops
// a heap profile file there.
func TestDebugJMAP(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	r, c := start(t, Config{})
	if got := sendLine(t, r, c, "DEBUG JMAP"); got != "+OK" {
		t.Fatalf("DEBUG JMAP = %q", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "aki-heap-") && strings.HasSuffix(e.Name(), ".prof") {
			found = true
		}
	}
	if !found {
		t.Fatalf("DEBUG JMAP wrote no heap profile, dir has %v", entries)
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
