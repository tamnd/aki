package command

import (
	"strings"
	"testing"
)

func TestObjectEncodingString(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET n 12345")
	if got := bulk(t, r, c, "OBJECT ENCODING n"); got != "int" {
		t.Fatalf("OBJECT ENCODING int = %q", got)
	}
	_ = sendLine(t, r, c, "SET s hello")
	if got := bulk(t, r, c, "OBJECT ENCODING s"); got != "embstr" {
		t.Fatalf("OBJECT ENCODING embstr = %q", got)
	}
	long := strings.Repeat("x", 60)
	_ = sendLine(t, r, c, "SET big "+long)
	if got := bulk(t, r, c, "OBJECT ENCODING big"); got != "raw" {
		t.Fatalf("OBJECT ENCODING raw = %q", got)
	}
}

func TestObjectEncodingAggregates(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "RPUSH l a b c")
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("OBJECT ENCODING list = %q", got)
	}
	_ = sendLine(t, r, c, "HSET h f v")
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "listpack" {
		t.Fatalf("OBJECT ENCODING hash = %q", got)
	}
	_ = sendLine(t, r, c, "SADD si 1 2 3")
	if got := bulk(t, r, c, "OBJECT ENCODING si"); got != "intset" {
		t.Fatalf("OBJECT ENCODING intset = %q", got)
	}
	_ = sendLine(t, r, c, "SADD ss a b c")
	if got := bulk(t, r, c, "OBJECT ENCODING ss"); got != "listpack" {
		t.Fatalf("OBJECT ENCODING set listpack = %q", got)
	}
	_ = sendLine(t, r, c, "ZADD z 1 a")
	if got := bulk(t, r, c, "OBJECT ENCODING z"); got != "listpack" {
		t.Fatalf("OBJECT ENCODING zset = %q", got)
	}
}

func TestObjectEncodingMissing(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "OBJECT ENCODING nope"); got != "-"+noSuchKeyError {
		t.Fatalf("OBJECT ENCODING missing = %q", got)
	}
}

func TestObjectRefcount(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "OBJECT REFCOUNT k"); got != ":1" {
		t.Fatalf("OBJECT REFCOUNT = %q", got)
	}
	if got := sendLine(t, r, c, "OBJECT REFCOUNT nope"); got != "-"+noSuchKeyError {
		t.Fatalf("OBJECT REFCOUNT missing = %q", got)
	}
}

func TestObjectIdletime(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "OBJECT IDLETIME k"); got != ":0" {
		t.Fatalf("OBJECT IDLETIME = %q", got)
	}
	if got := sendLine(t, r, c, "OBJECT IDLETIME nope"); got != "-"+noSuchKeyError {
		t.Fatalf("OBJECT IDLETIME missing = %q", got)
	}
}

func TestObjectFreq(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	got := sendLine(t, r, c, "OBJECT FREQ k")
	if !strings.HasPrefix(got, "-ERR An LFU maxmemory policy is not selected") {
		t.Fatalf("OBJECT FREQ = %q", got)
	}
}

func TestObjectHelp(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "OBJECT HELP")
	if !strings.HasPrefix(got, "*") {
		t.Fatalf("OBJECT HELP header = %q", got)
	}
}

func TestObjectUnknownSub(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "OBJECT FOO k")
	if !strings.HasPrefix(got, "-ERR Unknown subcommand") {
		t.Fatalf("OBJECT FOO = %q", got)
	}
}

func TestObjectArity(t *testing.T) {
	r, c := startData(t)
	got := sendLine(t, r, c, "OBJECT")
	if !strings.HasPrefix(got, "-ERR wrong number of arguments") {
		t.Fatalf("bare OBJECT = %q", got)
	}
}
