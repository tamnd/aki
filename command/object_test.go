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

// TestObjectIdletime checks that IDLETIME answers for a live key, is zero right
// after access, and is rejected under an LFU policy where it is not tracked.
func TestObjectIdletime(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "OBJECT IDLETIME k"); got != ":0" {
		t.Fatalf("OBJECT IDLETIME = %q", got)
	}
	if got := sendLine(t, r, c, "OBJECT IDLETIME nope"); got != "-"+noSuchKeyError {
		t.Fatalf("OBJECT IDLETIME missing = %q", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET maxmemory-policy allkeys-lfu")
	if got := sendLine(t, r, c, "OBJECT IDLETIME k"); !strings.HasPrefix(got, "-ERR An LFU") {
		t.Fatalf("OBJECT IDLETIME under LFU = %q want LFU error", got)
	}
}

// TestObjectFreq checks that FREQ is rejected unless an LFU policy is set and
// returns the seeded counter once it is.
func TestObjectFreq(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	got := sendLine(t, r, c, "OBJECT FREQ k")
	if !strings.HasPrefix(got, "-ERR An LFU maxmemory policy is not selected") {
		t.Fatalf("OBJECT FREQ = %q", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET maxmemory-policy allkeys-lfu")
	if got := sendLine(t, r, c, "OBJECT FREQ k"); got != ":5" {
		t.Fatalf("OBJECT FREQ seeded = %q want :5", got)
	}
	if got := sendLine(t, r, c, "OBJECT FREQ missing"); got != "-"+noSuchKeyError {
		t.Fatalf("OBJECT FREQ missing = %q", got)
	}
}

// TestObjectEncodingLiveConfig checks that CONFIG SET of the listpack thresholds
// changes the encoding a key reports on its next write, since aki reads the
// limits live per dispatcher rather than baking them in at compile time.
func TestObjectEncodingLiveConfig(t *testing.T) {
	r, c := startData(t)

	// A two-field hash reports listpack under the default entry cap.
	_ = sendLine(t, r, c, "HSET h a 1 b 2")
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "listpack" {
		t.Fatalf("hash before config = %q want listpack", got)
	}
	// Drop the entry cap below the field count and rewrite. The hash now spills
	// to hashtable.
	_ = sendLine(t, r, c, "CONFIG SET hash-max-listpack-entries 1")
	_ = sendLine(t, r, c, "HSET h c 3")
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "hashtable" {
		t.Fatalf("hash after config = %q want hashtable", got)
	}

	// Same for the value-size cap on a fresh hash.
	_ = sendLine(t, r, c, "CONFIG SET hash-max-listpack-value 3")
	_ = sendLine(t, r, c, "HSET hv f toolong")
	if got := bulk(t, r, c, "OBJECT ENCODING hv"); got != "hashtable" {
		t.Fatalf("hash value cap = %q want hashtable", got)
	}

	// A small zset is listpack until the entry cap is lowered under it.
	_ = sendLine(t, r, c, "ZADD z 1 a 2 b")
	if got := bulk(t, r, c, "OBJECT ENCODING z"); got != "listpack" {
		t.Fatalf("zset before config = %q want listpack", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET zset-max-listpack-entries 1")
	_ = sendLine(t, r, c, "ZADD z 3 c")
	if got := bulk(t, r, c, "OBJECT ENCODING z"); got != "skiplist" {
		t.Fatalf("zset after config = %q want skiplist", got)
	}

	// A small set is listpack until the entry cap is lowered under it.
	_ = sendLine(t, r, c, "SADD s a b")
	if got := bulk(t, r, c, "OBJECT ENCODING s"); got != "listpack" {
		t.Fatalf("set before config = %q want listpack", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET set-max-listpack-entries 1")
	_ = sendLine(t, r, c, "SADD s c")
	if got := bulk(t, r, c, "OBJECT ENCODING s"); got != "hashtable" {
		t.Fatalf("set after config = %q want hashtable", got)
	}

	// A short list is listpack until the size cap is lowered under it.
	_ = sendLine(t, r, c, "RPUSH l a b")
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("list before config = %q want listpack", got)
	}
	_ = sendLine(t, r, c, "CONFIG SET list-max-listpack-size 1")
	_ = sendLine(t, r, c, "RPUSH l c")
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("list after config = %q want quicklist", got)
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
