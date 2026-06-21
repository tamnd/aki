package command

import (
	"strconv"
	"strings"
	"testing"
)

// intReply parses a RESP integer line like ":42" and fails the test otherwise.
func intReply(t *testing.T, line string) int64 {
	t.Helper()
	if !strings.HasPrefix(line, ":") {
		t.Fatalf("expected integer reply, got %q", line)
	}
	n, err := strconv.ParseInt(line[1:], 10, 64)
	if err != nil {
		t.Fatalf("bad integer reply %q: %v", line, err)
	}
	return n
}

// farFuture is an absolute Unix-ms expiry well past any real clock, so tests can
// assert exact PEXPIRETIME values without pinning time.
const farFuture = 99999999999999

func TestExpireBasic(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "EXPIRE k 100"); got != ":1" {
		t.Fatalf("EXPIRE k 100 = %q want :1", got)
	}
	if n := intReply(t, sendLine(t, r, c, "TTL k")); n <= 0 || n > 100 {
		t.Fatalf("TTL k = %d want 1..100", n)
	}
	if n := intReply(t, sendLine(t, r, c, "PTTL k")); n <= 0 || n > 100000 {
		t.Fatalf("PTTL k = %d want 1..100000", n)
	}
}

func TestExpireMissingKey(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "EXPIRE nope 10"); got != ":0" {
		t.Fatalf("EXPIRE nope = %q want :0", got)
	}
}

func TestTTLStates(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "TTL missing"); got != ":-2" {
		t.Fatalf("TTL missing = %q want :-2", got)
	}
	if got := sendLine(t, r, c, "PTTL missing"); got != ":-2" {
		t.Fatalf("PTTL missing = %q want :-2", got)
	}
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "TTL k"); got != ":-1" {
		t.Fatalf("TTL persistent = %q want :-1", got)
	}
}

func TestExpireTimeExact(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture)); got != ":1" {
		t.Fatalf("PEXPIREAT = %q want :1", got)
	}
	if got := sendLine(t, r, c, "PEXPIRETIME k"); got != ":"+strconv.Itoa(farFuture) {
		t.Fatalf("PEXPIRETIME = %q", got)
	}
	if got := sendLine(t, r, c, "EXPIRETIME k"); got != ":"+strconv.Itoa(farFuture/1000) {
		t.Fatalf("EXPIRETIME = %q", got)
	}
	if got := sendLine(t, r, c, "EXPIRETIME missing"); got != ":-2" {
		t.Fatalf("EXPIRETIME missing = %q want :-2", got)
	}
	_ = sendLine(t, r, c, "SET p v")
	if got := sendLine(t, r, c, "EXPIRETIME p"); got != ":-1" {
		t.Fatalf("EXPIRETIME persistent = %q want :-1", got)
	}
}

func TestPersist(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "PERSIST k"); got != ":0" {
		t.Fatalf("PERSIST no-ttl = %q want :0", got)
	}
	_ = sendLine(t, r, c, "EXPIRE k 100")
	if got := sendLine(t, r, c, "PERSIST k"); got != ":1" {
		t.Fatalf("PERSIST = %q want :1", got)
	}
	if got := sendLine(t, r, c, "TTL k"); got != ":-1" {
		t.Fatalf("TTL after PERSIST = %q want :-1", got)
	}
	if got := sendLine(t, r, c, "PERSIST missing"); got != ":0" {
		t.Fatalf("PERSIST missing = %q want :0", got)
	}
}

func TestExpirePastDeletes(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	// A past absolute timestamp deletes the key and still returns 1.
	if got := sendLine(t, r, c, "EXPIREAT k 1"); got != ":1" {
		t.Fatalf("EXPIREAT past = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS k"); got != ":0" {
		t.Fatalf("EXISTS after past EXPIREAT = %q want :0", got)
	}
	// A negative relative delta does the same.
	_ = sendLine(t, r, c, "SET k2 v")
	if got := sendLine(t, r, c, "EXPIRE k2 -1"); got != ":1" {
		t.Fatalf("EXPIRE -1 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS k2"); got != ":0" {
		t.Fatalf("EXISTS after EXPIRE -1 = %q want :0", got)
	}
}

func TestExpireConditionsWithTTL(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	_ = sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture))
	// NX fails because a TTL is already set.
	if got := sendLine(t, r, c, "PEXPIREAT k 123 NX"); got != ":0" {
		t.Fatalf("NX with ttl = %q want :0", got)
	}
	// XX passes because a TTL exists.
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture+1)+" XX"); got != ":1" {
		t.Fatalf("XX with ttl = %q want :1", got)
	}
	// GT fails for a smaller new expiry, leaving the TTL untouched.
	if got := sendLine(t, r, c, "PEXPIREAT k 123 GT"); got != ":0" {
		t.Fatalf("GT smaller = %q want :0", got)
	}
	if got := sendLine(t, r, c, "PEXPIRETIME k"); got != ":"+strconv.Itoa(farFuture+1) {
		t.Fatalf("PEXPIRETIME after failed GT = %q", got)
	}
	// GT passes for a larger new expiry.
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture+2)+" GT"); got != ":1" {
		t.Fatalf("GT larger = %q want :1", got)
	}
}

func TestExpireConditionsNoTTL(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	// XX fails when the key has no expiry.
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture)+" XX"); got != ":0" {
		t.Fatalf("XX no-ttl = %q want :0", got)
	}
	// GT fails because no expiry is treated as infinity.
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture)+" GT"); got != ":0" {
		t.Fatalf("GT no-ttl = %q want :0", got)
	}
	// LT passes because any finite expiry is less than infinity.
	if got := sendLine(t, r, c, "PEXPIREAT k "+strconv.Itoa(farFuture)+" LT"); got != ":1" {
		t.Fatalf("LT no-ttl = %q want :1", got)
	}
	// NX passes when the key has no expiry.
	_ = sendLine(t, r, c, "SET n v")
	if got := sendLine(t, r, c, "PEXPIREAT n "+strconv.Itoa(farFuture)+" NX"); got != ":1" {
		t.Fatalf("NX no-ttl = %q want :1", got)
	}
}

func TestExpireErrors(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "SET k v")
	if got := sendLine(t, r, c, "EXPIRE k 10 NX XX"); got != "-ERR NX and XX, GT or LT options at the same time are not compatible" {
		t.Fatalf("NX XX = %q", got)
	}
	if got := sendLine(t, r, c, "EXPIRE k 10 GT LT"); got != "-ERR GT and LT options at the same time are not compatible" {
		t.Fatalf("GT LT = %q", got)
	}
	if got := sendLine(t, r, c, "EXPIRE k 10 BOGUS"); got != "-ERR Unsupported option BOGUS" {
		t.Fatalf("BOGUS = %q", got)
	}
	if got := sendLine(t, r, c, "EXPIRE k notanint"); got != "-ERR value is not an integer or out of range" {
		t.Fatalf("not-int = %q", got)
	}
	// A seconds value that overflows when scaled to milliseconds is rejected.
	if got := sendLine(t, r, c, "EXPIRE k 9223372036854776"); got != "-ERR invalid expire time in 'expire' command" {
		t.Fatalf("overflow = %q", got)
	}
}
