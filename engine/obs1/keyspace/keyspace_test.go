package keyspace

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/aki/engine/obs1/hash"
	"github.com/tamnd/aki/engine/obs1/list"
	"github.com/tamnd/aki/engine/obs1/set"
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/str"
	"github.com/tamnd/aki/engine/obs1/stream"
	"github.com/tamnd/aki/engine/obs1/zset"
)

// The key-level TTL harness: the real keyspace handlers plus one writer
// per type on a shard runtime, every keyed handler behind the same Reap
// wrapper dispatch installs, so the lazy-expiry guard runs exactly where
// it does in the server. Deadlines are either far future (never fire) or
// strictly past (fire on arrival), so nothing here races the clock; the
// one guard-fire test uses a short real deadline and polls, bounded.

const (
	opExpire byte = iota + 1
	opPexpire
	opExpireat
	opPexpireat
	opTTL
	opPttl
	opExpiretime
	opPexpiretime
	opPersist
	opType
	opExists
	opDel
	opSet
	opGet
	opSadd
	opHset
	opZadd
	opRpush
	opXadd
	opLast
)

func harnessHandlers() []shard.Handler {
	h := make([]shard.Handler, opLast)
	h[opExpire] = Expire
	h[opPexpire] = PExpire
	h[opExpireat] = ExpireAt
	h[opPexpireat] = PExpireAt
	h[opTTL] = TTL
	h[opPttl] = PTTL
	h[opExpiretime] = ExpireTime
	h[opPexpiretime] = PExpireTime
	h[opPersist] = Persist
	h[opType] = Type
	h[opExists] = Exists
	h[opDel] = Del
	h[opSet] = str.Set
	h[opGet] = str.Get
	h[opSadd] = set.Sadd
	h[opHset] = hash.Hset
	h[opZadd] = zset.Zadd
	h[opRpush] = list.Rpush
	h[opXadd] = stream.Xadd
	for i, orig := range h {
		if orig == nil {
			continue
		}
		inner := orig
		h[i] = func(cx *shard.Ctx, args [][]byte, r shard.Reply) {
			if err := Reap(cx, args[0]); err != nil {
				r.Err(err.Error())
				return
			}
			inner(cx, args, r)
		}
	}
	return h
}

func newHarness(t *testing.T) *shard.Runtime {
	t.Helper()
	rt := shard.New(1, 8<<20, 1<<18)
	rt.Use(harnessHandlers())
	rt.Start()
	t.Cleanup(rt.Stop)
	return rt
}

func do(t *testing.T, c *shard.Conn, op byte, a ...string) string {
	t.Helper()
	args := make([][]byte, len(a))
	for i := range a {
		args[i] = []byte(a[i])
	}
	if err := c.DoAt(op, 0, args); err != nil {
		t.Fatal(err)
	}
	c.Flush()
	var rep []byte
	deadline := time.Now().Add(10 * time.Second)
	for rep == nil {
		c.DrainReplies(func(b []byte) { rep = append([]byte(nil), b...) })
		if rep == nil {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for a reply")
			}
			runtime.Gosched()
		}
	}
	return string(rep)
}

func wantInt(t *testing.T, got string, n int64) {
	t.Helper()
	want := ":" + strconv.FormatInt(n, 10) + "\r\n"
	if got != want {
		t.Fatalf("reply %q, want %q", got, want)
	}
}

func wantStatus(t *testing.T, got, s string) {
	t.Helper()
	if got != "+"+s+"\r\n" {
		t.Fatalf("reply %q, want +%s", got, s)
	}
}

func wantErrHas(t *testing.T, got, frag string) {
	t.Helper()
	if !strings.HasPrefix(got, "-") || !strings.Contains(got, frag) {
		t.Fatalf("reply %q, want an error containing %q", got, frag)
	}
}

// intReply parses a :n reply.
func intReply(t *testing.T, got string) int64 {
	t.Helper()
	if !strings.HasPrefix(got, ":") {
		t.Fatalf("reply %q, want an integer", got)
	}
	n, err := strconv.ParseInt(strings.TrimSuffix(got[1:], "\r\n"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// farFutureSec is a year-2191 unix-seconds stamp, far past any test run.
const farFutureSec = int64(7_000_000_000)

// seed creates key as the named type and returns the TYPE answer to expect.
func seed(t *testing.T, c *shard.Conn, kind, key string) {
	t.Helper()
	switch kind {
	case "string":
		wantStatus(t, do(t, c, opSet, key, "v"), "OK")
	case "set":
		wantInt(t, do(t, c, opSadd, key, "m1", "m2"), 2)
	case "hash":
		wantInt(t, do(t, c, opHset, key, "f", "v"), 1)
	case "zset":
		wantInt(t, do(t, c, opZadd, key, "1", "m"), 1)
	case "list":
		wantInt(t, do(t, c, opRpush, key, "e1"), 1)
	case "stream":
		do(t, c, opXadd, key, "*", "f", "v")
	}
}

var allKinds = []string{"string", "set", "hash", "zset", "list", "stream"}

func TestExpireTTLPersistEveryType(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, kind := range allKinds {
		key := "k-" + kind
		seed(t, c, kind, key)
		wantStatus(t, do(t, c, opType, key), kind)
		wantInt(t, do(t, c, opTTL, key), -1)

		wantInt(t, do(t, c, opExpireat, key, strconv.FormatInt(farFutureSec, 10)), 1)
		ttl := intReply(t, do(t, c, opTTL, key))
		if ttl <= 0 {
			t.Fatalf("%s: TTL %d after EXPIREAT far future", kind, ttl)
		}
		pttl := intReply(t, do(t, c, opPttl, key))
		if pttl <= 0 || pttl > farFutureSec*1000 {
			t.Fatalf("%s: PTTL %d out of range", kind, pttl)
		}
		wantInt(t, do(t, c, opExpiretime, key), farFutureSec)
		wantInt(t, do(t, c, opPexpiretime, key), farFutureSec*1000)

		wantInt(t, do(t, c, opPersist, key), 1)
		wantInt(t, do(t, c, opTTL, key), -1)
		wantInt(t, do(t, c, opPersist, key), 0)
		wantStatus(t, do(t, c, opType, key), kind)
	}
}

func TestExpireMissesAndErrors(t *testing.T) {
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opExpire, "nope", "100"), 0)
	wantInt(t, do(t, c, opTTL, "nope"), -2)
	wantInt(t, do(t, c, opPttl, "nope"), -2)
	wantInt(t, do(t, c, opExpiretime, "nope"), -2)
	wantInt(t, do(t, c, opPersist, "nope"), 0)
	wantStatus(t, do(t, c, opType, "nope"), "none")

	seed(t, c, "set", "s")
	wantErrHas(t, do(t, c, opExpire, "s", "abc"), "not an integer")
	wantErrHas(t, do(t, c, opExpire, "s", "9999999999999999"), "invalid expire time in 'expire'")
	wantErrHas(t, do(t, c, opExpire, "s", "100", "BOGUS"), "Unsupported option")
	wantErrHas(t, do(t, c, opExpire, "s", "100", "NX", "XX"), "NX and XX")
	wantErrHas(t, do(t, c, opExpire, "s", "100", "GT", "LT"), "GT and LT")
}

func TestExpireConditionFlags(t *testing.T) {
	c := newHarness(t).NewConn()
	seed(t, c, "hash", "h")
	at := strconv.FormatInt(farFutureSec, 10)
	lower := strconv.FormatInt(farFutureSec-1000, 10)
	higher := strconv.FormatInt(farFutureSec+1000, 10)

	// XX and GT refuse a persistent key (no TTL counts as infinite for GT).
	wantInt(t, do(t, c, opExpireat, "h", at, "XX"), 0)
	wantInt(t, do(t, c, opExpireat, "h", at, "GT"), 0)
	wantInt(t, do(t, c, opTTL, "h"), -1)
	// LT treats no TTL as infinite, so it sets one.
	wantInt(t, do(t, c, opExpireat, "h", at, "LT"), 1)
	wantInt(t, do(t, c, opExpiretime, "h"), farFutureSec)
	// NX refuses now that a TTL exists.
	wantInt(t, do(t, c, opExpireat, "h", higher, "NX"), 0)
	// GT refuses a lower deadline, takes a higher one.
	wantInt(t, do(t, c, opExpireat, "h", lower, "GT"), 0)
	wantInt(t, do(t, c, opExpireat, "h", higher, "GT"), 1)
	wantInt(t, do(t, c, opExpiretime, "h"), farFutureSec+1000)
	// LT mirrors.
	wantInt(t, do(t, c, opExpireat, "h", higher, "LT"), 0)
	wantInt(t, do(t, c, opExpireat, "h", lower, "LT"), 1)
	wantInt(t, do(t, c, opExpiretime, "h"), farFutureSec-1000)

	// NX sets on a fresh key with no TTL.
	seed(t, c, "list", "l")
	wantInt(t, do(t, c, opExpireat, "l", at, "NX"), 1)
	wantInt(t, do(t, c, opExpiretime, "l"), farFutureSec)
}

func TestPastDeadlineDeletesOnArrival(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, kind := range allKinds {
		key := "gone-" + kind
		seed(t, c, kind, key)
		wantInt(t, do(t, c, opPexpireat, key, "1"), 1)
		wantStatus(t, do(t, c, opType, key), "none")
		wantInt(t, do(t, c, opExists, key), 0)
		wantInt(t, do(t, c, opTTL, key), -2)
	}
}

func TestDelAndExistsSpanEveryType(t *testing.T) {
	c := newHarness(t).NewConn()
	for _, kind := range allKinds {
		key := "d-" + kind
		wantInt(t, do(t, c, opDel, key), 0)
		seed(t, c, kind, key)
		wantInt(t, do(t, c, opExists, key), 1)
		wantInt(t, do(t, c, opDel, key), 1)
		wantInt(t, do(t, c, opExists, key), 0)
		wantStatus(t, do(t, c, opType, key), "none")
	}
}

func TestGuardReapsFiredCollection(t *testing.T) {
	c := newHarness(t).NewConn()
	seed(t, c, "zset", "z")
	wantInt(t, do(t, c, opPexpire, "z", "40"), 1)
	deadline := time.Now().Add(10 * time.Second)
	for {
		rep := do(t, c, opExists, "z")
		if rep == ":0\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fired zset never reaped by the guard")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A recreate after the reap starts clean: no inherited deadline.
	seed(t, c, "zset", "z")
	wantInt(t, do(t, c, opTTL, "z"), -1)
	wantStatus(t, do(t, c, opType, "z"), "zset")
}

func TestExpireCoversBothKeyspaceHalves(t *testing.T) {
	// The sanctioned non-unified keyspace can hold a string and a
	// collection under one key (SET does not see the registries, though
	// SADD's WRONGTYPE guard refuses the other order); the deadline lands
	// on both halves and a fired deadline or DEL removes both.
	c := newHarness(t).NewConn()
	wantInt(t, do(t, c, opSadd, "both", "m"), 1)
	wantStatus(t, do(t, c, opSet, "both", "v"), "OK")
	wantInt(t, do(t, c, opExpireat, "both", strconv.FormatInt(farFutureSec, 10)), 1)
	wantInt(t, do(t, c, opPexpireat, "both", "1"), 1)
	wantStatus(t, do(t, c, opType, "both"), "none")
	if got := do(t, c, opGet, "both"); got != "$-1\r\n" {
		t.Fatalf("string half survived the fired deadline: %q", got)
	}
	wantInt(t, do(t, c, opExists, "both"), 0)
}
