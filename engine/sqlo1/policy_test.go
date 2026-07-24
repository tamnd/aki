package sqlo1

// The maxmemory-policy surface, doc 11 section 5: the Redis names parse
// and print, each flavor ranks the way its Redis namesake would pick
// victims, the volatile families prefer volatile keys at equal rank,
// and no policy ever destroys data (E-I6) because eviction here is
// demotion.

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestEvictPolicyParseAndString(t *testing.T) {
	for p, name := range policyNames {
		got, ok := ParseEvictPolicy(name)
		if !ok || got != EvictPolicy(p) {
			t.Fatalf("ParseEvictPolicy(%q) = %v, %v", name, got, ok)
		}
		if s := EvictPolicy(p).String(); s != name {
			t.Fatalf("EvictPolicy(%d).String() = %q, want %q", p, s, name)
		}
	}
	// The comparison is exact and lowercase like Redis's config parser.
	for _, bad := range []string{"allkeys-LRU", "lru", "noeviction ", ""} {
		if _, ok := ParseEvictPolicy(bad); ok {
			t.Fatalf("ParseEvictPolicy(%q) accepted", bad)
		}
	}
	if s := EvictPolicy(200).String(); s != "noeviction" {
		t.Fatalf("out-of-range String() = %q", s)
	}
}

func TestPolicyScoreFlavors(t *testing.T) {
	ht := NewHotTable(8)
	ht.SetTick(100)
	e := newEvictor(ht, 1)

	// LRU is pure recency on the latest touch of either kind; untouched
	// headers rank lowest.
	e.policy = PolicyAllkeysLRU
	recent := hdr{lastRead: 99}
	stale := hdr{lastWrite: 50}
	if e.policyScore(&recent) <= e.policyScore(&stale) {
		t.Fatal("lru: recent read did not outrank stale write")
	}
	untouched := hdr{}
	if s := e.policyScore(&untouched); s != 0 {
		t.Fatalf("lru: untouched header scored %f", s)
	}

	// LFU weighs the read stamps double, the inverse of the WATT-lite
	// write bias, standing in for Redis's access counter.
	e.policy = PolicyAllkeysLFU
	reader := hdr{lastRead: 99, prevRead: 97}
	writer := hdr{lastWrite: 99, prevWrite: 97}
	if rs, ws := e.policyScore(&reader), e.policyScore(&writer); rs != 2*ws {
		t.Fatalf("lfu: read score %f, write score %f: reads must weigh 2x", rs, ws)
	}

	// Random ranks everyone equal so the sample order decides.
	e.policy = PolicyVolatileRandom
	if e.policyScore(&recent) != 0 || e.policyScore(&untouched) != 0 {
		t.Fatal("random: scores not flat")
	}

	// volatile-ttl orders volatile keys by remaining life and lifts
	// persistent keys past the floor so they demote last.
	e.policy = PolicyVolatileTTL
	var soon, late hdr
	soon.expireLo, soon.expireRem = splitExpMs(ht.nowMs + 1000)
	late.expireLo, late.expireRem = splitExpMs(ht.nowMs + 1_000_000)
	if e.policyScore(&soon) >= e.policyScore(&late) {
		t.Fatal("ttl: sooner death did not rank below later death")
	}
	if s := e.policyScore(&reader); s < ttlScoreFloor {
		t.Fatalf("ttl: persistent key scored %f, below the floor", s)
	}

	// noeviction keeps the doc 04 WATT-lite score untouched.
	e.policy = PolicyNoEviction
	if e.policyScore(&reader) != e.score(&reader) {
		t.Fatal("noeviction: score diverged from WATT-lite")
	}
}

func TestCandLessVolatileTiebreak(t *testing.T) {
	ht := NewHotTable(8)
	e := newEvictor(ht, 1)
	vol := evictCand{score: 0, vol: true}
	per := evictCand{score: 0, vol: false}

	e.policy = PolicyVolatileRandom
	if !e.candLess(&vol, &per) || e.candLess(&per, &vol) {
		t.Fatal("volatile-random: volatile key must win the tie")
	}
	e.policy = PolicyAllkeysRandom
	if e.candLess(&vol, &per) || e.candLess(&per, &vol) {
		t.Fatal("allkeys-random: no tiebreak expected")
	}
	// The score always beats the tiebreak.
	e.policy = PolicyVolatileLRU
	cheap := evictCand{score: 1, vol: false}
	dear := evictCand{score: 2, vol: true}
	if !e.candLess(&cheap, &dear) {
		t.Fatal("volatile-lru: lower score lost to the volatile bit")
	}
}

func TestEvictVolatileFirstUnderVolatileRandom(t *testing.T) {
	ht := NewHotTable(256)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 7)
	e.policy = PolicyVolatileRandom

	for i := range 40 {
		ht.Put(fmt.Appendf(nil, "per-%02d", i), []byte("value"), TagString)
	}
	for i := range 40 {
		k := fmt.Appendf(nil, "vol-%02d", i)
		ht.Put(k, []byte("value"), TagString)
		if _, _, ok := ht.setExpireMs(k, ht.nowMs+(1<<40)); !ok {
			t.Fatalf("expire on %s refused", k)
		}
	}
	drainAll(t, d)

	perKey := hdrSize + len("per-00") + len("value")
	e.evict(30 * perKey)

	volLeft, perLeft := 0, 0
	for i := range 40 {
		if _, ok := ht.Get(fmt.Appendf(nil, "vol-%02d", i)); ok {
			volLeft++
		}
		if _, ok := ht.Get(fmt.Appendf(nil, "per-%02d", i)); ok {
			perLeft++
		}
	}
	if perLeft != 40 {
		t.Fatalf("persistent survivors %d, want all 40: volatile must demote first", perLeft)
	}
	if volLeft >= 40 {
		t.Fatal("no volatile key demoted")
	}
}

func TestEvictTTLOrderUnderVolatileTTL(t *testing.T) {
	ht := NewHotTable(256)
	ht.SetTick(1)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 8)
	e.policy = PolicyVolatileTTL

	seed := func(prefix string, atMs int64) {
		for i := range 30 {
			k := fmt.Appendf(nil, "%s-%02d", prefix, i)
			ht.Put(k, []byte("value"), TagString)
			if atMs != 0 {
				if _, _, ok := ht.setExpireMs(k, atMs); !ok {
					t.Fatalf("expire on %s refused", k)
				}
			}
		}
	}
	seed("soon", ht.nowMs+10_000)
	seed("late", ht.nowMs+(1<<40))
	seed("keep", 0)
	drainAll(t, d)

	perKey := hdrSize + len("soon-00") + len("value")
	e.evict(20 * perKey)

	left := func(prefix string) int {
		n := 0
		for i := range 30 {
			if _, ok := ht.Get(fmt.Appendf(nil, "%s-%02d", prefix, i)); ok {
				n++
			}
		}
		return n
	}
	if got := left("keep"); got != 30 {
		t.Fatalf("persistent survivors %d, want all 30", got)
	}
	if got := left("late"); got != 30 {
		t.Fatalf("far-expiry survivors %d, want all 30: soonest death demotes first", got)
	}
	if got := left("soon"); got >= 30 {
		t.Fatal("no soon-to-die key demoted")
	}
}

func TestVictimsSkipPlaneRecords(t *testing.T) {
	ht := NewHotTable(64)
	ht.SetTick(10)
	d := newDrainer(ht, NewMemStore())
	e := newEvictor(ht, 9)
	e.policy = PolicyAllkeysLRU

	ht.Put([]byte("old"), []byte("v"), TagString)
	ht.SetTick(20)
	ht.Put([]byte("new"), []byte("v"), TagString)
	// Plane records are not addressable keys: a generation or the fence
	// bit keeps them out of the victim list. The fence bit is flipped on
	// after the drain because the store rejects a fence without a
	// generation, and the victim check must hold on either mark alone.
	ht.PutGen([]byte("seg"), []byte("v"), TagHash, 7)
	ht.Put([]byte("fence"), []byte("v"), TagString)
	drainAll(t, d)
	ht.hdrs[slotOf(t, ht, []byte("fence"))].typeTag |= TagFence

	vs := e.victims(10)
	if len(vs) != 2 {
		t.Fatalf("victims returned %d keys, want 2: %q", len(vs), vs)
	}
	// LRU ranks the stale key first.
	if string(vs[0]) != "old" || string(vs[1]) != "new" {
		t.Fatalf("victims order %q, want [old new]", vs)
	}
}

// TestPoliciesNeverDeleteData is E-I6: under every policy, eviction is
// demotion, so a record pushed out of the hot tier is still in the
// store.
func TestPoliciesNeverDeleteData(t *testing.T) {
	ctx := context.Background()
	for p := range EvictPolicy(len(policyNames)) {
		ms := NewMemStore()
		ht := NewHotTable(64)
		ht.SetTick(1)
		d := newDrainer(ht, ms)
		e := newEvictor(ht, 10+uint64(p))
		e.policy = p

		key := []byte("k-" + strconv.Itoa(int(p)))
		vol := []byte("v-" + strconv.Itoa(int(p)))
		ht.Put(key, []byte("value"), TagString)
		ht.Put(vol, []byte("value"), TagString)
		if _, _, ok := ht.setExpireMs(vol, ht.nowMs+(1<<40)); !ok {
			t.Fatalf("%v: expire refused", p)
		}
		drainAll(t, d)

		if freed := e.evict(1 << 30); freed <= 0 {
			t.Fatalf("%v: evict freed nothing", p)
		}
		for _, k := range [][]byte{key, vol} {
			rec, err := ms.Get(ctx, k)
			if err != nil || string(rec.Value) != "value" {
				t.Fatalf("%v: %q gone from the store after eviction: %v", p, k, err)
			}
		}
	}
}

func TestServerConfigPolicy(t *testing.T) {
	c, r := startServer(t)
	send := func(s string) {
		t.Helper()
		if _, err := c.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	send("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$16\r\nmaxmemory-policy\r\n")
	expect(t, r, "*2\r\n$16\r\nmaxmemory-policy\r\n$10\r\nnoeviction\r\n")

	send("*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lru\r\n")
	expect(t, r, "+OK\r\n")
	send("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$16\r\nmaxmemory-policy\r\n")
	expect(t, r, "*2\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lru\r\n")

	send("*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$5\r\nbogus\r\n")
	expect(t, r, "-ERR CONFIG SET failed - argument must be a valid maxmemory-policy\r\n")
	send("*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n")
	expect(t, r, "-ERR Unknown option or number of arguments for CONFIG SET - 'appendonly'\r\n")

	// maxmemory reads as 0: writes never fail on memory here, only on
	// disk-full backpressure. Unknown parameters answer empty like Redis.
	send("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n")
	expect(t, r, "*2\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n")
	send("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n")
	expect(t, r, "*0\r\n")

	// INFO reports the live policy.
	send("*1\r\n$4\r\nINFO\r\n")
	head, err := r.ReadString('\n')
	if err != nil || !strings.HasPrefix(head, "$") {
		t.Fatalf("INFO header %q: %v", head, err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(head[1:]))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, n+2)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "maxmemory_policy:allkeys-lru") {
		t.Fatalf("INFO missing policy line: %q", body)
	}
}
