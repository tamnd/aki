package command

import (
	"bufio"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
)

// TestChooseVictimByPolicy checks that each policy picks the candidate it should:
// the oldest access for lru, the lowest frequency for lfu, and the soonest expiry
// for volatile-ttl.
func TestChooseVictimByPolicy(t *testing.T) {
	cands := []keyspace.EvictionCandidate{
		{Key: []byte("a"), Atime: 300, Freq: 9, TTLms: 5000, HasTTL: true},
		{Key: []byte("b"), Atime: 100, Freq: 7, TTLms: 9000, HasTTL: true},
		{Key: []byte("c"), Atime: 200, Freq: 2, TTLms: 1000, HasTTL: true},
	}
	if v := chooseVictim("allkeys-lru", cands); string(v.Key) != "b" {
		t.Fatalf("lru victim = %q want b (oldest atime)", v.Key)
	}
	if v := chooseVictim("allkeys-lfu", cands); string(v.Key) != "c" {
		t.Fatalf("lfu victim = %q want c (lowest freq)", v.Key)
	}
	if v := chooseVictim("volatile-ttl", cands); string(v.Key) != "c" {
		t.Fatalf("ttl victim = %q want c (soonest expiry)", v.Key)
	}
}

// infoUsedMemory reads INFO memory and returns the used_memory figure.
func infoUsedMemory(t *testing.T, r *bufio.Reader, c net.Conn) int64 {
	t.Helper()
	header := sendLine(t, r, c, "INFO memory")
	body := readBulk(t, r, header)
	for ln := range strings.SplitSeq(body, "\r\n") {
		if rest, ok := strings.CutPrefix(ln, "used_memory:"); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64)
			if err != nil {
				t.Fatalf("parse used_memory %q: %v", rest, err)
			}
			return n
		}
	}
	t.Fatalf("used_memory not found in INFO memory:\n%s", body)
	return 0
}

// TestNoEvictionOOM checks that under the default noeviction policy a denyoom
// write is rejected once used memory passes maxmemory, while reads still work.
func TestNoEvictionOOM(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 250"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory = %q", got)
	}
	val := strings.Repeat("x", 100)
	// First two keys fit under the limit at check time; the third sees used memory
	// already over 250 and is rejected.
	if got := sendLine(t, r, c, "SET k1 "+val); got != "+OK" {
		t.Fatalf("SET k1 = %q", got)
	}
	if got := sendLine(t, r, c, "SET k2 "+val); got != "+OK" {
		t.Fatalf("SET k2 = %q", got)
	}
	if got := sendLine(t, r, c, "SET k3 "+val); got != "-"+oomError {
		t.Fatalf("SET k3 = %q want OOM", got)
	}
	// A read is not denyoom, so it still answers.
	if got := bulk(t, r, c, "GET k1"); got != val {
		t.Fatalf("GET k1 = %q", got)
	}
}

// TestAllkeysRandomEvicts checks that an eviction policy frees memory on demand
// and fires the evicted event, instead of rejecting the write.
func TestAllkeysRandomEvicts(t *testing.T) {
	r1, c1, r2, c2 := startDataTwo(t)
	if _, err := c2.Write([]byte("PSUBSCRIBE __keyevent@0__:evicted\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = readResp(t, r2)
	if got := sendLine(t, r1, c1, "CONFIG SET notify-keyspace-events KEA"); got != "+OK" {
		t.Fatalf("CONFIG SET events = %q", got)
	}
	if got := sendLine(t, r1, c1, "CONFIG SET maxmemory-policy allkeys-random"); got != "+OK" {
		t.Fatalf("CONFIG SET policy = %q", got)
	}
	if got := sendLine(t, r1, c1, "CONFIG SET maxmemory 400"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory = %q", got)
	}

	val := strings.Repeat("x", 100)
	// Every write succeeds: once memory is over the limit, eviction makes room
	// rather than returning OOM.
	for i := range 10 {
		if got := sendLine(t, r1, c1, "SET k"+string(rune('0'+i))+" "+val); got != "+OK" {
			t.Fatalf("SET k%d = %q want OK", i, got)
		}
	}

	// At least one eviction fired its event.
	if msg := readResp(t, r2); !slices.Contains(msg, "__keyevent@0__:evicted") {
		t.Fatalf("evicted push = %v", msg)
	}
	// The keyspace was held under the cap, so not all ten keys survive.
	if got := sendLine(t, r1, c1, "DBSIZE"); got == ":10" {
		t.Fatalf("DBSIZE = %q, expected eviction to drop keys", got)
	}
}

// TestVolatileNoVolatileKeysOOM checks that a volatile-* policy with no volatile
// keys to evict degrades to the OOM error rather than touching persistent keys.
func TestVolatileNoVolatileKeysOOM(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET maxmemory-policy volatile-lru"); got != "+OK" {
		t.Fatalf("CONFIG SET policy = %q", got)
	}
	if got := sendLine(t, r, c, "CONFIG SET maxmemory 250"); got != "+OK" {
		t.Fatalf("CONFIG SET maxmemory = %q", got)
	}
	val := strings.Repeat("x", 100)
	_ = sendLine(t, r, c, "SET k1 "+val)
	_ = sendLine(t, r, c, "SET k2 "+val)
	// No key has a TTL, so volatile-lru has nothing to evict and falls back to OOM.
	if got := sendLine(t, r, c, "SET k3 "+val); got != "-"+oomError {
		t.Fatalf("SET k3 = %q want OOM", got)
	}
}

// TestUsedMemoryTracksData checks that used_memory follows the live data and drops
// back when a key is removed.
func TestUsedMemoryTracksData(t *testing.T) {
	r, c := startData(t)
	base := infoUsedMemory(t, r, c)
	val := strings.Repeat("x", 100)
	_ = sendLine(t, r, c, "SET k "+val)
	grown := infoUsedMemory(t, r, c)
	if grown <= base {
		t.Fatalf("used_memory did not grow: base %d grown %d", base, grown)
	}
	_ = sendLine(t, r, c, "DEL k")
	shrunk := infoUsedMemory(t, r, c)
	if shrunk != base {
		t.Fatalf("used_memory after DEL = %d want %d", shrunk, base)
	}
}
