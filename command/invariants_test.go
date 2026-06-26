package command

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These tests cover the runtime invariants in doc 23 sections 9.5 through 9.8.
//
// Section 9.5 (MVCC version monotonicity) has no test here on purpose. aki does
// not keep a per-key version chain. A write replaces the value in place under
// the engine lock and durability is a whole-file property of the double-meta
// commit, not a per-key version counter. There is no version sequence to walk,
// so the invariant the spec describes does not map onto aki's model. The
// linearizability test in linearizability_test.go covers the property that
// concurrent writes do not lose or double-apply, which is the observable
// guarantee 9.5 is really after.

// 9.6 Encoding threshold invariants.
//
// OBJECT ENCODING must agree with the documented threshold rules. The oracle
// below recomputes the expected encoding straight from the thresholds, without
// calling the codec, so a codec bug that stored the wrong encoding header shows
// up as a mismatch. Each key is built fresh in one command so there is no prior
// encoding to pin the result.

const (
	thHashSetZsetEntries = 128  // hash/set/zset listpack entry cap
	thListpackValue      = 64   // per-element byte cap for listpack/intset members
	thIntsetEntries      = 512  // all-integer set stays intset up to here
	thListBytes          = 8192 // list listpack byte budget at the default -2 tier
)

func isInt(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func maxLen(ss []string) int {
	m := 0
	for _, s := range ss {
		if len(s) > m {
			m = len(s)
		}
	}
	return m
}

// wantSetEncoding mirrors the set threshold rules for a fresh key.
func wantSetEncoding(members []string) string {
	allInt := true
	for _, m := range members {
		if !isInt(m) {
			allInt = false
			break
		}
	}
	n := len(members)
	if allInt && n <= thIntsetEntries {
		return "intset"
	}
	if n <= thHashSetZsetEntries && maxLen(members) <= thListpackValue {
		return "listpack"
	}
	return "hashtable"
}

// wantHashEncoding mirrors the hash threshold rules for a fresh key. pairs holds
// field, value, field, value.
func wantHashEncoding(pairs []string) string {
	if len(pairs)/2 > thHashSetZsetEntries {
		return "hashtable"
	}
	if maxLen(pairs) > thListpackValue {
		return "hashtable"
	}
	return "listpack"
}

// wantZsetEncoding mirrors the zset threshold rules for a fresh key. members
// holds the member strings only, scores are assumed short.
func wantZsetEncoding(members []string) string {
	if len(members) > thHashSetZsetEntries {
		return "skiplist"
	}
	if maxLen(members) > thListpackValue {
		return "skiplist"
	}
	return "listpack"
}

// wantListEncoding mirrors the list threshold rule for a fresh key at the default
// list-max-listpack-size -2: an 8KB listpack byte budget, no entry-count cap and
// no per-element cap. The size is the 6-byte listpack header, a 1-byte terminator
// and each element's encoding plus its backlen, matching lpBytes.
func wantListEncoding(elems []string) string {
	total := 7
	for _, e := range elems {
		total += lpStrEntrySize(len(e))
	}
	if total > thListBytes {
		return "quicklist"
	}
	return "listpack"
}

// lpStrEntrySize returns the listpack bytes a string element of length n takes:
// the length-prefixed string encoding plus the backlen field.
func lpStrEntrySize(n int) int {
	var enc int
	switch {
	case n < 64:
		enc = 1 + n
	case n < 4096:
		enc = 2 + n
	default:
		enc = 5 + n
	}
	switch {
	case enc <= 127:
		return enc + 1
	case enc < 16384:
		return enc + 2
	default:
		return enc + 3
	}
}

func TestEncodingThresholdInvariants(t *testing.T) {
	c := newAkiClient(t)
	enc := func(key string) string {
		got := c.do([]string{"OBJECT", "ENCODING", key})
		s, ok := got.(string)
		if !ok {
			t.Fatalf("OBJECT ENCODING %s = %v want a string", key, got)
		}
		return s
	}

	big := strings.Repeat("x", 70) // over the 64-byte listpack value cap
	short := []string{"a", "b", "c"}

	manyInts := make([]string, 200) // all-int, under intset cap
	for i := range manyInts {
		manyInts[i] = strconv.Itoa(i)
	}
	manyShort := make([]string, 200) // 200 short elements, still under the 8KB budget
	for i := range manyShort {
		manyShort[i] = "m" + strconv.Itoa(i)
	}
	overBudget := make([]string, 1000) // 1000 ten-byte elements cross the 8KB budget
	for i := range overBudget {
		overBudget[i] = "0123456789"
	}

	// Sets.
	setCases := [][]string{
		{"1", "2", "3"}, // all int, small -> intset
		short,           // short non-int -> listpack
		{"a", "b", big}, // big member -> hashtable
		manyInts,        // all int, 200 -> intset
		manyShort,       // 200 non-int -> hashtable
	}
	for i, members := range setCases {
		key := "set" + strconv.Itoa(i)
		argv := append([]string{"SADD", key}, members...)
		c.do(argv)
		if got, want := enc(key), wantSetEncoding(members); got != want {
			t.Errorf("set case %d: OBJECT ENCODING = %q want %q (n=%d)", i, got, want, len(members))
		}
	}

	// Hashes.
	hashCases := [][]string{
		{"f1", "v1", "f2", "v2"}, // small -> listpack
		{"f", big},               // big value -> hashtable
		{big, "v"},               // big field -> hashtable
	}
	for i, pairs := range hashCases {
		key := "hash" + strconv.Itoa(i)
		argv := append([]string{"HSET", key}, pairs...)
		c.do(argv)
		if got, want := enc(key), wantHashEncoding(pairs); got != want {
			t.Errorf("hash case %d: OBJECT ENCODING = %q want %q", i, got, want)
		}
	}
	// A hash with 129 fields crosses the entry cap.
	bigHashKey := "hashbig"
	hargv := []string{"HSET", bigHashKey}
	bigHashPairs := []string{}
	for i := 0; i < 129; i++ {
		f, v := "f"+strconv.Itoa(i), "v"+strconv.Itoa(i)
		hargv = append(hargv, f, v)
		bigHashPairs = append(bigHashPairs, f, v)
	}
	c.do(hargv)
	if got, want := enc(bigHashKey), wantHashEncoding(bigHashPairs); got != want {
		t.Errorf("hash 129 fields: OBJECT ENCODING = %q want %q", got, want)
	}

	// Sorted sets. ZADD takes score member pairs.
	zCases := [][]string{short, {"a", big}}
	for i, members := range zCases {
		key := "zset" + strconv.Itoa(i)
		argv := []string{"ZADD", key}
		for j, m := range members {
			argv = append(argv, strconv.Itoa(j+1), m)
		}
		c.do(argv)
		if got, want := enc(key), wantZsetEncoding(members); got != want {
			t.Errorf("zset case %d: OBJECT ENCODING = %q want %q", i, got, want)
		}
	}
	// A zset with 129 members crosses the entry cap.
	bigZKey := "zsetbig"
	zargv := []string{"ZADD", bigZKey}
	bigZMembers := []string{}
	for i := 0; i < 129; i++ {
		m := "m" + strconv.Itoa(i)
		zargv = append(zargv, strconv.Itoa(i), m)
		bigZMembers = append(bigZMembers, m)
	}
	c.do(zargv)
	if got, want := enc(bigZKey), wantZsetEncoding(bigZMembers); got != want {
		t.Errorf("zset 129 members: OBJECT ENCODING = %q want %q", got, want)
	}

	// Lists.
	listCases := [][]string{short, {"a", big}, manyShort, overBudget}
	for i, elems := range listCases {
		key := "list" + strconv.Itoa(i)
		argv := append([]string{"RPUSH", key}, elems...)
		c.do(argv)
		if got, want := enc(key), wantListEncoding(elems); got != want {
			t.Errorf("list case %d: OBJECT ENCODING = %q want %q (n=%d)", i, got, want, len(elems))
		}
	}
}

// 9.7 TTL monotonicity.
//
// Once a TTL is set and nothing overwrites or persists the key, PTTL must not
// increase as time passes. PERSIST must clear it back to -1.
func TestTTLMonotonicity(t *testing.T) {
	c := newAkiClient(t)
	if got := c.do([]string{"SET", "k", "v"}); got != "OK" {
		t.Fatalf("SET = %v", got)
	}
	if got := c.do([]string{"PEXPIRE", "k", "10000"}); got != int64(1) {
		t.Fatalf("PEXPIRE = %v want 1", got)
	}

	prev := int64(1 << 62)
	for i := 0; i < 10; i++ {
		got := c.do([]string{"PTTL", "k"})
		ttl, ok := got.(int64)
		if !ok {
			t.Fatalf("PTTL = %v want an integer", got)
		}
		if ttl < 0 {
			t.Fatalf("PTTL = %d, key should still be alive", ttl)
		}
		// Allow a small slack for clock jitter, but the trend must be down.
		if ttl > prev+50 {
			t.Fatalf("PTTL went up: was %d, now %d", prev, ttl)
		}
		prev = ttl
		time.Sleep(5 * time.Millisecond)
	}

	if got := c.do([]string{"PERSIST", "k"}); got != int64(1) {
		t.Fatalf("PERSIST = %v want 1", got)
	}
	if got := c.do([]string{"PTTL", "k"}); got != int64(-1) {
		t.Fatalf("PTTL after PERSIST = %v want -1", got)
	}
}

// 9.8 Replication offset consistency.
//
// The replica's acknowledged offset must catch up to the master's offset and
// never exceed it, and the master offset must advance as writes flow.
func TestReplicationOffsetConsistency(t *testing.T) {
	mr, mc, mHost, mPort := startDataAddr(t)
	rr, rc, _, _ := startDataAddr(t)

	if got := sendLine(t, rr, rc, "REPLICAOF "+mHost+" "+mPort); got != "+OK" {
		t.Fatalf("REPLICAOF = %q", got)
	}
	// Wait for the link to come up.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		info, _ := sendArgs(t, mr, mc, "INFO", "replication").(string)
		if containsLine(info, "connected_slaves:1") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i := 0; i < 500; i++ {
		k := "k" + strconv.Itoa(i)
		if got := sendLine(t, mr, mc, "SET "+k+" v"+strconv.Itoa(i)); got != "+OK" {
			t.Fatalf("master SET %s = %q", k, got)
		}
	}

	masterOff := infoInt(t, mr, mc, "master_repl_offset")
	if masterOff <= 0 {
		t.Fatalf("master_repl_offset = %d, want positive after writes", masterOff)
	}

	// The replica's offset must reach the master's, and never run past it.
	var replicaOff int64
	caught := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		replicaOff = infoInt(t, rr, rc, "slave_repl_offset")
		if replicaOff > masterOff {
			t.Fatalf("replica offset %d ran past master offset %d", replicaOff, masterOff)
		}
		if replicaOff >= masterOff {
			caught = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !caught {
		t.Fatalf("replica offset %d never caught master offset %d", replicaOff, masterOff)
	}

	// A further write must move the master offset forward.
	if got := sendLine(t, mr, mc, "SET tail done"); got != "+OK" {
		t.Fatalf("master SET tail = %q", got)
	}
	after := infoInt(t, mr, mc, "master_repl_offset")
	if after <= masterOff {
		t.Fatalf("master offset did not advance: was %d, now %d", masterOff, after)
	}
}

// infoInt reads one integer field out of INFO replication.
func infoInt(t *testing.T, r *bufio.Reader, c net.Conn, field string) int64 {
	t.Helper()
	info, _ := sendArgs(t, r, c, "INFO", "replication").(string)
	for _, ln := range strings.Split(info, "\n") {
		ln = strings.TrimRight(ln, "\r")
		if rest, ok := strings.CutPrefix(ln, field+":"); ok {
			n, err := strconv.ParseInt(rest, 10, 64)
			if err != nil {
				t.Fatalf("parse %s value %q: %v", field, rest, err)
			}
			return n
		}
	}
	t.Fatalf("INFO replication has no %s line:\n%s", field, info)
	return 0
}
