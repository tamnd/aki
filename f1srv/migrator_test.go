package f1srv

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// dialMigratorServer starts a server with the larger-than-memory string tier engaged (a
// segmented arena, a cold record region, and the background migrator) on a deliberately small
// arena, then returns a connected client plus a cleanup. It is the server-level analog of the
// engine's churnSegColdStore: the arena is far smaller than the dataset the test writes, so the
// migrator has to sink full segments cold for every write to keep landing.
func dialMigratorServer(t *testing.T) (*bufio.ReadWriter, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		Addr:               "127.0.0.1:0",
		IndexBuckets:       1 << 14,
		ArenaBytes:         4 << 20, // 4 MiB: smaller than the dataset below, so the migrator must drain
		ReadBufSize:        16 << 10,
		IncrStripes:        64,
		ArenaSegmented:     true,
		ArenaSegmentBytes:  256 << 10, // small segments so the 4 MiB arena tiles into many drainable segments
		ArenaOverflowBytes: 256 << 10,
		ColdRecordsPath:    filepath.Join(dir, "f1raw-cold.recs"),
		Migrator:           true,
	}
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.ListenAndServe()
	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	cleanup := func() {
		conn.Close()
		srv.Close()
	}
	return rw, cleanup
}

// TestServerMigratorServesBeyondArena is the M3 slice-6b gate: with the migrator wired into the
// server, a string dataset larger than the resident arena is served correctly over the wire. It
// writes many more distinct keys than the 2 MiB arena can hold at once, so the migrator sinks
// full segments into the cold record region as they fill, and every SET must still return OK
// (the D12 backpressure waits on the migrator rather than returning an arena-full error). Then
// it reads every key back and checks its exact value, most now served from cold. Without the
// wiring the arena fills and SET errors after a few thousand keys.
func TestServerMigratorServesBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// Each value is 200 bytes, so the whole set is several arenas' worth of record bytes and the
	// arena cannot hold it at once. Pipeline the writes so the test is not one round trip per key.
	const n = 20000
	val := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		cmd(t, rw, "SET", fmt.Sprintf("k%08d", i), migVal(i, val))
	}
	for i := 0; i < n; i++ {
		expect(t, rw, "+OK")
	}

	// Every distinct key reads back its exact value, whether it ended up resident or cold.
	for i := 0; i < n; i++ {
		cmd(t, rw, "GET", fmt.Sprintf("k%08d", i))
	}
	for i := 0; i < n; i++ {
		want := "$" + migVal(i, val)
		if got := readReply(t, rw); got != want {
			t.Fatalf("GET k%08d = %q, want %q", i, got, want)
		}
	}
}

// TestServerMigratorServesHashBeyondArena is the D22 Option B gate at the server level: with the
// hash field kind admitted to the migrator (isMigratableKind), a hash whose field rows outgrow the
// resident arena is served correctly over the wire, most fields now cold. It fills one hash with
// far more large fields than the small arena can hold, so the migrator sinks full segments of field
// rows into the cold record region as they fill while the hash header row stays resident, and every
// HSET must still succeed (D12 backpressure waits on the migrator rather than erroring). Then HGET
// reads each field back across the tier, and a single HGETALL re-resolves every field through the
// tier-aware ordered index and reads its value from whichever tier holds it. Without the admission
// the field rows cannot migrate, the arena fills, and HSET errors partway through the load.
func TestServerMigratorServesHashBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// One hash, many 200-byte fields: the field rows are several arenas' worth of record bytes and
	// cannot all stay resident, while the single header row pins at most one segment. Pipeline the
	// writes so the load is not one round trip per field.
	const n = 20000
	val := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		cmd(t, rw, "HSET", "h", fmt.Sprintf("f%08d", i), migVal(i, val))
	}
	for i := 0; i < n; i++ {
		expect(t, rw, ":1") // each field is new, so HSET reports one field added
	}

	// Point path: HGET reads each field back, most from the cold region, all exact.
	for i := 0; i < n; i++ {
		cmd(t, rw, "HGET", "h", fmt.Sprintf("f%08d", i))
	}
	for i := 0; i < n; i++ {
		want := "$" + migVal(i, val)
		if got := readReply(t, rw); got != want {
			t.Fatalf("HGET h f%08d = %q, want %q", i, got, want)
		}
	}

	// Enumeration path: one HGETALL returns every field with its exact value, re-resolving the
	// migrated ones through the tier-aware ordered index.
	cmd(t, rw, "HGETALL", "h")
	got := readArrayMap(t, rw)
	if len(got) != n {
		t.Fatalf("HGETALL returned %d fields, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%08d", i)
		if got[f] != migVal(i, val) {
			t.Fatalf("HGETALL field %s = %q, want %q", f, got[f], migVal(i, val))
		}
	}
}

// TestServerMigratorServesZsetBeyondArena is the D22 Option B gate for the zset at the server level:
// with both zset element kinds admitted to the migrator (isMigratableKind), a sorted set whose
// element rows outgrow the resident arena is served correctly over the wire, most rows now cold. It
// fills one zset with far more large members than the small arena can hold, so the migrator sinks
// full segments of member and score rows into the cold record region as they fill while the zset
// header row stays resident, and every ZADD must still succeed (D12 backpressure waits on the
// migrator rather than erroring). Then ZSCORE reads each member's score back across the tier through
// the primary index, and a single ZRANGE WITHSCORES re-resolves every element through the tier-aware
// ordered index and reads its member from whichever tier holds it. Without the admission the element
// rows cannot migrate, the arena fills, and ZADD errors partway through the load.
func TestServerMigratorServesZsetBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// One zset, many large members: each member string carries the load, so both the member row and
	// the score row are several arenas' worth of record bytes and cannot all stay resident, while the
	// single header row pins at most one segment. Scores are the index i, and the member's %08d prefix
	// makes lexical member order agree with score order, so a by-rank range returns them in i order.
	// Pipeline the writes so the load is not one round trip per member.
	const n = 20000
	val := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		cmd(t, rw, "ZADD", "z", strconv.Itoa(i), migVal(i, val))
	}
	for i := 0; i < n; i++ {
		expect(t, rw, ":1") // each member is new, so ZADD reports one added
	}

	// Point path: ZSCORE reads each member's score back, most members now cold, all exact. An integer
	// score comes back as its plain decimal bulk string (ZSCORE board bob -> $2 in the point-path test).
	for i := 0; i < n; i++ {
		cmd(t, rw, "ZSCORE", "z", migVal(i, val))
	}
	for i := 0; i < n; i++ {
		want := "$" + strconv.Itoa(i)
		if got := readReply(t, rw); got != want {
			t.Fatalf("ZSCORE z member(%d) = %q, want %q", i, got, want)
		}
	}

	// Enumeration path: one ZRANGE WITHSCORES returns every member with its exact score, re-resolving
	// the migrated ones through the tier-aware ordered index. WITHSCORES interleaves member and score,
	// which readArrayMap reads as a member->score map.
	cmd(t, rw, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	got := readArrayMap(t, rw)
	if len(got) != n {
		t.Fatalf("ZRANGE WITHSCORES returned %d members, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		m := migVal(i, val)
		if got[m] != strconv.Itoa(i) {
			t.Fatalf("ZRANGE member(%d) score = %q, want %q", i, got[m], strconv.Itoa(i))
		}
	}
}

// TestServerMigratorServesListBeyondArena is the D22 Option B gate for the list at the server level:
// with the list element kind admitted to the migrator (isMigratableKind), a list whose element rows
// outgrow the resident arena is served correctly over the wire, most rows now cold. It pushes far
// more large elements than the small arena can hold, so the migrator sinks full segments of element
// rows into the cold record region as they fill while the list header row stays resident, and every
// RPUSH must still succeed (D12 backpressure waits on the migrator rather than erroring). Then LINDEX
// reads each element back across the tier through the primary index, and a single LRANGE 0 -1 re-reads
// every element in order. The list keeps no secondary structure at all, so each element read is a
// by-key GetKind on its positional key, which follows a migrated row across the tier. Without the
// admission the element rows cannot migrate, the arena fills, and RPUSH errors partway through the load.
func TestServerMigratorServesListBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// One list, many 200-byte elements: the element rows are several arenas' worth of record bytes and
	// cannot all stay resident, while the single header row pins at most one segment. Pipeline the
	// pushes so the load is not one round trip per element.
	const n = 20000
	val := strings.Repeat("x", 200)
	for i := 0; i < n; i++ {
		cmd(t, rw, "RPUSH", "l", migVal(i, val))
	}
	for i := 0; i < n; i++ {
		expect(t, rw, ":"+strconv.Itoa(i+1)) // RPUSH returns the new length after each append
	}

	// Point path: LINDEX reads each element back by position, most now cold, all exact.
	for i := 0; i < n; i++ {
		cmd(t, rw, "LINDEX", "l", strconv.Itoa(i))
	}
	for i := 0; i < n; i++ {
		want := "$" + migVal(i, val)
		if got := readReply(t, rw); got != want {
			t.Fatalf("LINDEX l %d = %q, want %q", i, got, want)
		}
	}

	// Enumeration path: one LRANGE 0 -1 returns every element in push order, re-reading each from
	// whichever tier holds it.
	cmd(t, rw, "LRANGE", "l", "0", "-1")
	got := readArray(t, rw)
	if len(got) != n {
		t.Fatalf("LRANGE returned %d elements, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != migVal(i, val) {
			t.Fatalf("LRANGE element %d = %q, want %q", i, got[i], migVal(i, val))
		}
	}
}

// TestServerMigratorServesStreamBeyondArena is the D22 Option B gate for the stream at the server
// level: with the four stream element kinds admitted to the migrator (isMigratableKind), a stream
// whose entry rows outgrow the resident arena is served correctly over the wire, most rows now cold.
// It appends far more large entries than the small arena can hold, so the migrator sinks full
// segments of entry rows into the cold record region as they fill while the stream header row stays
// resident, and every XADD must still succeed (D12 backpressure waits on the migrator rather than
// erroring). Then a per-entry XRANGE reads each entry back across the tier by its exact ID, and one
// full XRANGE - + re-reads every entry in ID order. Each entry value is read with a by-key GetKind
// off the ordered entry index, so a migrated entry row is followed across the tier. Without the
// admission the entry rows cannot migrate, the arena fills, and XADD errors partway through the load.
func TestServerMigratorServesStreamBeyondArena(t *testing.T) {
	rw, cleanup := dialMigratorServer(t)
	defer cleanup()

	// One stream, many 200-byte entry values under a single field: each entry row is arena-sized and
	// they cannot all stay resident, while the single header row pins at most one segment. Explicit
	// strictly increasing IDs keep insertion order equal to ID order, so a forward XRANGE returns
	// them in i order. Pipeline the appends so the load is not one round trip per entry.
	const n = 20000
	val := strings.Repeat("x", 200)
	idOf := func(i int) string { return strconv.Itoa(i+1) + "-1" }
	for i := 0; i < n; i++ {
		cmd(t, rw, "XADD", "st", idOf(i), "f", migVal(i, val))
	}
	for i := 0; i < n; i++ {
		if got := readReply(t, rw); got != "$"+idOf(i) {
			t.Fatalf("XADD %d = %q, want %q", i, got, "$"+idOf(i))
		}
	}

	// The header count stays exact across the tier.
	cmd(t, rw, "XLEN", "st")
	expect(t, rw, ":"+strconv.Itoa(n))

	// Point path: a single-ID XRANGE reads a spread of entries back, most now cold, all exact. One
	// entry serializes as [[id [f val]]]. A single-ID XRANGE resolves its bounds through the ordered
	// entry index by rank, whose cold-tier descent re-resolves each probed node through the primary
	// index, so sampling a spread of IDs proves the tier-following point read without paying an
	// O(n log n) rank search for every one of the n entries (the full enumeration below covers them
	// all in one O(n) forward scan).
	sample := make([]int, 0, 64)
	for i := 0; i < n; i += n / 64 {
		sample = append(sample, i)
	}
	sample = append(sample, n-1)
	for _, i := range sample {
		cmd(t, rw, "XRANGE", "st", idOf(i), idOf(i))
	}
	for _, i := range sample {
		want := "[[" + idOf(i) + " [f " + migVal(i, val) + "]]]"
		if got := readReplyDeep(t, rw); got != want {
			t.Fatalf("XRANGE st %s = %q, want %q", idOf(i), got, want)
		}
	}

	// Enumeration path: one XRANGE - + returns every entry in ID order, re-reading each from
	// whichever tier holds it.
	cmd(t, rw, "XRANGE", "st", "-", "+")
	h := readReply(t, rw)
	if h != "*"+strconv.Itoa(n) {
		t.Fatalf("XRANGE - + header = %q, want %q", h, "*"+strconv.Itoa(n))
	}
	for i := 0; i < n; i++ {
		want := "[" + idOf(i) + " [f " + migVal(i, val) + "]]"
		if got := readReplyDeep(t, rw); got != want {
			t.Fatalf("XRANGE - + entry %d = %q, want %q", i, got, want)
		}
	}
}

// TestServerMigratorConfigError checks that asking for the migrator without the segmented arena
// it needs is a clean configuration error surfaced by Listen, not a panic from EnableMigrator.
func TestServerMigratorConfigError(t *testing.T) {
	cfg := Config{
		Addr:            "127.0.0.1:0",
		IndexBuckets:    1 << 12,
		ArenaBytes:      1 << 20,
		ReadBufSize:     4 << 10,
		IncrStripes:     64,
		Migrator:        true, // but ArenaSegmented is false and ColdRecordsPath is empty
		ColdRecordsPath: "",
	}
	srv := New(cfg)
	if err := srv.Listen(); err == nil {
		srv.Close()
		t.Fatal("Listen accepted a migrator config with no segmented arena; want an error")
	}
}

// migVal tags the shared value body with the key index so a misrouted read (wrong key served)
// shows up as a value mismatch, not just a length match.
func migVal(i int, body string) string {
	return fmt.Sprintf("v%08d-%s", i, body)
}
