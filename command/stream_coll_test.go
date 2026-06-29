package command

import (
	"bufio"
	"fmt"
	"strconv"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/networking"
)

// A stream past streamCollThreshold entries lives in the btree-backed
// element-per-row form (stream_tree.go), the same machinery hash/set/zset/list
// use. These tests pin two properties of slice 1: the hot commands (XADD, XLEN,
// XRANGE, XREVRANGE, XREAD) keep the key in coll form rather than demoting it
// back to a single blob, and they return exactly what the blob path would. A
// third test witnesses that XADD on a large coll stream allocates a small
// constant, not O(entries): the materialize trap that demoting would reintroduce.

// streamIsColl reports whether key is stored in the btree-backed element-per-row
// form rather than as a single inline/overflow blob. OBJECT ENCODING on a stream
// always reports "stream", so the only way to see the storage form is to probe
// the value header directly.
func streamIsColl(t *testing.T, eng *Engine, key string) bool {
	t.Helper()
	var coll, found bool
	if err := eng.view(0, func(db *keyspace.DB) error {
		hdr, ok, err := streamHeader(db, []byte(key))
		if err != nil {
			return err
		}
		found = ok
		coll = ok && hdr.IsColl()
		return nil
	}); err != nil {
		t.Fatalf("view %q: %v", key, err)
	}
	if !found {
		t.Fatalf("key %q absent", key)
	}
	return coll
}

// TestStreamCollPromotesAndKeepsForm pushes a stream past the promote threshold
// and checks it lands in coll form, then runs each hot command and re-checks the
// form after every one. The trap is that a command routed through getStream then
// storeStream(blob) would demote the key, so the next read re-materializes the
// whole stream: O(n) alloc and an OOM kill under a tight cap. XADD, XLEN, XRANGE,
// XREVRANGE and XREAD must all leave the key coll.
func TestStreamCollPromotesAndKeepsForm(t *testing.T) {
	r, c, eng := startDataEng(t)
	const n = streamCollThreshold + 200

	for i := 1; i <= n; i++ {
		if got := bulk(t, r, c, fmt.Sprintf("XADD s %d-1 f %d", i, i)); got != fmt.Sprintf("%d-1", i) {
			t.Fatalf("XADD %d = %q", i, got)
		}
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("stream not in coll form after %d entries", n)
	}

	// Each hot command must leave the key coll.
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("XLEN = %q want :%d", got, n)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XLEN demoted the stream to blob")
	}
	_ = xentries(t, r, c, "XRANGE s - + COUNT 5")
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XRANGE demoted the stream to blob")
	}
	_ = xentries(t, r, c, "XREVRANGE s + - COUNT 5")
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREVRANGE demoted the stream to blob")
	}
	xreadDrain(t, r, sendLine(t, r, c, "XREAD COUNT 5 STREAMS s 0"))
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREAD demoted the stream to blob")
	}
	// One more append on the coll stream stays coll and lands at the tail.
	if got := bulk(t, r, c, fmt.Sprintf("XADD s %d-1 f tail", n+1)); got != fmt.Sprintf("%d-1", n+1) {
		t.Fatalf("XADD tail = %q", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XADD on coll stream demoted it to blob")
	}
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", n+1) {
		t.Fatalf("XLEN after tail append = %q want :%d", got, n+1)
	}
}

// xreadDrain consumes an XREAD reply given its outer header, so the next command
// reads from a clean buffer. A null reply (*-1 or _) has no body. Otherwise each
// stream is a two-element [name, entries] pair, and each entry is [id, fields].
func xreadDrain(t *testing.T, r *bufio.Reader, hdr string) {
	t.Helper()
	if hdr == "*-1" || hdr == "_" {
		return
	}
	for range arrayLen(t, hdr) {
		if got := sendLineRead(t, r); got != "*2" {
			t.Fatalf("XREAD pair header = %q want *2", got)
		}
		_ = readBulkRaw(t, r) // stream name
		eh := sendLineRead(t, r)
		for range arrayLen(t, eh) {
			if got := sendLineRead(t, r); got != "*2" {
				t.Fatalf("XREAD entry header = %q want *2", got)
			}
			_ = readBulkRaw(t, r) // id
			fh := sendLineRead(t, r)
			for range arrayLen(t, fh) {
				_ = readBulkRaw(t, r)
			}
		}
	}
}

// TestStreamCollMatchesBlob seeds the same explicit-ID entries in a small stream
// (blob form, below threshold) and a large stream (coll form, above threshold)
// and checks that XLEN, XRANGE, XREVRANGE with and without COUNT, exclusive
// bounds, and XREAD return the same shape from the coll path as from the blob
// path. The large stream's leading entries mirror the small one's, so the same
// queries over the shared prefix must agree.
func TestStreamCollMatchesBlob(t *testing.T) {
	r, c, eng := startDataEng(t)

	// Small stream: 5 entries, stays blob.
	for i := 1; i <= 5; i++ {
		_ = bulk(t, r, c, fmt.Sprintf("XADD small %d-0 f v%d", i, i))
	}
	if streamIsColl(t, eng, "small") {
		t.Fatalf("small stream unexpectedly in coll form")
	}
	// Large stream: same first 5 IDs then enough more to cross the threshold.
	const n = streamCollThreshold + 50
	for i := 1; i <= n; i++ {
		_ = bulk(t, r, c, fmt.Sprintf("XADD big %d-0 f v%d", i, i))
	}
	if !streamIsColl(t, eng, "big") {
		t.Fatalf("big stream not in coll form")
	}

	// XRANGE over the shared 1..5 prefix must match between the two forms.
	small := xentries(t, r, c, "XRANGE small 1 5")
	big := xentries(t, r, c, "XRANGE big 1-0 5-0")
	if !sameEntries(small, big) {
		t.Fatalf("XRANGE prefix mismatch: blob=%v coll=%v", small, big)
	}
	// COUNT caps the coll walk; first three entries ascending.
	got := xentries(t, r, c, "XRANGE big - + COUNT 3")
	want := [][]string{{"1-0", "f", "v1"}, {"2-0", "f", "v2"}, {"3-0", "f", "v3"}}
	if !sameEntries(got, want) {
		t.Fatalf("XRANGE big - + COUNT 3 = %v want %v", got, want)
	}
	// XREVRANGE returns highest first; COUNT walks down from the end.
	got = xentries(t, r, c, "XREVRANGE big + - COUNT 3")
	want = [][]string{
		{strconv.Itoa(n) + "-0", "f", "v" + strconv.Itoa(n)},
		{strconv.Itoa(n-1) + "-0", "f", "v" + strconv.Itoa(n-1)},
		{strconv.Itoa(n-2) + "-0", "f", "v" + strconv.Itoa(n-2)},
	}
	if !sameEntries(got, want) {
		t.Fatalf("XREVRANGE big + - COUNT 3 = %v want %v", got, want)
	}
	// Exclusive bounds skip the endpoints.
	got = xentries(t, r, c, "XRANGE big (1-0 (4-0")
	want = [][]string{{"2-0", "f", "v2"}, {"3-0", "f", "v3"}}
	if !sameEntries(got, want) {
		t.Fatalf("XRANGE big (1-0 (4-0 = %v want %v", got, want)
	}
	// XLEN matches the seeded count on both forms.
	if got := sendLine(t, r, c, "XLEN small"); got != ":5" {
		t.Fatalf("XLEN small = %q want :5", got)
	}
	if got := sendLine(t, r, c, "XLEN big"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("XLEN big = %q want :%d", got, n)
	}
}

// sameEntries reports whether two parsed entry lists are equal element for
// element.
func sameEntries(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// TestStreamCollMaterializePathStaysCorrect drives the commands slice 1 leaves on
// the materialize-and-rebuild path (XDEL, XTRIM, XSETID) over a coll-form stream.
// They are not yet bounded, but they must stay correct and must not demote the key
// to a blob: the coll-aware storeStream rewrites the sub-tree in place while the
// entry count holds above the threshold. We check the count, the surviving entries
// and the form after each.
func TestStreamCollMaterializePathStaysCorrect(t *testing.T) {
	r, c, eng := startDataEng(t)
	const n = streamCollThreshold + 100
	for i := 1; i <= n; i++ {
		_ = bulk(t, r, c, fmt.Sprintf("XADD s %d-0 f v%d", i, i))
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("stream not coll after seed")
	}

	// XDEL drops two specific entries; the count falls by two and they vanish.
	if got := sendLine(t, r, c, "XDEL s 3-0 7-0"); got != ":2" {
		t.Fatalf("XDEL = %q want :2", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", n-2) {
		t.Fatalf("XLEN after XDEL = %q want :%d", got, n-2)
	}
	if got := xentries(t, r, c, "XRANGE s 2-0 4-0"); !sameEntries(got, [][]string{{"2-0", "f", "v2"}, {"4-0", "f", "v4"}}) {
		t.Fatalf("XRANGE around deleted 3-0 = %v", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XDEL demoted the stream to blob")
	}

	// XTRIM MAXLEN keeps the highest entries, still above the threshold.
	keep := streamCollThreshold + 10
	if got := sendLine(t, r, c, fmt.Sprintf("XTRIM s MAXLEN %d", keep)); got != fmt.Sprintf(":%d", n-2-keep) {
		t.Fatalf("XTRIM = %q want :%d", got, n-2-keep)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", keep) {
		t.Fatalf("XLEN after XTRIM = %q want :%d", got, keep)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XTRIM demoted the stream to blob")
	}
	// The highest entry survives, the lowest was trimmed.
	if got := xentries(t, r, c, "XREVRANGE s + - COUNT 1"); !sameEntries(got, [][]string{{strconv.Itoa(n) + "-0", "f", "v" + strconv.Itoa(n)}}) {
		t.Fatalf("highest entry after XTRIM = %v", got)
	}

	// XSETID rewrites the last ID; a following XADD * must advance past it.
	if got := sendLine(t, r, c, "XSETID s 100000-0"); got != "+OK" {
		t.Fatalf("XSETID = %q", got)
	}
	id := bulk(t, r, c, "XADD s * f after")
	if id < "100000-1" {
		t.Fatalf("XADD * after XSETID = %q want >= 100000-1", id)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XSETID/XADD demoted the stream to blob")
	}
}

// TestStreamCollDelTrimSetID drives the slice-2 bounded write commands over a
// coll-form stream: XDEL point-deletes entry rows, XTRIM range-deletes from the
// low end, and XSETID edits the header. Each must stay correct, keep the key coll
// (no demote), and an emptied stream must still exist with its last ID rather than
// vanish.
func TestStreamCollDelTrimSetID(t *testing.T) {
	r, c, eng := startDataEng(t)
	seed := func(n int) {
		_ = sendLine(t, r, c, "DEL s")
		for i := 1; i <= n; i++ {
			_ = bulk(t, r, c, fmt.Sprintf("XADD s %d-0 f v%d", i, i))
		}
		if !streamIsColl(t, eng, "s") {
			t.Fatalf("stream not coll after seeding %d", n)
		}
	}
	const n = streamCollThreshold + 100

	// XDEL removes named entries and keeps the key coll.
	seed(n)
	if got := sendLine(t, r, c, "XDEL s 10-0 20-0 999-0"); got != ":2" {
		t.Fatalf("XDEL = %q want :2 (999-0 absent)", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", n-2) {
		t.Fatalf("XLEN after XDEL = %q want :%d", got, n-2)
	}
	if got := xentries(t, r, c, "XRANGE s 9-0 11-0"); !sameEntries(got, [][]string{{"9-0", "f", "v9"}, {"11-0", "f", "v11"}}) {
		t.Fatalf("XRANGE around deleted 10-0 = %v", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XDEL demoted to blob")
	}

	// XTRIM MAXLEN keeps the highest entries; the key stays coll.
	seed(n)
	keep := streamCollThreshold + 10
	if got := sendLine(t, r, c, fmt.Sprintf("XTRIM s MAXLEN %d", keep)); got != fmt.Sprintf(":%d", n-keep) {
		t.Fatalf("XTRIM = %q want :%d", got, n-keep)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", keep) {
		t.Fatalf("XLEN after XTRIM = %q want :%d", got, keep)
	}
	// The lowest surviving entry is n-keep+1; everything below was trimmed.
	low := n - keep + 1
	if got := xentries(t, r, c, "XRANGE s - + COUNT 1"); !sameEntries(got, [][]string{{fmt.Sprintf("%d-0", low), "f", "v" + strconv.Itoa(low)}}) {
		t.Fatalf("lowest entry after XTRIM = %v want %d-0", got, low)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XTRIM demoted to blob")
	}

	// XSETID rejects an ID below the highest present entry, accepts one above, and
	// a following XADD * advances past it.
	seed(n)
	if got := sendLine(t, r, c, "XSETID s 5-0"); got != "-"+errStreamSetIDSmall {
		t.Fatalf("XSETID below highest = %q want too-small error", got)
	}
	if got := sendLine(t, r, c, "XSETID s 1000000-0"); got != "+OK" {
		t.Fatalf("XSETID above = %q", got)
	}
	id := bulk(t, r, c, "XADD s * f after")
	if id < "1000000-1" {
		t.Fatalf("XADD * after XSETID = %q want >= 1000000-1", id)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XSETID demoted to blob")
	}

	// Emptying a coll stream by XDEL leaves an existing 0-length stream that keeps
	// its last ID, so the next XADD * advances past it.
	seed(n)
	for i := 1; i <= n; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("XDEL s %d-0", i))
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":0" {
		t.Fatalf("XLEN after deleting all = %q want :0", got)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":1" {
		t.Fatalf("EXISTS after emptying by XDEL = %q want :1 (an empty stream still exists)", got)
	}
	// The last ID survives the empty, so a small explicit ID is still rejected as
	// not greater than the highest ever seen (n-0).
	if got := sendLine(t, r, c, "XADD s 50-0 f x"); got != "-"+errStreamIDSmaller {
		t.Fatalf("XADD small ID after emptying by XDEL = %q want %q (last ID must persist)", got, errStreamIDSmaller)
	}

	// Emptying a coll stream by XTRIM MAXLEN 0 also keeps the key.
	seed(n)
	if got := sendLine(t, r, c, "XTRIM s MAXLEN 0"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("XTRIM MAXLEN 0 = %q want :%d", got, n)
	}
	if got := sendLine(t, r, c, "EXISTS s"); got != ":1" {
		t.Fatalf("EXISTS after XTRIM MAXLEN 0 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XLEN s"); got != ":0" {
		t.Fatalf("XLEN after XTRIM MAXLEN 0 = %q want :0", got)
	}
}

// TestStreamCollDelBounded witnesses that XDEL on a large coll stream allocates a
// small constant, not O(entries): it point-deletes the named rows rather than
// cloning the whole stream. We delete one entry and re-add it each run so the
// stream size holds steady.
func TestStreamCollDelBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	add := func(i int) {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XADD"), []byte("s"),
			[]byte(strconv.Itoa(i) + "-0"), []byte("f"), append([]byte("v"), pad...)})
	}
	for i := 1; i <= n; i++ {
		add(i)
	}

	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XDEL"), []byte("s"), []byte("1500-0")})
		add(1500)
	})
	if allocs > 400 {
		t.Fatalf("XDEL of one entry on a %d-entry stream allocated %.0f objects per run "+
			"(re-add included); a point delete should be a small constant, not O(n)", n, allocs)
	}
}

// TestStreamCollAddBounded witnesses that XADD on a large coll stream allocates a
// small constant, not O(entries). The demote trap is that routing XADD through
// getStream clones every entry onto the heap and storeStream(blob) rewrites the
// whole body, so an append to a million-entry stream moves the whole thing and an
// OOM kill follows under a tight cap. The bounded path writes one entry row and
// rewrites only the small header, so per-XADD allocation is independent of the
// entry count. We trim with MAXLEN so the stream size holds steady across runs.
func TestStreamCollAddBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	for i := 1; i <= n; i++ {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XADD"), []byte("s"),
			[]byte(strconv.Itoa(i) + "-0"), []byte("f"), append([]byte("v"), pad...)})
	}

	// Append-with-trim holds the size at n, so a whole-stream clone would move
	// about a megabyte each run while the bounded path moves one row plus the
	// header. id climbs so each XADD is a fresh tail.
	id := n
	allocs := testing.AllocsPerRun(50, func() {
		id++
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XADD"), []byte("s"), []byte("MAXLEN"), []byte(strconv.Itoa(n)),
			[]byte(strconv.Itoa(id) + "-0"), []byte("f"), append([]byte("v"), pad...)})
	})
	if allocs > 400 {
		t.Fatalf("XADD with MAXLEN trim on a %d-entry stream allocated %.0f objects per run; "+
			"a bounded append should be a small constant, not O(n)", n, allocs)
	}
}

// TestStreamCollGroupsMetadata drives the slice-3a consumer-group metadata
// commands over a coll-form stream: XGROUP CREATE/SETID/CREATECONSUMER/DELCONSUMER/
// DESTROY, XACK, XPENDING, and XINFO GROUPS/CONSUMERS. These touch only the group
// state in the header row, never the entry log, so each must stay correct and
// leave the key coll (no demote, no materialize). XREADGROUP delivers entries to
// build a PEL (it is not bounded until a later slice, but it must still keep the
// key coll so the metadata commands see a coll stream).
func TestStreamCollGroupsMetadata(t *testing.T) {
	r, c, eng := startDataEng(t)
	const n = streamCollThreshold + 100
	for i := 1; i <= n; i++ {
		_ = bulk(t, r, c, fmt.Sprintf("XADD s %d-0 f v%d", i, i))
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("stream not coll after seeding %d", n)
	}

	if got := sendLine(t, r, c, "XGROUP CREATE s g 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XGROUP CREATE demoted to blob")
	}
	// Deliver the first five entries into the group PEL.
	xreadDrain(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons COUNT 5 STREAMS s >"))
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREADGROUP demoted to blob")
	}

	// XPENDING summary reports five pending owned by cons.
	if got := sendLine(t, r, c, "XPENDING s g"); got != "*4" {
		t.Fatalf("XPENDING summary header = %q want *4", got)
	}
	if got := sendLineRead(t, r); got != ":5" {
		t.Fatalf("XPENDING pending count = %q want :5", got)
	}
	_ = readBulkRaw(t, r) // min id
	_ = readBulkRaw(t, r) // max id
	consumers := sendLineRead(t, r)
	if consumers != "*1" {
		t.Fatalf("XPENDING consumers header = %q want *1", consumers)
	}
	if got := sendLineRead(t, r); got != "*2" {
		t.Fatalf("XPENDING consumer pair header = %q want *2", got)
	}
	_ = readBulkRaw(t, r) // consumer name
	if got := readBulkRaw(t, r); got != "5" {
		t.Fatalf("XPENDING consumer count = %q want 5", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XPENDING demoted to blob")
	}

	// XACK two of the five; three remain pending.
	if got := sendLine(t, r, c, "XACK s g 1-0 2-0"); got != ":2" {
		t.Fatalf("XACK = %q want :2", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XACK demoted to blob")
	}
	if got := sendLine(t, r, c, "XPENDING s g"); got != "*4" {
		t.Fatalf("XPENDING after XACK header = %q want *4", got)
	}
	if got := sendLineRead(t, r); got != ":3" {
		t.Fatalf("XPENDING after XACK count = %q want :3", got)
	}
	_ = readBulkRaw(t, r)
	_ = readBulkRaw(t, r)
	_ = sendLineRead(t, r) // consumers array
	_ = sendLineRead(t, r)
	_ = readBulkRaw(t, r)
	_ = readBulkRaw(t, r)

	// XINFO GROUPS reports one group with three pending.
	if got := sendLine(t, r, c, "XINFO GROUPS s"); got != "*1" {
		t.Fatalf("XINFO GROUPS header = %q want *1", got)
	}
	groupInfo := drainFlat(t, r)
	if pending := flatField(groupInfo, "pending"); pending != ":3" {
		t.Fatalf("XINFO GROUPS pending = %q want :3", pending)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XINFO GROUPS demoted to blob")
	}

	// CREATECONSUMER / DELCONSUMER round-trip on the header row.
	if got := sendLine(t, r, c, "XGROUP CREATECONSUMER s g cons2"); got != ":1" {
		t.Fatalf("XGROUP CREATECONSUMER = %q want :1", got)
	}
	if got := sendLine(t, r, c, "XGROUP DELCONSUMER s g cons2"); got != ":0" {
		t.Fatalf("XGROUP DELCONSUMER = %q want :0 (cons2 had no pending)", got)
	}
	if got := sendLine(t, r, c, "XGROUP SETID s g 0"); got != "+OK" {
		t.Fatalf("XGROUP SETID = %q", got)
	}
	if got := sendLine(t, r, c, "XGROUP DESTROY s g"); got != ":1" {
		t.Fatalf("XGROUP DESTROY = %q want :1", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XGROUP family demoted to blob")
	}
	// The stream and its entries survive group teardown.
	if got := sendLine(t, r, c, "XLEN s"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("XLEN after XGROUP DESTROY = %q want :%d", got, n)
	}
}

// drainFlat reads a flat array reply (already given its *N header is the previous
// read) into a slice of its element lines. It is used for the XINFO GROUPS map,
// whose RESP2 form is a flat [k, v, k, v, ...] array of bulk strings and integers.
func drainFlat(t *testing.T, r *bufio.Reader) []string {
	t.Helper()
	hdr := sendLineRead(t, r)
	nfields := arrayLen(t, hdr)
	out := make([]string, 0, nfields)
	for i := 0; i < nfields; i++ {
		line := sendLineRead(t, r)
		if len(line) > 0 && line[0] == '$' {
			out = append(out, readBulkBody(t, r, line))
		} else {
			out = append(out, line)
		}
	}
	return out
}

// readBulkBody returns the body of a bulk string whose $len header has already
// been read into hdr; the payload line follows on the wire.
func readBulkBody(t *testing.T, r *bufio.Reader, hdr string) string {
	t.Helper()
	if len(hdr) == 0 || hdr[0] != '$' {
		t.Fatalf("bad bulk header %q", hdr)
	}
	payload, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read bulk payload: %v", err)
	}
	return payload[:len(payload)-2]
}

// flatField returns the value line that follows key in a flat key/value list, or
// "" when the key is absent.
func flatField(flat []string, key string) string {
	for i := 0; i+1 < len(flat); i += 2 {
		if flat[i] == key {
			return flat[i+1]
		}
	}
	return ""
}

// xreadGroupEntries parses a single-stream XREADGROUP/XREAD reply (RESP2) into a
// flat list per entry: ["id", "f", "v", ...]. A tombstone row (entry deleted from
// the stream but still pending) has null fields and is returned as ["id",
// tombstoneMark]. A null-array reply (nothing delivered) returns nil.
const tombstoneMark = "<nil-fields>"

func xreadGroupEntries(t *testing.T, r *bufio.Reader, hdr string) [][]string {
	t.Helper()
	if hdr == "*-1" || hdr == "_" {
		return nil
	}
	if arrayLen(t, hdr) != 1 {
		t.Fatalf("XREADGROUP outer header = %q want one stream", hdr)
	}
	if got := sendLineRead(t, r); got != "*2" {
		t.Fatalf("XREADGROUP stream pair header = %q want *2", got)
	}
	_ = readBulkRaw(t, r) // stream name
	eh := sendLineRead(t, r)
	ne := arrayLen(t, eh)
	out := make([][]string, 0, ne)
	for range ne {
		if got := sendLineRead(t, r); got != "*2" {
			t.Fatalf("XREADGROUP entry header = %q want *2", got)
		}
		id := readBulkRaw(t, r)
		fh := sendLineRead(t, r)
		if fh == "*-1" || fh == "_" {
			out = append(out, []string{id, tombstoneMark})
			continue
		}
		fn := arrayLen(t, fh)
		row := []string{id}
		for range fn {
			row = append(row, readBulkRaw(t, r))
		}
		out = append(out, row)
	}
	return out
}

// TestStreamCollReadGroup drives XREADGROUP over a coll-form stream end to end: a >
// delivery walks a bounded entry window and appends to the PEL, an explicit-ID
// re-read point-fetches each pending entry's body from the entry rows, XACK shrinks
// the pending set, a deleted-but-pending entry comes back as a tombstone, and a
// NOACK delivery keeps no PEL. The key must stay coll throughout, and every entry
// body must match what the blob path would return.
func TestStreamCollReadGroup(t *testing.T) {
	r, c, eng := startDataEng(t)
	const n = streamCollThreshold + 100
	for i := 1; i <= n; i++ {
		_ = bulk(t, r, c, fmt.Sprintf("XADD s %d-0 f v%d", i, i))
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("stream not coll after seeding %d", n)
	}
	if got := sendLine(t, r, c, "XGROUP CREATE s g 0"); got != "+OK" {
		t.Fatalf("XGROUP CREATE = %q", got)
	}

	// > delivery: the first five entries with their bodies, walked from the group
	// last ID through the bounded entry-row window.
	got := xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons COUNT 5 STREAMS s >"))
	want := [][]string{
		{"1-0", "f", "v1"}, {"2-0", "f", "v2"}, {"3-0", "f", "v3"},
		{"4-0", "f", "v4"}, {"5-0", "f", "v5"},
	}
	if !sameEntries(got, want) {
		t.Fatalf("XREADGROUP > = %v want %v", got, want)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREADGROUP > demoted to blob")
	}

	// Explicit-ID re-read from 0: the five pending entries come back with the same
	// bodies, point-fetched from the entry rows (not an in-memory slice).
	got = xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons COUNT 10 STREAMS s 0"))
	if !sameEntries(got, want) {
		t.Fatalf("XREADGROUP explicit re-read = %v want %v", got, want)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREADGROUP explicit demoted to blob")
	}

	// XACK the first two; the explicit re-read now returns the remaining three.
	if got := sendLine(t, r, c, "XACK s g 1-0 2-0"); got != ":2" {
		t.Fatalf("XACK = %q want :2", got)
	}
	got = xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons STREAMS s 0"))
	want = [][]string{{"3-0", "f", "v3"}, {"4-0", "f", "v4"}, {"5-0", "f", "v5"}}
	if !sameEntries(got, want) {
		t.Fatalf("XREADGROUP after XACK = %v want %v", got, want)
	}

	// Delete a still-pending entry; the explicit re-read returns it as a tombstone
	// (null fields) since the PEL record survives but the entry row is gone.
	if got := sendLine(t, r, c, "XDEL s 4-0"); got != ":1" {
		t.Fatalf("XDEL 4-0 = %q want :1", got)
	}
	got = xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons STREAMS s 0"))
	want = [][]string{{"3-0", "f", "v3"}, {"4-0", tombstoneMark}, {"5-0", "f", "v5"}}
	if !sameEntries(got, want) {
		t.Fatalf("XREADGROUP with tombstone = %v want %v", got, want)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREADGROUP tombstone path demoted to blob")
	}

	// NOACK delivery on a second consumer delivers the next three and keeps no PEL.
	got = xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons2 NOACK COUNT 3 STREAMS s >"))
	want = [][]string{{"6-0", "f", "v6"}, {"7-0", "f", "v7"}, {"8-0", "f", "v8"}}
	if !sameEntries(got, want) {
		t.Fatalf("XREADGROUP NOACK > = %v want %v", got, want)
	}
	// cons2 has nothing pending: a NOACK read records no PEL entries, so the
	// explicit-ID re-read yields an empty (but present) per-stream entry list.
	if got := xreadGroupEntries(t, r, sendLine(t, r, c, "XREADGROUP GROUP g cons2 STREAMS s 0")); len(got) != 0 {
		t.Fatalf("XREADGROUP NOACK pending re-read = %v want empty", got)
	}
	if !streamIsColl(t, eng, "s") {
		t.Fatalf("XREADGROUP NOACK demoted to blob")
	}
}

// TestStreamCollReadGroupBounded witnesses that an XREADGROUP > delivery on a large
// coll stream allocates a window-bounded constant, not O(entries). Each run delivers
// one fresh entry into the group (COUNT 1, advancing the group last ID) so the work
// is one entry-row fetch plus one header-row write, never a whole-stream clone.
func TestStreamCollReadGroupBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	for i := 1; i <= n; i++ {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XADD"), []byte("s"),
			[]byte(strconv.Itoa(i) + "-0"), []byte("f"), append([]byte("v"), pad...)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte("s"), []byte("g"), []byte("0")})

	// Each run delivers one new entry with > and COUNT 1: a bounded entry-row read
	// plus a header-row write. A whole-stream clone would move about a megabyte.
	allocs := testing.AllocsPerRun(200, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XREADGROUP"), []byte("GROUP"), []byte("g"), []byte("cons"),
			[]byte("COUNT"), []byte("1"), []byte("STREAMS"), []byte("s"), []byte(">")})
	})
	if allocs > 500 {
		t.Fatalf("XREADGROUP > on a %d-entry stream allocated %.0f objects per run; "+
			"a bounded delivery should be a small constant, not O(n)", n, allocs)
	}
}

// TestStreamCollGroupMetaBounded witnesses that the consumer-group metadata
// commands on a large coll stream allocate a small constant, not O(entries). With
// the groups in the header row and getStreamGroups/storeStreamGroups touching only
// that row, a CREATECONSUMER then DELCONSUMER round-trip never clones the entry
// log. The pair is self-reversing, so the group state holds steady across runs.
func TestStreamCollGroupMetaBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	for i := 1; i <= n; i++ {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XADD"), []byte("s"),
			[]byte(strconv.Itoa(i) + "-0"), []byte("f"), append([]byte("v"), pad...)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte("s"), []byte("g"), []byte("0")})

	// Each run creates then deletes a consumer: two header-row writes, no entry
	// touch. A whole-stream clone would move about a megabyte per run.
	allocs := testing.AllocsPerRun(50, func() {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XGROUP"), []byte("CREATECONSUMER"), []byte("s"), []byte("g"), []byte("tmp")})
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("XGROUP"), []byte("DELCONSUMER"), []byte("s"), []byte("g"), []byte("tmp")})
	})
	if allocs > 500 {
		t.Fatalf("CREATECONSUMER+DELCONSUMER on a %d-entry stream allocated %.0f objects per run; "+
			"a header-row group op should be a small constant, not O(n)", n, allocs)
	}
}
