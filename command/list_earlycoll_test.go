package command

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// Early-coll moves a list into the btree-backed element-per-row form once its blob
// would spill to overflow (body over MaxInlineBody), well before the 128-entry
// quicklist threshold, so a push persists one row instead of rewriting the whole
// body. The storage form is decoupled from the reported OBJECT ENCODING: such a
// list is stored coll yet still reports listpack until it crosses the real Redis
// threshold. These tests pin that decoupling and the correctness of every list op
// across the early-coll boundary.

// startDataEng is startData plus a handle to the engine, so a white-box test can
// inspect the stored form of a key (blob vs coll), which OBJECT ENCODING hides.
// It leaves the write-behind worker off so a synchronous write is durable in the
// B-tree by the time the command reply is read, and view() sees it immediately.
func startDataEng(t *testing.T) (*bufio.Reader, net.Conn, *Engine) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "data.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	eng := NewEngine(ks)
	r, c := start(t, Config{Engine: eng})
	return r, c, eng
}

// listIsColl reports whether key is stored in the btree-backed element-per-row
// form rather than as a single inline/overflow blob.
func listIsColl(t *testing.T, eng *Engine, key string) bool {
	t.Helper()
	var coll, found bool
	if err := eng.view(0, func(db *keyspace.DB) error {
		hdr, ok, err := listHeader(db, []byte(key))
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

// elem builds a value of exactly size bytes whose last digits encode i, so order
// can be checked after it round-trips through the list.
func elem(i, size int) string {
	s := fmt.Sprintf("v%d", i)
	if len(s) >= size {
		return s
	}
	return s + strings.Repeat("x", size-len(s))
}

// TestEarlyCollStoresUnderListpack pushes a list whose blob crosses the overflow
// boundary but stays well under the entry, element-size and byte caps: it must be
// stored coll yet still report listpack, the core early-coll decoupling.
func TestEarlyCollStoresUnderListpack(t *testing.T) {
	r, c, eng := startDataEng(t)
	// 40 elements of 40 bytes is ~1.7KB of body, past MaxInlineBody (1024), but the
	// count is under 128, each element is under 64 bytes, and the listpack byte
	// estimate (~2KB) is under the 8KB cap, so the reported encoding stays listpack.
	for i := 0; i < 40; i++ {
		if got := sendLine(t, r, c, "RPUSH l "+elem(i, 40)); got != fmt.Sprintf(":%d", i+1) {
			t.Fatalf("RPUSH %d = %q", i, got)
		}
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("encoding = %q want listpack", got)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list should be stored coll once its blob spills past MaxInlineBody")
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":40" {
		t.Fatalf("LLEN = %q want :40", got)
	}
	// Order survives, read through the coll cursor.
	if got := bulk(t, r, c, "LINDEX l 0"); got != elem(0, 40) {
		t.Fatalf("LINDEX 0 = %q", got)
	}
	if got := bulk(t, r, c, "LINDEX l -1"); got != elem(39, 40) {
		t.Fatalf("LINDEX -1 = %q", got)
	}
}

// TestEarlyCollStaysBlobWhenSmall checks a short list whose body fits inline stays
// in the blob form, so tiny lists do not pay for a sub-tree.
func TestEarlyCollStaysBlobWhenSmall(t *testing.T) {
	r, c, eng := startDataEng(t)
	_ = sendLine(t, r, c, "RPUSH l a b c d e")
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("encoding = %q want listpack", got)
	}
	if listIsColl(t, eng, "l") {
		t.Fatal("small list should stay in the inline blob form")
	}
}

// TestEarlyCollBoundaryCrossings walks one list through all three forms: inline
// blob (listpack), coll (still listpack) past the overflow boundary, then coll
// (quicklist) past the entry threshold. The encoding name must flip only at the
// Redis threshold, never at the storage boundary.
func TestEarlyCollBoundaryCrossings(t *testing.T) {
	r, c, eng := startDataEng(t)

	// Pin a positive entry cap so the listpack -> quicklist flip is driven by the
	// 128-entry count, the transition this test walks. Under the default -2 byte
	// tier there is no entry cap and 40-byte elements never reach the 8KB budget.
	if got := sendLine(t, r, c, "CONFIG SET list-max-listpack-size 128"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}

	// Small: inline blob, listpack.
	_ = sendLine(t, r, c, "RPUSH l a b c")
	if listIsColl(t, eng, "l") {
		t.Fatal("3-element list should be a blob")
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("3-element encoding = %q want listpack", got)
	}

	// Grow past the overflow boundary but under the entry cap: coll, still listpack.
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("list past MaxInlineBody should be coll")
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("coll-but-small encoding = %q want listpack", got)
	}

	// Grow past the 128-entry cap: quicklist.
	for i := 40; i < 140; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding past 128 entries = %q want quicklist", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":143" {
		t.Fatalf("LLEN = %q want :143", got)
	}
}

// TestEarlyCollByteCapPromotes drives a coll list past the listpack byte cap with
// 64-byte elements while the count stays under 128, so the promotion to quicklist
// is decided by the maintained byte total, not the entry count.
func TestEarlyCollByteCapPromotes(t *testing.T) {
	r, c, _ := startDataEng(t)
	// Under the default -2 tier the only cap is the 8KB listpack byte budget, with
	// no entry cap and no per-element cap. A 64-byte element costs 67 listpack bytes
	// (2+64 encoding, 1 backlen), so the 7-byte header plus 67*n crosses 8192 at
	// n=123. Push 140 to land safely past the byte budget.
	for i := 0; i < 140; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 64))
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":140" {
		t.Fatalf("LLEN = %q want :140", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding = %q want quicklist (byte budget crossed)", got)
	}
}

// TestEarlyCollBigElementPromotes checks that pushing a single element large
// enough to carry the listpack total past the 8KB byte budget flips an
// otherwise-listpack coll list to quicklist. Redis lists have no per-element cap,
// so a big element promotes only by crossing the byte budget, not on its own size.
func TestEarlyCollBigElementPromotes(t *testing.T) {
	r, c, _ := startDataEng(t)
	// Drop the byte budget to the 4KB tier so a single page-safe element can carry
	// the listpack total past it. A list element is stored one-per-row, so it must
	// fit a 4KB btree page, which rules out a single element bigger than the budget.
	if got := sendLine(t, r, c, "CONFIG SET list-max-listpack-size -1"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("pre encoding = %q want listpack", got)
	}
	// The 40 short elements cost ~1.7KB of listpack; a 3000-byte element adds ~3KB
	// and carries the total past the 4KB budget.
	if got := sendLine(t, r, c, "RPUSH l "+elem(99, 3000)); got != ":41" {
		t.Fatalf("RPUSH big = %q", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding after 3000-byte element = %q want quicklist", got)
	}
}

// TestEarlyCollLSetPromotes checks that LSET replacing a small element with one
// large enough to carry the listpack total past the 8KB byte budget flips a
// listpack coll list to quicklist, exercising the byte-total adjustment and
// re-derived encoding in listTreeSet. Redis lists have no per-element cap.
func TestEarlyCollLSetPromotes(t *testing.T) {
	r, c, eng := startDataEng(t)
	// Drop the byte budget to the 4KB tier so a single page-safe element crosses it.
	if got := sendLine(t, r, c, "CONFIG SET list-max-listpack-size -1"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if !listIsColl(t, eng, "l") || bulk(t, r, c, "OBJECT ENCODING l") != "listpack" {
		t.Fatal("setup: want coll listpack list")
	}
	if got := sendLine(t, r, c, "LSET l 10 "+elem(7, 3000)); got != "+OK" {
		t.Fatalf("LSET = %q", got)
	}
	if got := bulk(t, r, c, "LINDEX l 10"); got != elem(7, 3000) {
		t.Fatalf("LINDEX 10 = %q", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding after big LSET = %q want quicklist", got)
	}
}

// TestEarlyCollModifyOps runs the bulk modify commands against an early-coll list
// (stored coll, reporting listpack) and checks they read and rewrite it correctly.
// These demote to a blob via getList+Set, which must free the sub-tree; the next
// push re-promotes.
func TestEarlyCollModifyOps(t *testing.T) {
	r, c, _ := startDataEng(t)
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	// LINSERT before a known pivot.
	if got := sendLine(t, r, c, "LINSERT l BEFORE "+elem(20, 40)+" mid"); got != ":41" {
		t.Fatalf("LINSERT = %q want :41", got)
	}
	if got := bulk(t, r, c, "LINDEX l 20"); got != "mid" {
		t.Fatalf("LINDEX 20 = %q want mid", got)
	}
	// LREM the inserted marker.
	if got := sendLine(t, r, c, "LREM l 0 mid"); got != ":1" {
		t.Fatalf("LREM = %q want :1", got)
	}
	// LTRIM to a window.
	if got := sendLine(t, r, c, "LTRIM l 5 14"); got != "+OK" {
		t.Fatalf("LTRIM = %q", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":10" {
		t.Fatalf("LLEN after trim = %q want :10", got)
	}
	if got := bulk(t, r, c, "LINDEX l 0"); got != elem(5, 40) {
		t.Fatalf("LINDEX 0 after trim = %q", got)
	}
	// A push past the overflow boundary re-promotes to coll.
	for i := 100; i < 140; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":50" {
		t.Fatalf("LLEN final = %q want :50", got)
	}
}

// TestEarlyCollPopStaysListpack checks that popping a coll list down keeps it coll
// and keeps it reporting listpack, never demoting the encoding (Redis keeps the
// name once set; a small early-coll list keeps its listpack name throughout).
func TestEarlyCollPopStaysListpack(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("setup: want coll list")
	}
	for i := 0; i < 30; i++ {
		if got := bulk(t, r, c, "LPOP l"); got != elem(i, 40) {
			t.Fatalf("LPOP %d = %q", i, got)
		}
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":10" {
		t.Fatalf("LLEN = %q want :10", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("encoding after pops = %q want listpack", got)
	}
}

// TestEarlyCollDumpRestore round-trips an early-coll list (coll storage, listpack
// encoding) through DUMP/RESTORE and checks the elements, order and the listpack
// encoding all survive, with the restored copy also stored coll.
func TestEarlyCollDumpRestore(t *testing.T) {
	r, c, eng := startDataEng(t)
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	_ = dumpRestoreRoundTrip(t, r, c, "l")
	if got := sendLine(t, r, c, "LLEN l"); got != ":40" {
		t.Fatalf("LLEN after restore = %q want :40", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("encoding after restore = %q want listpack", got)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("restored early-coll list should be stored coll")
	}
	if got := bulk(t, r, c, "LINDEX l 25"); got != elem(25, 40) {
		t.Fatalf("LINDEX 25 after restore = %q", got)
	}
}

// TestEarlyCollDebugReload checks an early-coll list survives DEBUG RELOAD with its
// contents, order, listpack encoding and coll storage intact, proving the 32-byte
// coll metadata (with the byte total) round-trips through the on-disk format.
func TestEarlyCollDebugReload(t *testing.T) {
	r, c, eng := startDataEng(t)
	// Drop the byte budget to the 4KB tier so a single page-safe element crosses it
	// after reload. The server config survives a reload, only the data is re-read.
	if got := sendLine(t, r, c, "CONFIG SET list-max-listpack-size -1"); got != "+OK" {
		t.Fatalf("CONFIG SET = %q", got)
	}
	for i := 0; i < 40; i++ {
		_ = sendLine(t, r, c, "RPUSH l "+elem(i, 40))
	}
	if got := sendLine(t, r, c, "DEBUG RELOAD"); got != "+OK" {
		t.Fatalf("DEBUG RELOAD = %q", got)
	}
	if got := sendLine(t, r, c, "LLEN l"); got != ":40" {
		t.Fatalf("LLEN after reload = %q want :40", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "listpack" {
		t.Fatalf("encoding after reload = %q want listpack", got)
	}
	if !listIsColl(t, eng, "l") {
		t.Fatal("reloaded early-coll list should be stored coll")
	}
	// After reload the byte total is still tracked, so an element that carries the
	// listpack total past the 4KB budget still promotes.
	if got := sendLine(t, r, c, "RPUSH l "+elem(99, 3000)); got != ":41" {
		t.Fatalf("RPUSH big after reload = %q", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING l"); got != "quicklist" {
		t.Fatalf("encoding after big push post-reload = %q want quicklist", got)
	}
}
