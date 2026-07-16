package hash

import (
	"strconv"
	"testing"
)

// The HSCAN tests (spec 2064/f3/10 section 7.4), the hash twin of set/sscan_test.go.
// The internal half drives the field table's downward cursor directly to carry
// doc 20's swap-remove proof (every field present throughout a scan is returned at
// least once, and the scan terminates); the command half drives the wire verb
// through the harness for the reply shape, MATCH, NOVALUES, and the inline
// one-page case.

// drainNative pages a native hash to completion through the field-table cursor and
// returns how many times each field came back plus the page count. The guard
// catches a cursor that never reaches 0: the downward walk must terminate in about
// ceil(card/count) pages.
func drainNative(h *hash, count int) (seen map[string]int, pages int) {
	seen = map[string]int{}
	var cur uint64
	guard := h.card()/count + 8
	for {
		pages++
		next := h.scanPage(cur, count, nil, func(f, _ []byte) { seen[string(f)]++ })
		if next == 0 {
			return seen, pages
		}
		cur = next
		if pages > guard {
			return seen, pages // caller asserts pages <= guard
		}
	}
}

// TestScanPageFullPass is the base case of the carried proof: a static native hash
// paged with the downward cursor returns every field exactly once and terminates
// in the expected page count.
func TestScanPageFullPass(t *testing.T) {
	h := buildNative(pairsN(1000))
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", h.enc)
	}
	seen, pages := drainNative(h, 10)
	if pages != 100 {
		t.Fatalf("paged in %d pages, want 100 (1000 fields / count 10)", pages)
	}
	if len(seen) != 1000 {
		t.Fatalf("saw %d distinct fields, want 1000", len(seen))
	}
	for i := 0; i < 1000; i++ {
		if k := "f" + strconv.Itoa(i); seen[k] != 1 {
			t.Fatalf("field %q seen %d times, want exactly 1 on a static scan", k, seen[k])
		}
	}
}

// TestScanPageRemoveMidScan swap-removes fields during a scan and asserts every
// field that is never removed still comes back at least once, and the scan
// terminates. Swap-remove only slides the vector's last live ordinal into a
// vacated slot, so a stable field can move within the unscanned region but is
// never skipped past the cursor.
func TestScanPageRemoveMidScan(t *testing.T) {
	h := buildNative(pairsN(600))
	// Fields f0..f299 stay for the whole scan; f300..f599 are the churn victims.
	seen := map[string]int{}
	var cur uint64
	culled := false
	pages := 0
	guard := 600/10 + 300 + 8
	for {
		pages++
		next := h.scanPage(cur, 10, nil, func(f, _ []byte) { seen[string(f)]++ })
		if !culled && next != 0 && next <= uint64(h.card()/2) {
			for i := 300; i < 600; i++ {
				h.del([]byte("f" + strconv.Itoa(i)))
			}
			culled = true
		}
		if next == 0 {
			break
		}
		cur = next
		if pages > guard {
			t.Fatalf("scan ran %d pages without terminating", pages)
		}
	}
	if !culled {
		t.Fatal("scan finished before the mid-scan removal fired")
	}
	for i := 0; i < 300; i++ {
		if k := "f" + strconv.Itoa(i); seen[k] == 0 {
			t.Fatalf("field %q present throughout the scan was never returned", k)
		}
	}
}

// --- command level --------------------------------------------------------

// TestHscanInlineOnePage checks a small inline hash comes back whole in one page
// with cursor 0, in insertion order (the listpack parity the differential also
// pins), as field-value pairs.
func TestHscanInlineOnePage(t *testing.T) {
	cc := newHarness(t).NewConn()
	do(t, cc, opHset, "h", "a", "1", "b", "2", "c", "3")
	raw := do(t, cc, opHscan, "h", "0")
	arr := decodeReply(t, raw).([]any)
	if arr[0].(string) != "0" {
		t.Fatalf("inline cursor = %v, want 0", arr[0])
	}
	page := arr[1].([]any)
	// Insertion order, pairs flat: a 1 b 2 c 3.
	want := []string{"a", "1", "b", "2", "c", "3"}
	if len(page) != len(want) {
		t.Fatalf("inline page len %d, want %d (%v)", len(page), len(want), page)
	}
	for i := range want {
		if page[i].(string) != want[i] {
			t.Fatalf("inline page[%d] = %v, want %q", i, page[i], want[i])
		}
	}
	// Redis ignores the cursor on a listpack hash: any cursor returns the whole
	// page and cursor 0, so a fabricated nonzero cursor replies the full hash too.
	raw = do(t, cc, opHscan, "h", "99")
	arr = decodeReply(t, raw).([]any)
	if arr[0].(string) != "0" || len(arr[1].([]any)) != len(want) {
		t.Fatalf("inline nonzero cursor = %v, want the full page and cursor 0", arr)
	}
}

// TestHscanNativePagedFull drives a promoted hash across many pages and checks the
// union of the pages is exactly the full field set, each field once, with its
// value.
func TestHscanNativePagedFull(t *testing.T) {
	cc := newHarness(t).NewConn()
	const n = 600 // past the 512 inline cap, so native
	for i := 0; i < n; i++ {
		do(t, cc, opHset, "h", "f"+strconv.Itoa(i), "v"+strconv.Itoa(i))
	}
	wantBulk(t, doAt(t, cc, opObject, 1, "ENCODING", "h"), "hashtable")

	got := map[string]string{}
	cursor := "0"
	for pages := 0; ; pages++ {
		raw := do(t, cc, opHscan, "h", cursor, "COUNT", "7")
		arr := decodeReply(t, raw).([]any)
		page := arr[1].([]any)
		for j := 0; j+1 < len(page); j += 2 {
			f := page[j].(string)
			if _, dup := got[f]; dup {
				t.Fatalf("field %q returned twice across a static native scan", f)
			}
			got[f] = page[j+1].(string)
		}
		if arr[0].(string) == "0" {
			break
		}
		cursor = arr[0].(string)
		if pages > 100000 {
			t.Fatal("native HSCAN never terminated")
		}
	}
	if len(got) != n {
		t.Fatalf("native scan collected %d fields, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if f := "f" + strconv.Itoa(i); got[f] != "v"+strconv.Itoa(i) {
			t.Fatalf("field %q scanned with value %q, want %q", f, got[f], "v"+strconv.Itoa(i))
		}
	}
}

// TestHscanMatchAndNoValues covers the MATCH glob filter and the NOVALUES option
// (Redis 7.4): MATCH filters on the field name, NOVALUES drops the value column.
func TestHscanMatchAndNoValues(t *testing.T) {
	cc := newHarness(t).NewConn()
	do(t, cc, opHset, "h", "user:1", "a", "user:2", "b", "post:1", "c")
	// MATCH user:* returns only the two user fields, paired.
	raw := do(t, cc, opHscan, "h", "0", "MATCH", "user:*")
	page := decodeReply(t, raw).([]any)[1].([]any)
	fields := map[string]string{}
	for j := 0; j+1 < len(page); j += 2 {
		fields[page[j].(string)] = page[j+1].(string)
	}
	if len(fields) != 2 || fields["user:1"] != "a" || fields["user:2"] != "b" {
		t.Fatalf("MATCH user:* returned %v, want the two user fields", fields)
	}
	// NOVALUES: a flat field list, no values.
	raw = do(t, cc, opHscan, "h", "0", "NOVALUES")
	page = decodeReply(t, raw).([]any)[1].([]any)
	if len(page) != 3 {
		t.Fatalf("NOVALUES page len %d, want 3 fields", len(page))
	}
	names := map[string]bool{}
	for _, e := range page {
		names[e.(string)] = true
	}
	if !names["user:1"] || !names["user:2"] || !names["post:1"] {
		t.Fatalf("NOVALUES fields = %v, want all three names", names)
	}
}

// TestHscanErrors pins the argument errors: a non-numeric cursor, an unknown
// option, and a COUNT that is not a positive integer.
func TestHscanErrors(t *testing.T) {
	cc := newHarness(t).NewConn()
	do(t, cc, opHset, "h", "a", "1")
	wantErr(t, do(t, cc, opHscan, "h", "notacursor"), "ERR invalid cursor")
	wantErr(t, do(t, cc, opHscan, "h", "0", "BOGUS"), "ERR syntax error")
	wantErr(t, do(t, cc, opHscan, "h", "0", "COUNT", "0"), "ERR syntax error")
	wantErr(t, do(t, cc, opHscan, "h", "0", "MATCH"), "ERR syntax error")
	// A missing key scans empty: cursor 0, no fields.
	raw := do(t, cc, opHscan, "missing", "0")
	arr := decodeReply(t, raw).([]any)
	if arr[0].(string) != "0" || len(arr[1].([]any)) != 0 {
		t.Fatalf("HSCAN on a missing key = %v, want [0 []]", arr)
	}
}
