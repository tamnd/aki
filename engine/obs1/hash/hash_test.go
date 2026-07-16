package hash

import (
	"sort"
	"strconv"
	"strings"
	"testing"
)

// fields drains a hash through each into a fresh sorted list of "field=value"
// strings, so a test can compare contents without depending on the band's
// iteration order.
func fields(h *hash) []string {
	var out []string
	h.each(func(f, v []byte) { out = append(out, string(f)+"="+string(v)) })
	sort.Strings(out)
	return out
}

func TestInlinePointOps(t *testing.T) {
	h := newHash()
	if h.enc != encListpack {
		t.Fatalf("a fresh hash should open inline, got %s", h.enc)
	}
	if !h.set([]byte("f1"), []byte("v1")) {
		t.Fatal("first HSET of f1 should report a new field")
	}
	if h.set([]byte("f1"), []byte("v1b")) {
		t.Fatal("overwriting f1 should report not-new")
	}
	h.set([]byte("f2"), []byte("")) // empty value is legal and distinct from absent
	h.set([]byte("f3"), []byte("v3"))

	if h.card() != 3 {
		t.Fatalf("card = %d, want 3", h.card())
	}
	if v, ok := h.get([]byte("f1")); !ok || string(v) != "v1b" {
		t.Fatalf("get(f1) = %q,%v, want v1b,true", v, ok)
	}
	if v, ok := h.get([]byte("f2")); !ok || string(v) != "" {
		t.Fatalf("get(f2) = %q,%v, want empty,true", v, ok)
	}
	if _, ok := h.get([]byte("nope")); ok {
		t.Fatal("get(nope) should miss")
	}
	if !h.has([]byte("f3")) || h.has([]byte("nope")) {
		t.Fatal("has disagrees with get")
	}
	if h.strlen([]byte("f1")) != 3 || h.strlen([]byte("f2")) != 0 || h.strlen([]byte("nope")) != 0 {
		t.Fatal("strlen wrong on inline band")
	}
	if !h.del([]byte("f2")) || h.del([]byte("f2")) {
		t.Fatal("del(f2) should report true once then false")
	}
	if h.card() != 2 || h.has([]byte("f2")) {
		t.Fatalf("after del: card=%d has(f2)=%v", h.card(), h.has([]byte("f2")))
	}
	// The surviving fields ride the mid-blob splice intact.
	if v, _ := h.get([]byte("f1")); string(v) != "v1b" {
		t.Fatal("f1 lost across the f2 splice")
	}
	if v, _ := h.get([]byte("f3")); string(v) != "v3" {
		t.Fatal("f3 lost across the f2 splice")
	}
}

// The entry-count threshold: a hash stays inline through 512 fields and promotes
// on the 513th (spec 2064/f3/10 sections 2.1 and 4.2).
func TestEntryCountPromotion(t *testing.T) {
	h := newHash()
	for i := 0; i < maxListpackEntries; i++ {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"))
	}
	if h.enc != encListpack || h.card() != maxListpackEntries {
		t.Fatalf("at cap: enc=%s card=%d, want listpack %d", h.enc, h.card(), maxListpackEntries)
	}
	h.set([]byte("one-more"), []byte("v"))
	if h.enc != encHashtable {
		t.Fatalf("the 513th field should promote to hashtable, got %s", h.enc)
	}
	if h.card() != maxListpackEntries+1 {
		t.Fatalf("card after promotion = %d, want %d", h.card(), maxListpackEntries+1)
	}
	// Every field survives the replay.
	for i := 0; i < maxListpackEntries; i++ {
		if !h.has([]byte("f" + strconv.Itoa(i))) {
			t.Fatalf("f%d lost across promotion", i)
		}
	}
	if !h.has([]byte("one-more")) {
		t.Fatal("the promoting field itself was lost")
	}
	// Overwriting an existing field at cap does not promote: no new field.
	h2 := newHash()
	for i := 0; i < maxListpackEntries; i++ {
		h2.set([]byte("f"+strconv.Itoa(i)), []byte("v"))
	}
	h2.set([]byte("f0"), []byte("v2"))
	if h2.enc != encListpack {
		t.Fatalf("overwrite at cap must not promote, got %s", h2.enc)
	}
}

// The value-width threshold, both keys: a 64-byte field name or value stays
// inline, a 65-byte one promotes (spec 2064/f3/10 section 2.1).
func TestValueWidthPromotion(t *testing.T) {
	cap64 := strings.Repeat("a", maxListpackValue)
	over := strings.Repeat("b", maxListpackValue+1)

	// Value at the limit stays inline, one over promotes.
	h := newHash()
	h.set([]byte("f"), []byte(cap64))
	if h.enc != encListpack {
		t.Fatalf("a %d-byte value is at the limit, want listpack, got %s", maxListpackValue, h.enc)
	}
	h.set([]byte("g"), []byte(over))
	if h.enc != encHashtable {
		t.Fatalf("a %d-byte value should promote, got %s", maxListpackValue+1, h.enc)
	}
	if v, ok := h.get([]byte("g")); !ok || string(v) != over {
		t.Fatal("the wide value was lost across promotion")
	}

	// A field name one over the limit promotes on its own.
	h2 := newHash()
	h2.set([]byte(over), []byte("v"))
	if h2.enc != encHashtable {
		t.Fatalf("a %d-byte field name should promote, got %s", maxListpackValue+1, h2.enc)
	}
	if v, ok := h2.get([]byte(over)); !ok || string(v) != "v" {
		t.Fatal("the wide-named field was lost")
	}
}

// Native band swap-remove keeps every other field probeable, including after the
// deleted field's slot is reused by a later insert.
func TestNativeSwapRemove(t *testing.T) {
	h := forceNative(newHash())
	for i := 0; i < 300; i++ { // forceNative already promoted; this fills the table
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", h.enc)
	}
	// Delete a scattered third of the fields.
	for i := 0; i < 300; i += 3 {
		if !h.del([]byte("f" + strconv.Itoa(i))) {
			t.Fatalf("del(f%d) missed", i)
		}
	}
	for i := 0; i < 300; i++ {
		f := []byte("f" + strconv.Itoa(i))
		want := i%3 != 0
		if h.has(f) != want {
			t.Fatalf("after deletes has(f%d)=%v, want %v", i, h.has(f), want)
		}
		if want {
			if v, _ := h.get(f); string(v) != "v"+strconv.Itoa(i) {
				t.Fatalf("f%d value corrupted after swap-remove", i)
			}
		}
	}
	// Reinsert into the freed slots and confirm the value re-seats cleanly.
	for i := 0; i < 300; i += 3 {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("again"+strconv.Itoa(i)))
	}
	for i := 0; i < 300; i += 3 {
		if v, ok := h.get([]byte("f" + strconv.Itoa(i))); !ok || string(v) != "again"+strconv.Itoa(i) {
			t.Fatalf("reinserted f%d = %q,%v", i, v, ok)
		}
	}
}

// Native overwrite grows and shrinks a value in place or by re-seating it, with
// the field probe unaffected either way.
func TestNativeOverwrite(t *testing.T) {
	h := forceNative(newHash())
	h.set([]byte("k"), []byte("small"))
	if h.set([]byte("k"), []byte(strings.Repeat("z", 200))) {
		t.Fatal("overwrite reported new")
	}
	if v, _ := h.get([]byte("k")); string(v) != strings.Repeat("z", 200) {
		t.Fatal("grow-overwrite lost the value")
	}
	h.set([]byte("k"), []byte("tiny"))
	if v, _ := h.get([]byte("k")); string(v) != "tiny" {
		t.Fatal("shrink-overwrite lost the value")
	}
	if h.strlen([]byte("k")) != 4 {
		t.Fatalf("strlen after shrink = %d, want 4", h.strlen([]byte("k")))
	}
}

// forceNative promotes an empty hash to the native band by seating and removing a
// throwaway over-cap field, so a test can build native content that is logically
// identical to an inline build (spec 2064/f3/10 section 4.2, one-way).
func forceNative(h *hash) *hash {
	sentinel := []byte("\x00__f3_force_native__")
	h.set(sentinel, []byte(strings.Repeat("x", maxListpackValue+1)))
	h.del(sentinel)
	return h
}

// Removal never converts a native hash back to inline (F4, one-way).
func TestOneWayNoReconvert(t *testing.T) {
	h := forceNative(newHash())
	h.set([]byte("a"), []byte("1"))
	h.set([]byte("b"), []byte("2"))
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", h.enc)
	}
	h.del([]byte("a"))
	h.del([]byte("b"))
	if h.enc != encHashtable {
		t.Fatalf("an emptied native hash must stay hashtable, got %s", h.enc)
	}
}

// The promotion replay preserves every field-value pair exactly.
func TestPromotionPreservesContent(t *testing.T) {
	inline := newHash()
	for i := 0; i < 100; i++ {
		inline.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	before := fields(inline)
	inline.inlineToNative()
	if inline.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable", inline.enc)
	}
	if after := fields(inline); !equalStrings(before, after) {
		t.Fatalf("content changed across promotion:\n before %v\n after  %v", before, after)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
