package hash

import (
	"strconv"
	"strings"
	"testing"
)

// Field-TTL coverage (spec 2064/f3/10 section 6). The band-level tests drive a
// fixed clock straight into the hash struct, so reaping, the next-expire hint,
// and the two storage layouts are all deterministic without a sleep. The harness
// tests then confirm the command surface frames its per-field status arrays the
// way Redis does; the live differential in redis_test.go byte-compares the same
// replies against a real 8.8 server.

// a far-future absolute expiry (year 2191), well under the 2^46-1 ms ceiling, so a
// test sets it and reads it back without the wall clock ever reaching it.
const farFuture = uint64(7_000_000_000_000)

// --- native band ------------------------------------------------------------

func TestNativeFieldExpLazyColumn(t *testing.T) {
	h := forceNative(newHash())
	h.set([]byte("a"), []byte("1"))
	h.set([]byte("b"), []byte("2"))
	// A native hash that never set a field TTL carries no expiry column at all: the
	// memory bar says the machinery costs nothing until a field uses it.
	if h.ft.exp != nil {
		t.Fatal("exp column allocated before any HEXPIRE")
	}
	if h.fieldExp([]byte("a")) != 0 {
		t.Fatal("fieldExp on a no-TTL field should be 0")
	}
	if !h.setFieldExp([]byte("a"), farFuture) {
		t.Fatal("setFieldExp on a present field should report true")
	}
	// First use allocates the column exactly as long as the record vector, so an
	// ordinal always indexes both.
	if len(h.ft.exp) != len(h.ft.ents) {
		t.Fatalf("exp column len %d, want %d (== ents)", len(h.ft.exp), len(h.ft.ents))
	}
	if got := h.fieldExp([]byte("a")); got != farFuture {
		t.Fatalf("fieldExp(a) = %d, want %d", got, farFuture)
	}
	if h.fieldExp([]byte("b")) != 0 {
		t.Fatal("b took a TTL it was never given")
	}
	// The native band always reports hashtable, with or without a field TTL.
	if h.encName() != "hashtable" {
		t.Fatalf("encName = %q, want hashtable", h.encName())
	}
}

func TestNativeFieldExpReuseClearsExpiry(t *testing.T) {
	h := forceNative(newHash())
	h.set([]byte("a"), []byte("1"))
	h.setFieldExp([]byte("a"), farFuture)
	h.del([]byte("a"))              // frees the ordinal, whose exp slot must reset
	h.set([]byte("a"), []byte("2")) // reuses that ordinal
	if got := h.fieldExp([]byte("a")); got != 0 {
		t.Fatalf("a reused ordinal inherited an expiry: %d", got)
	}
}

func TestNativeReapDeletesFired(t *testing.T) {
	h := forceNative(newHash())
	for i, at := range []uint64{100, 200, 300} {
		f := []byte{'f', byte('0' + i)}
		h.set(f, []byte("v"))
		h.setFieldExp(f, at)
	}
	h.set([]byte("keep"), []byte("v")) // no TTL, must survive every reap
	// nextExp is the smallest live expiry after each set.
	if h.nextExp != 100 {
		t.Fatalf("nextExp = %d, want 100", h.nextExp)
	}
	h.reap(150) // f0@100 fires
	if h.has([]byte("f0")) {
		t.Fatal("f0 survived past its expiry")
	}
	if h.nextExp != 200 {
		t.Fatalf("after reap(150) nextExp = %d, want 200", h.nextExp)
	}
	h.reap(250) // f1@200 fires
	if h.has([]byte("f1")) || h.nextExp != 300 {
		t.Fatalf("after reap(250) has(f1)=%v nextExp=%d", h.has([]byte("f1")), h.nextExp)
	}
	// The boundary is inclusive: an expiry equal to now fires.
	h.reap(300)
	if h.has([]byte("f2")) {
		t.Fatal("f2@300 should fire at exactly now=300")
	}
	if h.nextExp != 0 {
		t.Fatalf("nextExp = %d, want 0 once every TTL field is gone", h.nextExp)
	}
	if !h.has([]byte("keep")) || h.card() != 1 {
		t.Fatalf("the no-TTL field was reaped: has=%v card=%d", h.has([]byte("keep")), h.card())
	}
}

// --- inline band ------------------------------------------------------------

func TestInlineFieldExpStickyEncoding(t *testing.T) {
	h := newHash()
	h.set([]byte("a"), []byte("1"))
	h.set([]byte("b"), []byte("2"))
	before := len(h.blob)
	if h.encName() != "listpack" {
		t.Fatalf("encName = %q, want listpack before any TTL", h.encName())
	}
	if h.fieldExp([]byte("a")) != 0 {
		t.Fatal("inline fieldExp should be 0 before any TTL")
	}
	if !h.setFieldExp([]byte("a"), farFuture) {
		t.Fatal("inline setFieldExp on a present field should report true")
	}
	// Flipping to listpackex grows the blob by one eight-byte slot per entry.
	if want := before + inlineExpSize*2; len(h.blob) != want {
		t.Fatalf("blob len %d after listpackex flip, want %d", len(h.blob), want)
	}
	if h.encName() != "listpackex" {
		t.Fatalf("encName = %q, want listpackex once a field took a TTL", h.encName())
	}
	if got := h.fieldExp([]byte("a")); got != farFuture {
		t.Fatalf("inline fieldExp(a) = %d, want %d", got, farFuture)
	}
	if h.fieldExp([]byte("b")) != 0 {
		t.Fatal("b took a TTL it was never given")
	}
	// The values ride the re-encode intact.
	if v, _ := h.get([]byte("a")); string(v) != "1" {
		t.Fatal("a lost its value across the listpackex re-encode")
	}
	if v, _ := h.get([]byte("b")); string(v) != "2" {
		t.Fatal("b lost its value across the listpackex re-encode")
	}
}

func TestInlineClearKeepsSticky(t *testing.T) {
	h := newHash()
	h.set([]byte("a"), []byte("1"))
	h.setFieldExp([]byte("a"), farFuture)
	h.clearFieldExp([]byte("a")) // the HPERSIST path
	if h.fieldExp([]byte("a")) != 0 {
		t.Fatal("clearFieldExp left a TTL behind")
	}
	// listpackex is sticky: HPERSIST drops the TTL but not the encoding, matching
	// Redis (spec 2064/f3/10 section 6.4).
	if h.encName() != "listpackex" {
		t.Fatalf("encName = %q, want listpackex to stay sticky after clear", h.encName())
	}
}

func TestInlineReapDeletesFired(t *testing.T) {
	h := newHash()
	h.set([]byte("a"), []byte("1"))
	h.set([]byte("b"), []byte("2"))
	h.set([]byte("c"), []byte("3"))
	h.setFieldExp([]byte("a"), 100)
	h.setFieldExp([]byte("c"), 300)
	if h.nextExp != 100 {
		t.Fatalf("nextExp = %d, want 100", h.nextExp)
	}
	h.reap(200) // a@100 fires, c@300 survives, b has no TTL
	if h.has([]byte("a")) {
		t.Fatal("a survived past its inline expiry")
	}
	if !h.has([]byte("b")) || !h.has([]byte("c")) {
		t.Fatal("reap took a field it should have kept")
	}
	if h.nextExp != 300 {
		t.Fatalf("after reap(200) nextExp = %d, want 300", h.nextExp)
	}
	// The surviving values are intact across the splice.
	if v, _ := h.get([]byte("b")); string(v) != "2" {
		t.Fatal("b corrupted by the inline reap splice")
	}
	if v, _ := h.get([]byte("c")); string(v) != "3" {
		t.Fatal("c corrupted by the inline reap splice")
	}
}

// --- promotion --------------------------------------------------------------

func TestPromotionCarriesExpiries(t *testing.T) {
	h := newHash()
	for i := 0; i < 20; i++ {
		h.set([]byte("f"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	// Give a few fields distinct TTLs on the inline band.
	h.setFieldExp([]byte("f3"), farFuture)
	h.setFieldExp([]byte("f7"), farFuture+50)
	if h.encName() != "listpackex" {
		t.Fatalf("encName = %q, want listpackex before promotion", h.encName())
	}
	// Force the one-way promotion with an over-cap value.
	h.set([]byte("wide"), []byte(strings.Repeat("z", maxListpackValue+1)))
	if h.enc != encHashtable {
		t.Fatalf("enc = %s, want hashtable after the wide write", h.enc)
	}
	// The sticky bit does not carry: OBJECT ENCODING is hashtable once promoted.
	if h.encName() != "hashtable" {
		t.Fatalf("encName = %q, want hashtable after promotion", h.encName())
	}
	// Every field TTL rode into the native column.
	if got := h.fieldExp([]byte("f3")); got != farFuture {
		t.Fatalf("f3 TTL lost across promotion: %d", got)
	}
	if got := h.fieldExp([]byte("f7")); got != farFuture+50 {
		t.Fatalf("f7 TTL lost across promotion: %d", got)
	}
	if h.fieldExp([]byte("f1")) != 0 || h.fieldExp([]byte("wide")) != 0 {
		t.Fatal("a no-TTL field gained one across promotion")
	}
	// nextExp is the exact minimum after the native reap recomputes it.
	if h.nextExp != farFuture {
		t.Fatalf("nextExp = %d, want %d after promotion", h.nextExp, farFuture)
	}
}

// --- pure helpers -----------------------------------------------------------

func TestCondAllows(t *testing.T) {
	// cur == 0 means the field has no current TTL.
	cases := []struct {
		cond     condFlag
		cur, at  uint64
		expected bool
	}{
		{condNone, 0, 500, true},
		{condNone, 100, 500, true},
		{condNX, 0, 500, true},    // no TTL, NX sets
		{condNX, 100, 500, false}, // has TTL, NX refuses
		{condXX, 0, 500, false},   // no TTL, XX refuses
		{condXX, 100, 500, true},  // has TTL, XX sets
		{condGT, 0, 500, false},   // nothing exceeds "no expiry" (infinity)
		{condGT, 100, 500, true},  // 500 > 100
		{condGT, 500, 500, false}, // not strictly greater
		{condLT, 0, 500, true},    // everything precedes infinity
		{condLT, 500, 100, true},  // 100 < 500
		{condLT, 100, 500, false}, // 500 !< 100
	}
	for i, c := range cases {
		if got := condAllows(c.cond, c.cur, c.at); got != c.expected {
			t.Errorf("case %d condAllows(%d,%d,%d) = %v, want %v", i, c.cond, c.cur, c.at, got, c.expected)
		}
	}
}

func TestApplyExpiryStatusCodes(t *testing.T) {
	h := newHash()
	h.set([]byte("a"), []byte("1"))
	const now = uint64(1_000_000)
	// absent field -> -2
	if got := applyExpiry(h, []byte("zzz"), int64(now)+100, condNone, now); got != -2 {
		t.Fatalf("absent field -> %d, want -2", got)
	}
	// future expiry -> 1, and it is stored
	if got := applyExpiry(h, []byte("a"), int64(farFuture), condNone, now); got != 1 {
		t.Fatalf("future set -> %d, want 1", got)
	}
	if h.fieldExp([]byte("a")) != farFuture {
		t.Fatal("applyExpiry did not store the future expiry")
	}
	// a refused condition -> 0, leaving the field alone
	if got := applyExpiry(h, []byte("a"), int64(farFuture)+100, condNX, now); got != 0 {
		t.Fatalf("NX on a field with a TTL -> %d, want 0", got)
	}
	if h.fieldExp([]byte("a")) != farFuture {
		t.Fatal("a refused NX still changed the expiry")
	}
	// expiry at or before now -> 2, and the field is deleted on the spot
	if got := applyExpiry(h, []byte("a"), int64(now), condNone, now); got != 2 {
		t.Fatalf("set-to-now -> %d, want 2", got)
	}
	if h.has([]byte("a")) {
		t.Fatal("a should be deleted when its expiry is at or before now")
	}
}

func TestTTLValueRounding(t *testing.T) {
	h := newHash()
	h.set([]byte("a"), []byte("1"))
	h.set([]byte("no"), []byte("2")) // present, no TTL
	const now = int64(1_000_000_000)
	// 2500 ms remaining rounds half-up to 3 seconds; the absolute is 1_000_002_500.
	h.setFieldExp([]byte("a"), uint64(now)+2500)

	if got := ttlValue(h, []byte("zzz"), ttlSeconds, now); got != -2 {
		t.Fatalf("absent field HTTL -> %d, want -2", got)
	}
	if got := ttlValue(h, []byte("no"), ttlSeconds, now); got != -1 {
		t.Fatalf("no-TTL field HTTL -> %d, want -1", got)
	}
	if got := ttlValue(h, []byte("a"), ttlSeconds, now); got != 3 {
		t.Fatalf("HTTL round-half-up -> %d, want 3", got)
	}
	if got := ttlValue(h, []byte("a"), ttlMillis, now); got != 2500 {
		t.Fatalf("HPTTL -> %d, want 2500", got)
	}
	if got := ttlValue(h, []byte("a"), ttlAtMillis, now); got != now+2500 {
		t.Fatalf("HPEXPIRETIME -> %d, want %d", got, now+2500)
	}
	// 1_000_002_500 ms rounds half-up to 1_000_003 seconds.
	if got := ttlValue(h, []byte("a"), ttlAtSecs, now); got != (now+2500+500)/1000 {
		t.Fatalf("HEXPIRETIME round-half-up -> %d, want %d", got, (now+2500+500)/1000)
	}
}

// --- command surface --------------------------------------------------------

// The harness runs on the wall clock, so these use absolute expiries far enough
// out (or in the past) that the reply is deterministic regardless of when the
// batch samples now.

func TestHexpireCommandFraming(t *testing.T) {
	c := newHarness(t).NewConn()
	future := strconv.FormatInt(int64(farFuture/1000), 10) // seconds

	// Missing key answers -2 for every requested field.
	wantArray(t, do(t, c, opHexpire, "nokey", "100", "FIELDS", "2", "a", "b"), "-2", "-2")
	wantArray(t, do(t, c, opHttl, "nokey", "FIELDS", "1", "a"), "-2")
	wantArray(t, do(t, c, opHpersist, "nokey", "FIELDS", "1", "a"), "-2")

	do(t, c, opHset, "h", "a", "1", "b", "2", "c", "3")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")

	// Set on a present field, -2 on an absent one.
	wantArray(t, do(t, c, opHexpireat, "h", future, "FIELDS", "2", "a", "zzz"), "1", "-2")
	// The inline hash reports listpackex once a field takes a TTL.
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpackex")

	// HEXPIRETIME echoes the stored absolute seconds; -1 for a live field with no
	// TTL, -2 for an absent one.
	wantArray(t, do(t, c, opHexpiretime, "h", "FIELDS", "3", "a", "b", "zzz"), future, "-1", "-2")

	// HPERSIST: 1 cleared, -1 present without a TTL, -2 absent.
	wantArray(t, do(t, c, opHpersist, "h", "FIELDS", "3", "a", "b", "zzz"), "1", "-1", "-2")
	wantArray(t, do(t, c, opHexpiretime, "h", "FIELDS", "1", "a"), "-1")
}

func TestHexpireConditions(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1")
	at7 := strconv.FormatInt(7_000_000_000, 10)
	at8 := strconv.FormatInt(8_000_000_000, 10)
	at9 := strconv.FormatInt(9_000_000_000, 10)

	wantArray(t, do(t, c, opHexpireat, "h", at7, "FIELDS", "1", "a"), "1")       // first set
	wantArray(t, do(t, c, opHexpireat, "h", at7, "NX", "FIELDS", "1", "a"), "0") // has TTL, NX refuses
	wantArray(t, do(t, c, opHexpireat, "h", at8, "XX", "FIELDS", "1", "a"), "1") // has TTL, XX sets
	wantArray(t, do(t, c, opHexpireat, "h", at7, "GT", "FIELDS", "1", "a"), "0") // 7 < 8, GT refuses
	wantArray(t, do(t, c, opHexpireat, "h", at9, "GT", "FIELDS", "1", "a"), "1") // 9 > 8, GT sets
	wantArray(t, do(t, c, opHexpireat, "h", at8, "LT", "FIELDS", "1", "a"), "1") // 8 < 9, LT sets
	wantArray(t, do(t, c, opHexpireat, "h", at9, "LT", "FIELDS", "1", "a"), "0") // 9 !< 8, LT refuses
}

func TestHexpireSetToPastDropsKey(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "only", "1")
	// An absolute expiry in the past deletes the field on the spot; the last field
	// leaving drops the whole key.
	wantArray(t, do(t, c, opHexpireat, "h", "1", "FIELDS", "1", "only"), "2")
	wantInt(t, do(t, c, opHexists, "h", "only"), 0)
	wantInt(t, do(t, c, opHlen, "h"), 0)
	// A fresh write after the drop starts a new listpack, not a lingering
	// listpackex.
	do(t, c, opHset, "h", "x", "1")
	wantBulk(t, doAt(t, c, opObject, 1, "ENCODING", "h"), "listpack")
}

func TestHsetClearsHincrbyPreservesTTL(t *testing.T) {
	c := newHarness(t).NewConn()
	future := strconv.FormatInt(7_000_000_000, 10)

	// HSET overwrite clears a field TTL.
	do(t, c, opHset, "h", "a", "1")
	wantArray(t, do(t, c, opHexpireat, "h", future, "FIELDS", "1", "a"), "1")
	wantInt(t, do(t, c, opHset, "h", "a", "99"), 0) // overwrite, not new
	wantArray(t, do(t, c, opHexpiretime, "h", "FIELDS", "1", "a"), "-1")

	// HINCRBY preserves it.
	do(t, c, opHset, "h", "n", "10")
	wantArray(t, do(t, c, opHexpireat, "h", future, "FIELDS", "1", "n"), "1")
	wantInt(t, do(t, c, opHincrby, "h", "n", "5"), 15)
	wantArray(t, do(t, c, opHexpiretime, "h", "FIELDS", "1", "n"), future)
}

func TestHexpireErrorsAndWrongType(t *testing.T) {
	c := newHarness(t).NewConn()
	do(t, c, opHset, "h", "a", "1")

	// Syntax errors are byte-identical to Redis 8.8 and outrank the key lookup.
	wantErr(t, do(t, c, opHexpire, "h", "-5", "FIELDS", "1", "a"), "ERR invalid expire time, must be >= 0")
	wantErr(t, do(t, c, opHexpire, "h", "99999999999999999", "FIELDS", "1", "a"), "ERR invalid expire time in 'hexpire' command")
	wantErr(t, do(t, c, opHexpire, "h", "notanint", "FIELDS", "1", "a"), "ERR value is not an integer or out of range")
	wantErr(t, do(t, c, opHexpire, "h", "100", "FIELDS", "0", "a"), "ERR Parameter `numFields` should be greater than 0")
	wantErr(t, do(t, c, opHexpire, "h", "100", "FIELDS", "3", "a"), "ERR wrong number of arguments")
	wantErr(t, do(t, c, opHexpire, "h", "100", "FIELDS", "1", "a", "b"), "ERR unknown argument: b")
	wantErr(t, do(t, c, opHexpire, "h", "100", "a"), "ERR wrong number of arguments for 'hexpire' command")
	wantErr(t, do(t, c, opHexpire, "h", "100", "ZZ", "FIELDS", "1", "a"), "ERR unknown argument: ZZ")
	wantErr(t, do(t, c, opHexpire, "h", "100", "NX", "XX", "FIELDS", "1", "a"), "ERR Multiple condition flags specified")

	// WRONGTYPE against a string key, every verb in the family.
	do(t, c, opSet, "s", "v")
	for _, op := range []byte{opHexpire, opHexpireat, opHpexpire, opHpexpireat} {
		wantErr(t, do(t, c, op, "s", "100", "FIELDS", "1", "a"), wrongType)
	}
	for _, op := range []byte{opHttl, opHpttl, opHexpiretime, opHpexpiretime, opHpersist} {
		wantErr(t, do(t, c, op, "s", "FIELDS", "1", "a"), wrongType)
	}
}
