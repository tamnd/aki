package command

import (
	"fmt"
	"testing"
)

// The in-memory hash write overlay (aki-hash-overlay) absorbs HSET/HDEL element
// writes for a btree-backed hash into a resident map and folds them back into the
// sub-tree in batches. While a hash is resident with unfolded writes, the resident
// copy is authoritative: every read path must observe the absorbed writes, and the
// sub-tree is stale until a fold. These tests turn the gate on and pin that the
// whole hash read surface, plus the ring-2 key commands, see an unfolded resident
// hash exactly as they would a folded one.

func TestHashOverlayReadSurface(t *testing.T) {
	r, c := startData(t)
	if got := sendLine(t, r, c, "CONFIG SET aki-hash-overlay yes"); got != "+OK" {
		t.Fatalf("CONFIG SET aki-hash-overlay = %q want +OK", got)
	}
	// 300 distinct fields cross the 128-entry listpack threshold into coll form, and
	// the fields added past the promotion point are absorbed into the resident copy
	// and stay unfolded (well under the 256 fold threshold), so the reads below are
	// served from memory over a stale sub-tree.
	const n = 300
	for i := 0; i < n; i++ {
		field := fmt.Sprintf("f%03d", i)
		val := fmt.Sprintf("v%03d", i)
		if got := sendLine(t, r, c, fmt.Sprintf("HSET h %s %s", field, val)); got != ":1" {
			t.Fatalf("HSET %s = %q want :1", field, got)
		}
	}

	// HLEN reflects every absorbed write.
	if got := sendLine(t, r, c, "HLEN h"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HLEN = %q want :%d", got, n)
	}
	// HGET on the most recently added (certainly unfolded) field.
	if got := bulk(t, r, c, "HGET h f299"); got != "v299" {
		t.Fatalf("HGET f299 = %q want v299", got)
	}
	// HGET on an early (folded into the promotion, then resident) field.
	if got := bulk(t, r, c, "HGET h f000"); got != "v000" {
		t.Fatalf("HGET f000 = %q want v000", got)
	}
	if got := bulk(t, r, c, "HGET h missing"); got != "<nil>" {
		t.Fatalf("HGET missing = %q want nil", got)
	}
	// HEXISTS both ways.
	if got := sendLine(t, r, c, "HEXISTS h f150"); got != ":1" {
		t.Fatalf("HEXISTS f150 = %q want :1", got)
	}
	if got := sendLine(t, r, c, "HEXISTS h nope"); got != ":0" {
		t.Fatalf("HEXISTS nope = %q want :0", got)
	}
	// HMGET mixes present and absent fields and keeps order.
	mg := array(t, r, c, "HMGET h f010 missing f290")
	if !equalSlice(mg, []string{"v010", "<nil>", "v290"}) {
		t.Fatalf("HMGET = %v", mg)
	}
	// HGETALL/HKEYS/HVALS all report the full set, sorted by field, matching the
	// sub-tree byte order the non-overlay path would return.
	all := array(t, r, c, "HGETALL h")
	if len(all) != 2*n {
		t.Fatalf("HGETALL len = %d want %d", len(all), 2*n)
	}
	if all[0] != "f000" || all[1] != "v000" || all[2*n-2] != "f299" || all[2*n-1] != "v299" {
		t.Fatalf("HGETALL order wrong: first=%q/%q last=%q/%q", all[0], all[1], all[2*n-2], all[2*n-1])
	}
	keys := array(t, r, c, "HKEYS h")
	if len(keys) != n || keys[0] != "f000" || keys[n-1] != "f299" {
		t.Fatalf("HKEYS len=%d first=%q last=%q", len(keys), keys[0], keys[n-1])
	}
	vals := array(t, r, c, "HVALS h")
	if len(vals) != n || vals[0] != "v000" || vals[n-1] != "v299" {
		t.Fatalf("HVALS len=%d first=%q last=%q", len(vals), vals[0], vals[n-1])
	}

	// Ring-2 key commands see the resident hash.
	if got := sendLine(t, r, c, "TYPE h"); got != "+hash" {
		t.Fatalf("TYPE = %q want +hash", got)
	}
	if got := bulk(t, r, c, "OBJECT ENCODING h"); got != "hashtable" {
		t.Fatalf("OBJECT ENCODING = %q want hashtable", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":1" {
		t.Fatalf("EXISTS = %q want :1", got)
	}
	if got := sendLine(t, r, c, "TTL h"); got != ":-1" {
		t.Fatalf("TTL = %q want :-1 (no expiry)", got)
	}
}

func TestHashOverlayWriteThenDelete(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET aki-hash-overlay yes")
	const n = 200
	for i := 0; i < n; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h f%03d v%03d", i, i))
	}
	// Overwrite an existing field in place: HSET reports 0 added, HGET sees the new
	// value, HLEN is unchanged.
	if got := sendLine(t, r, c, "HSET h f100 changed"); got != ":0" {
		t.Fatalf("HSET overwrite = %q want :0", got)
	}
	if got := bulk(t, r, c, "HGET h f100"); got != "changed" {
		t.Fatalf("HGET f100 after overwrite = %q want changed", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HLEN after overwrite = %q want :%d", got, n)
	}
	// HDEL a handful of fields, including a freshly overwritten one.
	if got := sendLine(t, r, c, "HDEL h f100 f101 f102 nope"); got != ":3" {
		t.Fatalf("HDEL = %q want :3", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != fmt.Sprintf(":%d", n-3) {
		t.Fatalf("HLEN after HDEL = %q want :%d", got, n-3)
	}
	if got := sendLine(t, r, c, "HEXISTS h f100"); got != ":0" {
		t.Fatalf("HEXISTS f100 after HDEL = %q want :0", got)
	}
	// Delete the whole key while resident: it must be gone, not shadowed by a stale
	// resident copy.
	if got := sendLine(t, r, c, "DEL h"); got != ":1" {
		t.Fatalf("DEL = %q want :1", got)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS after DEL = %q want :0", got)
	}
	if got := sendLine(t, r, c, "TYPE h"); got != "+none" {
		t.Fatalf("TYPE after DEL = %q want +none", got)
	}
}

func TestHashOverlayHDelEmptiesKey(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET aki-hash-overlay yes")
	const n = 200
	for i := 0; i < n; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h f%03d v%03d", i, i))
	}
	// Remove every field: the key must delete itself (Redis semantics), not linger
	// as an empty resident hash.
	for i := 0; i < n; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("HDEL h f%03d", i))
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS after emptying = %q want :0", got)
	}
	if got := sendLine(t, r, c, "HLEN h"); got != ":0" {
		t.Fatalf("HLEN after emptying = %q want :0", got)
	}
}

func TestHashOverlayExpireRenameCopy(t *testing.T) {
	r, c := startData(t)
	_ = sendLine(t, r, c, "CONFIG SET aki-hash-overlay yes")
	const n = 200
	for i := 0; i < n; i++ {
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h f%03d v%03d", i, i))
	}
	// EXPIRE on a resident hash sets the key TTL on the metadata; TTL reads it back.
	if got := sendLine(t, r, c, "EXPIRE h 1000"); got != ":1" {
		t.Fatalf("EXPIRE = %q want :1", got)
	}
	if got := sendLine(t, r, c, "TTL h"); got == ":-1" || got == ":-2" {
		t.Fatalf("TTL after EXPIRE = %q want a positive value", got)
	}
	if got := sendLine(t, r, c, "PERSIST h"); got != ":1" {
		t.Fatalf("PERSIST = %q want :1", got)
	}
	if got := sendLine(t, r, c, "TTL h"); got != ":-1" {
		t.Fatalf("TTL after PERSIST = %q want :-1", got)
	}
	// COPY of a resident hash must carry the unfolded element writes, not just the
	// stale sub-tree.
	if got := sendLine(t, r, c, "COPY h h2"); got != ":1" {
		t.Fatalf("COPY = %q want :1", got)
	}
	if got := sendLine(t, r, c, "HLEN h2"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HLEN h2 after COPY = %q want :%d", got, n)
	}
	if got := bulk(t, r, c, "HGET h2 f199"); got != "v199" {
		t.Fatalf("HGET h2 f199 after COPY = %q want v199", got)
	}
	// RENAME likewise.
	if got := sendLine(t, r, c, "RENAME h h3"); got != "+OK" {
		t.Fatalf("RENAME = %q want +OK", got)
	}
	if got := sendLine(t, r, c, "HLEN h3"); got != fmt.Sprintf(":%d", n) {
		t.Fatalf("HLEN h3 after RENAME = %q want :%d", got, n)
	}
	if got := sendLine(t, r, c, "EXISTS h"); got != ":0" {
		t.Fatalf("EXISTS h after RENAME = %q want :0", got)
	}
}
