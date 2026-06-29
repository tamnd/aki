package command

import (
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/networking"
)

// TestHashFullCollIsBounded guards HGETALL, HKEYS, and HVALS against the full-reply
// materialize trap on a coll-form hash. They used to route through hashMaterialize,
// which clones every field and value into a []hashField before writing the reply:
// O(n) transient heap on top of the O(n) reply bytes, an OOM under a tight cap on a
// hash larger than RAM. The streaming path writes each field straight off a
// sub-tree cursor into the encoder, so retained memory is the cursor pages plus the
// flush buffer, never a whole-hash clone.
//
// The witness is allocation count with the output buffer reused across runs: the
// streamed reply allocates a small constant, far below the per-run clone.
func TestHashFullCollIsBounded(t *testing.T) {
	skipAllocWitnessUnderRace(t)
	d := newFuzzDispatcher(t)
	conn := networking.NewOfflineConn()

	const n = 4000
	pad := make([]byte, 240)
	for i := range pad {
		pad[i] = 'x'
	}
	for i := range n {
		conn.ResetOut()
		d.Handle(conn, [][]byte{[]byte("HSET"), []byte("h"),
			[]byte(fmt.Sprintf("f:%08d", i)), append([]byte("v"), pad...)})
	}
	conn.ResetOut()
	d.Handle(conn, [][]byte{[]byte("OBJECT"), []byte("ENCODING"), []byte("h")})
	if got := string(conn.OutBytes()); got != "$9\r\nhashtable\r\n" {
		t.Fatalf("hash not in coll form: OBJECT ENCODING = %q", got)
	}

	for _, cmd := range []string{"HGETALL", "HKEYS", "HVALS"} {
		allocs := testing.AllocsPerRun(20, func() {
			conn.ResetOut()
			d.Handle(conn, [][]byte{[]byte(cmd), []byte("h")})
		})
		// A materialize clones all n fields (and values) every run, well past 4000
		// objects. The streamed reply touches only the cursor and reader, a small
		// constant; the two-pass live-field count adds a second walk but no
		// allocation. The gate is generous against that constant.
		if allocs > 60 {
			t.Fatalf("%s on a %d-field coll-form hash allocated %.0f objects per run; "+
				"a streamed reply should be a small constant, not O(n)", cmd, n, allocs)
		}
	}
}

// TestHashFullCollMatchesBlob checks the streamed coll-form replies carry exactly
// what the materialized path would: HGETALL the field/value map, HKEYS the field
// set, HVALS the value multiset, with the right lengths, and empty replies for a
// missing key.
func TestHashFullCollMatchesBlob(t *testing.T) {
	r, c := startData(t)
	const n = 1000

	wantKeys := make([]string, 0, n)
	wantVals := make([]string, 0, n)
	for i := range n {
		f := fmt.Sprintf("f:%06d", i)
		v := fmt.Sprintf("v:%06d", i)
		wantKeys = append(wantKeys, f)
		wantVals = append(wantVals, v)
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h %s %s", f, v))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING h"); enc != "hashtable" {
		t.Fatalf("hash encoding = %q want hashtable", enc)
	}

	// HGETALL: the field->value pairs, every one present and correct.
	flat := readArray(t, r, c, "HGETALL h")
	if len(flat) != 2*n {
		t.Fatalf("HGETALL h returned %d elements want %d", len(flat), 2*n)
	}
	got := map[string]string{}
	for i := 0; i < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	if len(got) != n {
		t.Fatalf("HGETALL h returned %d distinct fields want %d", len(got), n)
	}
	for i := range wantKeys {
		if got[wantKeys[i]] != wantVals[i] {
			t.Fatalf("HGETALL h[%s] = %q want %q", wantKeys[i], got[wantKeys[i]], wantVals[i])
		}
	}

	// HKEYS: the field set.
	keys := readArray(t, r, c, "HKEYS h")
	sort.Strings(keys)
	if len(keys) != n || keys[0] != wantKeys[0] || keys[n-1] != wantKeys[n-1] {
		t.Fatalf("HKEYS h returned %d keys, bounds %q..%q", len(keys), keys[0], keys[len(keys)-1])
	}

	// HVALS: the value multiset.
	vals := readArray(t, r, c, "HVALS h")
	sort.Strings(vals)
	if len(vals) != n || vals[0] != wantVals[0] || vals[n-1] != wantVals[n-1] {
		t.Fatalf("HVALS h returned %d vals, bounds %q..%q", len(vals), vals[0], vals[len(vals)-1])
	}

	// Missing key: empty replies, not errors.
	if got := readArray(t, r, c, "HGETALL missing"); len(got) != 0 {
		t.Fatalf("HGETALL missing = %v want empty", got)
	}
	if got := readArray(t, r, c, "HKEYS missing"); len(got) != 0 {
		t.Fatalf("HKEYS missing = %v want empty", got)
	}
}

// TestHashFullCollExcludesExpiredField checks a field whose TTL has passed is gone
// from the streamed reply, so the reply header (the live count from the first pass)
// and the emitted fields agree. HPEXPIREAT to the past (1ms) retires the field
// without a real-time wait.
func TestHashFullCollExcludesExpiredField(t *testing.T) {
	r, c := startData(t)
	const n = 600
	for i := range n {
		_ = sendLine(t, r, c, fmt.Sprintf("HSET h f:%06d v%d", i, i))
	}
	if enc := bulk(t, r, c, "OBJECT ENCODING h"); enc != "hashtable" {
		t.Fatalf("hash encoding = %q want hashtable", enc)
	}
	if codes := codeArray(t, r, c, "HPEXPIREAT h 1 FIELDS 1 f:000000"); len(codes) != 1 || codes[0] != "2" {
		t.Fatalf("HPEXPIREAT to the past = %v want [2] (field deleted)", codes)
	}

	keys := readArray(t, r, c, "HKEYS h")
	if len(keys) != n-1 {
		t.Fatalf("HKEYS h after one field expired returned %d want %d", len(keys), n-1)
	}
	for _, k := range keys {
		if k == "f:000000" {
			t.Fatalf("HKEYS h returned the expired field f:000000")
		}
	}
	flat := readArray(t, r, c, "HGETALL h")
	if len(flat) != 2*(n-1) {
		t.Fatalf("HGETALL h after one field expired returned %d elements want %d", len(flat), 2*(n-1))
	}
}
