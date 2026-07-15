package sqlo1a

import (
	"bytes"
	"path/filepath"
	"testing"
)

// The statements are prepared once at Open and reused with Reset between
// executions; this exercises the full put, get, delete, scan cycle through
// the catalog to prove the bind shapes match the schema before the Store
// slices build on them.
func TestStmtsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	put := func(k string, v string, exp int64, gen int64) {
		t.Helper()
		s := db.st.kvPut
		s.BindBlob(1, []byte(k))
		s.BindInt64(2, 0)
		s.BindInt64(3, exp)
		s.BindInt64(4, gen)
		s.BindBlob(5, []byte(v))
		s.BindInt64(6, 0)
		if s.Step() {
			t.Fatalf("put %s returned a row", k)
		}
		if err := s.Err(); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
		if err := s.Reset(); err != nil {
			t.Fatalf("put reset: %v", err)
		}
	}

	// Twice through the same prepared statement, second pass an upsert
	// over the first: the replay path ApplyBatch depends on.
	put("alpha", "one", 0, 1)
	put("alpha", "two", 42, 3)
	put("beta", "three", 0, 1)

	g := db.st.kvGet
	g.BindBlob(1, []byte("alpha"))
	if !g.Step() {
		t.Fatalf("get alpha: no row, err %v", g.Err())
	}
	if got := g.ColumnRawBlob(3); !bytes.Equal(got, []byte("two")) {
		t.Fatalf("get alpha v = %q, want %q (upsert did not replace)", got, "two")
	}
	if got := g.ColumnInt64(1); got != 42 {
		t.Fatalf("get alpha exp = %d, want 42", got)
	}
	if got := g.ColumnInt64(2); got != 3 {
		t.Fatalf("get alpha gen = %d, want 3", got)
	}
	if err := g.Reset(); err != nil {
		t.Fatalf("get reset: %v", err)
	}

	sc := db.st.kvScan
	sc.BindBlob(1, []byte(""))
	sc.BindInt64(2, 10)
	var keys []string
	for sc.Step() {
		keys = append(keys, string(sc.ColumnRawBlob(0)))
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(keys) != 2 || keys[0] != "alpha" || keys[1] != "beta" {
		t.Fatalf("scan keys = %v, want [alpha beta]", keys)
	}
	if err := sc.Reset(); err != nil {
		t.Fatalf("scan reset: %v", err)
	}

	// Resume from the cursor: k > alpha yields only beta.
	sc.BindBlob(1, []byte("alpha"))
	sc.BindInt64(2, 10)
	keys = nil
	for sc.Step() {
		keys = append(keys, string(sc.ColumnRawBlob(0)))
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan resume: %v", err)
	}
	if len(keys) != 1 || keys[0] != "beta" {
		t.Fatalf("scan after alpha = %v, want [beta]", keys)
	}
	if err := sc.Reset(); err != nil {
		t.Fatalf("scan resume reset: %v", err)
	}

	d := db.st.kvDel
	d.BindBlob(1, []byte("alpha"))
	if d.Step() {
		t.Fatal("del returned a row")
	}
	if err := d.Err(); err != nil {
		t.Fatalf("del: %v", err)
	}
	if err := d.Reset(); err != nil {
		t.Fatalf("del reset: %v", err)
	}
	// Deleting an absent key is a clean no-op, the replay semantics
	// ApplyBatch wants.
	d.BindBlob(1, []byte("alpha"))
	if d.Step() {
		t.Fatal("replayed del returned a row")
	}
	if err := d.Err(); err != nil {
		t.Fatalf("replayed del: %v", err)
	}
	if err := d.Reset(); err != nil {
		t.Fatalf("replayed del reset: %v", err)
	}

	g.BindBlob(1, []byte("alpha"))
	if g.Step() {
		t.Fatal("get found alpha after delete")
	}
	if err := g.Err(); err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if err := g.Reset(); err != nil {
		t.Fatalf("get after delete reset: %v", err)
	}
}

// Close must finalize the catalog before closing the connection, or ncruces
// refuses the close and the store leaks a file handle. A clean nil from
// Close after real statement use is the proof.
func TestCloseFinalizesStatements(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.sqlo1")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	g := db.st.kvGet
	g.BindBlob(1, []byte("nothing"))
	g.Step()
	if err := g.Err(); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := g.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close with prepared statements: %v", err)
	}
}
